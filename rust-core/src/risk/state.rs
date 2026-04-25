use chrono::{DateTime, FixedOffset, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;

use super::manager::RiskVerdict;

/// UTC+8 offset for Taiwan midnight reset.
const TAIPEI_OFFSET_SECS: i32 = 8 * 3600;

pub struct RiskState {
    pub position_by_instrument: HashMap<String, Decimal>,
    /// Per-instrument |signed_position * last_mark_price|; recomputed each
    /// `apply_fill` so closes shrink the entry. `total_notional_exposure` is
    /// the sum across this map.
    pub position_notional: HashMap<String, Decimal>,
    /// Last known mark price per instrument, used by `update_mark_price` to
    /// refresh notional between fills.
    mark_price_by_instrument: HashMap<String, Decimal>,
    /// Per-instrument timestamp of the last mark price update. Used by
    /// `evict_stale_marks` so a disconnected venue's stale mark stops
    /// feeding into MaxNotionalExposure (review §2.5).
    mark_price_ts: HashMap<String, DateTime<Utc>>,
    pub total_notional_exposure: Decimal,
    pub realized_pnl_today: Decimal,
    pub max_position_size: Decimal,
    pub max_notional_exposure: Decimal,
    pub max_daily_loss: Decimal,
    pub halted: bool,
    /// Last calendar date (in UTC+8) when `realized_pnl_today` was reset.
    last_reset_date: Option<chrono::NaiveDate>,
}

impl RiskState {
    pub fn new(
        max_position_size: Decimal,
        max_notional_exposure: Decimal,
        max_daily_loss: Decimal,
    ) -> Self {
        Self {
            position_by_instrument: HashMap::new(),
            position_notional: HashMap::new(),
            mark_price_by_instrument: HashMap::new(),
            mark_price_ts: HashMap::new(),
            total_notional_exposure: Decimal::ZERO,
            realized_pnl_today: Decimal::ZERO,
            max_position_size,
            max_notional_exposure,
            max_daily_loss,
            halted: false,
            last_reset_date: None,
        }
    }

    /// Reset `realized_pnl_today` to zero if the UTC+8 calendar date has
    /// changed since the last reset. Called from the engine's main loop.
    /// Returns `Some(prev_pnl)` when an actual reset occurred (post-init,
    /// not the initial "first call" set), so callers can emit a
    /// DailyPnLReset event. Returns `None` otherwise.
    pub fn maybe_reset_daily(&mut self, now: DateTime<Utc>) -> Option<Decimal> {
        let taipei = FixedOffset::east_opt(TAIPEI_OFFSET_SECS).unwrap();
        let today = now.with_timezone(&taipei).date_naive();
        if self.last_reset_date != Some(today) {
            let prev = self.realized_pnl_today;
            let was_initialized = self.last_reset_date.is_some();
            if was_initialized {
                tracing::info!(
                    prev_pnl = %prev,
                    "daily PnL reset (midnight UTC+8)"
                );
            }
            self.realized_pnl_today = Decimal::ZERO;
            self.last_reset_date = Some(today);
            if was_initialized {
                return Some(prev);
            }
        }
        None
    }

    pub fn check_order(
        &self,
        instrument_key: &str,
        quantity: Decimal,
        notional: Decimal,
    ) -> RiskVerdict {
        if self.halted {
            return RiskVerdict::rejected("halted");
        }
        if self.realized_pnl_today < -self.max_daily_loss {
            return RiskVerdict::rejected("daily loss breached");
        }

        let current_qty = self
            .position_by_instrument
            .get(instrument_key)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let projected_qty = current_qty.abs() + quantity;

        if projected_qty > self.max_position_size {
            let room = self.max_position_size - current_qty.abs();
            if room > Decimal::ZERO {
                return RiskVerdict::adjusted(room, "position capped");
            }
            return RiskVerdict::rejected("max position reached");
        }

        if self.total_notional_exposure + notional > self.max_notional_exposure {
            return RiskVerdict::rejected("notional exposure exceeded");
        }

        RiskVerdict::approved()
    }

    /// Apply a fill and rebuild that instrument's notional from
    /// |new_position * mark_price|; critical so closes shrink exposure
    /// instead of accumulating forever.
    pub fn apply_fill(
        &mut self,
        instrument_key: &str,
        quantity: Decimal,
        mark_price: Decimal,
        realized_pnl: Decimal,
    ) {
        let pos = self
            .position_by_instrument
            .entry(instrument_key.to_string())
            .or_insert(Decimal::ZERO);
        *pos += quantity;
        self.mark_price_by_instrument
            .insert(instrument_key.to_string(), mark_price);
        self.mark_price_ts
            .insert(instrument_key.to_string(), Utc::now());
        let new_notional = (*pos * mark_price).abs();
        if new_notional.is_zero() {
            self.position_notional.remove(instrument_key);
        } else {
            self.position_notional
                .insert(instrument_key.to_string(), new_notional);
        }
        self.total_notional_exposure = self.position_notional.values().copied().sum();
        self.realized_pnl_today += realized_pnl;
    }

    /// Refresh mark price for an instrument and recompute its notional
    /// contribution to `total_notional_exposure`. O(1) per call — only
    /// updates the delta for the single instrument, no iteration over all
    /// positions. Updates the last-seen timestamp so `evict_stale_marks`
    /// can later clear marks from a disconnected venue.
    pub fn update_mark_price_at(
        &mut self,
        instrument_key: &str,
        mark_price: Decimal,
        now: DateTime<Utc>,
    ) {
        self.mark_price_by_instrument
            .insert(instrument_key.to_string(), mark_price);
        self.mark_price_ts.insert(instrument_key.to_string(), now);
        // Only recompute if we hold a position in this instrument.
        let pos = match self.position_by_instrument.get(instrument_key) {
            Some(p) if !p.is_zero() => *p,
            _ => return,
        };
        let old_notional = self
            .position_notional
            .get(instrument_key)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let new_notional = (pos * mark_price).abs();
        self.position_notional
            .insert(instrument_key.to_string(), new_notional);
        self.total_notional_exposure += new_notional - old_notional;
    }

    /// Backwards-compatible wrapper for callers that don't have an injected
    /// clock — stamps the update with `Utc::now()`. Prefer
    /// `update_mark_price_at` for testability.
    pub fn update_mark_price(&mut self, instrument_key: &str, mark_price: Decimal) {
        self.update_mark_price_at(instrument_key, mark_price, Utc::now());
    }

    /// Drop marks older than `max_age` and zero their notional contribution.
    /// Used to keep MaxNotionalExposure honest when a venue disconnects —
    /// without this a stale mark sits in the gauge forever, either inflating
    /// (real price dropped) or under-reporting (real price rose) exposure.
    pub fn evict_stale_marks(&mut self, now: DateTime<Utc>, max_age: chrono::Duration) {
        let stale_keys: Vec<String> = self
            .mark_price_ts
            .iter()
            .filter(|(_, ts)| now.signed_duration_since(**ts) > max_age)
            .map(|(k, _)| k.clone())
            .collect();
        for key in stale_keys {
            self.mark_price_ts.remove(&key);
            self.mark_price_by_instrument.remove(&key);
            if let Some(notional) = self.position_notional.remove(&key) {
                self.total_notional_exposure -= notional;
            }
            tracing::warn!(
                instrument = key.as_str(),
                "stale mark evicted; notional contribution zeroed"
            );
        }
    }

    pub fn halt(&mut self) {
        self.halted = true;
    }

    pub fn resume(&mut self) {
        self.halted = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn default_state() -> RiskState {
        RiskState::new(dec!(10), dec!(100000), dec!(1000))
    }

    #[test]
    fn check_order_approves_within_limits() {
        let state = default_state();
        let v = state.check_order("BTC-USDT", dec!(5), dec!(50000));
        assert!(v.approved);
        assert!(v.adjusted_qty.is_none());
    }

    #[test]
    fn check_order_rejects_position_exceeded() {
        let mut state = default_state();
        state
            .position_by_instrument
            .insert("BTC-USDT".into(), dec!(10));
        let v = state.check_order("BTC-USDT", dec!(1), dec!(50000));
        assert!(!v.approved);
        assert!(v.reason.as_deref().unwrap().contains("max position"));
    }

    #[test]
    fn check_order_adjusts_partial_room() {
        let mut state = default_state();
        state
            .position_by_instrument
            .insert("BTC-USDT".into(), dec!(8));
        let v = state.check_order("BTC-USDT", dec!(5), dec!(50000));
        assert!(v.approved);
        assert_eq!(v.adjusted_qty, Some(dec!(2)));
    }

    #[test]
    fn check_order_rejects_daily_loss() {
        let mut state = default_state();
        state.realized_pnl_today = dec!(-1500);
        let v = state.check_order("BTC-USDT", dec!(1), dec!(50000));
        assert!(!v.approved);
        assert!(v.reason.as_deref().unwrap().contains("daily loss"));
    }

    #[test]
    fn check_order_rejects_notional() {
        let state = default_state();
        let v = state.check_order("BTC-USDT", dec!(1), dec!(200000));
        assert!(!v.approved);
        assert!(v.reason.as_deref().unwrap().contains("notional"));
    }

    #[test]
    fn apply_fill_updates_position() {
        let mut state = default_state();
        // Buy 3 @ 50000 → position +3, notional |3*50000|=150000
        state.apply_fill("BTC-USDT", dec!(3), dec!(50000), Decimal::ZERO);
        assert_eq!(
            *state.position_by_instrument.get("BTC-USDT").unwrap(),
            dec!(3)
        );
        // Sell 1 @ 50000 → position +2, notional |2*50000|=100000
        state.apply_fill("BTC-USDT", dec!(-1), dec!(50000), Decimal::ZERO);
        assert_eq!(
            *state.position_by_instrument.get("BTC-USDT").unwrap(),
            dec!(2)
        );
    }

    #[test]
    fn apply_fill_updates_pnl() {
        let mut state = default_state();
        state.apply_fill("BTC-USDT", dec!(1), dec!(50000), dec!(100));
        assert_eq!(state.realized_pnl_today, dec!(100));
        state.apply_fill("BTC-USDT", dec!(-1), dec!(50000), dec!(-200));
        assert_eq!(state.realized_pnl_today, dec!(-100));
    }

    #[test]
    fn apply_fill_recomputes_notional_so_close_shrinks_exposure() {
        // Regression for the unbounded-accumulation bug: closing a position
        // must reduce total_notional_exposure, not grow it.
        let mut state = default_state();
        // Buy 2 BTC @ 50000 → exposure 100_000
        state.apply_fill("BTC-USDT", dec!(2), dec!(50000), Decimal::ZERO);
        assert_eq!(state.total_notional_exposure, dec!(100000));

        // Sell 1 BTC @ 50000 → position 1, exposure 50_000 (not 150_000!)
        state.apply_fill("BTC-USDT", dec!(-1), dec!(50000), Decimal::ZERO);
        assert_eq!(state.total_notional_exposure, dec!(50000));

        // Sell 1 BTC @ 50000 → position 0, exposure removed entirely
        state.apply_fill("BTC-USDT", dec!(-1), dec!(50000), Decimal::ZERO);
        assert_eq!(state.total_notional_exposure, Decimal::ZERO);
        assert!(!state.position_notional.contains_key("BTC-USDT"));
    }

    #[test]
    fn apply_fill_aggregates_notional_across_instruments() {
        let mut state = default_state();
        state.apply_fill("BTC-USDT", dec!(1), dec!(50000), Decimal::ZERO);
        state.apply_fill("ETH-USDT", dec!(10), dec!(3000), Decimal::ZERO);
        // 50_000 + 30_000
        assert_eq!(state.total_notional_exposure, dec!(80000));
    }

    #[test]
    fn apply_fill_short_position_uses_absolute_notional() {
        let mut state = default_state();
        // Sell 2 BTC @ 50000 (open short) → position -2, notional |−2*50000|=100_000
        state.apply_fill("BTC-USDT", dec!(-2), dec!(50000), Decimal::ZERO);
        assert_eq!(state.total_notional_exposure, dec!(100000));
    }

    #[test]
    fn halt_rejects_all() {
        let mut state = default_state();
        state.halt();
        let v = state.check_order("BTC-USDT", dec!(1), dec!(50000));
        assert!(!v.approved);
        assert!(v.reason.as_deref().unwrap().contains("halted"));

        state.resume();
        let v = state.check_order("BTC-USDT", dec!(1), dec!(50000));
        assert!(v.approved);
    }

    #[test]
    fn check_order_is_o1() {
        let mut state = default_state();
        for i in 0..10_000 {
            state
                .position_by_instrument
                .insert(format!("INST-{i}"), dec!(1));
        }
        let start = std::time::Instant::now();
        for _ in 0..1_000 {
            let _ = state.check_order("BTC-USDT", dec!(1), dec!(50000));
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed.as_millis() < 50,
            "1000 checks took {elapsed:?}, expected <50ms"
        );
    }

    #[test]
    fn update_mark_price_refreshes_notional() {
        let mut state = default_state();
        // Open 1 BTC @ 50000
        state.apply_fill("BTC-USDT", dec!(1), dec!(50000), Decimal::ZERO);
        assert_eq!(state.total_notional_exposure, dec!(50000));

        // Price moves to 55000 — exposure should update without a fill.
        state.update_mark_price("BTC-USDT", dec!(55000));
        assert_eq!(state.total_notional_exposure, dec!(55000));
        assert_eq!(
            *state.position_notional.get("BTC-USDT").unwrap(),
            dec!(55000)
        );
    }

    #[test]
    fn update_mark_price_no_position_is_noop() {
        let mut state = default_state();
        // No position — update should not add spurious notional.
        state.update_mark_price("BTC-USDT", dec!(55000));
        assert_eq!(state.total_notional_exposure, Decimal::ZERO);
        assert!(!state.position_notional.contains_key("BTC-USDT"));
    }

    #[test]
    fn update_mark_price_multi_instrument_only_updates_one() {
        let mut state = default_state();
        state.apply_fill("BTC-USDT", dec!(1), dec!(50000), Decimal::ZERO);
        state.apply_fill("ETH-USDT", dec!(10), dec!(3000), Decimal::ZERO);
        // total = 50000 + 30000 = 80000
        assert_eq!(state.total_notional_exposure, dec!(80000));

        // BTC moves 50k→60k, ETH unchanged.
        state.update_mark_price("BTC-USDT", dec!(60000));
        // total = 60000 + 30000 = 90000
        assert_eq!(state.total_notional_exposure, dec!(90000));
        assert_eq!(
            *state.position_notional.get("ETH-USDT").unwrap(),
            dec!(30000)
        );
    }

    #[test]
    fn update_mark_price_short_position() {
        let mut state = default_state();
        state.apply_fill("BTC-USDT", dec!(-2), dec!(50000), Decimal::ZERO);
        assert_eq!(state.total_notional_exposure, dec!(100000));

        // Price drops to 45000 — short exposure shrinks.
        state.update_mark_price("BTC-USDT", dec!(45000));
        assert_eq!(state.total_notional_exposure, dec!(90000));
    }

    #[test]
    fn evict_stale_marks_zeros_notional_for_old_marks() {
        let mut state = default_state();
        let t0 = Utc::now() - chrono::Duration::seconds(120);
        let t_now = Utc::now();
        state.apply_fill("BTC-USDT", dec!(1), dec!(50000), Decimal::ZERO);
        // Force mark to be old.
        state.update_mark_price_at("BTC-USDT", dec!(50000), t0);
        state.apply_fill("ETH-USDT", dec!(10), dec!(3000), Decimal::ZERO);
        state.update_mark_price_at("ETH-USDT", dec!(3000), t_now);
        // Both contribute: 50_000 + 30_000.
        assert_eq!(state.total_notional_exposure, dec!(80000));

        // Evict marks older than 60s — BTC-USDT goes, ETH-USDT stays.
        state.evict_stale_marks(t_now, chrono::Duration::seconds(60));
        assert_eq!(state.total_notional_exposure, dec!(30000));
        assert!(!state.position_notional.contains_key("BTC-USDT"));
        assert!(state.position_notional.contains_key("ETH-USDT"));
    }

    #[test]
    fn evict_stale_marks_no_op_when_all_fresh() {
        let mut state = default_state();
        let t_now = Utc::now();
        state.apply_fill("BTC-USDT", dec!(1), dec!(50000), Decimal::ZERO);
        state.update_mark_price_at("BTC-USDT", dec!(50000), t_now);
        state.evict_stale_marks(t_now, chrono::Duration::seconds(60));
        assert_eq!(state.total_notional_exposure, dec!(50000));
    }
}
