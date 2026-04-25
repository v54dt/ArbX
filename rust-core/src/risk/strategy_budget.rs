use chrono::{DateTime, FixedOffset, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;

/// Per-strategy risk budget. Checked inside `process_opportunity` BEFORE the
/// global `RiskState` / `RiskManager` chain. When a strategy exceeds its own
/// budget the opportunity is skipped (with a metric + warn log) without
/// tripping the global circuit breaker — other strategies keep running.
///
/// All fields are optional: `None` means "no per-strategy cap, defer to
/// global limits". When both per-strategy and global caps exist, the
/// tighter one wins.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct StrategyRiskBudgetConfig {
    pub max_daily_loss: Option<Decimal>,
    pub max_notional: Option<Decimal>,
    /// Hour in UTC at which the daily budget resets. Examples:
    /// - `None` or absent → default UTC+8 midnight (legacy, = UTC 16:00)
    /// - `0` → UTC 00:00 (crypto markets)
    /// - `5` → UTC 05:00 = UTC+8 13:00 (near TW stock close)
    pub reset_hour_utc: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct StrategyRiskBudget {
    pub daily_pnl: Decimal,
    pub notional_submitted: Decimal,
    pub order_count: u64,
    config: StrategyRiskBudgetConfig,
    last_reset_date: Option<chrono::NaiveDate>,
}

impl StrategyRiskBudget {
    pub fn new(config: StrategyRiskBudgetConfig) -> Self {
        Self {
            daily_pnl: Decimal::ZERO,
            notional_submitted: Decimal::ZERO,
            order_count: 0,
            config,
            last_reset_date: None,
        }
    }

    /// Reset budget counters if the trading-day boundary has crossed.
    /// Returns `Some(prev_pnl)` when an actual reset occurred (post-init),
    /// so callers can emit a DailyPnLReset event.
    pub fn maybe_reset_daily(&mut self, now: DateTime<Utc>) -> Option<Decimal> {
        // Compute the "trading date" — the calendar date boundary shifts
        // depending on reset_hour_utc. For example, reset_hour_utc=0 means
        // the trading day flips at UTC midnight; reset_hour_utc=16 (default,
        // = UTC+8 midnight) means 15:59 UTC is still "yesterday".
        let reset_hour = self.config.reset_hour_utc.unwrap_or(16); // 16 UTC = midnight UTC+8
        let offset =
            FixedOffset::east_opt(-reset_hour * 3600).unwrap_or(FixedOffset::east_opt(0).unwrap());
        let today = now.with_timezone(&offset).date_naive();
        if self.last_reset_date != Some(today) {
            let prev = self.daily_pnl;
            let was_initialized = self.last_reset_date.is_some();
            self.daily_pnl = Decimal::ZERO;
            self.notional_submitted = Decimal::ZERO;
            self.order_count = 0;
            self.last_reset_date = Some(today);
            if was_initialized {
                return Some(prev);
            }
        }
        None
    }

    /// Returns `true` if the strategy is within its budget.
    pub fn is_within_budget(&self) -> bool {
        if let Some(max_loss) = self.config.max_daily_loss
            && self.daily_pnl < -max_loss
        {
            return false;
        }
        if let Some(max_notional) = self.config.max_notional
            && self.notional_submitted > max_notional
        {
            return false;
        }
        true
    }

    pub fn record_order(&mut self, notional: Decimal) {
        self.notional_submitted += notional;
        self.order_count += 1;
    }

    /// Apply a realized PnL delta from a fill (positive = profit, negative =
    /// loss). Called from the engine fill handler so `daily_pnl` reflects
    /// actual filled PnL rather than the strategy's self-reported expected
    /// edge at submit time.
    pub fn record_realized_pnl(&mut self, realized_delta: Decimal) {
        self.daily_pnl += realized_delta;
    }

    pub fn rejection_reason(&self) -> Option<&'static str> {
        if let Some(max_loss) = self.config.max_daily_loss
            && self.daily_pnl < -max_loss
        {
            return Some("per-strategy max_daily_loss exceeded");
        }
        if let Some(max_notional) = self.config.max_notional
            && self.notional_submitted > max_notional
        {
            return Some("per-strategy max_notional exceeded");
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn within_budget_when_no_caps() {
        let b = StrategyRiskBudget::new(StrategyRiskBudgetConfig::default());
        assert!(b.is_within_budget());
    }

    #[test]
    fn daily_loss_cap_trips_on_negative_pnl() {
        let mut b = StrategyRiskBudget::new(StrategyRiskBudgetConfig {
            max_daily_loss: Some(dec!(100)),
            max_notional: None,
            ..Default::default()
        });
        b.record_realized_pnl(dec!(-50));
        assert!(b.is_within_budget());
        b.record_realized_pnl(dec!(-60));
        assert!(!b.is_within_budget());
        assert_eq!(
            b.rejection_reason(),
            Some("per-strategy max_daily_loss exceeded")
        );
    }

    #[test]
    fn notional_cap_trips_on_cumulative_orders() {
        let mut b = StrategyRiskBudget::new(StrategyRiskBudgetConfig {
            max_daily_loss: None,
            max_notional: Some(dec!(1000)),
            ..Default::default()
        });
        b.record_order(dec!(600));
        assert!(b.is_within_budget());
        b.record_order(dec!(500));
        assert!(!b.is_within_budget());
        assert_eq!(
            b.rejection_reason(),
            Some("per-strategy max_notional exceeded")
        );
    }

    #[test]
    fn both_caps_tightest_wins() {
        let mut b = StrategyRiskBudget::new(StrategyRiskBudgetConfig {
            max_daily_loss: Some(dec!(100)),
            max_notional: Some(dec!(5000)),
            ..Default::default()
        });
        b.record_realized_pnl(dec!(-101));
        assert!(!b.is_within_budget());
        assert!(b.rejection_reason().unwrap().contains("daily_loss"));
    }

    #[test]
    fn maybe_reset_daily_resets_on_new_day() {
        let mut b = StrategyRiskBudget::new(StrategyRiskBudgetConfig {
            max_daily_loss: Some(dec!(100)),
            max_notional: Some(dec!(1000)),
            ..Default::default()
        });
        b.record_realized_pnl(dec!(-101));
        b.record_order(dec!(1001));
        assert!(!b.is_within_budget());

        // Advance to next day (UTC+8)
        let tomorrow = Utc::now() + chrono::Duration::days(1);
        b.maybe_reset_daily(tomorrow);
        assert!(b.is_within_budget());
        assert_eq!(b.daily_pnl, Decimal::ZERO);
        assert_eq!(b.notional_submitted, Decimal::ZERO);
        assert_eq!(b.order_count, 0);
    }

    #[test]
    fn reset_hour_utc_zero_resets_at_utc_midnight() {
        let mut b = StrategyRiskBudget::new(StrategyRiskBudgetConfig {
            max_daily_loss: Some(dec!(100)),
            max_notional: None,
            reset_hour_utc: Some(0),
        });
        // 2026-04-22 23:59 UTC — still "today"
        let before = chrono::DateTime::parse_from_rfc3339("2026-04-22T23:59:00Z")
            .unwrap()
            .with_timezone(&Utc);
        b.maybe_reset_daily(before);
        b.record_realized_pnl(dec!(-50));

        // 2026-04-23 00:01 UTC — new day, should reset
        let after = chrono::DateTime::parse_from_rfc3339("2026-04-23T00:01:00Z")
            .unwrap()
            .with_timezone(&Utc);
        b.maybe_reset_daily(after);
        assert_eq!(b.daily_pnl, Decimal::ZERO);
    }

    #[test]
    fn reset_hour_utc_5_resets_at_utc_0500() {
        let mut b = StrategyRiskBudget::new(StrategyRiskBudgetConfig {
            max_daily_loss: Some(dec!(100)),
            max_notional: None,
            reset_hour_utc: Some(5),
        });
        // 2026-04-22 04:59 UTC — still in previous trading day
        let before = chrono::DateTime::parse_from_rfc3339("2026-04-22T04:59:00Z")
            .unwrap()
            .with_timezone(&Utc);
        b.maybe_reset_daily(before);
        b.record_realized_pnl(dec!(-50));

        // 2026-04-22 05:01 UTC — new trading day, should reset
        let after = chrono::DateTime::parse_from_rfc3339("2026-04-22T05:01:00Z")
            .unwrap()
            .with_timezone(&Utc);
        b.maybe_reset_daily(after);
        assert_eq!(b.daily_pnl, Decimal::ZERO);
    }

    #[test]
    fn reset_hour_utc_default_matches_utc_plus_8_midnight() {
        // Default (None) should behave like reset_hour_utc=16 (UTC+8 midnight)
        let mut b = StrategyRiskBudget::new(StrategyRiskBudgetConfig {
            max_daily_loss: Some(dec!(100)),
            max_notional: None,
            reset_hour_utc: None,
        });
        // 2026-04-22 15:59 UTC = 2026-04-22 23:59 UTC+8 — still today
        let before = chrono::DateTime::parse_from_rfc3339("2026-04-22T15:59:00Z")
            .unwrap()
            .with_timezone(&Utc);
        b.maybe_reset_daily(before);
        b.record_realized_pnl(dec!(-50));

        // 2026-04-22 16:01 UTC = 2026-04-23 00:01 UTC+8 — new day
        let after = chrono::DateTime::parse_from_rfc3339("2026-04-22T16:01:00Z")
            .unwrap()
            .with_timezone(&Utc);
        b.maybe_reset_daily(after);
        assert_eq!(b.daily_pnl, Decimal::ZERO);
    }

    #[test]
    fn maybe_reset_daily_no_reset_same_day() {
        let mut b = StrategyRiskBudget::new(StrategyRiskBudgetConfig {
            max_daily_loss: Some(dec!(100)),
            max_notional: None,
            ..Default::default()
        });
        let now = Utc::now();
        b.maybe_reset_daily(now);
        b.record_realized_pnl(dec!(-50));
        b.maybe_reset_daily(now);
        assert_eq!(b.daily_pnl, dec!(-50)); // not reset
    }
}
