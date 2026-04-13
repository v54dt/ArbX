use rust_decimal::Decimal;
use std::collections::HashMap;

use super::manager::RiskVerdict;

pub struct RiskState {
    pub position_by_instrument: HashMap<String, Decimal>,
    pub total_notional_exposure: Decimal,
    pub realized_pnl_today: Decimal,
    pub max_position_size: Decimal,
    pub max_notional_exposure: Decimal,
    pub max_daily_loss: Decimal,
    pub halted: bool,
}

impl RiskState {
    pub fn new(
        max_position_size: Decimal,
        max_notional_exposure: Decimal,
        max_daily_loss: Decimal,
    ) -> Self {
        Self {
            position_by_instrument: HashMap::new(),
            total_notional_exposure: Decimal::ZERO,
            realized_pnl_today: Decimal::ZERO,
            max_position_size,
            max_notional_exposure,
            max_daily_loss,
            halted: false,
        }
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

    pub fn apply_fill(
        &mut self,
        instrument_key: &str,
        quantity: Decimal,
        notional: Decimal,
        realized_pnl: Decimal,
    ) {
        let pos = self
            .position_by_instrument
            .entry(instrument_key.to_string())
            .or_insert(Decimal::ZERO);
        *pos += quantity;
        self.total_notional_exposure += notional.abs();
        self.realized_pnl_today += realized_pnl;
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
        state.apply_fill("BTC-USDT", dec!(3), dec!(150000), Decimal::ZERO);
        assert_eq!(
            *state.position_by_instrument.get("BTC-USDT").unwrap(),
            dec!(3)
        );
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
}
