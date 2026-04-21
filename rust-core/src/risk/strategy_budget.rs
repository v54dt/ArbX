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

    pub fn maybe_reset_daily(&mut self, now: DateTime<Utc>) {
        let taipei = FixedOffset::east_opt(8 * 3600).unwrap();
        let today = now.with_timezone(&taipei).date_naive();
        if self.last_reset_date != Some(today) {
            self.daily_pnl = Decimal::ZERO;
            self.notional_submitted = Decimal::ZERO;
            self.order_count = 0;
            self.last_reset_date = Some(today);
        }
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

    pub fn record_pnl(&mut self, expected_net: Decimal) {
        self.daily_pnl += expected_net;
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
        });
        b.record_pnl(dec!(-50));
        assert!(b.is_within_budget());
        b.record_pnl(dec!(-60));
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
        });
        b.record_pnl(dec!(-101));
        assert!(!b.is_within_budget());
        assert!(b.rejection_reason().unwrap().contains("daily_loss"));
    }

    #[test]
    fn maybe_reset_daily_resets_on_new_day() {
        let mut b = StrategyRiskBudget::new(StrategyRiskBudgetConfig {
            max_daily_loss: Some(dec!(100)),
            max_notional: Some(dec!(1000)),
        });
        b.record_pnl(dec!(-101));
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
    fn maybe_reset_daily_no_reset_same_day() {
        let mut b = StrategyRiskBudget::new(StrategyRiskBudgetConfig {
            max_daily_loss: Some(dec!(100)),
            max_notional: None,
        });
        let now = Utc::now();
        b.maybe_reset_daily(now);
        b.record_pnl(dec!(-50));
        b.maybe_reset_daily(now);
        assert_eq!(b.daily_pnl, dec!(-50)); // not reset
    }
}
