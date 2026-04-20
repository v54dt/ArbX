use rust_decimal::Decimal;
use tracing::{info, warn};

use crate::models::order::OrderRequest;
use crate::models::position::PortfolioSnapshot;

use super::limits::RiskLimit;

#[derive(Debug, Clone)]
pub struct RiskVerdict {
    pub approved: bool,
    pub reason: Option<String>,
    pub adjusted_qty: Option<Decimal>,
}

impl RiskVerdict {
    pub fn approved() -> Self {
        Self {
            approved: true,
            reason: None,
            adjusted_qty: None,
        }
    }

    pub fn rejected(reason: &str) -> Self {
        Self {
            approved: false,
            reason: Some(reason.to_string()),
            adjusted_qty: None,
        }
    }

    pub fn adjusted(qty: Decimal, reason: &str) -> Self {
        Self {
            approved: true,
            reason: Some(reason.to_string()),
            adjusted_qty: Some(qty),
        }
    }
}

pub struct RiskManager {
    limits: Vec<Box<dyn RiskLimit>>,
    halted: bool,
}

impl RiskManager {
    pub fn new(limits: Vec<Box<dyn RiskLimit>>) -> Self {
        Self {
            limits,
            halted: false,
        }
    }

    pub fn check_pre_trade(
        &self,
        order: &OrderRequest,
        portfolio: &PortfolioSnapshot,
    ) -> RiskVerdict {
        if self.halted {
            return RiskVerdict::rejected("risk manager halted (circuit breaker)");
        }

        let mut min_adjusted_qty: Option<Decimal> = None;

        for limit in &self.limits {
            let verdict = limit.check(order, portfolio);

            if !verdict.approved {
                warn!(
                    limit = limit.name(),
                    reason = verdict.reason.as_deref().unwrap_or(""),
                    "order rejected by risk limit"
                );
                return verdict;
            }

            if let Some(adj) = verdict.adjusted_qty {
                min_adjusted_qty =
                    Some(min_adjusted_qty.map_or(adj, |current: Decimal| current.min(adj)));
            }
        }

        if let Some(qty) = min_adjusted_qty {
            if qty == Decimal::ZERO {
                warn!("adjusted quantity is zero — rejecting order");
                return RiskVerdict::rejected("adjusted quantity is zero");
            }
            info!(adjusted_qty = %qty, "order quantity adjusted by risk limits");
            RiskVerdict::adjusted(qty, "quantity adjusted by risk limits")
        } else {
            RiskVerdict::approved()
        }
    }

    pub fn halt(&mut self) {
        warn!("risk manager HALTED - all orders will be rejected");
        self.halted = true;
    }

    pub fn resume(&mut self) {
        info!("risk manager resumed");
        self.halted = false;
    }

    pub fn is_halted(&self) -> bool {
        self.halted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::enums::{OrderType, Side, Venue};
    use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
    use crate::models::order::OrderRequest;
    use crate::models::position::PortfolioSnapshot;
    use crate::risk::limits::{MaxDailyLoss, MaxPositionSize};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn test_instrument() -> Instrument {
        Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: InstrumentType::Spot,
            base: "BTC".to_string(),
            quote: "USDT".to_string(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        }
    }

    fn buy_order(qty: Decimal, price: Option<Decimal>) -> OrderRequest {
        OrderRequest {
            venue: Venue::Binance,
            instrument: test_instrument(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            time_in_force: None,
            price,
            quantity: qty,
            estimated_notional: None,
        }
    }

    fn empty_portfolio() -> PortfolioSnapshot {
        PortfolioSnapshot {
            venue: Venue::Binance,
            positions: vec![],
            total_equity: Decimal::ZERO,
            available_balance: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
        }
    }

    #[test]
    fn empty_chain_approves_everything() {
        let manager = RiskManager::new(vec![]);
        let order = buy_order(dec!(100), Some(dec!(50000)));
        let verdict = manager.check_pre_trade(&order, &empty_portfolio());

        assert!(verdict.approved);
        assert_eq!(verdict.adjusted_qty, None);
    }

    #[test]
    fn chain_short_circuits_on_first_rejection() {
        let manager = RiskManager::new(vec![
            Box::new(MaxDailyLoss {
                max_loss: dec!(1000),
            }),
            Box::new(MaxPositionSize {
                max_quantity: dec!(10),
            }),
        ]);
        let mut portfolio = empty_portfolio();
        portfolio.realized_pnl = dec!(-1500);
        // Order qty would otherwise be within MaxPositionSize limit.
        let order = buy_order(dec!(5), Some(dec!(100)));
        let verdict = manager.check_pre_trade(&order, &portfolio);

        assert!(!verdict.approved);
        assert!(
            verdict
                .reason
                .as_deref()
                .unwrap_or("")
                .contains("daily loss")
        );
    }

    #[test]
    fn chain_returns_minimum_adjusted_quantity() {
        let manager = RiskManager::new(vec![
            Box::new(MaxPositionSize {
                max_quantity: dec!(10),
            }),
            Box::new(MaxPositionSize {
                max_quantity: dec!(5),
            }),
        ]);
        // Empty portfolio, order qty=8 → first limit returns adjusted=10? No.
        // With empty portfolio: current=0, projected=0+8=8. First cap=10:
        //   projected (8) <= 10 → approved (no adjustment)
        // Second cap=5: projected (8) > 5 → allowed=5-0=5 → adjusted=Some(5)
        // Manager takes min of all adjusted_qty seen → Some(5).
        let order = buy_order(dec!(8), Some(dec!(100)));
        let verdict = manager.check_pre_trade(&order, &empty_portfolio());

        assert!(verdict.approved);
        assert_eq!(verdict.adjusted_qty, Some(dec!(5)));
    }

    #[test]
    fn halted_manager_rejects_all_orders() {
        let mut manager = RiskManager::new(vec![]);
        manager.halt();
        let order = buy_order(dec!(1), Some(dec!(100)));
        let verdict = manager.check_pre_trade(&order, &empty_portfolio());

        assert!(!verdict.approved);
        let reason = verdict.reason.as_deref().unwrap_or("");
        assert!(reason.contains("halted") || reason.contains("circuit breaker"));

        // After resume, next check proceeds normally.
        manager.resume();
        let verdict2 = manager.check_pre_trade(&order, &empty_portfolio());
        assert!(verdict2.approved);
    }

    #[test]
    fn is_halted_reflects_state_transitions() {
        let mut manager = RiskManager::new(vec![]);
        assert!(!manager.is_halted());
        manager.halt();
        assert!(manager.is_halted());
        manager.resume();
        assert!(!manager.is_halted());
    }
}
