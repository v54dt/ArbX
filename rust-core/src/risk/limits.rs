use rust_decimal::Decimal;

use crate::models::order::Order;
use crate::models::position::PortfolioSnapshot;

use super::manager::RiskVerdict;

pub trait RiskLimit: Send + Sync {
    fn check(&self, order: &Order, portfolio: &PortfolioSnapshot) -> RiskVerdict;
    fn name(&self) -> &str;
}

pub struct MaxPositionSize {
    pub max_quantity: Decimal,
}

impl RiskLimit for MaxPositionSize {
    fn check(&self, order: &Order, portfolio: &PortfolioSnapshot) -> RiskVerdict {
        let current = portfolio
            .positions
            .iter()
            .find(|p| p.instrument == order.instrument)
            .map(|p| p.quantity.abs())
            .unwrap_or(Decimal::ZERO);

        let projected = current + order.quantity;

        if projected > self.max_quantity {
            let allowed = self.max_quantity - current;
            if allowed > Decimal::ZERO {
                RiskVerdict::adjusted(allowed, "position size capped")
            } else {
                RiskVerdict::rejected("max position size reached")
            }
        } else {
            RiskVerdict::approved()
        }
    }

    fn name(&self) -> &str {
        "max_position_size"
    }
}

pub struct MaxDailyLoss {
    pub max_loss: Decimal,
}

impl RiskLimit for MaxDailyLoss {
    fn check(&self, _order: &Order, portfolio: &PortfolioSnapshot) -> RiskVerdict {
        if portfolio.realized_pnl < -self.max_loss {
            RiskVerdict::rejected("daily loss limit breached")
        } else {
            RiskVerdict::approved()
        }
    }

    fn name(&self) -> &str {
        "max_daily_loss"
    }
}

pub struct MaxNotionalExposure {
    pub max_notional: Decimal,
}

impl RiskLimit for MaxNotionalExposure {
    fn check(&self, order: &Order, portfolio: &PortfolioSnapshot) -> RiskVerdict {
        let current_notional: Decimal = portfolio
            .positions
            .iter()
            .map(|p| (p.quantity * p.average_cost).abs())
            .sum();

        let order_notional = order.quantity * order.price.unwrap_or(Decimal::ZERO);

        if current_notional + order_notional > self.max_notional {
            RiskVerdict::rejected("max notional exposure exceeded")
        } else {
            RiskVerdict::approved()
        }
    }

    fn name(&self) -> &str {
        "max_notional_exposure"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::enums::{OrderType, Side, Venue};
    use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
    use crate::models::order::Order;
    use crate::models::position::{PortfolioSnapshot, Position};
    use chrono::Utc;
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
        }
    }

    fn buy_order(qty: Decimal, price: Option<Decimal>) -> Order {
        Order {
            id: "test-order".to_string(),
            venue: Venue::Binance,
            instrument: test_instrument(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            time_in_force: None,
            price,
            quantity: qty,
            created_at: Utc::now(),
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

    fn portfolio_with_position(qty: Decimal, avg_cost: Decimal) -> PortfolioSnapshot {
        PortfolioSnapshot {
            venue: Venue::Binance,
            positions: vec![Position {
                venue: Venue::Binance,
                instrument: test_instrument(),
                quantity: qty,
                average_cost: avg_cost,
                unrealized_pnl: Decimal::ZERO,
                realized_pnl: Decimal::ZERO,
            }],
            total_equity: Decimal::ZERO,
            available_balance: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
        }
    }

    #[test]
    fn max_position_size_approves_when_under_cap() {
        let limit = MaxPositionSize {
            max_quantity: dec!(10),
        };
        let order = buy_order(dec!(5), Some(dec!(100)));
        let verdict = limit.check(&order, &empty_portfolio());

        assert!(verdict.approved);
        assert_eq!(verdict.adjusted_qty, None);
    }

    #[test]
    fn max_position_size_adjusts_when_partial_room_left() {
        let limit = MaxPositionSize {
            max_quantity: dec!(10),
        };
        let portfolio = portfolio_with_position(dec!(7), dec!(100));
        let order = buy_order(dec!(5), Some(dec!(100)));
        let verdict = limit.check(&order, &portfolio);

        assert!(verdict.approved);
        assert_eq!(verdict.adjusted_qty, Some(dec!(3)));
    }

    #[test]
    fn max_position_size_rejects_when_cap_reached() {
        let limit = MaxPositionSize {
            max_quantity: dec!(10),
        };
        let portfolio = portfolio_with_position(dec!(10), dec!(100));
        let order = buy_order(dec!(1), Some(dec!(100)));
        let verdict = limit.check(&order, &portfolio);

        assert!(!verdict.approved);
        assert!(
            verdict
                .reason
                .as_deref()
                .unwrap_or("")
                .contains("max position size")
        );
    }

    #[test]
    fn max_position_size_uses_absolute_value_of_existing_position() {
        // TODO: possibly a bug — documents current behavior
        // The impl uses `.abs()` on the existing quantity, so a short position
        // is treated the same as a long one for cap purposes.
        let limit = MaxPositionSize {
            max_quantity: dec!(10),
        };
        let portfolio = portfolio_with_position(dec!(-7), dec!(100));
        let order = buy_order(dec!(5), Some(dec!(100)));
        let verdict = limit.check(&order, &portfolio);

        assert!(verdict.approved);
        assert_eq!(verdict.adjusted_qty, Some(dec!(3)));
    }

    #[test]
    fn max_daily_loss_approves_when_within_budget() {
        let limit = MaxDailyLoss {
            max_loss: dec!(1000),
        };
        let mut portfolio = empty_portfolio();
        portfolio.realized_pnl = dec!(-500);
        let order = buy_order(dec!(1), Some(dec!(100)));
        let verdict = limit.check(&order, &portfolio);

        assert!(verdict.approved);
    }

    #[test]
    fn max_daily_loss_rejects_when_breached() {
        let limit = MaxDailyLoss {
            max_loss: dec!(1000),
        };
        let mut portfolio = empty_portfolio();
        portfolio.realized_pnl = dec!(-1500);
        let order = buy_order(dec!(1), Some(dec!(100)));
        let verdict = limit.check(&order, &portfolio);

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
    fn max_daily_loss_boundary_exact_match_is_approved() {
        // The impl uses strict `<`, so realized_pnl == -max_loss is still approved.
        let limit = MaxDailyLoss {
            max_loss: dec!(1000),
        };
        let mut portfolio = empty_portfolio();
        portfolio.realized_pnl = dec!(-1000);
        let order = buy_order(dec!(1), Some(dec!(100)));
        let verdict = limit.check(&order, &portfolio);

        assert!(verdict.approved);
    }

    #[test]
    fn max_notional_exposure_approves_when_room_available() {
        let limit = MaxNotionalExposure {
            max_notional: dec!(100000),
        };
        let order = buy_order(dec!(5), Some(dec!(10000)));
        let verdict = limit.check(&order, &empty_portfolio());

        assert!(verdict.approved);
    }

    #[test]
    fn max_notional_exposure_rejects_when_over_cap() {
        let limit = MaxNotionalExposure {
            max_notional: dec!(100000),
        };
        // Existing position: qty=8, avg_cost=10000 → notional=80000
        let portfolio = portfolio_with_position(dec!(8), dec!(10000));
        // New order: qty=5, price=6000 → notional=30000; total=110000 > cap
        let order = buy_order(dec!(5), Some(dec!(6000)));
        let verdict = limit.check(&order, &portfolio);

        assert!(!verdict.approved);
        assert!(
            verdict
                .reason
                .as_deref()
                .unwrap_or("")
                .contains("max notional exposure")
        );
    }

    #[test]
    fn max_notional_exposure_handles_missing_order_price() {
        let limit = MaxNotionalExposure {
            max_notional: dec!(100000),
        };
        // Existing position: qty=5, avg_cost=10000 → notional=50000 (under cap)
        let portfolio = portfolio_with_position(dec!(5), dec!(10000));
        // Order has no price → order_notional = 0
        let order = buy_order(dec!(5), None);
        let verdict = limit.check(&order, &portfolio);

        assert!(verdict.approved);
    }
}
