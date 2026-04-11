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
            .find(|p| p.symbol == order.symbol)
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
