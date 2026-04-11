use rust_decimal::Decimal;
use tracing::{info, warn};

use crate::models::order::Order;
use crate::models::position::PortfolioSnapshot;

use super::limits::RiskLimit;

/// Result of a risk check
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

/// Chains multiple RiskLimit checks.
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

    pub fn check_pre_trade(&self, order: &Order, portfolio: &PortfolioSnapshot) -> RiskVerdict {
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
