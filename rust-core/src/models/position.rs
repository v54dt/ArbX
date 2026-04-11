use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::enums::Venue;
use super::instrument::Instrument;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub venue: Venue,
    pub instrument: Instrument,
    pub quantity: Decimal,
    pub average_cost: Decimal,
    pub unrealized_pnl: Decimal,
    pub realized_pnl: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioSnapshot {
    pub venue: Venue,
    pub positions: Vec<Position>,
    pub total_equity: Decimal,
    pub available_balance: Decimal,
    pub unrealized_pnl: Decimal,
    pub realized_pnl: Decimal,
}
