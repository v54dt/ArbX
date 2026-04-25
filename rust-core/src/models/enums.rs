use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderType {
    Limit,
    Market,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TimeInForce {
    Rod,
    Ioc,
    Fok,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderStatus {
    Pending,
    Submitted,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
    Expired,
    PendingCancel,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Venue {
    Fubon,
    Shioaji,
    #[default]
    Binance,
    Bybit,
    Okx,
}

impl Venue {
    /// Lowercase label suitable for Prometheus metric labels. Static — avoids
    /// `format!("{:?}", venue).to_lowercase()` allocations in the fill /
    /// submit hot paths (review §3.8).
    pub fn label(self) -> &'static str {
        match self {
            Venue::Fubon => "fubon",
            Venue::Shioaji => "shioaji",
            Venue::Binance => "binance",
            Venue::Bybit => "bybit",
            Venue::Okx => "okx",
        }
    }
}
