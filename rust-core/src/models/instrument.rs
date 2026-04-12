use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AssetClass {
    Equity,
    Index,
    Commodity,
    Crypto,
    Fx,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum InstrumentType {
    Spot,
    Futures,
    Option,
    Swap, // Perpetual (no expiry)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Instrument {
    pub asset_class: AssetClass,
    pub instrument_type: InstrumentType,
    pub base: String,
    pub quote: String,
    pub settle_currency: Option<String>,
    pub expiry: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_trade_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub settlement_time: Option<DateTime<Utc>>,
}
