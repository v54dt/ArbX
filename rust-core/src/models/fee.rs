use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::enums::Venue;

/// Fee rates for a single venue.
/// TODO: fetch from exchange API instead of hardcoding
/// - Binance: GET /fapi/v1/commissionRate (futures), GET /sapi/v1/asset/tradeFee (spot)
/// - Rate depends on account VIP level and BNB discount
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeSchedule {
    pub venue: Venue,
    pub maker_rate: Decimal,
    pub taker_rate: Decimal,
}

impl FeeSchedule {
    pub fn new(venue: Venue, maker_rate: Decimal, taker_rate: Decimal) -> Self {
        Self {
            venue,
            maker_rate,
            taker_rate,
        }
    }

    /// For market/IOC orders we pay taker fee. For resting limit orders, maker fee.
    /// Arbitrage legs are IOC (taker), so this is the default for arb strategies.
    pub fn taker(&self) -> Decimal {
        self.taker_rate
    }

    pub fn maker(&self) -> Decimal {
        self.maker_rate
    }
}
