use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::enums::{OrderStatus, OrderType, Side, TimeInForce, Venue};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: String,
    pub venue: Venue,
    pub symbol: String,
    pub side: Side,
    pub order_type: OrderType,
    pub time_in_force: Option<TimeInForce>,
    pub price: Option<Decimal>,
    pub quantity: Decimal,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    pub order_id: String,
    pub venue: Venue,
    pub symbol: String,
    pub side: Side,
    pub price: Decimal,
    pub quantity: Decimal,
    pub fee: Decimal,
    pub fee_currency: String,
    pub filled_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderUpdate {
    pub order_id: String,
    pub status: OrderStatus,
    pub filled_quantity: Decimal,
    pub remaining_quantity: Decimal,
    pub average_price: Option<Decimal>,
    pub updated_at: DateTime<Utc>,
}
