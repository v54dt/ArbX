use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::enums::{OrderStatus, OrderType, Side, TimeInForce, Venue};
use super::instrument::Instrument;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub venue: Venue,
    pub instrument: Instrument,
    pub side: Side,
    pub order_type: OrderType,
    pub time_in_force: Option<TimeInForce>,
    pub price: Option<Decimal>,
    pub quantity: Decimal,
}

impl OrderRequest {
    pub fn into_order(self) -> Order {
        Order {
            id: String::new(),
            venue: self.venue,
            instrument: self.instrument,
            side: self.side,
            order_type: self.order_type,
            time_in_force: self.time_in_force,
            price: self.price,
            quantity: self.quantity,
            created_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: String,
    pub venue: Venue,
    pub instrument: Instrument,
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
    pub instrument: Instrument,
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
