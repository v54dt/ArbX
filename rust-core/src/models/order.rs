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
    /// Pre-computed notional estimate for market orders (price=None).
    /// Set by the engine from the order book before risk checks.
    /// Risk limits use this when `price` is None to avoid 0-notional bypass.
    #[serde(default)]
    pub estimated_notional: Option<Decimal>,
}

impl OrderRequest {
    /// Returns the best notional estimate: price*qty if price is set,
    /// otherwise falls back to `estimated_notional`, then ZERO.
    pub fn effective_notional(&self) -> Decimal {
        if let Some(p) = self.price {
            p * self.quantity
        } else {
            self.estimated_notional.unwrap_or(Decimal::ZERO)
        }
    }

    pub fn into_order(self) -> Order {
        // Market orders should not carry a price — silently clear it to
        // prevent adapters from receiving a contradictory field.
        let price = if self.order_type == OrderType::Market {
            None
        } else {
            self.price
        };
        Order {
            id: String::new(),
            client_order_id: uuid::Uuid::new_v4().to_string(),
            venue: self.venue,
            instrument: self.instrument,
            side: self.side,
            order_type: self.order_type,
            time_in_force: self.time_in_force,
            price,
            quantity: self.quantity,
            created_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: String,
    /// Engine-generated UUID v4. Sent to venue as newClientOrderId / orderLinkId /
    /// clOrdId so crash-recovery can correlate orders without relying on the
    /// venue-assigned `id` (which the engine may never have received if the ack
    /// was lost mid-submit).
    pub client_order_id: String,
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
    /// Client-assigned order ID (if available from the venue's WS message).
    /// Used as a fallback key for intended_fills lookup when fills arrive
    /// before the engine re-keys from client_order_id to venue order_id.
    #[serde(default)]
    pub client_order_id: Option<String>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
    use rust_decimal_macros::dec;

    fn sample_instrument() -> Instrument {
        Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: InstrumentType::Spot,
            base: "BTC".into(),
            quote: "USDT".into(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        }
    }

    fn sample_order_request() -> OrderRequest {
        OrderRequest {
            venue: Venue::Binance,
            instrument: sample_instrument(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::Ioc),
            price: Some(dec!(50000)),
            quantity: dec!(1),
            estimated_notional: None,
        }
    }

    #[test]
    fn order_request_into_order_sets_empty_id() {
        let order = sample_order_request().into_order();
        assert!(order.id.is_empty());
    }

    #[test]
    fn order_request_into_order_sets_timestamp() {
        let before = Utc::now();
        let order = sample_order_request().into_order();
        let after = Utc::now();
        assert!(order.created_at >= before && order.created_at <= after);
    }

    #[test]
    fn order_request_into_order_preserves_fields() {
        let req = sample_order_request();
        let order = req.clone().into_order();
        assert_eq!(order.venue, Venue::Binance);
        assert_eq!(order.instrument, sample_instrument());
        assert_eq!(order.side, Side::Buy);
        assert_eq!(order.price, Some(dec!(50000)));
        assert_eq!(order.quantity, dec!(1));
        assert_eq!(order.order_type, OrderType::Limit);
        assert_eq!(order.time_in_force, Some(TimeInForce::Ioc));
    }

    #[test]
    fn order_request_serialization_roundtrip() {
        let req = sample_order_request();
        let json = serde_json::to_string(&req).unwrap();
        let deserialized: OrderRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.venue, req.venue);
        assert_eq!(deserialized.instrument, req.instrument);
        assert_eq!(deserialized.side, req.side);
        assert_eq!(deserialized.order_type, req.order_type);
        assert_eq!(deserialized.time_in_force, req.time_in_force);
        assert_eq!(deserialized.price, req.price);
        assert_eq!(deserialized.quantity, req.quantity);
    }
}
