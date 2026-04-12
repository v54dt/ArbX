use std::collections::HashMap;

use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use super::market_data::BybitMarket;
use super::rest_client::BybitRestClient;
use crate::adapters::order_executor::{OrderExecutor, OrderReceivers};
use crate::adapters::rest_client::{ExchangeRestClient, HttpMethod, RestRequest};
use crate::models::enums::{OrderStatus, OrderType, Side, TimeInForce};
use crate::models::instrument::Instrument;
use crate::models::order::{Fill, Order, OrderUpdate};

pub struct BybitOrderExecutor {
    market: BybitMarket,
    rest_client: BybitRestClient,
    fills_tx: Option<mpsc::UnboundedSender<Fill>>,
    updates_tx: Option<mpsc::UnboundedSender<OrderUpdate>>,
}

impl BybitOrderExecutor {
    pub fn new(market: BybitMarket, api_key: String, api_secret: String) -> anyhow::Result<Self> {
        let rest_client = BybitRestClient::new("https://api.bybit.com", &api_key, &api_secret)?;

        Ok(Self {
            market,
            rest_client,
            fills_tx: None,
            updates_tx: None,
        })
    }

    fn category(&self) -> &'static str {
        match self.market {
            BybitMarket::Spot => "spot",
            BybitMarket::Linear => "linear",
            BybitMarket::Inverse => "inverse",
        }
    }

    fn market_label(&self) -> &'static str {
        match self.market {
            BybitMarket::Spot => "spot",
            BybitMarket::Linear => "linear",
            BybitMarket::Inverse => "inverse",
        }
    }

    fn instrument_to_symbol(instrument: &Instrument) -> String {
        format!("{}{}", instrument.base, instrument.quote).to_uppercase()
    }

    fn side_str(side: Side) -> &'static str {
        match side {
            Side::Buy => "Buy",
            Side::Sell => "Sell",
        }
    }

    fn order_type_str(ot: OrderType) -> &'static str {
        match ot {
            OrderType::Limit => "Limit",
            OrderType::Market => "Market",
        }
    }

    fn tif_to_string(tif: Option<TimeInForce>) -> &'static str {
        match tif {
            Some(TimeInForce::Ioc) => "IOC",
            Some(TimeInForce::Fok) => "FOK",
            Some(TimeInForce::Rod) | None => "GTC",
        }
    }
}

#[async_trait]
impl OrderExecutor for BybitOrderExecutor {
    async fn connect(&mut self) -> anyhow::Result<OrderReceivers> {
        let (fills_tx, fills) = mpsc::unbounded_channel::<Fill>();
        let (updates_tx, updates) = mpsc::unbounded_channel::<OrderUpdate>();
        self.fills_tx = Some(fills_tx);
        self.updates_tx = Some(updates_tx);
        tracing::info!(
            market = self.market_label(),
            "bybit executor connected (REST)"
        );
        Ok(OrderReceivers { fills, updates })
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.fills_tx = None;
        self.updates_tx = None;
        tracing::info!("bybit executor disconnected");
        Ok(())
    }

    async fn submit_order(&self, order: &Order) -> anyhow::Result<String> {
        let symbol = Self::instrument_to_symbol(&order.instrument);

        let mut params = HashMap::new();
        params.insert("category".to_string(), self.category().to_string());
        params.insert("symbol".to_string(), symbol);
        params.insert("side".to_string(), Self::side_str(order.side).to_string());
        params.insert(
            "orderType".to_string(),
            Self::order_type_str(order.order_type).to_string(),
        );
        params.insert("qty".to_string(), order.quantity.to_string());
        params.insert(
            "timeInForce".to_string(),
            Self::tif_to_string(order.time_in_force).to_string(),
        );

        if order.order_type == OrderType::Limit
            && let Some(price) = order.price
        {
            params.insert("price".to_string(), price.to_string());
        }

        let request = RestRequest {
            method: HttpMethod::Post,
            path: "/v5/order/create".to_string(),
            params,
        };

        let response = self.rest_client.send(request).await?;

        if response.status < 200 || response.status >= 300 {
            anyhow::bail!(
                "bybit order rejected ({}): {}",
                response.status,
                response.body
            );
        }

        let json: serde_json::Value = serde_json::from_str(&response.body)?;
        let order_id = match json["result"]["orderId"].as_str() {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => anyhow::bail!("missing orderId in response: {}", response.body),
        };

        tracing::info!(
            order_id,
            market = self.market_label(),
            side = ?order.side,
            qty = %order.quantity,
            "order submitted"
        );

        Ok(order_id)
    }

    async fn cancel_order(&self, order_id: &str) -> anyhow::Result<bool> {
        tracing::info!(order_id, "cancel_order (stub — needs symbol)");
        Ok(true)
    }

    async fn get_order_status(&self, order_id: &str) -> anyhow::Result<OrderUpdate> {
        Ok(OrderUpdate {
            order_id: order_id.to_string(),
            status: OrderStatus::Pending,
            filled_quantity: Decimal::ZERO,
            remaining_quantity: Decimal::ZERO,
            average_price: None,
            updated_at: chrono::Utc::now(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::instrument::{AssetClass, InstrumentType};

    fn btc_usdt_spot() -> Instrument {
        Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: InstrumentType::Spot,
            base: "BTC".to_string(),
            quote: "USDT".to_string(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        }
    }

    #[test]
    fn side_str_maps_correctly() {
        assert_eq!(BybitOrderExecutor::side_str(Side::Buy), "Buy");
        assert_eq!(BybitOrderExecutor::side_str(Side::Sell), "Sell");
    }

    #[test]
    fn order_type_str_maps_correctly() {
        assert_eq!(
            BybitOrderExecutor::order_type_str(OrderType::Limit),
            "Limit"
        );
        assert_eq!(
            BybitOrderExecutor::order_type_str(OrderType::Market),
            "Market"
        );
    }

    #[test]
    fn symbol_format() {
        let inst = btc_usdt_spot();
        assert_eq!(BybitOrderExecutor::instrument_to_symbol(&inst), "BTCUSDT");
    }

    #[test]
    fn tif_to_string_defaults_to_gtc() {
        assert_eq!(BybitOrderExecutor::tif_to_string(None), "GTC");
        assert_eq!(
            BybitOrderExecutor::tif_to_string(Some(TimeInForce::Rod)),
            "GTC"
        );
        assert_eq!(
            BybitOrderExecutor::tif_to_string(Some(TimeInForce::Ioc)),
            "IOC"
        );
        assert_eq!(
            BybitOrderExecutor::tif_to_string(Some(TimeInForce::Fok)),
            "FOK"
        );
    }
}
