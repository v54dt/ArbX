use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::{RwLock, mpsc};

use super::market_data::BybitMarket;
use super::rest_client::BybitRestClient;
use crate::adapters::order_executor::{OrderExecutor, OrderReceivers};
use crate::adapters::rest_client::{ExchangeRestClient, HttpMethod, RestRequest};
use crate::models::enums::{OrderStatus, OrderType, Side, TimeInForce};
use crate::models::instrument::Instrument;
use crate::models::order::{Fill, Order, OrderUpdate};

struct BybitOrderEntry {
    symbol: String,
    #[allow(dead_code)]
    instrument: Instrument,
}

pub struct BybitOrderExecutor {
    market: BybitMarket,
    rest_client: BybitRestClient,
    fills_tx: Option<mpsc::UnboundedSender<Fill>>,
    updates_tx: Option<mpsc::UnboundedSender<OrderUpdate>>,
    order_map: Arc<RwLock<HashMap<String, BybitOrderEntry>>>,
}

impl BybitOrderExecutor {
    pub fn new(market: BybitMarket, api_key: String, api_secret: String) -> anyhow::Result<Self> {
        let rest_client = BybitRestClient::new("https://api.bybit.com", &api_key, &api_secret)?;

        Ok(Self {
            market,
            rest_client,
            fills_tx: None,
            updates_tx: None,
            order_map: Arc::new(RwLock::new(HashMap::new())),
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

    fn parse_status(s: &str) -> OrderStatus {
        match s {
            "Filled" => OrderStatus::Filled,
            "PartiallyFilled" => OrderStatus::PartiallyFilled,
            // "New" = accepted and resting — matches private_stream mapping.
            "New" => OrderStatus::Submitted,
            "Cancelled" => OrderStatus::Cancelled,
            "Rejected" => OrderStatus::Rejected,
            _ => OrderStatus::Pending,
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
        params.insert("symbol".to_string(), symbol.clone());
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

        self.order_map.write().await.insert(
            order_id.clone(),
            BybitOrderEntry {
                symbol,
                instrument: order.instrument.clone(),
            },
        );

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
        let symbol = self
            .order_map
            .read()
            .await
            .get(order_id)
            .map(|e| e.symbol.clone())
            .ok_or_else(|| anyhow::anyhow!("unknown order_id: {}", order_id))?;

        let mut params = HashMap::new();
        params.insert("category".to_string(), self.category().to_string());
        params.insert("symbol".to_string(), symbol);
        params.insert("orderId".to_string(), order_id.to_string());

        let request = RestRequest {
            method: HttpMethod::Post,
            path: "/v5/order/cancel".to_string(),
            params,
        };

        let response = self.rest_client.send(request).await?;

        if response.status < 200 || response.status >= 300 {
            anyhow::bail!(
                "bybit cancel rejected ({}): {}",
                response.status,
                response.body
            );
        }

        let json: serde_json::Value = serde_json::from_str(&response.body)?;
        if json["retCode"].as_i64() != Some(0) {
            anyhow::bail!(
                "bybit cancel failed: {}",
                json["retMsg"].as_str().unwrap_or(&response.body)
            );
        }

        Ok(true)
    }

    async fn get_order_status(&self, order_id: &str) -> anyhow::Result<OrderUpdate> {
        let (symbol, category) = {
            let map = self.order_map.read().await;
            let entry = map
                .get(order_id)
                .ok_or_else(|| anyhow::anyhow!("unknown order_id: {}", order_id))?;
            (entry.symbol.clone(), self.category().to_string())
        };

        let mut params = HashMap::new();
        params.insert("category".to_string(), category);
        params.insert("symbol".to_string(), symbol);
        params.insert("orderId".to_string(), order_id.to_string());

        let request = RestRequest {
            method: HttpMethod::Get,
            path: "/v5/order/realtime".to_string(),
            params,
        };

        let response = self.rest_client.send(request).await?;

        if response.status < 200 || response.status >= 300 {
            anyhow::bail!(
                "bybit order status error ({}): {}",
                response.status,
                response.body
            );
        }

        let json: serde_json::Value = serde_json::from_str(&response.body)?;
        let item = &json["result"]["list"][0];
        let status = Self::parse_status(item["orderStatus"].as_str().unwrap_or(""));
        let filled_quantity = item["cumExecQty"]
            .as_str()
            .unwrap_or("0")
            .parse::<Decimal>()
            .unwrap_or(Decimal::ZERO);
        let orig_qty = item["qty"]
            .as_str()
            .unwrap_or("0")
            .parse::<Decimal>()
            .unwrap_or(Decimal::ZERO);
        let remaining_quantity = orig_qty - filled_quantity;
        let average_price = item["avgPrice"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok())
            .filter(|p| !p.is_zero());

        Ok(OrderUpdate {
            order_id: order_id.to_string(),
            status,
            filled_quantity,
            remaining_quantity,
            average_price,
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

    fn make_executor(market: BybitMarket) -> BybitOrderExecutor {
        BybitOrderExecutor::new(market, "key".into(), "secret".into()).unwrap()
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

    #[test]
    fn parse_status_maps_all_states() {
        assert_eq!(
            BybitOrderExecutor::parse_status("Filled"),
            OrderStatus::Filled
        );
        assert_eq!(
            BybitOrderExecutor::parse_status("PartiallyFilled"),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(
            BybitOrderExecutor::parse_status("New"),
            OrderStatus::Submitted
        );
        assert_eq!(
            BybitOrderExecutor::parse_status("Cancelled"),
            OrderStatus::Cancelled
        );
        assert_eq!(
            BybitOrderExecutor::parse_status("Rejected"),
            OrderStatus::Rejected
        );
        assert_eq!(
            BybitOrderExecutor::parse_status("Unknown"),
            OrderStatus::Pending
        );
    }

    #[tokio::test]
    async fn order_map_starts_empty() {
        let executor = make_executor(BybitMarket::Spot);
        assert!(executor.order_map.read().await.is_empty());
    }

    #[tokio::test]
    async fn get_order_status_unknown_order_returns_error() {
        let executor = make_executor(BybitMarket::Spot);
        let result = executor.get_order_status("nonexistent-id").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown order_id"));
    }

    #[tokio::test]
    async fn cancel_order_unknown_order_returns_error() {
        let executor = make_executor(BybitMarket::Spot);
        let result = executor.cancel_order("nonexistent-id").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown order_id"));
    }

    #[tokio::test]
    async fn order_entry_stores_correct_symbol() {
        let executor = make_executor(BybitMarket::Spot);
        let inst = btc_usdt_spot();
        let symbol = BybitOrderExecutor::instrument_to_symbol(&inst);
        executor.order_map.write().await.insert(
            "test-order-1".to_string(),
            BybitOrderEntry {
                symbol: symbol.clone(),
                instrument: inst,
            },
        );
        let stored = executor.order_map.read().await;
        let entry = stored.get("test-order-1").unwrap();
        assert_eq!(entry.symbol, "BTCUSDT");
    }
}
