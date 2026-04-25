use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::{RwLock, mpsc};

use super::rest_client::OkxRestClient;
use crate::adapters::order_executor::{OrderExecutor, OrderReceivers};
use crate::adapters::rest_client::{ExchangeRestClient, HttpMethod, RestRequest};
use crate::models::enums::{OrderStatus, OrderType, Side};
use crate::models::instrument::Instrument;
use crate::models::order::{Fill, Order, OrderUpdate};

struct OkxOrderEntry {
    inst_id: String,
    #[allow(dead_code)]
    instrument: Instrument,
}

pub struct OkxOrderExecutor {
    rest_client: OkxRestClient,
    fills_tx: Option<mpsc::UnboundedSender<Fill>>,
    updates_tx: Option<mpsc::UnboundedSender<OrderUpdate>>,
    order_map: Arc<RwLock<HashMap<String, OkxOrderEntry>>>,
}

impl OkxOrderExecutor {
    pub fn new(api_key: String, api_secret: String, passphrase: String) -> anyhow::Result<Self> {
        let rest_client =
            OkxRestClient::new("https://www.okx.com", &api_key, &api_secret, &passphrase)?;
        Ok(Self {
            rest_client,
            fills_tx: None,
            updates_tx: None,
            order_map: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    fn instrument_to_inst_id(instrument: &Instrument) -> String {
        format!("{}-{}", instrument.base, instrument.quote).to_uppercase()
    }

    fn side_str(side: Side) -> &'static str {
        match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    }

    fn order_type_str(ot: OrderType) -> &'static str {
        match ot {
            OrderType::Limit => "limit",
            OrderType::Market => "market",
        }
    }

    fn td_mode(instrument: &Instrument) -> &'static str {
        use crate::models::instrument::InstrumentType;
        match instrument.instrument_type {
            InstrumentType::Spot => "cash",
            _ => "cross",
        }
    }

    fn parse_status(s: &str) -> OrderStatus {
        match s {
            "filled" => OrderStatus::Filled,
            "partially_filled" => OrderStatus::PartiallyFilled,
            "live" => OrderStatus::Submitted,
            "canceled" => OrderStatus::Cancelled,
            "expired" => OrderStatus::Expired,
            "canceling" => OrderStatus::PendingCancel,
            _ => OrderStatus::Pending,
        }
    }
}

#[async_trait]
impl OrderExecutor for OkxOrderExecutor {
    async fn connect(&mut self) -> anyhow::Result<OrderReceivers> {
        let (fills_tx, fills) = mpsc::unbounded_channel::<Fill>();
        let (updates_tx, updates) = mpsc::unbounded_channel::<OrderUpdate>();
        self.fills_tx = Some(fills_tx);
        self.updates_tx = Some(updates_tx);
        tracing::info!("okx executor connected (REST)");
        Ok(OrderReceivers { fills, updates })
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.fills_tx = None;
        self.updates_tx = None;
        tracing::info!("okx executor disconnected");
        Ok(())
    }

    async fn submit_order(&self, order: &Order) -> anyhow::Result<String> {
        let inst_id = Self::instrument_to_inst_id(&order.instrument);

        let mut params = HashMap::new();
        params.insert("instId".to_string(), inst_id.clone());
        params.insert(
            "tdMode".to_string(),
            Self::td_mode(&order.instrument).to_string(),
        );
        params.insert("side".to_string(), Self::side_str(order.side).to_string());
        params.insert(
            "ordType".to_string(),
            Self::order_type_str(order.order_type).to_string(),
        );
        params.insert("sz".to_string(), order.quantity.to_string());

        if order.order_type == OrderType::Limit
            && let Some(price) = order.price
        {
            params.insert("px".to_string(), price.to_string());
        }
        if !order.client_order_id.is_empty() {
            params.insert("clOrdId".to_string(), order.client_order_id.clone());
        }

        let request = RestRequest {
            method: HttpMethod::Post,
            path: "/api/v5/trade/order".to_string(),
            params,
            raw_body: None,
        };

        let response = self.rest_client.send(request).await?;

        if response.status < 200 || response.status >= 300 {
            anyhow::bail!(
                "okx order rejected ({}): {}",
                response.status,
                response.body
            );
        }

        let json: serde_json::Value = serde_json::from_str(&response.body)?;
        let order_id = match json["data"][0]["ordId"].as_str() {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => anyhow::bail!("missing ordId in response: {}", response.body),
        };

        self.order_map.write().await.insert(
            order_id.clone(),
            OkxOrderEntry {
                inst_id,
                instrument: order.instrument.clone(),
            },
        );

        tracing::info!(
            order_id,
            side = ?order.side,
            qty = %order.quantity,
            "okx order submitted"
        );

        Ok(order_id)
    }

    async fn cancel_order(&self, order_id: &str) -> anyhow::Result<bool> {
        let inst_id = self
            .order_map
            .read()
            .await
            .get(order_id)
            .map(|e| e.inst_id.clone())
            .ok_or_else(|| anyhow::anyhow!("unknown order_id: {}", order_id))?;

        let mut params = HashMap::new();
        params.insert("instId".to_string(), inst_id);
        params.insert("ordId".to_string(), order_id.to_string());

        let request = RestRequest {
            method: HttpMethod::Post,
            path: "/api/v5/trade/cancel-order".to_string(),
            params,
            raw_body: None,
        };

        let response = self.rest_client.send(request).await?;

        if response.status < 200 || response.status >= 300 {
            anyhow::bail!(
                "okx cancel rejected ({}): {}",
                response.status,
                response.body
            );
        }

        let json: serde_json::Value = serde_json::from_str(&response.body)?;
        if json["code"].as_str() != Some("0") {
            anyhow::bail!(
                "okx cancel failed: {}",
                json["msg"].as_str().unwrap_or(&response.body)
            );
        }

        Ok(true)
    }

    async fn get_order_status(&self, order_id: &str) -> anyhow::Result<OrderUpdate> {
        let inst_id = self
            .order_map
            .read()
            .await
            .get(order_id)
            .map(|e| e.inst_id.clone())
            .ok_or_else(|| anyhow::anyhow!("unknown order_id: {}", order_id))?;

        let mut params = HashMap::new();
        params.insert("instId".to_string(), inst_id);
        params.insert("ordId".to_string(), order_id.to_string());

        let request = RestRequest {
            method: HttpMethod::Get,
            path: "/api/v5/trade/order".to_string(),
            params,
            raw_body: None,
        };

        let response = self.rest_client.send(request).await?;

        if response.status < 200 || response.status >= 300 {
            anyhow::bail!(
                "okx order status error ({}): {}",
                response.status,
                response.body
            );
        }

        let json: serde_json::Value = serde_json::from_str(&response.body)?;
        let data = &json["data"][0];
        let status = Self::parse_status(data["state"].as_str().unwrap_or(""));
        let filled_quantity = data["accFillSz"]
            .as_str()
            .unwrap_or("0")
            .parse::<Decimal>()
            .unwrap_or(Decimal::ZERO);
        let orig_qty = data["sz"]
            .as_str()
            .unwrap_or("0")
            .parse::<Decimal>()
            .unwrap_or(Decimal::ZERO);
        let remaining_quantity = orig_qty - filled_quantity;
        let average_price = data["avgPx"]
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

    #[test]
    fn side_to_okx_maps_correctly() {
        assert_eq!(OkxOrderExecutor::side_str(Side::Buy), "buy");
        assert_eq!(OkxOrderExecutor::side_str(Side::Sell), "sell");
    }

    #[test]
    fn order_type_to_okx_maps_correctly() {
        assert_eq!(OkxOrderExecutor::order_type_str(OrderType::Limit), "limit");
        assert_eq!(
            OkxOrderExecutor::order_type_str(OrderType::Market),
            "market"
        );
    }

    #[test]
    fn instrument_to_inst_id_formats_correctly() {
        let inst = btc_usdt_spot();
        assert_eq!(OkxOrderExecutor::instrument_to_inst_id(&inst), "BTC-USDT");
    }

    #[test]
    fn td_mode_spot_is_cash() {
        let inst = btc_usdt_spot();
        assert_eq!(OkxOrderExecutor::td_mode(&inst), "cash");
    }

    #[test]
    fn td_mode_swap_is_cross() {
        let inst = Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: InstrumentType::Swap,
            base: "BTC".to_string(),
            quote: "USDT".to_string(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        };
        assert_eq!(OkxOrderExecutor::td_mode(&inst), "cross");
    }

    #[test]
    fn parse_status_maps_all_states() {
        assert_eq!(
            OkxOrderExecutor::parse_status("filled"),
            OrderStatus::Filled
        );
        assert_eq!(
            OkxOrderExecutor::parse_status("partially_filled"),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(
            OkxOrderExecutor::parse_status("live"),
            OrderStatus::Submitted
        );
        assert_eq!(
            OkxOrderExecutor::parse_status("canceled"),
            OrderStatus::Cancelled
        );
        assert_eq!(
            OkxOrderExecutor::parse_status("unknown"),
            OrderStatus::Pending
        );
    }

    #[tokio::test]
    async fn order_map_starts_empty() {
        let executor = OkxOrderExecutor::new("k".into(), "s".into(), "p".into()).unwrap();
        assert!(executor.order_map.read().await.is_empty());
    }

    #[tokio::test]
    async fn get_order_status_unknown_order_returns_error() {
        let executor = OkxOrderExecutor::new("k".into(), "s".into(), "p".into()).unwrap();
        let result = executor.get_order_status("nonexistent-id").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown order_id"));
    }

    #[tokio::test]
    async fn cancel_order_unknown_order_returns_error() {
        let executor = OkxOrderExecutor::new("k".into(), "s".into(), "p".into()).unwrap();
        let result = executor.cancel_order("nonexistent-id").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown order_id"));
    }

    #[tokio::test]
    async fn order_entry_stores_correct_inst_id() {
        let executor = OkxOrderExecutor::new("k".into(), "s".into(), "p".into()).unwrap();
        let inst = btc_usdt_spot();
        let inst_id = OkxOrderExecutor::instrument_to_inst_id(&inst);
        executor.order_map.write().await.insert(
            "test-order-1".to_string(),
            OkxOrderEntry {
                inst_id: inst_id.clone(),
                instrument: inst,
            },
        );
        let stored = executor.order_map.read().await;
        let entry = stored.get("test-order-1").unwrap();
        assert_eq!(entry.inst_id, "BTC-USDT");
    }
}
