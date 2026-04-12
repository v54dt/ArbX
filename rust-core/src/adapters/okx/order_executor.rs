use std::collections::HashMap;

use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use super::rest_client::OkxRestClient;
use crate::adapters::order_executor::{OrderExecutor, OrderReceivers};
use crate::adapters::rest_client::{ExchangeRestClient, HttpMethod, RestRequest};
use crate::models::enums::{OrderStatus, OrderType, Side};
use crate::models::instrument::Instrument;
use crate::models::order::{Fill, Order, OrderUpdate};

pub struct OkxOrderExecutor {
    rest_client: OkxRestClient,
    fills_tx: Option<mpsc::UnboundedSender<Fill>>,
    updates_tx: Option<mpsc::UnboundedSender<OrderUpdate>>,
}

impl OkxOrderExecutor {
    pub fn new(api_key: String, api_secret: String, passphrase: String) -> Self {
        let rest_client =
            OkxRestClient::new("https://www.okx.com", &api_key, &api_secret, &passphrase);
        Self {
            rest_client,
            fills_tx: None,
            updates_tx: None,
        }
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
        params.insert("instId".to_string(), inst_id);
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

        let request = RestRequest {
            method: HttpMethod::Post,
            path: "/api/v5/trade/order".to_string(),
            params,
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
        let order_id = json["data"][0]["ordId"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        tracing::info!(
            order_id,
            side = ?order.side,
            qty = %order.quantity,
            "okx order submitted"
        );

        Ok(order_id)
    }

    async fn cancel_order(&self, order_id: &str) -> anyhow::Result<bool> {
        tracing::info!(order_id, "okx cancel_order (stub - needs instId)");
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
        };
        assert_eq!(OkxOrderExecutor::td_mode(&inst), "cross");
    }
}
