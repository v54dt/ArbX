use std::collections::HashMap;

use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use super::market_data::BinanceMarket;
use super::rest_client::BinanceRestClient;
use crate::adapters::order_executor::{OrderExecutor, OrderReceivers};
use crate::adapters::rest_client::{ExchangeRestClient, HttpMethod, RestRequest};
use crate::models::enums::{OrderStatus, OrderType, Side, TimeInForce};
use crate::models::instrument::Instrument;
use crate::models::order::{Fill, Order, OrderUpdate};

pub struct BinanceOrderExecutor {
    market: BinanceMarket,
    rest_client: BinanceRestClient,
    fills_tx: Option<mpsc::UnboundedSender<Fill>>,
    updates_tx: Option<mpsc::UnboundedSender<OrderUpdate>>,
}

impl BinanceOrderExecutor {
    pub fn new(market: BinanceMarket, api_key: String, api_secret: String) -> anyhow::Result<Self> {
        let base_url = match market {
            BinanceMarket::Spot => "https://api.binance.com",
            BinanceMarket::UsdtFutures => "https://fapi.binance.com",
            BinanceMarket::CoinFutures => "https://dapi.binance.com",
        };

        let rest_client = BinanceRestClient::new(base_url, &api_key, &api_secret)?;

        Ok(Self {
            market,
            rest_client,
            fills_tx: None,
            updates_tx: None,
        })
    }

    fn order_path(&self) -> &'static str {
        match self.market {
            BinanceMarket::Spot => "/api/v3/order",
            BinanceMarket::UsdtFutures => "/fapi/v1/order",
            BinanceMarket::CoinFutures => "/dapi/v1/order",
        }
    }

    fn market_label(&self) -> &'static str {
        match self.market {
            BinanceMarket::Spot => "spot",
            BinanceMarket::UsdtFutures => "usdt-futures",
            BinanceMarket::CoinFutures => "coin-futures",
        }
    }

    fn instrument_to_symbol(instrument: &Instrument) -> String {
        format!("{}{}", instrument.base, instrument.quote).to_uppercase()
    }

    fn side_str(side: Side) -> &'static str {
        match side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }
    }

    fn order_type_str(ot: OrderType) -> &'static str {
        match ot {
            OrderType::Limit => "LIMIT",
            OrderType::Market => "MARKET",
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
impl OrderExecutor for BinanceOrderExecutor {
    async fn connect(&mut self) -> anyhow::Result<OrderReceivers> {
        let (fills_tx, fills) = mpsc::unbounded_channel::<Fill>();
        let (updates_tx, updates) = mpsc::unbounded_channel::<OrderUpdate>();
        self.fills_tx = Some(fills_tx);
        self.updates_tx = Some(updates_tx);
        tracing::info!(
            market = self.market_label(),
            "binance executor connected (REST)"
        );
        Ok(OrderReceivers { fills, updates })
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.fills_tx = None;
        self.updates_tx = None;
        tracing::info!("binance executor disconnected");
        Ok(())
    }

    async fn submit_order(&self, order: &Order) -> anyhow::Result<String> {
        let symbol = Self::instrument_to_symbol(&order.instrument);

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol);
        params.insert("side".to_string(), Self::side_str(order.side).to_string());
        params.insert(
            "type".to_string(),
            Self::order_type_str(order.order_type).to_string(),
        );
        params.insert("quantity".to_string(), order.quantity.to_string());
        params.insert("newOrderRespType".to_string(), "RESULT".to_string());

        if order.order_type == OrderType::Limit {
            params.insert(
                "timeInForce".to_string(),
                Self::tif_to_string(order.time_in_force).to_string(),
            );
            if let Some(price) = order.price {
                params.insert("price".to_string(), price.to_string());
            }
        }

        let request = RestRequest {
            method: HttpMethod::Post,
            path: self.order_path().to_string(),
            params,
        };

        let response = self.rest_client.send(request).await?;

        if response.status < 200 || response.status >= 300 {
            anyhow::bail!(
                "binance order rejected ({}): {}",
                response.status,
                response.body
            );
        }

        let json: serde_json::Value = serde_json::from_str(&response.body)?;
        let order_id = json["orderId"]
            .as_u64()
            .map(|id| id.to_string())
            .or_else(|| json["orderId"].as_str().map(|s| s.to_string()));
        let order_id = match order_id {
            Some(id) if !id.is_empty() => id,
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
        // TODO: need symbol for cancel, consider storing order->symbol mapping
        tracing::info!(order_id, "cancel_order (stub — needs symbol)");
        Ok(true)
    }

    async fn get_order_status(&self, order_id: &str) -> anyhow::Result<OrderUpdate> {
        // TODO: need symbol for status query, consider storing order->symbol mapping
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

    fn make_instrument(base: &str, quote: &str) -> Instrument {
        Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: InstrumentType::Spot,
            base: base.to_string(),
            quote: quote.to_string(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        }
    }

    fn make_executor(market: BinanceMarket) -> BinanceOrderExecutor {
        BinanceOrderExecutor::new(market, "key".into(), "secret".into()).unwrap()
    }

    #[test]
    fn instrument_to_symbol_formats_correctly() {
        let inst = make_instrument("BTC", "USDT");
        assert_eq!(BinanceOrderExecutor::instrument_to_symbol(&inst), "BTCUSDT");

        let inst2 = make_instrument("eth", "btc");
        assert_eq!(BinanceOrderExecutor::instrument_to_symbol(&inst2), "ETHBTC");
    }

    #[test]
    fn tif_to_string_maps_correctly() {
        assert_eq!(
            BinanceOrderExecutor::tif_to_string(Some(TimeInForce::Ioc)),
            "IOC"
        );
        assert_eq!(
            BinanceOrderExecutor::tif_to_string(Some(TimeInForce::Fok)),
            "FOK"
        );
        assert_eq!(
            BinanceOrderExecutor::tif_to_string(Some(TimeInForce::Rod)),
            "GTC"
        );
        assert_eq!(BinanceOrderExecutor::tif_to_string(None), "GTC");
    }

    #[test]
    fn side_str_maps_correctly() {
        assert_eq!(BinanceOrderExecutor::side_str(Side::Buy), "BUY");
        assert_eq!(BinanceOrderExecutor::side_str(Side::Sell), "SELL");
    }

    #[test]
    fn order_type_str_maps_correctly() {
        assert_eq!(
            BinanceOrderExecutor::order_type_str(OrderType::Limit),
            "LIMIT"
        );
        assert_eq!(
            BinanceOrderExecutor::order_type_str(OrderType::Market),
            "MARKET"
        );
    }

    #[test]
    fn order_path_matches_market() {
        assert_eq!(
            make_executor(BinanceMarket::Spot).order_path(),
            "/api/v3/order"
        );
        assert_eq!(
            make_executor(BinanceMarket::UsdtFutures).order_path(),
            "/fapi/v1/order"
        );
        assert_eq!(
            make_executor(BinanceMarket::CoinFutures).order_path(),
            "/dapi/v1/order"
        );
    }

    #[test]
    fn new_sets_correct_base_url() {
        assert_eq!(
            BinanceMarket::Spot.rest_base_url(),
            "https://api.binance.com"
        );
        assert_eq!(
            BinanceMarket::UsdtFutures.rest_base_url(),
            "https://fapi.binance.com"
        );
        assert_eq!(
            BinanceMarket::CoinFutures.rest_base_url(),
            "https://dapi.binance.com"
        );
    }
}
