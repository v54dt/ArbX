use std::collections::HashMap;

use async_trait::async_trait;
use futures_util::StreamExt;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tracing::{error, info, warn};

use crate::adapters::binance::market_data::BinanceMarket;
use crate::adapters::binance::rest_client::BinanceRestClient;
use crate::adapters::private_stream::{PrivateStream, PrivateStreamReceivers};
use crate::adapters::rest_client::{ExchangeRestClient, HttpMethod, RestRequest};
use crate::models::enums::{OrderStatus, Side, Venue};
use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
use crate::models::order::{Fill, OrderUpdate};

pub struct BinancePrivateStream {
    market: BinanceMarket,
    rest_client: BinanceRestClient,
    rest_base_url: String,
    api_key: String,
    api_secret: String,
    listen_key: Option<String>,
    ws_base_url: String,
    ws_task: Option<JoinHandle<()>>,
    keepalive_task: Option<JoinHandle<()>>,
}

impl BinancePrivateStream {
    pub fn new(
        market: BinanceMarket,
        rest_base_url: &str,
        api_key: &str,
        api_secret: &str,
    ) -> anyhow::Result<Self> {
        let ws_base_url = match &market {
            BinanceMarket::Spot => "wss://stream.binance.com:9443/ws/".to_string(),
            BinanceMarket::UsdtFutures => "wss://fstream.binance.com/ws/".to_string(),
            BinanceMarket::CoinFutures => "wss://dstream.binance.com/ws/".to_string(),
        };
        let rest_client = BinanceRestClient::new(rest_base_url, api_key, api_secret)?;
        Ok(Self {
            market,
            rest_client,
            rest_base_url: rest_base_url.to_string(),
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            listen_key: None,
            ws_base_url,
            ws_task: None,
            keepalive_task: None,
        })
    }

    fn listen_key_path(&self) -> &'static str {
        match self.market {
            BinanceMarket::Spot => "/api/v3/userDataStream",
            BinanceMarket::UsdtFutures | BinanceMarket::CoinFutures => "/fapi/v1/listenKey",
        }
    }

    async fn create_listen_key(&self) -> anyhow::Result<String> {
        let request = RestRequest {
            method: HttpMethod::Post,
            path: self.listen_key_path().to_string(),
            params: HashMap::new(),
        };
        let response = self.rest_client.send(request).await?;
        if response.status != 200 {
            anyhow::bail!(
                "failed to create listenKey: status={} body={}",
                response.status,
                response.body
            );
        }
        let json: serde_json::Value = serde_json::from_str(&response.body)?;
        let key = json["listenKey"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing listenKey in response"))?
            .to_string();
        Ok(key)
    }

    fn parse_execution_report(msg: &serde_json::Value) -> Option<(Fill, OrderUpdate)> {
        if msg.get("e")?.as_str()? != "executionReport" {
            return None;
        }

        let symbol = msg.get("s")?.as_str()?;
        let side = match msg.get("S")?.as_str()? {
            "BUY" => Side::Buy,
            "SELL" => Side::Sell,
            _ => return None,
        };
        let status = match msg.get("X")?.as_str()? {
            "NEW" => OrderStatus::Submitted,
            "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
            "FILLED" => OrderStatus::Filled,
            "CANCELED" => OrderStatus::Cancelled,
            "REJECTED" => OrderStatus::Rejected,
            _ => OrderStatus::Pending,
        };

        let last_qty: Decimal = msg.get("l")?.as_str()?.parse().ok()?;
        let last_price: Decimal = msg.get("L")?.as_str()?.parse().ok()?;
        let commission: Decimal = msg.get("n")?.as_str()?.parse().ok()?;
        let commission_asset = msg.get("N")?.as_str().unwrap_or("").to_string();
        let order_id = msg.get("i")?.as_u64()?.to_string();
        let cum_qty: Decimal = msg.get("z")?.as_str()?.parse().ok()?;
        let cum_quote: Decimal = msg.get("Z")?.as_str()?.parse().ok()?;
        let orig_qty: Decimal = msg.get("q")?.as_str()?.parse().ok()?;

        let avg_price = if !cum_qty.is_zero() {
            Some(cum_quote / cum_qty)
        } else {
            None
        };

        let instrument = Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: InstrumentType::Spot,
            base: symbol.to_string(),
            quote: String::new(),
            settle_currency: None,
            expiry: None,
        };

        let fill = Fill {
            order_id: order_id.clone(),
            venue: Venue::Binance,
            instrument: instrument.clone(),
            side,
            price: last_price,
            quantity: last_qty,
            fee: commission,
            fee_currency: commission_asset,
            filled_at: chrono::Utc::now(),
        };

        let order_update = OrderUpdate {
            order_id,
            status,
            filled_quantity: cum_qty,
            remaining_quantity: orig_qty - cum_qty,
            average_price: avg_price,
            updated_at: chrono::Utc::now(),
        };

        Some((fill, order_update))
    }
}

#[async_trait]
impl PrivateStream for BinancePrivateStream {
    async fn connect(&mut self) -> anyhow::Result<PrivateStreamReceivers> {
        let listen_key = self.create_listen_key().await?;
        let url = format!("{}{}", self.ws_base_url, listen_key);

        let (ws_stream, _) = connect_async(&url).await?;
        info!(url = url.as_str(), "connected to Binance private WebSocket");

        self.listen_key = Some(listen_key.clone());

        let (fill_tx, fill_rx) = mpsc::unbounded_channel();
        let (order_tx, order_rx) = mpsc::unbounded_channel();

        let (_, mut read) = ws_stream.split();

        let ws_task = tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                        let text = text.to_string();
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text)
                            && let Some((fill, update)) =
                                BinancePrivateStream::parse_execution_report(&json)
                        {
                            let _ = fill_tx.send(fill);
                            let _ = order_tx.send(update);
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Binance private WebSocket error");
                        break;
                    }
                    _ => {}
                }
            }
        });
        self.ws_task = Some(ws_task);

        let keepalive_path = self.listen_key_path().to_string();
        let keepalive_key = listen_key;
        let keepalive_rest =
            BinanceRestClient::new(&self.rest_base_url, &self.api_key, &self.api_secret)?;
        let keepalive_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(25 * 60));
            interval.tick().await;
            loop {
                interval.tick().await;
                let mut params = HashMap::new();
                params.insert("listenKey".to_string(), keepalive_key.clone());
                let request = RestRequest {
                    method: HttpMethod::Post,
                    path: keepalive_path.clone(),
                    params,
                };
                if let Err(e) = keepalive_rest.send(request).await {
                    warn!(error = %e, "listenKey keepalive failed");
                }
            }
        });
        self.keepalive_task = Some(keepalive_task);

        Ok(PrivateStreamReceivers {
            fills: fill_rx,
            order_updates: order_rx,
        })
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if let Some(task) = self.ws_task.take() {
            task.abort();
        }
        if let Some(task) = self.keepalive_task.take() {
            task.abort();
        }
        self.listen_key = None;
        info!("disconnected from Binance private WebSocket");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_execution_report_valid() {
        let msg = serde_json::json!({
            "e": "executionReport",
            "s": "BTCUSDT",
            "S": "BUY",
            "X": "FILLED",
            "l": "0.001",
            "L": "50000.00",
            "n": "0.01",
            "N": "USDT",
            "i": 12345,
            "z": "0.001",
            "Z": "50.00",
            "q": "0.001"
        });
        let result = BinancePrivateStream::parse_execution_report(&msg);
        assert!(result.is_some());
        let (fill, update) = result.unwrap();
        assert_eq!(fill.order_id, "12345");
        assert_eq!(fill.side, Side::Buy);
        assert_eq!(fill.price, Decimal::new(5000000, 2));
        assert_eq!(fill.quantity, Decimal::new(1, 3));
        assert_eq!(fill.fee, Decimal::new(1, 2));
        assert_eq!(fill.fee_currency, "USDT");
        assert_eq!(update.status, OrderStatus::Filled);
        assert_eq!(update.filled_quantity, Decimal::new(1, 3));
        assert!(update.remaining_quantity.is_zero());
    }

    #[test]
    fn parse_execution_report_wrong_event() {
        let msg = serde_json::json!({
            "e": "outboundAccountPosition",
            "s": "BTCUSDT"
        });
        assert!(BinancePrivateStream::parse_execution_report(&msg).is_none());
    }

    #[test]
    fn parse_execution_report_partial_fill() {
        let msg = serde_json::json!({
            "e": "executionReport",
            "s": "ETHUSDT",
            "S": "SELL",
            "X": "PARTIALLY_FILLED",
            "l": "0.5",
            "L": "3000.00",
            "n": "0.005",
            "N": "ETH",
            "i": 99999,
            "z": "0.5",
            "Z": "1500.00",
            "q": "1.0"
        });
        let result = BinancePrivateStream::parse_execution_report(&msg);
        assert!(result.is_some());
        let (fill, update) = result.unwrap();
        assert_eq!(fill.side, Side::Sell);
        assert_eq!(update.status, OrderStatus::PartiallyFilled);
        assert_eq!(update.remaining_quantity, Decimal::new(5, 1));
    }
}
