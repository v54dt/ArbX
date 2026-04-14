use async_trait::async_trait;
use chrono::Utc;
use futures_util::StreamExt;
use hmac::{Hmac, Mac};
use rust_decimal::Decimal;
use sha2::Sha256;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tracing::{info, warn};

use crate::adapters::private_stream::{PrivateStream, PrivateStreamReceivers};
use crate::models::enums::{OrderStatus, Side, Venue};
use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
use crate::models::order::{Fill, OrderUpdate};

const WS_URL: &str = "wss://stream.bybit.com/v5/private";

pub struct BybitPrivateStream {
    api_key: String,
    api_secret: String,
    ws_task: Option<JoinHandle<()>>,
}

impl BybitPrivateStream {
    pub fn new(api_key: &str, api_secret: &str) -> Self {
        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            ws_task: None,
        }
    }

    fn build_auth_msg(api_key: &str, secret: &str) -> String {
        let expires = Utc::now().timestamp_millis() + 5000;
        let msg = format!("GET/realtime{}", expires);
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
            .expect("HMAC can take any key length");
        mac.update(msg.as_bytes());
        let sign = hex::encode(mac.finalize().into_bytes());
        serde_json::json!({
            "op": "auth",
            "args": [api_key, expires, sign]
        })
        .to_string()
    }

    fn parse_category(category: &str) -> InstrumentType {
        match category {
            "linear" | "inverse" => InstrumentType::Swap,
            _ => InstrumentType::Spot,
        }
    }

    /// Split a concatenated symbol like "BTCUSDT" into (base, quote).
    /// Tries quote currencies in order: USDT, USDC, BTC.
    fn split_symbol(symbol: &str) -> (String, String) {
        for quote in ["USDT", "USDC", "BTC"] {
            if let Some(base) = symbol.strip_suffix(quote)
                && !base.is_empty()
            {
                return (base.to_string(), quote.to_string());
            }
        }
        // Fallback: assume last 3 chars are quote
        let (base, quote) = symbol.split_at(symbol.len().saturating_sub(3));
        (base.to_string(), quote.to_string())
    }

    fn parse_side(side: &str) -> Option<Side> {
        match side {
            "Buy" => Some(Side::Buy),
            "Sell" => Some(Side::Sell),
            _ => None,
        }
    }

    fn parse_status(status: &str) -> OrderStatus {
        match status {
            "New" => OrderStatus::Submitted,
            "PartiallyFilled" => OrderStatus::PartiallyFilled,
            "Filled" => OrderStatus::Filled,
            "Cancelled" => OrderStatus::Cancelled,
            "Rejected" => OrderStatus::Rejected,
            _ => OrderStatus::Pending,
        }
    }

    fn parse_execution(data: &serde_json::Value) -> Option<(Option<Fill>, OrderUpdate)> {
        let order_id = data.get("orderId")?.as_str()?.to_string();
        let symbol = data.get("symbol")?.as_str()?;
        let side_str = data.get("side")?.as_str()?;
        let exec_qty_str = data.get("execQty")?.as_str().unwrap_or("0");
        let exec_price_str = data.get("execPrice")?.as_str().unwrap_or("0");
        let order_status_str = data.get("orderStatus")?.as_str().unwrap_or("New");
        let exec_time_str = data.get("execTime")?.as_str().unwrap_or("0");
        let category = data.get("category")?.as_str().unwrap_or("spot");
        let cum_exec_qty_str = data.get("cumExecQty")?.as_str().unwrap_or("0");
        let leaves_qty_str = data.get("leavesQty")?.as_str().unwrap_or("0");

        let side = Self::parse_side(side_str)?;
        let (base, quote) = Self::split_symbol(symbol);
        let instrument_type = Self::parse_category(category);
        let settle_currency = match instrument_type {
            InstrumentType::Spot => None,
            _ => Some(quote.clone()),
        };
        let instrument = Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type,
            base,
            quote,
            settle_currency,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        };

        let status = Self::parse_status(order_status_str);
        let exec_qty: Decimal = exec_qty_str.parse().ok()?;
        let exec_price: Decimal = exec_price_str.parse().unwrap_or_default();
        let cum_exec_qty: Decimal = cum_exec_qty_str.parse().unwrap_or_default();
        let leaves_qty: Decimal = leaves_qty_str.parse().unwrap_or_default();

        let ts_ms: i64 = exec_time_str.parse().unwrap_or(0);
        let filled_at = chrono::DateTime::from_timestamp_millis(ts_ms).unwrap_or_else(Utc::now);

        let fill = if exec_qty > Decimal::ZERO {
            Some(Fill {
                order_id: order_id.clone(),
                venue: Venue::Bybit,
                instrument: instrument.clone(),
                side,
                price: exec_price,
                quantity: exec_qty,
                fee: Decimal::ZERO,
                fee_currency: String::new(),
                filled_at,
            })
        } else {
            None
        };

        let order_update = OrderUpdate {
            order_id,
            status,
            filled_quantity: cum_exec_qty,
            remaining_quantity: leaves_qty,
            average_price: None,
            updated_at: filled_at,
        };

        Some((fill, order_update))
    }
}

#[async_trait]
impl PrivateStream for BybitPrivateStream {
    async fn connect(&mut self) -> anyhow::Result<PrivateStreamReceivers> {
        let (fill_tx, fill_rx) = mpsc::unbounded_channel();
        let (order_tx, order_rx) = mpsc::unbounded_channel();

        let api_key = self.api_key.clone();
        let api_secret = self.api_secret.clone();

        let ws_task = tokio::spawn(async move {
            let mut backoff = std::time::Duration::from_secs(1);
            let max_backoff = std::time::Duration::from_secs(60);
            let mut first_connect = true;
            loop {
                match run_bybit_stream(&api_key, &api_secret, &fill_tx, &order_tx, first_connect)
                    .await
                {
                    Ok(()) => {
                        info!("Bybit private stream ended cleanly, exiting reconnect loop");
                        crate::metrics::set_ws_private_connected("bybit", false);
                        break;
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            backoff_ms = backoff.as_millis() as u64,
                            "Bybit private WS disconnected, reconnecting"
                        );
                        crate::metrics::set_ws_private_connected("bybit", false);
                    }
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
                first_connect = false;
            }
        });
        self.ws_task = Some(ws_task);

        Ok(PrivateStreamReceivers {
            fills: fill_rx,
            order_updates: order_rx,
        })
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if let Some(task) = self.ws_task.take() {
            task.abort();
        }
        info!("disconnected from Bybit private WebSocket");
        Ok(())
    }
}

async fn run_bybit_stream(
    api_key: &str,
    api_secret: &str,
    fill_tx: &mpsc::UnboundedSender<Fill>,
    order_tx: &mpsc::UnboundedSender<OrderUpdate>,
    first_connect: bool,
) -> anyhow::Result<()> {
    use futures_util::SinkExt;

    let (ws_stream, _) = connect_async(WS_URL).await?;
    info!(url = WS_URL, "connected to Bybit private WebSocket");
    if !first_connect {
        crate::metrics::record_ws_private_reconnect("bybit");
    }
    crate::metrics::set_ws_private_connected("bybit", true);

    let auth_msg = BybitPrivateStream::build_auth_msg(api_key, api_secret);
    let subscribe_msg = serde_json::json!({"op": "subscribe", "args": ["execution"]}).to_string();

    let (mut write, mut read) = ws_stream.split();
    write
        .send(tokio_tungstenite::tungstenite::Message::Text(
            auth_msg.into(),
        ))
        .await?;

    let mut subscribed = false;
    let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(20));
    ping_interval.tick().await;

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                let ping = tokio_tungstenite::tungstenite::Message::Text(
                    r#"{"op":"ping"}"#.into(),
                );
                write.send(ping).await?;
            }
            maybe_msg = read.next() => {
                let Some(msg) = maybe_msg else {
                    anyhow::bail!("stream ended");
                };
                match msg {
                    Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                        crate::metrics::record_ws_private_message("bybit");
                        let text = text.to_string();
                        let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) else {
                            continue;
                        };

                        if !subscribed {
                            let op = json.get("op").and_then(|v| v.as_str());
                            let success = json
                                .get("success")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);
                            if op == Some("auth") && success {
                                write
                                    .send(tokio_tungstenite::tungstenite::Message::Text(
                                        subscribe_msg.clone().into(),
                                    ))
                                    .await?;
                                subscribed = true;
                                info!(
                                    "Bybit private stream: authenticated and subscribed to execution"
                                );
                            }
                            continue;
                        }

                        let topic = json.get("topic").and_then(|v| v.as_str());
                        if topic != Some("execution") {
                            continue;
                        }
                        let Some(data_arr) = json.get("data").and_then(|v| v.as_array()) else {
                            continue;
                        };
                        for item in data_arr {
                            if let Some((fill_opt, update)) =
                                BybitPrivateStream::parse_execution(item)
                            {
                                if let Some(fill) = fill_opt {
                                    let _ = fill_tx.send(fill);
                                }
                                let _ = order_tx.send(update);
                            }
                        }
                    }
                    Err(e) => anyhow::bail!("WS error: {}", e),
                    _ => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn parse_bybit_execution_event() {
        let data = serde_json::json!({
            "orderId": "ord-bybit-001",
            "symbol": "BTCUSDT",
            "side": "Buy",
            "execQty": "0.01",
            "execPrice": "50000.00",
            "orderStatus": "Filled",
            "execTime": "1620000000000",
            "category": "spot",
            "cumExecQty": "0.01",
            "leavesQty": "0"
        });
        let result = BybitPrivateStream::parse_execution(&data);
        assert!(result.is_some());
        let (fill_opt, update) = result.unwrap();
        let fill = fill_opt.expect("expected a fill");
        assert_eq!(fill.order_id, "ord-bybit-001");
        assert_eq!(fill.venue, Venue::Bybit);
        assert_eq!(fill.instrument.base, "BTC");
        assert_eq!(fill.instrument.quote, "USDT");
        assert_eq!(fill.instrument.instrument_type, InstrumentType::Spot);
        assert_eq!(fill.side, Side::Buy);
        assert_eq!(fill.price, dec!(50000.00));
        assert_eq!(fill.quantity, dec!(0.01));
        assert_eq!(update.status, OrderStatus::Filled);
        assert!(update.remaining_quantity.is_zero());
    }

    #[test]
    fn parse_bybit_order_status() {
        assert_eq!(
            BybitPrivateStream::parse_status("New"),
            OrderStatus::Submitted
        );
        assert_eq!(
            BybitPrivateStream::parse_status("PartiallyFilled"),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(
            BybitPrivateStream::parse_status("Filled"),
            OrderStatus::Filled
        );
        assert_eq!(
            BybitPrivateStream::parse_status("Cancelled"),
            OrderStatus::Cancelled
        );
        assert_eq!(
            BybitPrivateStream::parse_status("Rejected"),
            OrderStatus::Rejected
        );
        assert_eq!(
            BybitPrivateStream::parse_status("unknown"),
            OrderStatus::Pending
        );
    }

    #[test]
    fn parse_bybit_side() {
        assert_eq!(BybitPrivateStream::parse_side("Buy"), Some(Side::Buy));
        assert_eq!(BybitPrivateStream::parse_side("Sell"), Some(Side::Sell));
        assert_eq!(BybitPrivateStream::parse_side("buy"), None);
        assert_eq!(BybitPrivateStream::parse_side("SELL"), None);
    }

    #[test]
    fn symbol_to_instrument_btcusdt() {
        let (base, quote) = BybitPrivateStream::split_symbol("BTCUSDT");
        assert_eq!(base, "BTC");
        assert_eq!(quote, "USDT");
    }

    #[test]
    fn parse_bybit_execution_with_partial_fill() {
        let data = serde_json::json!({
            "orderId": "ord-partial-005",
            "symbol": "ETHUSDT",
            "side": "Sell",
            "execQty": "1.0",
            "execPrice": "3000.00",
            "orderStatus": "PartiallyFilled",
            "execTime": "1620000003000",
            "category": "linear",
            "cumExecQty": "1.0",
            "leavesQty": "1.0"
        });
        let result = BybitPrivateStream::parse_execution(&data);
        assert!(result.is_some());
        let (fill_opt, update) = result.unwrap();
        let fill = fill_opt.expect("expected a fill for partial");
        assert_eq!(fill.instrument.base, "ETH");
        assert_eq!(fill.instrument.instrument_type, InstrumentType::Swap);
        assert_eq!(fill.instrument.settle_currency, Some("USDT".to_string()));
        assert_eq!(fill.side, Side::Sell);
        assert_eq!(fill.quantity, dec!(1.0));
        assert_eq!(update.status, OrderStatus::PartiallyFilled);
        assert_eq!(update.remaining_quantity, dec!(1.0));
    }
}
