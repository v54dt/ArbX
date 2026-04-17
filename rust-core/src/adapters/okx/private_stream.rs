use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use futures_util::StreamExt;
use hmac::{Hmac, Mac};
use rust_decimal::Decimal;
use sha2::Sha256;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tracing::{info, warn};

use std::collections::HashMap;

use crate::adapters::okx::rest_client::OkxRestClient;
use crate::adapters::private_stream::{PrivateStream, PrivateStreamReceivers};
use crate::adapters::rest_client::{ExchangeRestClient, HttpMethod, RestRequest};
use crate::models::enums::{OrderStatus, Side, Venue};
use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
use crate::models::order::{Fill, OrderUpdate};

const WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/private";

pub struct OkxPrivateStream {
    api_key: String,
    api_secret: String,
    passphrase: String,
    ws_task: Option<JoinHandle<()>>,
}

impl OkxPrivateStream {
    pub fn new(api_key: &str, api_secret: &str, passphrase: &str) -> Self {
        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            passphrase: passphrase.to_string(),
            ws_task: None,
        }
    }

    fn build_sign(secret: &str, timestamp: &str) -> String {
        let msg = format!("{}GET/users/self/verify", timestamp);
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
            .expect("HMAC can take any key length");
        mac.update(msg.as_bytes());
        base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
    }

    fn build_login_msg(api_key: &str, secret: &str, passphrase: &str) -> String {
        let timestamp = Utc::now().timestamp().to_string();
        let sign = Self::build_sign(secret, &timestamp);
        serde_json::json!({
            "op": "login",
            "args": [{
                "apiKey": api_key,
                "passphrase": passphrase,
                "timestamp": timestamp,
                "sign": sign,
            }]
        })
        .to_string()
    }

    fn parse_inst_type(inst_type: &str) -> InstrumentType {
        match inst_type {
            "SWAP" => InstrumentType::Swap,
            "FUTURES" => InstrumentType::Futures,
            _ => InstrumentType::Spot,
        }
    }

    fn parse_instrument(inst_id: &str, inst_type: &str) -> Option<Instrument> {
        let parts: Vec<&str> = inst_id.split('-').collect();
        if parts.len() < 2 {
            return None;
        }
        let base = parts[0].to_string();
        let quote = parts[1].to_string();
        let instrument_type = Self::parse_inst_type(inst_type);
        let settle_currency = match instrument_type {
            InstrumentType::Spot => None,
            _ => Some(quote.clone()),
        };
        Some(Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type,
            base,
            quote,
            settle_currency,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        })
    }

    fn parse_side(side: &str) -> Option<Side> {
        match side {
            "buy" => Some(Side::Buy),
            "sell" => Some(Side::Sell),
            _ => None,
        }
    }

    fn parse_status(state: &str) -> OrderStatus {
        match state {
            "partially_filled" => OrderStatus::PartiallyFilled,
            "filled" => OrderStatus::Filled,
            "canceled" => OrderStatus::Cancelled,
            "live" => OrderStatus::Submitted,
            _ => OrderStatus::Pending,
        }
    }

    fn parse_order_event(data: &serde_json::Value) -> Option<(Option<Fill>, OrderUpdate)> {
        let ord_id = data.get("ordId")?.as_str()?.to_string();
        let inst_id = data.get("instId")?.as_str()?;
        let inst_type = data.get("instType")?.as_str().unwrap_or("SPOT");
        let side_str = data.get("side")?.as_str()?;
        let state = data.get("state")?.as_str()?;
        let fill_sz_str = data.get("fillSz")?.as_str().unwrap_or("0");
        let fill_px_str = data.get("fillPx")?.as_str().unwrap_or("0");
        let acc_fill_sz_str = data.get("accFillSz")?.as_str().unwrap_or("0");
        let ts_str = data.get("ts")?.as_str().unwrap_or("0");
        let orig_sz_str = data.get("sz")?.as_str().unwrap_or("0");
        let fee_str = data.get("fee").and_then(|v| v.as_str()).unwrap_or("0");
        let fee_currency = data
            .get("feeCcy")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let side = Self::parse_side(side_str)?;
        let instrument = Self::parse_instrument(inst_id, inst_type)?;
        let status = Self::parse_status(state);

        let fill_sz: Decimal = fill_sz_str.parse().ok()?;
        let fill_px: Decimal = fill_px_str.parse().unwrap_or_default();
        let acc_fill_sz: Decimal = acc_fill_sz_str.parse().unwrap_or_default();
        let orig_sz: Decimal = orig_sz_str.parse().unwrap_or_default();

        let ts_ms: i64 = ts_str.parse().unwrap_or(0);
        let filled_at = chrono::DateTime::from_timestamp_millis(ts_ms).unwrap_or_else(Utc::now);

        // OKX convention: fee is negative when the user pays, positive for rebates.
        // We store it as-is so the downstream knows the sign.
        let fee: Decimal = fee_str.parse().unwrap_or_default();
        let fill = if fill_sz > Decimal::ZERO {
            Some(Fill {
                order_id: ord_id.clone(),
                venue: Venue::Okx,
                instrument: instrument.clone(),
                side,
                price: fill_px,
                quantity: fill_sz,
                fee,
                fee_currency,
                filled_at,
            })
        } else {
            None
        };

        let order_update = OrderUpdate {
            order_id: ord_id,
            status,
            filled_quantity: acc_fill_sz,
            remaining_quantity: orig_sz - acc_fill_sz,
            average_price: None,
            updated_at: filled_at,
        };

        Some((fill, order_update))
    }
}

#[async_trait]
impl PrivateStream for OkxPrivateStream {
    async fn connect(&mut self) -> anyhow::Result<PrivateStreamReceivers> {
        let (fill_tx, fill_rx) = mpsc::unbounded_channel();
        let (order_tx, order_rx) = mpsc::unbounded_channel();

        let api_key = self.api_key.clone();
        let api_secret = self.api_secret.clone();
        let passphrase = self.passphrase.clone();

        let ws_task = tokio::spawn(async move {
            let mut backoff = std::time::Duration::from_secs(1);
            let max_backoff = std::time::Duration::from_secs(60);
            let mut first_connect = true;
            loop {
                match run_okx_stream(
                    &api_key,
                    &api_secret,
                    &passphrase,
                    &fill_tx,
                    &order_tx,
                    first_connect,
                )
                .await
                {
                    Ok(()) => {
                        info!("OKX private stream ended cleanly, exiting reconnect loop");
                        crate::metrics::set_ws_private_connected("okx", false);
                        break;
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            backoff_ms = backoff.as_millis() as u64,
                            "OKX private WS disconnected, reconnecting"
                        );
                        crate::metrics::set_ws_private_connected("okx", false);
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
        info!("disconnected from OKX private WebSocket");
        Ok(())
    }
}

async fn run_okx_stream(
    api_key: &str,
    api_secret: &str,
    passphrase: &str,
    fill_tx: &mpsc::UnboundedSender<Fill>,
    order_tx: &mpsc::UnboundedSender<OrderUpdate>,
    first_connect: bool,
) -> anyhow::Result<()> {
    use futures_util::SinkExt;

    // Startup reconciliation: query pending orders and cancel each (D-1A port).
    let rest = OkxRestClient::new("https://www.okx.com", api_key, api_secret, passphrase)?;
    let pending_req = RestRequest {
        method: HttpMethod::Get,
        path: "/api/v5/trade/orders-pending".to_string(),
        params: HashMap::new(),
    };
    match rest.send(pending_req).await {
        Ok(resp) if resp.status == 200 => {
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&resp.body)
                && let Some(orders) = json["data"].as_array()
            {
                info!(count = orders.len(), "OKX startup: pending orders found");
                for order in orders {
                    let inst_id = order["instId"].as_str().unwrap_or("");
                    let ord_id = order["ordId"].as_str().unwrap_or("");
                    if inst_id.is_empty() || ord_id.is_empty() {
                        continue;
                    }
                    let mut params = HashMap::new();
                    params.insert("instId".to_string(), inst_id.to_string());
                    params.insert("ordId".to_string(), ord_id.to_string());
                    let cancel_req = RestRequest {
                        method: HttpMethod::Post,
                        path: "/api/v5/trade/cancel-order".to_string(),
                        params,
                    };
                    match rest.send(cancel_req).await {
                        Ok(r) if (200..300).contains(&r.status) => {
                            info!(ord_id, inst_id, "OKX startup: cancelled order");
                        }
                        Ok(r) => {
                            warn!(ord_id, inst_id, status = r.status, "OKX cancel rejected");
                        }
                        Err(e) => {
                            warn!(ord_id, inst_id, error = %e, "OKX cancel failed");
                        }
                    }
                }
            }
        }
        Ok(resp) => {
            warn!(
                status = resp.status,
                "OKX startup: orders-pending query non-200"
            );
        }
        Err(e) => warn!(error = %e, "OKX startup: orders-pending query failed"),
    }

    let (ws_stream, _) = connect_async(WS_URL).await?;
    info!(url = WS_URL, "connected to OKX private WebSocket");
    if !first_connect {
        crate::metrics::record_ws_private_reconnect("okx");
    }
    crate::metrics::set_ws_private_connected("okx", true);

    let login_msg = OkxPrivateStream::build_login_msg(api_key, api_secret, passphrase);
    let subscribe_msg = serde_json::json!({
        "op": "subscribe",
        "args": [{"channel": "orders", "instType": "ANY"}]
    })
    .to_string();

    let (mut write, mut read) = ws_stream.split();

    write
        .send(tokio_tungstenite::tungstenite::Message::Text(
            login_msg.into(),
        ))
        .await?;

    let mut subscribed = false;
    let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(25));
    ping_interval.tick().await;

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                let ping = tokio_tungstenite::tungstenite::Message::Text("ping".into());
                write.send(ping).await?;
            }
            maybe_msg = read.next() => {
                let Some(msg) = maybe_msg else {
                    anyhow::bail!("stream ended");
                };
                match msg {
                    Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                        crate::metrics::record_ws_private_message("okx");
                        let text = text.to_string();
                        let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) else {
                            continue;
                        };

                        if !subscribed {
                            let event = json.get("event").and_then(|v| v.as_str());
                            let code = json.get("code").and_then(|v| v.as_str());
                            if event == Some("login") && code == Some("0") {
                                write
                                    .send(tokio_tungstenite::tungstenite::Message::Text(
                                        subscribe_msg.clone().into(),
                                    ))
                                    .await?;
                                subscribed = true;
                                info!("OKX private stream: logged in and subscribed to orders");
                            }
                            continue;
                        }

                        let op = json.get("op").and_then(|v| v.as_str());
                        if op != Some("order") {
                            continue;
                        }
                        let Some(data_arr) = json.get("data").and_then(|v| v.as_array()) else {
                            continue;
                        };
                        for item in data_arr {
                            if let Some((fill_opt, update)) =
                                OkxPrivateStream::parse_order_event(item)
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
    fn parse_okx_fill_event_spot() {
        let data = serde_json::json!({
            "ordId": "ord-spot-001",
            "instId": "BTC-USDT",
            "instType": "SPOT",
            "side": "buy",
            "fillSz": "0.01",
            "fillPx": "50000.00",
            "accFillSz": "0.01",
            "state": "filled",
            "sz": "0.01",
            "ts": "1620000000000",
            "fee": "-0.5",
            "feeCcy": "USDT"
        });
        let result = OkxPrivateStream::parse_order_event(&data);
        assert!(result.is_some());
        let (fill_opt, update) = result.unwrap();
        let fill = fill_opt.expect("expected a fill");
        assert_eq!(fill.order_id, "ord-spot-001");
        assert_eq!(fill.venue, Venue::Okx);
        assert_eq!(fill.instrument.base, "BTC");
        assert_eq!(fill.instrument.quote, "USDT");
        assert_eq!(fill.instrument.instrument_type, InstrumentType::Spot);
        assert_eq!(fill.side, Side::Buy);
        assert_eq!(fill.price, dec!(50000.00));
        assert_eq!(fill.quantity, dec!(0.01));
        // OKX reports fee as negative when user pays — preserve sign.
        assert_eq!(fill.fee, dec!(-0.5));
        assert_eq!(fill.fee_currency, "USDT");
        assert_eq!(update.status, OrderStatus::Filled);
        assert_eq!(update.filled_quantity, dec!(0.01));
        assert!(update.remaining_quantity.is_zero());
    }

    #[test]
    fn parse_okx_fill_missing_fee_defaults_to_zero() {
        let data = serde_json::json!({
            "ordId": "ord-nofee",
            "instId": "BTC-USDT",
            "instType": "SPOT",
            "side": "buy",
            "fillSz": "0.01",
            "fillPx": "50000",
            "accFillSz": "0.01",
            "state": "filled",
            "sz": "0.01",
            "ts": "1620000000000"
        });
        let (fill_opt, _) = OkxPrivateStream::parse_order_event(&data).unwrap();
        let fill = fill_opt.expect("fill");
        assert_eq!(fill.fee, Decimal::ZERO);
        assert_eq!(fill.fee_currency, "");
    }

    #[test]
    fn parse_okx_fill_event_swap() {
        let data = serde_json::json!({
            "ordId": "ord-swap-002",
            "instId": "ETH-USDT",
            "instType": "SWAP",
            "side": "sell",
            "fillSz": "2.5",
            "fillPx": "3000.00",
            "accFillSz": "2.5",
            "state": "partially_filled",
            "sz": "5.0",
            "ts": "1620000001000"
        });
        let result = OkxPrivateStream::parse_order_event(&data);
        assert!(result.is_some());
        let (fill_opt, update) = result.unwrap();
        let fill = fill_opt.expect("expected a fill");
        assert_eq!(fill.instrument.instrument_type, InstrumentType::Swap);
        assert_eq!(fill.instrument.settle_currency, Some("USDT".to_string()));
        assert_eq!(fill.side, Side::Sell);
        assert_eq!(fill.quantity, dec!(2.5));
        assert_eq!(update.status, OrderStatus::PartiallyFilled);
        assert_eq!(update.remaining_quantity, dec!(2.5));
    }

    #[test]
    fn parse_okx_order_update_state_change() {
        // fillSz = "0" → no Fill emitted, but OrderUpdate is
        let data = serde_json::json!({
            "ordId": "ord-cancel-003",
            "instId": "SOL-USDT",
            "instType": "SPOT",
            "side": "buy",
            "fillSz": "0",
            "fillPx": "0",
            "accFillSz": "0",
            "state": "canceled",
            "sz": "10",
            "ts": "1620000002000"
        });
        let result = OkxPrivateStream::parse_order_event(&data);
        assert!(result.is_some());
        let (fill_opt, update) = result.unwrap();
        assert!(fill_opt.is_none(), "no fill expected for zero fillSz");
        assert_eq!(update.status, OrderStatus::Cancelled);
        assert_eq!(update.order_id, "ord-cancel-003");
    }

    #[test]
    fn auth_message_structure() {
        let ts = "1620000000";
        let secret = "test_secret";
        let sign = OkxPrivateStream::build_sign(secret, ts);
        // Sign must be non-empty base64
        assert!(!sign.is_empty());
        // Deterministic — same inputs → same sign
        let sign2 = OkxPrivateStream::build_sign(secret, ts);
        assert_eq!(sign, sign2);
        // Different timestamp → different sign
        let sign3 = OkxPrivateStream::build_sign(secret, "1620000001");
        assert_ne!(sign, sign3);
    }

    #[test]
    fn parse_okx_side() {
        assert_eq!(OkxPrivateStream::parse_side("buy"), Some(Side::Buy));
        assert_eq!(OkxPrivateStream::parse_side("sell"), Some(Side::Sell));
        assert_eq!(OkxPrivateStream::parse_side("BUY"), None);
        assert_eq!(OkxPrivateStream::parse_side("unknown"), None);
    }
}
