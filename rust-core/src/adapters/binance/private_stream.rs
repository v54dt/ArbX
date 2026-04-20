use std::collections::HashMap;

use async_trait::async_trait;
use chrono::Utc;
use futures_util::StreamExt;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tracing::{info, warn};

use crate::adapters::binance::market_data::BinanceMarket;
use crate::adapters::binance::rest_client::BinanceRestClient;
use crate::adapters::private_stream::{PrivateStream, PrivateStreamReceivers};
use crate::adapters::rest_client::{ExchangeRestClient, HttpMethod, RestRequest};
use crate::models::enums::{OrderStatus, Side, Venue};
use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
use crate::models::order::{Fill, OrderUpdate};

/// One open order discovered during startup reconciliation. Carries the
/// minimum (`order_id`, `symbol`) needed to cancel via the Binance REST
/// `DELETE /api/v3/order` endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoveredOrder {
    pub order_id: String,
    pub symbol: String,
}

pub struct BinancePrivateStream {
    market: BinanceMarket,
    rest_base_url: String,
    api_key: String,
    api_secret: String,
    ws_base_url: String,
    ws_task: Option<JoinHandle<()>>,
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
        Ok(Self {
            market,
            rest_base_url: rest_base_url.to_string(),
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            ws_base_url,
            ws_task: None,
        })
    }

    /// Split a concatenated Binance symbol like "BTCUSDT" into (base, quote).
    /// Tries common quote currencies by longest-suffix-first to handle "USDC" vs "USDT".
    fn split_symbol(symbol: &str) -> (String, String) {
        for quote in ["USDT", "USDC", "BUSD", "FDUSD", "TUSD", "BTC", "ETH", "BNB"] {
            if let Some(base) = symbol.strip_suffix(quote)
                && !base.is_empty()
            {
                return (base.to_string(), quote.to_string());
            }
        }
        let (base, quote) = symbol.split_at(symbol.len().saturating_sub(3));
        (base.to_string(), quote.to_string())
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
        let client_order_id = msg.get("c").and_then(|v| v.as_str()).map(|s| s.to_string());
        let cum_qty: Decimal = msg.get("z")?.as_str()?.parse().ok()?;
        let cum_quote: Decimal = msg.get("Z")?.as_str()?.parse().ok()?;
        let orig_qty: Decimal = msg.get("q")?.as_str()?.parse().ok()?;
        let tx_time_ms = msg.get("T").and_then(|v| v.as_i64()).unwrap_or(0);
        let event_time_ms = msg.get("E").and_then(|v| v.as_i64()).unwrap_or(0);
        let stamp_ms = if tx_time_ms > 0 {
            tx_time_ms
        } else {
            event_time_ms
        };
        let filled_at = chrono::DateTime::from_timestamp_millis(stamp_ms).unwrap_or_else(Utc::now);

        let avg_price = if !cum_qty.is_zero() {
            Some(cum_quote / cum_qty)
        } else {
            None
        };

        let (base, quote_ccy) = Self::split_symbol(symbol);
        let instrument = Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: InstrumentType::Spot,
            base,
            quote: quote_ccy,
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        };

        let fill = Fill {
            order_id: order_id.clone(),
            client_order_id,
            venue: Venue::Binance,
            instrument: instrument.clone(),
            side,
            price: last_price,
            quantity: last_qty,
            fee: commission,
            fee_currency: commission_asset,
            filled_at,
        };

        let order_update = OrderUpdate {
            order_id,
            status,
            filled_quantity: cum_qty,
            remaining_quantity: orig_qty - cum_qty,
            average_price: avg_price,
            updated_at: filled_at,
        };

        Some((fill, order_update))
    }
}

#[async_trait]
impl PrivateStream for BinancePrivateStream {
    async fn connect(&mut self) -> anyhow::Result<PrivateStreamReceivers> {
        if let Some(old_task) = self.ws_task.take() {
            old_task.abort();
        }

        let (fill_tx, fill_rx) = mpsc::unbounded_channel();
        let (order_tx, order_rx) = mpsc::unbounded_channel();

        let market = self.market;
        let rest_base_url = self.rest_base_url.clone();
        let api_key = self.api_key.clone();
        let api_secret = self.api_secret.clone();
        let ws_base_url = self.ws_base_url.clone();

        let ws_task = tokio::spawn(async move {
            let mut backoff = std::time::Duration::from_secs(1);
            let max_backoff = std::time::Duration::from_secs(60);
            let retry_window = std::time::Duration::from_secs(30 * 60);
            let mut first_failure_at: Option<std::time::Instant> = None;
            let mut first_connect = true;
            loop {
                match run_binance_stream(
                    market,
                    &rest_base_url,
                    &api_key,
                    &api_secret,
                    &ws_base_url,
                    &fill_tx,
                    &order_tx,
                    first_connect,
                )
                .await
                {
                    Ok(()) => {
                        info!("Binance private stream ended cleanly, exiting reconnect loop");
                        crate::metrics::set_ws_private_connected("binance", false);
                        break;
                    }
                    Err(e) => {
                        let started = *first_failure_at.get_or_insert_with(std::time::Instant::now);
                        if started.elapsed() > retry_window {
                            tracing::error!(
                                venue = "binance",
                                elapsed_mins = started.elapsed().as_secs() / 60,
                                "private stream circuit-break: failures exceeded 30 min window, giving up"
                            );
                            crate::metrics::set_ws_private_connected("binance", false);
                            break;
                        }
                        warn!(
                            error = %e,
                            backoff_ms = backoff.as_millis() as u64,
                            "Binance private WS disconnected, reconnecting (will recreate listenKey)"
                        );
                        crate::metrics::set_ws_private_connected("binance", false);
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
        info!("disconnected from Binance private WebSocket");
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_binance_stream(
    market: BinanceMarket,
    rest_base_url: &str,
    api_key: &str,
    api_secret: &str,
    ws_base_url: &str,
    fill_tx: &mpsc::UnboundedSender<Fill>,
    order_tx: &mpsc::UnboundedSender<OrderUpdate>,
    first_connect: bool,
) -> anyhow::Result<()> {
    let rest = BinanceRestClient::new(rest_base_url, api_key, api_secret)?;
    let listen_key_path = match market {
        BinanceMarket::Spot => "/api/v3/userDataStream",
        BinanceMarket::UsdtFutures | BinanceMarket::CoinFutures => "/fapi/v1/listenKey",
    };

    let create_req = RestRequest {
        method: HttpMethod::Post,
        path: listen_key_path.to_string(),
        params: HashMap::new(),
    };
    let resp = rest.send(create_req).await?;
    if resp.status != 200 {
        anyhow::bail!(
            "create listenKey failed: status={} body={}",
            resp.status,
            resp.body
        );
    }
    let json: serde_json::Value = serde_json::from_str(&resp.body)?;
    let listen_key = json["listenKey"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("missing listenKey in response"))?
        .to_string();

    // Startup reconciliation policy = cancel-all (NEEDS-DECISION D-1 / Option A).
    // Anything resting on the book at connect time is from a previous run we
    // can no longer reason about — `intended_fills` / `pending_cancels` are
    // gone, so we cannot tell working orders from stale ones. Cancelling all
    // is the simplest correct choice; D-1B (persisted intended_fills) is the
    // future upgrade.
    match fetch_open_orders(&rest, market).await {
        Ok(recovered) => {
            info!(
                count = recovered.len(),
                "Binance startup reconciliation: cancelling all open orders"
            );
            let cancelled = cancel_recovered_orders(&rest, market, &recovered).await;
            info!(
                attempted = recovered.len(),
                succeeded = cancelled,
                "Binance startup reconciliation complete"
            );
        }
        Err(e) => warn!(error = %e, "Binance startup reconciliation failed (continuing)"),
    }

    let url = format!("{}{}", ws_base_url, listen_key);
    let (ws_stream, _) = connect_async(&url).await?;
    info!(url = url.as_str(), "connected to Binance private WebSocket");
    if !first_connect {
        crate::metrics::record_ws_private_reconnect("binance");
    }
    crate::metrics::set_ws_private_connected("binance", true);

    let keepalive_path = listen_key_path.to_string();
    let keepalive_key = listen_key.clone();
    let keepalive_rest = BinanceRestClient::new(rest_base_url, api_key, api_secret)?;
    let keepalive_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(25 * 60));
        interval.tick().await;
        loop {
            interval.tick().await;
            let mut params = HashMap::new();
            params.insert("listenKey".to_string(), keepalive_key.clone());
            let request = RestRequest {
                method: HttpMethod::Put,
                path: keepalive_path.clone(),
                params,
            };
            if let Err(e) = keepalive_rest.send(request).await {
                warn!(error = %e, "listenKey keepalive failed");
            }
        }
    });
    let _keepalive_guard = AbortOnDrop(keepalive_task);

    let (_, mut read) = ws_stream.split();
    while let Some(msg) = read.next().await {
        match msg {
            Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                crate::metrics::record_ws_private_message("binance");
                let text = text.to_string();
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text)
                    && let Some((fill, update)) =
                        BinancePrivateStream::parse_execution_report(&json)
                {
                    let _ = fill_tx.send(fill);
                    let _ = order_tx.send(update);
                }
            }
            Err(e) => anyhow::bail!("WS error: {}", e),
            _ => {}
        }
    }
    anyhow::bail!("stream ended");
}

/// Query Binance for any orders that are still resting on the book at
/// connect time. Returns a list of (order_id, symbol) pairs so the caller
/// can cancel them via REST during startup reconciliation.
async fn fetch_open_orders(
    rest: &BinanceRestClient,
    market: BinanceMarket,
) -> anyhow::Result<Vec<RecoveredOrder>> {
    let path = match market {
        BinanceMarket::Spot => "/api/v3/openOrders",
        BinanceMarket::UsdtFutures | BinanceMarket::CoinFutures => "/fapi/v1/openOrders",
    };
    let req = RestRequest {
        method: HttpMethod::Get,
        path: path.to_string(),
        params: HashMap::new(),
    };
    let resp = rest.send(req).await?;
    if resp.status != 200 {
        anyhow::bail!(
            "openOrders failed: status={} body={}",
            resp.status,
            resp.body
        );
    }
    let arr: serde_json::Value = serde_json::from_str(&resp.body)?;
    Ok(parse_open_orders(&arr))
}

/// Parse the openOrders REST response shape into `RecoveredOrder` entries.
/// Extracted so the JSON shape can be tested without a real REST client.
fn parse_open_orders(payload: &serde_json::Value) -> Vec<RecoveredOrder> {
    let items = payload.as_array().cloned().unwrap_or_default();
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let order_id = item["orderId"]
            .as_u64()
            .map(|n| n.to_string())
            .or_else(|| item["orderId"].as_str().map(|s| s.to_string()))
            .unwrap_or_default();
        if order_id.is_empty() {
            continue;
        }
        let symbol = item["symbol"].as_str().unwrap_or("").to_string();
        if symbol.is_empty() {
            continue;
        }
        out.push(RecoveredOrder { order_id, symbol });
    }
    out
}

/// Build the REST request body for cancelling a single Binance order.
/// Pure helper, separated from network I/O so the loop is unit-testable.
fn build_cancel_request(market: BinanceMarket, order_id: &str, symbol: &str) -> RestRequest {
    let path = match market {
        BinanceMarket::Spot => "/api/v3/order",
        BinanceMarket::UsdtFutures => "/fapi/v1/order",
        BinanceMarket::CoinFutures => "/dapi/v1/order",
    };
    let mut params = HashMap::new();
    params.insert("symbol".to_string(), symbol.to_string());
    params.insert("orderId".to_string(), order_id.to_string());
    RestRequest {
        method: HttpMethod::Delete,
        path: path.to_string(),
        params,
    }
}

/// Iterate `recovered` and DELETE each via REST. Per-order errors are logged
/// at `warn!` and do not abort the loop — a single 404 (already filled) or
/// 5xx shouldn't strand the rest of the cancel batch. Returns the count of
/// orders successfully cancelled.
async fn cancel_recovered_orders(
    rest: &BinanceRestClient,
    market: BinanceMarket,
    recovered: &[RecoveredOrder],
) -> usize {
    let mut succeeded = 0usize;
    for order in recovered {
        let req = build_cancel_request(market, &order.order_id, &order.symbol);
        match rest.send(req).await {
            Ok(resp) if (200..300).contains(&resp.status) => {
                info!(
                    order_id = order.order_id.as_str(),
                    symbol = order.symbol.as_str(),
                    "cancelled recovered order"
                );
                succeeded += 1;
            }
            Ok(resp) => {
                warn!(
                    order_id = order.order_id.as_str(),
                    symbol = order.symbol.as_str(),
                    status = resp.status,
                    body = resp.body.as_str(),
                    "recovered-order cancel rejected"
                );
            }
            Err(e) => {
                warn!(
                    order_id = order.order_id.as_str(),
                    symbol = order.symbol.as_str(),
                    error = %e,
                    "recovered-order cancel network error"
                );
            }
        }
    }
    succeeded
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
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
        assert_eq!(fill.instrument.base, "BTC");
        assert_eq!(fill.instrument.quote, "USDT");
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

    #[test]
    fn parse_execution_report_uses_transaction_time() {
        let tx_time_ms: i64 = 1_700_000_000_000;
        let msg = serde_json::json!({
            "e": "executionReport",
            "s": "BTCUSDT",
            "S": "BUY",
            "X": "FILLED",
            "l": "0.1",
            "L": "50000",
            "n": "0.05",
            "N": "USDT",
            "i": 1,
            "z": "0.1",
            "Z": "5000",
            "q": "0.1",
            "T": tx_time_ms,
            "E": tx_time_ms - 50,
        });
        let (fill, update) = BinancePrivateStream::parse_execution_report(&msg).unwrap();
        assert_eq!(fill.filled_at.timestamp_millis(), tx_time_ms);
        assert_eq!(update.updated_at.timestamp_millis(), tx_time_ms);
    }

    #[test]
    fn parse_open_orders_returns_recovered_per_entry() {
        let payload = serde_json::json!([
            {
                "symbol": "BTCUSDT",
                "orderId": 12345,
                "origQty": "0.01",
                "executedQty": "0.003",
                "price": "50000.00",
                "status": "PARTIALLY_FILLED"
            },
            {
                "symbol": "ETHUSDT",
                "orderId": 67890,
                "origQty": "0.5",
                "executedQty": "0",
                "price": "3000.00",
                "status": "NEW"
            }
        ]);
        let recovered = parse_open_orders(&payload);
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].order_id, "12345");
        assert_eq!(recovered[0].symbol, "BTCUSDT");
        assert_eq!(recovered[1].order_id, "67890");
        assert_eq!(recovered[1].symbol, "ETHUSDT");
    }

    #[test]
    fn parse_open_orders_returns_empty_for_no_orders() {
        let payload = serde_json::json!([]);
        assert!(parse_open_orders(&payload).is_empty());
    }

    #[test]
    fn parse_open_orders_skips_entries_with_no_order_id() {
        let payload = serde_json::json!([
            { "symbol": "BTCUSDT", "origQty": "0.01" }
        ]);
        assert!(parse_open_orders(&payload).is_empty());
    }

    #[test]
    fn parse_open_orders_skips_entries_with_no_symbol() {
        let payload = serde_json::json!([
            { "orderId": 12345, "origQty": "0.01" }
        ]);
        assert!(parse_open_orders(&payload).is_empty());
    }

    #[test]
    fn build_cancel_request_spot_targets_v3_order() {
        let req = build_cancel_request(BinanceMarket::Spot, "12345", "BTCUSDT");
        assert_eq!(req.path, "/api/v3/order");
        assert!(matches!(req.method, HttpMethod::Delete));
        assert_eq!(req.params.get("orderId"), Some(&"12345".to_string()));
        assert_eq!(req.params.get("symbol"), Some(&"BTCUSDT".to_string()));
    }

    #[test]
    fn build_cancel_request_usdt_futures_targets_fapi() {
        let req = build_cancel_request(BinanceMarket::UsdtFutures, "1", "BTCUSDT");
        assert_eq!(req.path, "/fapi/v1/order");
    }

    #[test]
    fn build_cancel_request_coin_futures_targets_dapi() {
        let req = build_cancel_request(BinanceMarket::CoinFutures, "1", "BTCUSD_PERP");
        assert_eq!(req.path, "/dapi/v1/order");
    }

    #[test]
    fn split_symbol_common_quote_currencies() {
        assert_eq!(
            BinancePrivateStream::split_symbol("BTCUSDT"),
            ("BTC".to_string(), "USDT".to_string())
        );
        assert_eq!(
            BinancePrivateStream::split_symbol("ETHUSDC"),
            ("ETH".to_string(), "USDC".to_string())
        );
        assert_eq!(
            BinancePrivateStream::split_symbol("SOLBTC"),
            ("SOL".to_string(), "BTC".to_string())
        );
        assert_eq!(
            BinancePrivateStream::split_symbol("MATICBUSD"),
            ("MATIC".to_string(), "BUSD".to_string())
        );
    }
}
