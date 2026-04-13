use anyhow::Result;
use async_trait::async_trait;
use futures_util::StreamExt;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tracing::{info, warn};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::adapters::market_data::{MarketDataFeed, MarketDataReceivers};
use crate::models::enums::Venue;
use crate::models::instrument::Instrument;
use crate::models::market::OrderBook;
use crate::models::market::Quote;

pub enum BinanceMarket {
    Spot,
    UsdtFutures,
    CoinFutures,
}

impl BinanceMarket {
    fn ws_base_url(&self) -> &'static str {
        match self {
            BinanceMarket::Spot => "wss://stream.binance.com:9443",
            BinanceMarket::UsdtFutures => "wss://fstream.binance.com",
            BinanceMarket::CoinFutures => "wss://dstream.binance.com",
        }
    }

    fn ws_base_url_testnet(&self) -> &'static str {
        match self {
            BinanceMarket::Spot => "wss://testnet.binance.vision/ws",
            BinanceMarket::UsdtFutures => "wss://stream.binancefuture.com/ws",
            BinanceMarket::CoinFutures => "wss://dstream.binancefuture.com/ws",
        }
    }

    pub fn rest_base_url(&self) -> &'static str {
        match self {
            BinanceMarket::Spot => "https://api.binance.com",
            BinanceMarket::UsdtFutures => "https://fapi.binance.com",
            BinanceMarket::CoinFutures => "https://dapi.binance.com",
        }
    }

    pub fn rest_base_url_testnet(&self) -> &'static str {
        match self {
            BinanceMarket::Spot => "https://testnet.binance.vision",
            BinanceMarket::UsdtFutures => "https://testnet.binancefuture.com",
            BinanceMarket::CoinFutures => "https://testnet.binancefuture.com",
        }
    }

    pub fn ws_url(&self, testnet: bool) -> &'static str {
        if testnet {
            self.ws_base_url_testnet()
        } else {
            self.ws_base_url()
        }
    }

    pub fn rest_url(&self, testnet: bool) -> &'static str {
        if testnet {
            self.rest_base_url_testnet()
        } else {
            self.rest_base_url()
        }
    }
}

/// Binance WebSocket book ticker message
#[derive(Debug, Deserialize)]
struct BookTickerMsg {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b", deserialize_with = "de_decimal_str")]
    bid_price: Decimal,
    #[serde(rename = "B", deserialize_with = "de_decimal_str")]
    bid_qty: Decimal,
    #[serde(rename = "a", deserialize_with = "de_decimal_str")]
    ask_price: Decimal,
    #[serde(rename = "A", deserialize_with = "de_decimal_str")]
    ask_qty: Decimal,
}

fn de_decimal_str<'de, D>(deserializer: D) -> std::result::Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = serde::Deserialize::deserialize(deserializer)?;
    s.parse().map_err(serde::de::Error::custom)
}

pub struct BinanceMarketData {
    market: BinanceMarket,
    testnet: bool,
    instruments: HashMap<String, Instrument>,
    ws_task: Option<JoinHandle<()>>,
    quote_tx: Option<mpsc::UnboundedSender<Quote>>,
    book_tx: Option<mpsc::UnboundedSender<OrderBook>>,
    ws_write_tx: Option<mpsc::UnboundedSender<String>>,
    next_req_id: u64,
}

impl BinanceMarketData {
    pub fn new(market: BinanceMarket) -> Self {
        Self::with_testnet(market, false)
    }

    pub fn with_testnet(market: BinanceMarket, testnet: bool) -> Self {
        Self {
            market,
            testnet,
            instruments: HashMap::new(),
            ws_task: None,
            quote_tx: None,
            book_tx: None,
            ws_write_tx: None,
            next_req_id: 1,
        }
    }

    /// Register a Binance symbol -> Instrument mapping.
    pub fn register_instrument(&mut self, binance_symbol: &str, instrument: Instrument) {
        self.instruments
            .insert(binance_symbol.to_uppercase(), instrument);
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_req_id;
        self.next_req_id += 1;
        id
    }
}

#[async_trait]
impl MarketDataFeed for BinanceMarketData {
    async fn connect(&mut self) -> Result<MarketDataReceivers> {
        let (quote_tx, quote_rx) = mpsc::unbounded_channel();
        let (book_tx, book_rx) = mpsc::unbounded_channel();

        self.quote_tx = Some(quote_tx.clone());
        self.book_tx = Some(book_tx);

        let streams: Vec<String> = self
            .instruments
            .keys()
            .map(|s| format!("{}@bookTicker", s.to_lowercase()))
            .collect();

        if streams.is_empty() {
            anyhow::bail!("no symbols subscribed, call subscribe() before connect()");
        }

        let base = self.market.ws_url(self.testnet);
        let url = format!("{}/stream?streams={}", base, streams.join("/"));

        let (ws_write_tx, ws_write_rx) = mpsc::unbounded_channel::<String>();
        self.ws_write_tx = Some(ws_write_tx);

        let instruments = Arc::new(self.instruments.clone());
        let tx = quote_tx;
        let task = tokio::spawn(async move {
            use futures_util::SinkExt;

            let mut backoff = Duration::from_secs(1);
            let max_backoff = Duration::from_secs(60);
            let mut ws_write_rx = ws_write_rx;

            loop {
                match connect_async(&url).await {
                    Ok((ws_stream, _)) => {
                        backoff = Duration::from_secs(1);
                        info!(url = url.as_str(), "connected to Binance WebSocket");
                        let (mut write, mut read) = ws_stream.split();

                        'msg: loop {
                            tokio::select! {
                                Some(msg) = read.next() => {
                                    match msg {
                                        Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                                            info!("Binance sent close frame (24h limit), reconnecting");
                                            break 'msg;
                                        }
                                        Ok(msg) => {
                                            if let tokio_tungstenite::tungstenite::Message::Text(text) = msg {
                                                let text = text.to_string();
                                                if let Ok(wrapper) = serde_json::from_str::<serde_json::Value>(&text) {
                                                    let data = wrapper.get("data").unwrap_or(&wrapper);
                                                    if let Ok(ticker) = serde_json::from_value::<BookTickerMsg>(data.clone())
                                                        && let Some(instrument) = instruments.get(&ticker.symbol)
                                                    {
                                                        let quote = Quote {
                                                            venue: Venue::Binance,
                                                            instrument: instrument.clone(),
                                                            bid: ticker.bid_price,
                                                            ask: ticker.ask_price,
                                                            bid_size: ticker.bid_qty,
                                                            ask_size: ticker.ask_qty,
                                                            timestamp: chrono::Utc::now(),
                                                        };
                                                        if tx.send(quote).is_err() {
                                                            return;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            warn!(error = %e, "Binance WS error, will reconnect");
                                            break 'msg;
                                        }
                                    }
                                }

                                Some(msg) = ws_write_rx.recv() => {
                                    let ws_msg = tokio_tungstenite::tungstenite::Message::Text(msg.into());
                                    if let Err(e) = write.send(ws_msg).await {
                                        warn!(error = %e, "Binance WS write error, will reconnect");
                                        break 'msg;
                                    }
                                }
                                else => return,
                            }
                        }
                        info!("Binance WS disconnected, reconnecting");
                    }
                    Err(e) => {
                        warn!(error = %e, backoff_ms = backoff.as_millis() as u64, "Binance WS connect failed");
                    }
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        });

        self.ws_task = Some(task);

        Ok(MarketDataReceivers {
            quotes: quote_rx,
            order_books: book_rx,
        })
    }

    async fn disconnect(&mut self) -> Result<()> {
        if let Some(task) = self.ws_task.take() {
            task.abort();
        }
        self.quote_tx = None;
        self.book_tx = None;
        self.ws_write_tx = None;
        info!("Disconnected from Binance");
        Ok(())
    }

    async fn subscribe(&mut self, symbols: &[String]) -> Result<()> {
        // Symbols must already be registered via `register_instrument()`.
        // This method only sends the dynamic SUBSCRIBE frame if already connected.
        let id = self.next_id();
        if let Some(tx) = &self.ws_write_tx {
            let params: Vec<String> = symbols
                .iter()
                .map(|s| format!("{}@bookTicker", s.to_lowercase()))
                .collect();
            let msg = serde_json::json!({
                "method": "SUBSCRIBE",
                "params": params,
                "id": id
            });
            tx.send(msg.to_string())?;
        }
        Ok(())
    }

    async fn unsubscribe(&mut self, symbols: &[String]) -> Result<()> {
        for s in symbols {
            self.instruments.remove(&s.to_uppercase());
        }

        let id = self.next_id();
        if let Some(tx) = &self.ws_write_tx {
            let params: Vec<String> = symbols
                .iter()
                .map(|s| format!("{}@bookTicker", s.to_lowercase()))
                .collect();
            let msg = serde_json::json!({
                "method": "UNSUBSCRIBE",
                "params": params,
                "id": id
            });
            tx.send(msg.to_string())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::instrument::{AssetClass, InstrumentType};
    use rust_decimal_macros::dec;

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

    #[test]
    fn parse_book_ticker_valid_json() {
        let json = r#"{
            "s": "BTCUSDT",
            "b": "50000.10",
            "B": "1.5",
            "a": "50001.20",
            "A": "2.3"
        }"#;
        let msg: BookTickerMsg = serde_json::from_str(json).unwrap();
        assert_eq!(msg.symbol, "BTCUSDT");
        assert_eq!(msg.bid_price, dec!(50000.10));
        assert_eq!(msg.bid_qty, dec!(1.5));
        assert_eq!(msg.ask_price, dec!(50001.20));
        assert_eq!(msg.ask_qty, dec!(2.3));
    }

    #[test]
    fn parse_book_ticker_missing_field_fails() {
        let json = r#"{"s": "BTCUSDT", "b": "50000.10"}"#;
        let result = serde_json::from_str::<BookTickerMsg>(json);
        assert!(result.is_err());
    }

    #[test]
    fn register_instrument_stores_correctly() {
        let mut md = BinanceMarketData::new(BinanceMarket::Spot);
        let inst = make_instrument("BTC", "USDT");
        md.register_instrument("btcusdt", inst.clone());

        assert_eq!(md.instruments.len(), 1);
        assert_eq!(md.instruments.get("BTCUSDT"), Some(&inst));

        let inst2 = make_instrument("ETH", "USDT");
        md.register_instrument("ETHUSDT", inst2.clone());
        assert_eq!(md.instruments.len(), 2);
        assert_eq!(md.instruments.get("ETHUSDT"), Some(&inst2));
    }
}
