use anyhow::Result;
use async_trait::async_trait;
use futures_util::StreamExt;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tracing::{error, info};

use std::collections::HashMap;
use std::sync::Arc;

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
    /// Map of Binance symbol (e.g. "BTCUSDT") -> canonical Instrument.
    /// Registered via `register_instrument()` before `connect()`.
    instruments: HashMap<String, Instrument>,
    ws_task: Option<JoinHandle<()>>,
    quote_tx: Option<mpsc::UnboundedSender<Quote>>,
    book_tx: Option<mpsc::UnboundedSender<OrderBook>>,
    ws_write_tx: Option<mpsc::UnboundedSender<String>>,
    next_req_id: u64,
}

impl BinanceMarketData {
    pub fn new(market: BinanceMarket) -> Self {
        Self {
            market,
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

        let base = self.market.ws_base_url();
        let url = format!("{}/stream?streams={}", base, streams.join("/"));

        let (ws_stream, _) = connect_async(&url).await?;
        info!(url = url.as_str(), "connected to Binance WebSocket");

        let (ws_write_tx, mut ws_write_rx) = mpsc::unbounded_channel::<String>();
        self.ws_write_tx = Some(ws_write_tx);

        let instruments = Arc::new(self.instruments.clone());
        let tx = quote_tx;
        let task = tokio::spawn(async move {
            let (mut write, mut read) = ws_stream.split();

            use futures_util::SinkExt;

            loop {
                tokio::select! {
                    Some(msg) = read.next() => {
                        match msg {
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
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "Binance WebSocket error");
                                break;
                            }
                        }
                    }

                    Some(msg) = ws_write_rx.recv() => {
                        let ws_msg = tokio_tungstenite::tungstenite::Message::Text(msg.into());
                        if let Err(e) = write.send(ws_msg).await {
                            error!(error = %e, "Binance WebSocket write error");
                            break;
                        }
                    }
                    else => break,
                }
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
