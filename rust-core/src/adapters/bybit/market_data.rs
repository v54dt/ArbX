use anyhow::Result;
use async_trait::async_trait;
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

pub enum BybitMarket {
    Spot,
    Linear,
    Inverse,
}

impl BybitMarket {
    fn ws_base_url(&self) -> &'static str {
        match self {
            BybitMarket::Spot => "wss://stream.bybit.com/v5/public/spot",
            BybitMarket::Linear => "wss://stream.bybit.com/v5/public/linear",
            BybitMarket::Inverse => "wss://stream.bybit.com/v5/public/inverse",
        }
    }
}

fn de_decimal_str<'de, D>(deserializer: D) -> std::result::Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = serde::Deserialize::deserialize(deserializer)?;
    s.parse().map_err(serde::de::Error::custom)
}

#[derive(Debug, Deserialize)]
struct TickerData {
    symbol: String,
    #[serde(rename = "bid1Price", deserialize_with = "de_decimal_str")]
    bid_price: Decimal,
    #[serde(rename = "bid1Size", deserialize_with = "de_decimal_str")]
    bid_size: Decimal,
    #[serde(rename = "ask1Price", deserialize_with = "de_decimal_str")]
    ask_price: Decimal,
    #[serde(rename = "ask1Size", deserialize_with = "de_decimal_str")]
    ask_size: Decimal,
}

#[derive(Debug, Deserialize)]
struct TickerMessage {
    data: TickerData,
}

pub struct BybitMarketData {
    market: BybitMarket,
    instruments: HashMap<String, Instrument>,
    ws_task: Option<JoinHandle<()>>,
    quote_tx: Option<mpsc::UnboundedSender<Quote>>,
    book_tx: Option<mpsc::UnboundedSender<OrderBook>>,
    ws_write_tx: Option<mpsc::UnboundedSender<String>>,
}

impl BybitMarketData {
    pub fn new(market: BybitMarket) -> Self {
        Self {
            market,
            instruments: HashMap::new(),
            ws_task: None,
            quote_tx: None,
            book_tx: None,
            ws_write_tx: None,
        }
    }

    pub fn register_instrument(&mut self, symbol: &str, instrument: Instrument) {
        self.instruments.insert(symbol.to_uppercase(), instrument);
    }
}

#[async_trait]
impl MarketDataFeed for BybitMarketData {
    async fn connect(&mut self) -> Result<MarketDataReceivers> {
        let (quote_tx, quote_rx) = mpsc::unbounded_channel();
        let (book_tx, book_rx) = mpsc::unbounded_channel();

        self.quote_tx = Some(quote_tx.clone());
        self.book_tx = Some(book_tx);

        if self.instruments.is_empty() {
            anyhow::bail!("no symbols registered, call register_instrument() before connect()");
        }

        let url = self.market.ws_base_url().to_string();
        let (ws_stream, _) = connect_async(&url).await?;
        info!(url = url.as_str(), "connected to Bybit WebSocket");

        let args: Vec<String> = self
            .instruments
            .keys()
            .map(|s| format!("tickers.{s}"))
            .collect();
        let sub_msg = serde_json::json!({
            "op": "subscribe",
            "args": args,
        });

        let (ws_write_tx, mut ws_write_rx) = mpsc::unbounded_channel::<String>();
        ws_write_tx.send(sub_msg.to_string())?;
        self.ws_write_tx = Some(ws_write_tx);

        let instruments = Arc::new(self.instruments.clone());
        let tx = quote_tx;
        let task = tokio::spawn(async move {
            use futures_util::{SinkExt, StreamExt};
            let (mut write, mut read) = ws_stream.split();

            loop {
                tokio::select! {
                    Some(msg) = read.next() => {
                        match msg {
                            Ok(msg) => {
                                if let tokio_tungstenite::tungstenite::Message::Text(text) = msg {
                                    let text = text.to_string();
                                    if let Ok(ticker_msg) = serde_json::from_str::<TickerMessage>(&text)
                                        && let Some(instrument) = instruments.get(&ticker_msg.data.symbol)
                                    {
                                        let quote = Quote {
                                            venue: Venue::Bybit,
                                            instrument: instrument.clone(),
                                            bid: ticker_msg.data.bid_price,
                                            ask: ticker_msg.data.ask_price,
                                            bid_size: ticker_msg.data.bid_size,
                                            ask_size: ticker_msg.data.ask_size,
                                            timestamp: chrono::Utc::now(),
                                        };
                                        if tx.send(quote).is_err() {
                                            break;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "Bybit WebSocket error");
                                break;
                            }
                        }
                    }

                    Some(msg) = ws_write_rx.recv() => {
                        let ws_msg = tokio_tungstenite::tungstenite::Message::Text(msg.into());
                        if let Err(e) = write.send(ws_msg).await {
                            error!(error = %e, "Bybit WebSocket write error");
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
        info!("Disconnected from Bybit");
        Ok(())
    }

    async fn subscribe(&mut self, symbols: &[String]) -> Result<()> {
        if let Some(tx) = &self.ws_write_tx {
            let args: Vec<String> = symbols.iter().map(|s| format!("tickers.{s}")).collect();
            let msg = serde_json::json!({
                "op": "subscribe",
                "args": args,
            });
            tx.send(msg.to_string())?;
        }
        Ok(())
    }

    async fn unsubscribe(&mut self, symbols: &[String]) -> Result<()> {
        for s in symbols {
            self.instruments.remove(&s.to_uppercase());
        }

        if let Some(tx) = &self.ws_write_tx {
            let args: Vec<String> = symbols.iter().map(|s| format!("tickers.{s}")).collect();
            let msg = serde_json::json!({
                "op": "unsubscribe",
                "args": args,
            });
            tx.send(msg.to_string())?;
        }
        Ok(())
    }
}
