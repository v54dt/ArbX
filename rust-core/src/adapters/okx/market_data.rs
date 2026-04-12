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
use crate::models::market::{OrderBook, Quote};

use futures_util::StreamExt;

#[derive(Debug, Deserialize)]
struct TickerData {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "bidPx", deserialize_with = "de_decimal_str")]
    bid_px: Decimal,
    #[serde(rename = "bidSz", deserialize_with = "de_decimal_str")]
    bid_sz: Decimal,
    #[serde(rename = "askPx", deserialize_with = "de_decimal_str")]
    ask_px: Decimal,
    #[serde(rename = "askSz", deserialize_with = "de_decimal_str")]
    ask_sz: Decimal,
}

#[derive(Debug, Deserialize)]
struct WsMessage {
    data: Option<Vec<TickerData>>,
}

fn de_decimal_str<'de, D>(deserializer: D) -> std::result::Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = serde::Deserialize::deserialize(deserializer)?;
    s.parse().map_err(serde::de::Error::custom)
}

pub struct OkxMarketData {
    instruments: HashMap<String, Instrument>,
    ws_task: Option<JoinHandle<()>>,
    quote_tx: Option<mpsc::UnboundedSender<Quote>>,
    book_tx: Option<mpsc::UnboundedSender<OrderBook>>,
    ws_write_tx: Option<mpsc::UnboundedSender<String>>,
}

impl OkxMarketData {
    pub fn new() -> Self {
        Self {
            instruments: HashMap::new(),
            ws_task: None,
            quote_tx: None,
            book_tx: None,
            ws_write_tx: None,
        }
    }

    pub fn register_instrument(&mut self, inst_id: &str, instrument: Instrument) {
        self.instruments.insert(inst_id.to_uppercase(), instrument);
    }
}

#[async_trait]
impl MarketDataFeed for OkxMarketData {
    async fn connect(&mut self) -> Result<MarketDataReceivers> {
        let (quote_tx, quote_rx) = mpsc::unbounded_channel();
        let (book_tx, book_rx) = mpsc::unbounded_channel();

        self.quote_tx = Some(quote_tx.clone());
        self.book_tx = Some(book_tx);

        if self.instruments.is_empty() {
            anyhow::bail!("no instruments registered, call register_instrument() before connect()");
        }

        let url = "wss://ws.okx.com:8443/ws/v5/public";
        let (ws_stream, _) = connect_async(url).await?;
        info!(url, "connected to OKX WebSocket");

        let args: Vec<serde_json::Value> = self
            .instruments
            .keys()
            .map(|id| {
                serde_json::json!({
                    "channel": "tickers",
                    "instId": id
                })
            })
            .collect();

        let sub_msg = serde_json::json!({
            "op": "subscribe",
            "args": args
        })
        .to_string();

        let (ws_write_tx, mut ws_write_rx) = mpsc::unbounded_channel::<String>();
        self.ws_write_tx = Some(ws_write_tx);

        let instruments = Arc::new(self.instruments.clone());
        let tx = quote_tx;
        let task = tokio::spawn(async move {
            let (mut write, mut read) = ws_stream.split();

            use futures_util::SinkExt;

            let ws_msg = tokio_tungstenite::tungstenite::Message::Text(sub_msg.into());
            if let Err(e) = write.send(ws_msg).await {
                error!(error = %e, "OKX WebSocket subscribe error");
                return;
            }

            loop {
                tokio::select! {
                    Some(msg) = read.next() => {
                        match msg {
                            Ok(msg) => {
                                if let tokio_tungstenite::tungstenite::Message::Text(text) = msg {
                                    let text = text.to_string();
                                    if let Ok(ws_msg) = serde_json::from_str::<WsMessage>(&text)
                                        && let Some(data) = ws_msg.data
                                    {
                                        for ticker in data {
                                            if let Some(instrument) = instruments.get(&ticker.inst_id) {
                                                let quote = Quote {
                                                    venue: Venue::Okx,
                                                    instrument: instrument.clone(),
                                                    bid: ticker.bid_px,
                                                    ask: ticker.ask_px,
                                                    bid_size: ticker.bid_sz,
                                                    ask_size: ticker.ask_sz,
                                                    timestamp: chrono::Utc::now(),
                                                };
                                                if tx.send(quote).is_err() {
                                                    return;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "OKX WebSocket error");
                                break;
                            }
                        }
                    }

                    Some(msg) = ws_write_rx.recv() => {
                        let ws_msg = tokio_tungstenite::tungstenite::Message::Text(msg.into());
                        if let Err(e) = write.send(ws_msg).await {
                            error!(error = %e, "OKX WebSocket write error");
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
        info!("disconnected from OKX");
        Ok(())
    }

    async fn subscribe(&mut self, symbols: &[String]) -> Result<()> {
        if let Some(tx) = &self.ws_write_tx {
            let args: Vec<serde_json::Value> = symbols
                .iter()
                .map(|s| {
                    serde_json::json!({
                        "channel": "tickers",
                        "instId": s.to_uppercase()
                    })
                })
                .collect();
            let msg = serde_json::json!({
                "op": "subscribe",
                "args": args
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
            let args: Vec<serde_json::Value> = symbols
                .iter()
                .map(|s| {
                    serde_json::json!({
                        "channel": "tickers",
                        "instId": s.to_uppercase()
                    })
                })
                .collect();
            let msg = serde_json::json!({
                "op": "unsubscribe",
                "args": args
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
    fn parse_ticker_valid_json() {
        let json = r#"{
            "data": [{
                "instId": "BTC-USDT",
                "bidPx": "50000.5",
                "bidSz": "1.2",
                "askPx": "50001.0",
                "askSz": "0.8"
            }]
        }"#;
        let msg: WsMessage = serde_json::from_str(json).unwrap();
        let data = msg.data.unwrap();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].inst_id, "BTC-USDT");
        assert_eq!(data[0].bid_px, dec!(50000.5));
        assert_eq!(data[0].ask_px, dec!(50001.0));
        assert_eq!(data[0].bid_sz, dec!(1.2));
        assert_eq!(data[0].ask_sz, dec!(0.8));
    }

    #[test]
    fn register_instrument_stores_correctly() {
        let mut md = OkxMarketData::new();
        let inst = btc_usdt_spot();
        md.register_instrument("btc-usdt", inst.clone());
        assert_eq!(md.instruments.len(), 1);
        assert!(md.instruments.contains_key("BTC-USDT"));
        assert_eq!(md.instruments["BTC-USDT"], inst);
    }
}
