use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tracing::{error, info, warn};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

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

#[derive(Debug, Deserialize)]
struct OrderBookData {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b", default)]
    bids: Vec<Vec<String>>,
    #[serde(rename = "a", default)]
    asks: Vec<Vec<String>>,
}

impl OrderBookData {
    fn parse_levels(
        raw: &[Vec<String>],
    ) -> smallvec::SmallVec<[crate::models::market::OrderBookLevel; 20]> {
        raw.iter()
            .filter_map(|pair| {
                let price = pair.first()?.parse::<Decimal>().ok()?;
                let size = pair.get(1)?.parse::<Decimal>().ok()?;
                Some(crate::models::market::OrderBookLevel { price, size })
            })
            .collect()
    }
}

#[derive(Debug, Deserialize)]
struct OrderBookMessage {
    data: OrderBookData,
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

        let ticker_args: Vec<String> = self
            .instruments
            .keys()
            .map(|s| format!("tickers.{s}"))
            .collect();
        let book_args: Vec<String> = self
            .instruments
            .keys()
            .map(|s| format!("orderbook.50.{s}"))
            .collect();
        let mut sub_args = ticker_args;
        sub_args.extend(book_args);
        let sub_msg = serde_json::json!({
            "op": "subscribe",
            "args": sub_args,
        })
        .to_string();

        let (ws_write_tx, ws_write_rx) = mpsc::unbounded_channel::<String>();
        self.ws_write_tx = Some(ws_write_tx);

        let instruments = Arc::new(self.instruments.clone());
        let tx = quote_tx;
        let btx = book_tx;
        let task = tokio::spawn(async move {
            use futures_util::{SinkExt, StreamExt};

            let mut backoff = Duration::from_secs(1);
            let max_backoff = Duration::from_secs(60);
            let mut ws_write_rx = ws_write_rx;
            let mut first_connect = true;

            loop {
                match connect_async(&url).await {
                    Ok((ws_stream, _)) => {
                        backoff = Duration::from_secs(1);
                        info!(url = url.as_str(), "connected to Bybit WebSocket");
                        let (mut write, mut read) = ws_stream.split();

                        let ws_sub =
                            tokio_tungstenite::tungstenite::Message::Text(sub_msg.clone().into());
                        if let Err(e) = write.send(ws_sub).await {
                            error!(error = %e, "Bybit WS subscribe error");
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(max_backoff);
                            continue;
                        }
                        if !first_connect {
                            crate::metrics::record_ws_reconnect("bybit");
                        }
                        first_connect = false;
                        crate::metrics::set_ws_connected("bybit", true);

                        let mut ping_interval = tokio::time::interval(Duration::from_secs(20));
                        ping_interval.tick().await;

                        'msg: loop {
                            tokio::select! {
                                Some(msg) = read.next() => {
                                    match msg {
                                        Ok(msg) => {
                                            if let tokio_tungstenite::tungstenite::Message::Text(text) = msg {
                                                crate::metrics::record_ws_message("bybit");
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
                                                        return;
                                                    }
                                                }
                                                if let Ok(book_msg) = serde_json::from_str::<OrderBookMessage>(&text)
                                                    && let Some(instrument) = instruments.get(&book_msg.data.symbol)
                                                {
                                                    let now = chrono::Utc::now();
                                                    let ob = OrderBook {
                                                        venue: Venue::Bybit,
                                                        instrument: instrument.clone(),
                                                        bids: OrderBookData::parse_levels(&book_msg.data.bids),
                                                        asks: OrderBookData::parse_levels(&book_msg.data.asks),
                                                        timestamp: now,
                                                        local_timestamp: now,
                                                    };
                                                    let _ = btx.send(ob);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            warn!(error = %e, "Bybit WS error, will reconnect");
                                            break 'msg;
                                        }
                                    }
                                }

                                Some(msg) = ws_write_rx.recv() => {
                                    let ws_msg = tokio_tungstenite::tungstenite::Message::Text(msg.into());
                                    if let Err(e) = write.send(ws_msg).await {
                                        warn!(error = %e, "Bybit WS write error, will reconnect");
                                        break 'msg;
                                    }
                                }

                                _ = ping_interval.tick() => {
                                    let ping = tokio_tungstenite::tungstenite::Message::Text(
                                        r#"{"op":"ping"}"#.into(),
                                    );
                                    if let Err(e) = write.send(ping).await {
                                        warn!(error = %e, "Bybit WS ping failed, will reconnect");
                                        break 'msg;
                                    }
                                }
                                else => return,
                            }
                        }
                        info!("Bybit WS disconnected, reconnecting");
                        crate::metrics::set_ws_connected("bybit", false);
                    }
                    Err(e) => {
                        warn!(error = %e, backoff_ms = backoff.as_millis() as u64, "Bybit WS connect failed");
                        crate::metrics::set_ws_connected("bybit", false);
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
            fills: None,
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
            last_trade_time: None,
            settlement_time: None,
        }
    }

    #[test]
    fn parse_ticker_valid_json() {
        let json = r#"{
            "data": {
                "symbol": "BTCUSDT",
                "bid1Price": "50000.5",
                "bid1Size": "1.2",
                "ask1Price": "50001.0",
                "ask1Size": "0.8"
            }
        }"#;
        let msg: TickerMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.data.symbol, "BTCUSDT");
        assert_eq!(msg.data.bid_price, dec!(50000.5));
        assert_eq!(msg.data.ask_price, dec!(50001.0));
        assert_eq!(msg.data.bid_size, dec!(1.2));
        assert_eq!(msg.data.ask_size, dec!(0.8));
    }

    #[test]
    fn register_instrument_stores_correctly() {
        let mut md = BybitMarketData::new(BybitMarket::Spot);
        let inst = btc_usdt_spot();
        md.register_instrument("btcusdt", inst.clone());
        assert_eq!(md.instruments.len(), 1);
        assert!(md.instruments.contains_key("BTCUSDT"));
        assert_eq!(md.instruments["BTCUSDT"], inst);
    }
}
