use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;
use std::str::FromStr;
use tokio::sync::mpsc;

use crate::adapters::market_data::{MarketDataFeed, MarketDataReceivers};
use crate::models::enums::Venue;
use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
use crate::models::market::Quote;

pub struct HistoricalDataFeed {
    quotes: Vec<Quote>,
}

impl HistoricalDataFeed {
    pub fn from_quotes(quotes: Vec<Quote>) -> Self {
        let mut quotes = quotes;
        quotes.sort_by_key(|q| q.timestamp);
        Self { quotes }
    }

    pub fn into_quotes(self) -> Vec<Quote> {
        self.quotes
    }

    pub fn from_csv(path: &str) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let mut quotes = Vec::new();
        for (i, line) in contents.lines().enumerate() {
            if i == 0 && line.contains("timestamp") {
                continue;
            }
            let fields: Vec<&str> = line.split(',').collect();
            if fields.len() < 8 {
                tracing::warn!(
                    line = i + 1,
                    fields = fields.len(),
                    "skipping malformed CSV row"
                );
                continue;
            }
            let timestamp_ms: i64 = match fields[0].trim().parse() {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(line = i + 1, error = %e, "skipping row: bad timestamp");
                    continue;
                }
            };
            let Some(timestamp) = Utc.timestamp_millis_opt(timestamp_ms).single() else {
                tracing::warn!(line = i + 1, "skipping row: invalid timestamp_ms");
                continue;
            };
            let venue = match parse_venue(fields[1].trim()) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(line = i + 1, error = %e, "skipping row: bad venue");
                    continue;
                }
            };
            let base = fields[2].trim().to_string();
            let quote_ccy = fields[3].trim().to_string();
            let Ok(bid) = Decimal::from_str(fields[4].trim()) else {
                tracing::warn!(line = i + 1, "skipping row: bad bid");
                continue;
            };
            let Ok(ask) = Decimal::from_str(fields[5].trim()) else {
                tracing::warn!(line = i + 1, "skipping row: bad ask");
                continue;
            };
            let Ok(bid_size) = Decimal::from_str(fields[6].trim()) else {
                tracing::warn!(line = i + 1, "skipping row: bad bid_size");
                continue;
            };
            let Ok(ask_size) = Decimal::from_str(fields[7].trim()) else {
                tracing::warn!(line = i + 1, "skipping row: bad ask_size");
                continue;
            };

            quotes.push(Quote {
                venue,
                instrument: Instrument {
                    asset_class: AssetClass::Crypto,
                    instrument_type: InstrumentType::Spot,
                    base,
                    quote: quote_ccy,
                    settle_currency: None,
                    expiry: None,
                    last_trade_time: None,
                    settlement_time: None,
                },
                bid,
                ask,
                bid_size,
                ask_size,
                timestamp,
            });
        }
        Ok(Self::from_quotes(quotes))
    }
}

fn parse_venue(s: &str) -> anyhow::Result<Venue> {
    match s.to_lowercase().as_str() {
        "binance" => Ok(Venue::Binance),
        "bybit" => Ok(Venue::Bybit),
        "okx" => Ok(Venue::Okx),
        "fubon" => Ok(Venue::Fubon),
        "shioaji" => Ok(Venue::Shioaji),
        other => anyhow::bail!("unknown venue: {}", other),
    }
}

#[async_trait]
impl MarketDataFeed for HistoricalDataFeed {
    async fn connect(&mut self) -> anyhow::Result<MarketDataReceivers> {
        let (quote_tx, quote_rx) = mpsc::unbounded_channel();
        let (_book_tx, book_rx) = mpsc::unbounded_channel();
        let quotes = std::mem::take(&mut self.quotes);

        tokio::spawn(async move {
            for q in quotes {
                if quote_tx.send(q).is_err() {
                    break;
                }
            }
        });

        Ok(MarketDataReceivers {
            quotes: quote_rx,
            order_books: book_rx,
            fills: None,
            signals: None,
        })
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn subscribe(&mut self, _symbols: &[String]) -> anyhow::Result<()> {
        Ok(())
    }

    async fn unsubscribe(&mut self, _symbols: &[String]) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn make_quote(venue: Venue, bid: Decimal, ask: Decimal, ts_ms: i64) -> Quote {
        Quote {
            venue,
            instrument: Instrument {
                asset_class: AssetClass::Crypto,
                instrument_type: InstrumentType::Spot,
                base: "BTC".into(),
                quote: "USDT".into(),
                settle_currency: None,
                expiry: None,
                last_trade_time: None,
                settlement_time: None,
            },
            bid,
            ask,
            bid_size: dec!(1),
            ask_size: dec!(1),
            timestamp: Utc.timestamp_millis_opt(ts_ms).unwrap(),
        }
    }

    #[test]
    fn from_quotes_sorts_by_timestamp() {
        let feed = HistoricalDataFeed::from_quotes(vec![
            make_quote(Venue::Binance, dec!(100), dec!(101), 3000),
            make_quote(Venue::Binance, dec!(100), dec!(101), 1000),
            make_quote(Venue::Binance, dec!(100), dec!(101), 2000),
        ]);
        let timestamps: Vec<i64> = feed
            .quotes
            .iter()
            .map(|q| q.timestamp.timestamp_millis())
            .collect();
        assert_eq!(timestamps, vec![1000, 2000, 3000]);
    }

    #[tokio::test]
    async fn connect_sends_all_quotes() {
        let mut feed = HistoricalDataFeed::from_quotes(vec![
            make_quote(Venue::Binance, dec!(100), dec!(101), 1000),
            make_quote(Venue::Binance, dec!(102), dec!(103), 2000),
        ]);
        let receivers = feed.connect().await.unwrap();
        let mut quotes_rx = receivers.quotes;
        let mut received = vec![];
        while let Some(q) = quotes_rx.recv().await {
            received.push(q);
        }
        assert_eq!(received.len(), 2);
        assert_eq!(received[0].bid, dec!(100));
        assert_eq!(received[1].bid, dec!(102));
    }

    #[test]
    fn into_quotes_returns_sorted_quotes_for_chunking() {
        let feed = HistoricalDataFeed::from_quotes(vec![
            make_quote(Venue::Binance, dec!(1), dec!(2), 3000),
            make_quote(Venue::Binance, dec!(1), dec!(2), 1000),
            make_quote(Venue::Binance, dec!(1), dec!(2), 2000),
        ]);
        let quotes = feed.into_quotes();
        assert_eq!(quotes.len(), 3);
        let chunks: Vec<Vec<_>> = quotes.chunks(2).map(|c| c.to_vec()).collect();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 2);
        assert_eq!(chunks[1].len(), 1);
        assert_eq!(chunks[0][0].timestamp.timestamp_millis(), 1000);
        assert_eq!(chunks[1][0].timestamp.timestamp_millis(), 3000);
    }

    #[test]
    fn from_csv_parses_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.csv");
        std::fs::write(
            &path,
            "timestamp_ms,venue,base,quote,bid,ask,bid_size,ask_size\n\
             1000,binance,BTC,USDT,50000,50010,1.5,2.0\n\
             2000,bybit,BTC,USDT,50005,50015,1.0,1.0\n",
        )
        .unwrap();
        let feed = HistoricalDataFeed::from_csv(path.to_str().unwrap()).unwrap();
        assert_eq!(feed.quotes.len(), 2);
        assert_eq!(feed.quotes[0].venue, Venue::Binance);
        assert_eq!(feed.quotes[1].venue, Venue::Bybit);
        assert_eq!(feed.quotes[0].bid, dec!(50000));
    }
}
