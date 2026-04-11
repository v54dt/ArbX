use std::collections::HashMap;

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::adapters::market_data::{MarketDataFeed, MarketDataReceivers};
use crate::models::market::{OrderBook, OrderBookLevel, Quote};
use crate::models::position::PortfolioSnapshot;
use crate::strategy::base::ArbitrageStrategy;

/// Main loop: receive quote → update local book → evaluate strategy → log opportunity.
pub struct ArbitrageEngine {
    feeds: Vec<Box<dyn MarketDataFeed>>,
    strategy: Box<dyn ArbitrageStrategy>,
    books: HashMap<String, OrderBook>,
    portfolios: HashMap<String, PortfolioSnapshot>,
}

impl ArbitrageEngine {
    pub fn new(
        feeds: Vec<Box<dyn MarketDataFeed>>,
        strategy: Box<dyn ArbitrageStrategy>,
    ) -> Self {
        Self {
            feeds,
            strategy,
            books: HashMap::new(),
            portfolios: HashMap::new(),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        info!(
            strategy = self.strategy.name(),
            feeds = self.feeds.len(),
            "starting arbitrage engine"
        );

        // Merge all feed quote streams into a single channel.
        let (merged_tx, mut merged_rx) = mpsc::unbounded_channel::<Quote>();

        for feed in self.feeds.iter_mut() {
            let MarketDataReceivers {
                mut quotes,
                order_books: _,
            } = feed.connect().await?;
            let tx = merged_tx.clone();
            tokio::spawn(async move {
                while let Some(q) = quotes.recv().await {
                    if tx.send(q).is_err() {
                        break;
                    }
                }
            });
        }

        // Drop our own sender so the channel closes when all feeds end.
        drop(merged_tx);

        while let Some(quote) = merged_rx.recv().await {
            let key = book_key(&quote);
            self.books.insert(key, quote_to_book(&quote));

            if let Some(opp) = self.strategy.evaluate(&self.books, &self.portfolios).await {
                info!(
                    id = opp.id.as_str(),
                    net_profit = %opp.economics.net_profit,
                    net_profit_bps = %opp.economics.net_profit_bps,
                    legs = opp.legs.len(),
                    "opportunity detected"
                );
            }
        }

        warn!("all quote streams ended");
        Ok(())
    }
}

/// Canonical book key used by strategies and the engine.
/// Format: `"venue:base-quote:instrument_type"` all lowercase.
fn book_key(quote: &Quote) -> String {
    format!(
        "{:?}:{}-{}:{:?}",
        quote.venue,
        quote.instrument.base,
        quote.instrument.quote,
        quote.instrument.instrument_type
    )
    .to_lowercase()
}

/// Convert a top-of-book Quote into a minimal OrderBook (single level each side).
fn quote_to_book(q: &Quote) -> OrderBook {
    OrderBook {
        venue: q.venue,
        instrument: q.instrument.clone(),
        bids: vec![OrderBookLevel {
            price: q.bid,
            size: q.bid_size,
        }],
        asks: vec![OrderBookLevel {
            price: q.ask,
            size: q.ask_size,
        }],
        timestamp: q.timestamp,
        local_timestamp: chrono::Utc::now(),
    }
}
