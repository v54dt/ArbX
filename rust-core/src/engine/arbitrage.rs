use std::collections::HashMap;

use anyhow::Result;
use tracing::{info, warn};

use crate::adapters::market_data::{MarketDataFeed, MarketDataReceivers};
use crate::models::market::{OrderBook, OrderBookLevel, Quote};
use crate::models::position::PortfolioSnapshot;
use crate::strategy::base::ArbitrageStrategy;

/// Main loop: receive quote → update local book → evaluate strategy → log opportunity.
pub struct ArbitrageEngine {
    feed: Box<dyn MarketDataFeed>,
    strategy: Box<dyn ArbitrageStrategy>,
    books: HashMap<String, OrderBook>,
    portfolios: HashMap<String, PortfolioSnapshot>,
}

impl ArbitrageEngine {
    pub fn new(feed: Box<dyn MarketDataFeed>, strategy: Box<dyn ArbitrageStrategy>) -> Self {
        Self {
            feed,
            strategy,
            books: HashMap::new(),
            portfolios: HashMap::new(),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        info!(strategy = self.strategy.name(), "starting arbitrage engine");

        let MarketDataReceivers {
            mut quotes,
            order_books: _,
        } = self.feed.connect().await?;

        while let Some(quote) = quotes.recv().await {
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

        warn!("quote stream ended");
        Ok(())
    }
}

/// Canonical book key used by strategies and the engine.
/// Format: `"venue:symbol"` all lowercase.
/// FIXME: replace string symbol with Instrument struct
fn book_key(quote: &Quote) -> String {
    format!("{:?}:{}", quote.venue, quote.symbol).to_lowercase()
}

/// Convert a top-of-book Quote into a minimal OrderBook (single level each side).
fn quote_to_book(q: &Quote) -> OrderBook {
    OrderBook {
        venue: q.venue,
        symbol: q.symbol.clone(),
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
