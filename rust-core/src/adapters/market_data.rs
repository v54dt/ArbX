use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::models::market::{OrderBook, Quote};

pub struct MarketDataReceivers {
    pub quotes: mpsc::UnboundedReceiver<Quote>,
    pub order_books: mpsc::UnboundedReceiver<OrderBook>,
}

#[async_trait]
pub trait MarketDataFeed: Send + Sync {
    async fn connect(&mut self) -> anyhow::Result<MarketDataReceivers>;
    async fn disconnect(&mut self) -> anyhow::Result<()>;

    async fn subscribe(&mut self, symbols: &[String]) -> anyhow::Result<()>;
    async fn unsubscribe(&mut self, symbols: &[String]) -> anyhow::Result<()>;
}
