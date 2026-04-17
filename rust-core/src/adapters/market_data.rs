use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::models::market::{OrderBook, Quote};
use crate::models::order::Fill;

pub struct MarketDataReceivers {
    pub quotes: mpsc::UnboundedReceiver<Quote>,
    pub order_books: mpsc::UnboundedReceiver<OrderBook>,
    /// Fills arriving over the same feed (e.g. Aeron IPC where Python sidecar
    /// forwards TW broker fills as MSG_TAG_FILL). `None` for feeds that don't
    /// carry fills (all REST/WS adapters — those use PrivateStream instead).
    pub fills: Option<mpsc::UnboundedReceiver<Fill>>,
}

#[async_trait]
pub trait MarketDataFeed: Send + Sync {
    async fn connect(&mut self) -> anyhow::Result<MarketDataReceivers>;
    async fn disconnect(&mut self) -> anyhow::Result<()>;

    async fn subscribe(&mut self, symbols: &[String]) -> anyhow::Result<()>;
    async fn unsubscribe(&mut self, symbols: &[String]) -> anyhow::Result<()>;
}
