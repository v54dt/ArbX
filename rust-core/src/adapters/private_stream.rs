use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::models::order::{Fill, OrderUpdate};

pub struct PrivateStreamReceivers {
    pub fills: mpsc::UnboundedReceiver<Fill>,
    pub order_updates: mpsc::UnboundedReceiver<OrderUpdate>,
}

#[async_trait]
pub trait PrivateStream: Send + Sync {
    async fn connect(&mut self) -> anyhow::Result<PrivateStreamReceivers>;
    async fn disconnect(&mut self) -> anyhow::Result<()>;
}
