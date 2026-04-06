use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::models::order::{Fill, Order, OrderUpdate};

pub struct OrderReceivers {
    pub fills: mpsc::UnboundedReceiver<Fill>,
    pub updates: mpsc::UnboundedReceiver<OrderUpdate>,
}

/// Trait for order execution on any venue.
#[async_trait]
pub trait OrderExecutor: Send + Sync {
    async fn connect(&mut self) -> anyhow::Result<OrderReceivers>;
    async fn disconnect(&mut self) -> anyhow::Result<()>;

    async fn submit_order(&self, order: &Order) -> anyhow::Result<String>;

    /// Cancel an order by its venue-assigned ID.
    async fn cancel_order(&self, order_id: &str) -> anyhow::Result<bool>;

    /// Query current status of an order.
    async fn get_order_status(&self, order_id: &str) -> anyhow::Result<OrderUpdate>;
}
