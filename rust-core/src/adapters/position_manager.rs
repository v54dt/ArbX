use async_trait::async_trait;

use crate::models::order::Fill;
use crate::models::position::{PortfolioSnapshot, Position};

/// Trait for position tracking and reconciliation.
#[async_trait]
pub trait PositionManager: Send + Sync {
    async fn get_position(&self, symbol: &str) -> anyhow::Result<Option<Position>>;
    async fn get_portfolio(&self) -> anyhow::Result<PortfolioSnapshot>;
    async fn sync_positions(&mut self) -> anyhow::Result<()>;
    async fn apply_fill(&mut self, fill: &Fill) -> anyhow::Result<()>;
}
