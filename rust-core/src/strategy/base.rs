use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::engine::signal::SignalCache;
use crate::models::market::BookMap;
use crate::models::order::OrderRequest;
use crate::models::position::PortfolioSnapshot;

use super::Opportunity;

#[async_trait]
pub trait ArbitrageStrategy: Send + Sync {
    /// `now` is the engine's clock timestamp — use this instead of
    /// `Utc::now()` for staleness checks and opportunity timestamps
    /// so backtest / replay produce deterministic results.
    async fn evaluate(
        &self,
        books: &BookMap,
        portfolios: &HashMap<String, PortfolioSnapshot>,
        now: DateTime<Utc>,
        signals: &SignalCache,
    ) -> Option<Opportunity>;

    fn compute_hedge_orders(&self, opportunity: &Opportunity) -> Vec<OrderRequest>;

    /// Re-check the opportunity against the CURRENT book snapshot right before
    /// submit. Returns `None` to skip (book moved, no longer profitable) or
    /// `Some(updated)` with refreshed economics. Default impl is a passthrough
    /// — strategies override when they're spread-sensitive.
    fn re_verify(&self, opp: &Opportunity, _books: &BookMap) -> Option<Opportunity> {
        Some(opp.clone())
    }

    fn name(&self) -> &str;
}
