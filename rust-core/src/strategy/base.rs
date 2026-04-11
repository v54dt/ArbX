use std::collections::HashMap;

use async_trait::async_trait;

use crate::models::market::OrderBook;
use crate::models::order::Order;
use crate::models::position::PortfolioSnapshot;

use super::Opportunity;

/// Trait for arbitrage strategy evaluation.
#[async_trait]
pub trait ArbitrageStrategy: Send + Sync {
    async fn evaluate(
        &self,
        books: &HashMap<String, OrderBook>,
        portfolios: &HashMap<String, PortfolioSnapshot>,
    ) -> Option<Opportunity>;

    fn compute_hedge_orders(&self, opportunity: &Opportunity) -> Vec<Order>;

    fn name(&self) -> &str;
}
