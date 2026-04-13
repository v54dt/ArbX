use std::collections::HashMap;

use async_trait::async_trait;

use crate::models::market::BookMap;
use crate::models::order::OrderRequest;
use crate::models::position::PortfolioSnapshot;

use super::Opportunity;

#[async_trait]
pub trait ArbitrageStrategy: Send + Sync {
    async fn evaluate(
        &self,
        books: &BookMap,
        portfolios: &HashMap<String, PortfolioSnapshot>,
    ) -> Option<Opportunity>;

    fn compute_hedge_orders(&self, opportunity: &Opportunity) -> Vec<OrderRequest>;

    fn name(&self) -> &str;
}
