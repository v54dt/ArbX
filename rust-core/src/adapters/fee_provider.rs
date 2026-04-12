use async_trait::async_trait;

use crate::models::fee::FeeSchedule;

#[async_trait]
pub trait FeeProvider: Send + Sync {
    async fn get_fee_schedule(&self) -> anyhow::Result<FeeSchedule>;
}
