pub mod aeron;
pub mod flatbuf_codec;

use async_trait::async_trait;

#[async_trait]
pub trait IpcPublisher: Send + Sync {
    async fn publish(&self, data: &[u8]) -> anyhow::Result<()>;
}

#[async_trait]
pub trait IpcSubscriber: Send + Sync {
    async fn poll(&mut self) -> anyhow::Result<Option<Vec<u8>>>;
}
