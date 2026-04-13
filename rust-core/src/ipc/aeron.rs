use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context as _;
use async_trait::async_trait;
use rusteron_client::*;

use super::{IpcPublisher, IpcSubscriber};

const DEFAULT_STREAM_ID: i32 = 1001;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

pub struct AeronPublisher {
    publication: AeronPublication,
}

impl AeronPublisher {
    pub fn new(stream_id: i32) -> anyhow::Result<Self> {
        let ctx = AeronContext::new().context("aeron context")?;
        let aeron = Aeron::new(&ctx).context("aeron client")?;
        aeron.start().context("aeron start")?;

        let publication = aeron
            .add_publication(AERON_IPC_STREAM, stream_id, CONNECT_TIMEOUT)
            .map_err(|e| anyhow::anyhow!("add publication: {:?}", e))?;

        Ok(Self { publication })
    }

    pub fn with_default_stream() -> anyhow::Result<Self> {
        Self::new(DEFAULT_STREAM_ID)
    }
}

#[async_trait]
impl IpcPublisher for AeronPublisher {
    async fn publish(&self, data: &[u8]) -> anyhow::Result<()> {
        let result = self
            .publication
            .offer(data, Handlers::no_reserved_value_supplier_handler());
        if result >= 0 {
            Ok(())
        } else {
            anyhow::bail!("offer returned {}", result)
        }
    }
}

pub struct AeronSubscriber {
    subscription: AeronSubscription,
    buffer: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl AeronSubscriber {
    pub fn new(stream_id: i32) -> anyhow::Result<Self> {
        let ctx = AeronContext::new().context("aeron context")?;
        let aeron = Aeron::new(&ctx).context("aeron client")?;
        aeron.start().context("aeron start")?;

        let subscription = aeron
            .add_subscription(
                AERON_IPC_STREAM,
                stream_id,
                Handlers::no_available_image_handler(),
                Handlers::no_unavailable_image_handler(),
                CONNECT_TIMEOUT,
            )
            .map_err(|e| anyhow::anyhow!("add subscription: {:?}", e))?;

        Ok(Self {
            subscription,
            buffer: Arc::new(Mutex::new(Vec::new())),
        })
    }

    pub fn with_default_stream() -> anyhow::Result<Self> {
        Self::new(DEFAULT_STREAM_ID)
    }
}

#[async_trait]
impl IpcSubscriber for AeronSubscriber {
    async fn poll(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        let buf = Arc::clone(&self.buffer);
        self.subscription
            .poll_once(
                move |data: &[u8], _header: AeronHeader| {
                    if let Ok(mut b) = buf.lock() {
                        b.push(data.to_vec());
                    }
                },
                10,
            )
            .map_err(|e| anyhow::anyhow!("poll error: {:?}", e))?;

        let mut guard = self
            .buffer
            .lock()
            .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
        if guard.is_empty() {
            Ok(None)
        } else {
            Ok(Some(guard.remove(0)))
        }
    }
}
