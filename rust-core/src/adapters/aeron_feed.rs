use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use super::market_data::{MarketDataFeed, MarketDataReceivers};
use crate::ipc::IpcSubscriber;
use crate::ipc::aeron::AeronSubscriber;
use crate::ipc::flatbuf_codec::{decode_order_book, decode_quote};

/// MarketDataFeed backed by an Aeron Subscriber. Polls the IPC stream,
/// tries Quote then OrderBook flatbuffers decoding, forwards to engine.
pub struct AeronMarketDataFeed {
    stream_id: i32,
    poll_task: Option<JoinHandle<()>>,
}

impl AeronMarketDataFeed {
    pub fn new(stream_id: i32) -> Self {
        Self {
            stream_id,
            poll_task: None,
        }
    }
}

#[async_trait]
impl MarketDataFeed for AeronMarketDataFeed {
    async fn connect(&mut self) -> anyhow::Result<MarketDataReceivers> {
        let mut subscriber = AeronSubscriber::new(self.stream_id)?;
        info!(stream_id = self.stream_id, "AeronMarketDataFeed connected");

        let (quote_tx, quote_rx) = mpsc::unbounded_channel();
        let (book_tx, book_rx) = mpsc::unbounded_channel();

        let task = tokio::spawn(async move {
            loop {
                match subscriber.poll().await {
                    Ok(Some(bytes)) => {
                        if let Ok(book) = decode_order_book(&bytes) {
                            if book_tx.send(book).is_err() {
                                break;
                            }
                        } else if let Ok(quote) = decode_quote(&bytes) {
                            if quote_tx.send(quote).is_err() {
                                break;
                            }
                        } else {
                            tracing::debug!(
                                bytes = bytes.len(),
                                "aeron payload not Quote or OrderBook; skipping"
                            );
                        }
                    }
                    Ok(None) => {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    }
                    Err(e) => {
                        warn!(error = %e, "Aeron poll error; sleeping before retry");
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                }
            }
        });

        self.poll_task = Some(task);
        Ok(MarketDataReceivers {
            quotes: quote_rx,
            order_books: book_rx,
        })
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if let Some(t) = self.poll_task.take() {
            t.abort();
        }
        Ok(())
    }

    async fn subscribe(&mut self, _symbols: &[String]) -> anyhow::Result<()> {
        Ok(())
    }

    async fn unsubscribe(&mut self, _symbols: &[String]) -> anyhow::Result<()> {
        Ok(())
    }
}
