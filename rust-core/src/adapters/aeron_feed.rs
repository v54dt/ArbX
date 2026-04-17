use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use super::market_data::{MarketDataFeed, MarketDataReceivers};
use crate::ipc::IpcSubscriber;
use crate::ipc::aeron::AeronSubscriber;
use crate::ipc::flatbuf_codec::{
    MSG_TAG_FILL, MSG_TAG_ORDER_BOOK, MSG_TAG_QUOTE, decode_fill, decode_order_book, decode_quote,
};
use crate::models::order::Fill;

/// MarketDataFeed backed by an Aeron Subscriber. Payloads carry a 1-byte type
/// tag (see flatbuf_codec::MSG_TAG_*) because FlatBuffers can't reject the
/// wrong table type at decode time.
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
        let (fill_tx, fill_rx) = mpsc::unbounded_channel::<Fill>();

        let task = tokio::spawn(async move {
            loop {
                match subscriber.poll().await {
                    Ok(Some(bytes)) => {
                        let Some((tag, payload)) = bytes.split_first() else {
                            tracing::debug!("aeron payload empty; skipping");
                            continue;
                        };
                        match *tag {
                            MSG_TAG_QUOTE => match decode_quote(payload) {
                                Ok(quote) => {
                                    if quote_tx.send(quote).is_err() {
                                        break;
                                    }
                                }
                                Err(e) => tracing::debug!(error = %e, "decode_quote failed"),
                            },
                            MSG_TAG_ORDER_BOOK => match decode_order_book(payload) {
                                Ok(book) => {
                                    if book_tx.send(book).is_err() {
                                        break;
                                    }
                                }
                                Err(e) => tracing::debug!(error = %e, "decode_order_book failed"),
                            },
                            MSG_TAG_FILL => match decode_fill(payload) {
                                Ok(fill) => {
                                    if fill_tx.send(fill).is_err() {
                                        break;
                                    }
                                }
                                Err(e) => tracing::debug!(error = %e, "decode_fill failed"),
                            },
                            other => tracing::debug!(tag = other, "unknown msg tag; skipping"),
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
            fills: Some(fill_rx),
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
