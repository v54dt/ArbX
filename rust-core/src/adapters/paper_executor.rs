use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use super::order_executor::{OrderExecutor, OrderReceivers};
use crate::models::enums::OrderStatus;
use crate::models::order::{Fill, Order, OrderUpdate};

pub struct PaperExecutor {
    inner: Box<dyn OrderExecutor>,
    fills_tx: Option<mpsc::UnboundedSender<Fill>>,
    updates_tx: Option<mpsc::UnboundedSender<OrderUpdate>>,
    order_counter: AtomicU64,
}

impl PaperExecutor {
    pub fn new(inner: Box<dyn OrderExecutor>) -> Self {
        Self {
            inner,
            fills_tx: None,
            updates_tx: None,
            order_counter: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl OrderExecutor for PaperExecutor {
    async fn connect(&mut self) -> anyhow::Result<OrderReceivers> {
        let (fills_tx, fills) = mpsc::unbounded_channel::<Fill>();
        let (updates_tx, updates) = mpsc::unbounded_channel::<OrderUpdate>();
        self.fills_tx = Some(fills_tx);
        self.updates_tx = Some(updates_tx);
        tracing::info!("[PAPER] executor connected");
        Ok(OrderReceivers { fills, updates })
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.fills_tx = None;
        self.updates_tx = None;
        tracing::info!("[PAPER] executor disconnected");
        Ok(())
    }

    async fn submit_order(&self, order: &Order) -> anyhow::Result<String> {
        let order_id = format!(
            "paper-{}",
            self.order_counter.fetch_add(1, Ordering::Relaxed)
        );

        tracing::info!(
            order_id,
            side = ?order.side,
            qty = %order.quantity,
            price = ?order.price,
            "[PAPER] order submitted"
        );

        let fill_price = match order.price {
            Some(p) => p,
            None => {
                tracing::warn!("paper fill: no price on order, using placeholder");
                Decimal::ONE
            }
        };

        if let Some(tx) = &self.fills_tx {
            let _ = tx.send(Fill {
                order_id: order_id.clone(),
                venue: order.venue,
                instrument: order.instrument.clone(),
                side: order.side,
                price: fill_price,
                quantity: order.quantity,
                fee: Decimal::ZERO,
                fee_currency: order.instrument.quote.clone(),
                filled_at: chrono::Utc::now(),
            });
        }

        if let Some(tx) = &self.updates_tx {
            let _ = tx.send(OrderUpdate {
                order_id: order_id.clone(),
                status: OrderStatus::Filled,
                filled_quantity: order.quantity,
                remaining_quantity: Decimal::ZERO,
                average_price: Some(fill_price),
                updated_at: chrono::Utc::now(),
            });
        }

        Ok(order_id)
    }

    async fn cancel_order(&self, order_id: &str) -> anyhow::Result<bool> {
        tracing::info!(order_id, "[PAPER] cancel");
        Ok(true)
    }

    async fn get_order_status(&self, order_id: &str) -> anyhow::Result<OrderUpdate> {
        Ok(OrderUpdate {
            order_id: order_id.to_string(),
            status: OrderStatus::Filled,
            filled_quantity: Decimal::ZERO,
            remaining_quantity: Decimal::ZERO,
            average_price: None,
            updated_at: chrono::Utc::now(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::binance::market_data::BinanceMarket;
    use crate::adapters::binance::order_executor::BinanceOrderExecutor;
    use crate::models::enums::{OrderType, Side, Venue};
    use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
    use rust_decimal_macros::dec;

    fn test_instrument() -> Instrument {
        Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: InstrumentType::Spot,
            base: "BTC".into(),
            quote: "USDT".into(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        }
    }

    fn test_order() -> Order {
        Order {
            id: String::new(),
            venue: Venue::Binance,
            instrument: test_instrument(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            time_in_force: None,
            price: Some(dec!(50000)),
            quantity: dec!(0.1),
            created_at: chrono::Utc::now(),
        }
    }

    fn make_paper() -> PaperExecutor {
        let inner = Box::new(
            BinanceOrderExecutor::new(BinanceMarket::Spot, String::new(), String::new()).unwrap(),
        );
        PaperExecutor::new(inner)
    }

    #[tokio::test]
    async fn paper_submit_returns_paper_id() {
        let executor = make_paper();
        let id = executor.submit_order(&test_order()).await.unwrap();
        assert!(id.starts_with("paper-"));
    }

    #[tokio::test]
    async fn paper_submit_generates_fill() {
        let mut executor = make_paper();
        let receivers = executor.connect().await.unwrap();
        let mut fills = receivers.fills;

        executor.submit_order(&test_order()).await.unwrap();

        let fill = fills.try_recv().unwrap();
        assert!(fill.order_id.starts_with("paper-"));
        assert_eq!(fill.price, dec!(50000));
        assert_eq!(fill.quantity, dec!(0.1));
    }

    #[tokio::test]
    async fn paper_submit_generates_filled_update() {
        let mut executor = make_paper();
        let receivers = executor.connect().await.unwrap();
        let mut updates = receivers.updates;

        executor.submit_order(&test_order()).await.unwrap();

        let update = updates.try_recv().unwrap();
        assert_eq!(update.status, OrderStatus::Filled);
        assert_eq!(update.filled_quantity, dec!(0.1));
        assert_eq!(update.remaining_quantity, Decimal::ZERO);
    }

    #[tokio::test]
    async fn paper_cancel_always_succeeds() {
        let executor = make_paper();
        let result = executor.cancel_order("paper-0").await.unwrap();
        assert!(result);
    }

    #[tokio::test]
    async fn paper_ids_increment() {
        let executor = make_paper();
        let id1 = executor.submit_order(&test_order()).await.unwrap();
        let id2 = executor.submit_order(&test_order()).await.unwrap();
        assert_eq!(id1, "paper-0");
        assert_eq!(id2, "paper-1");
    }
}
