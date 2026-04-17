use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use super::order_executor::{OrderExecutor, OrderReceivers};
use crate::models::enums::{OrderStatus, Side};
use crate::models::order::{Fill, Order, OrderUpdate};

#[derive(Default)]
pub struct PaperExecutor {
    fills_tx: Option<mpsc::UnboundedSender<Fill>>,
    updates_tx: Option<mpsc::UnboundedSender<OrderUpdate>>,
    order_counter: AtomicU64,
    fill_delay_ms: u64,
    slippage_bps: Decimal,
}

impl PaperExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Delay between submit and fill emission. 0 = instant (legacy default).
    pub fn with_fill_delay_ms(mut self, ms: u64) -> Self {
        self.fill_delay_ms = ms;
        self
    }

    /// Adverse slippage applied to fill price. Buy fills get order_price * (1 + bps/1e4),
    /// sell fills get order_price * (1 - bps/1e4). 0 = fill at order price (legacy default).
    pub fn with_slippage_bps(mut self, bps: Decimal) -> Self {
        self.slippage_bps = bps;
        self
    }

    fn apply_slippage(&self, side: Side, order_price: Decimal) -> Decimal {
        if self.slippage_bps.is_zero() {
            return order_price;
        }
        let adj = self.slippage_bps / Decimal::from(10_000);
        match side {
            Side::Buy => order_price * (Decimal::ONE + adj),
            Side::Sell => order_price * (Decimal::ONE - adj),
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

        let order_price = match order.price {
            Some(p) => p,
            None => {
                tracing::warn!("paper fill: no price on order, using placeholder");
                Decimal::ONE
            }
        };
        let fill_price = self.apply_slippage(order.side, order_price);

        let fill = Fill {
            order_id: order_id.clone(),
            venue: order.venue,
            instrument: order.instrument.clone(),
            side: order.side,
            price: fill_price,
            quantity: order.quantity,
            fee: Decimal::ZERO,
            fee_currency: order.instrument.quote.clone(),
            filled_at: chrono::Utc::now(),
        };
        let update = OrderUpdate {
            order_id: order_id.clone(),
            status: OrderStatus::Filled,
            filled_quantity: order.quantity,
            remaining_quantity: Decimal::ZERO,
            average_price: Some(fill_price),
            updated_at: chrono::Utc::now(),
        };

        if self.fill_delay_ms == 0 {
            if let Some(tx) = &self.fills_tx {
                let _ = tx.send(fill);
            }
            if let Some(tx) = &self.updates_tx {
                let _ = tx.send(update);
            }
        } else {
            let delay = self.fill_delay_ms;
            let fills_tx = self.fills_tx.clone();
            let updates_tx = self.updates_tx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                if let Some(tx) = fills_tx {
                    let _ = tx.send(fill);
                }
                if let Some(tx) = updates_tx {
                    let _ = tx.send(update);
                }
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
            client_order_id: "test-coid".to_string(),
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
        PaperExecutor::new()
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

    #[tokio::test]
    async fn paper_buy_applies_adverse_slippage() {
        let mut executor = PaperExecutor::new().with_slippage_bps(dec!(10)); // 10 bps
        let receivers = executor.connect().await.unwrap();
        let mut fills = receivers.fills;

        executor.submit_order(&test_order()).await.unwrap();

        let fill = fills.try_recv().unwrap();
        // Buy at 50000 * (1 + 0.001) = 50050
        assert_eq!(fill.price, dec!(50050));
    }

    #[tokio::test]
    async fn paper_sell_applies_adverse_slippage() {
        let mut order = test_order();
        order.side = Side::Sell;

        let mut executor = PaperExecutor::new().with_slippage_bps(dec!(10));
        let receivers = executor.connect().await.unwrap();
        let mut fills = receivers.fills;

        executor.submit_order(&order).await.unwrap();

        let fill = fills.try_recv().unwrap();
        // Sell at 50000 * (1 - 0.001) = 49950
        assert_eq!(fill.price, dec!(49950));
    }

    #[tokio::test]
    async fn paper_fill_delay_defers_emission() {
        let mut executor = PaperExecutor::new().with_fill_delay_ms(50);
        let receivers = executor.connect().await.unwrap();
        let mut fills = receivers.fills;

        executor.submit_order(&test_order()).await.unwrap();

        // Immediately after submit, no fill yet.
        assert!(fills.try_recv().is_err(), "fill should be deferred");

        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        let fill = fills.recv().await.expect("fill arrives after delay");
        assert_eq!(fill.price, dec!(50000));
    }

    #[tokio::test]
    async fn paper_no_slippage_by_default() {
        let mut executor = make_paper();
        let receivers = executor.connect().await.unwrap();
        let mut fills = receivers.fills;
        executor.submit_order(&test_order()).await.unwrap();
        let fill = fills.try_recv().unwrap();
        assert_eq!(fill.price, dec!(50000));
    }
}
