use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

use super::market_data::{MarketDataFeed, MarketDataReceivers};
use super::order_executor::{OrderExecutor, OrderReceivers};
use super::position_manager::PositionManager;
use crate::engine::signal::ExternalSignal;
use crate::models::enums::{OrderStatus, Side};
use crate::models::market::{OrderBook, Quote};
use crate::models::order::{Fill, Order, OrderUpdate};
use crate::models::position::{PortfolioSnapshot, Position};

pub struct MockExchange {
    quotes: Vec<Quote>,
    quote_interval_ms: u64,
    orders: Arc<Mutex<HashMap<String, Order>>>,
    next_order_id: AtomicU64,
    positions: Arc<Mutex<HashMap<String, Position>>>,
    fill_delay_ms: u64,
    fill_rate: f64,
    fills_tx: Option<mpsc::UnboundedSender<Fill>>,
    updates_tx: Option<mpsc::UnboundedSender<OrderUpdate>>,
    fail_submit: bool,
    cancels: Arc<Mutex<Vec<String>>>,
    partial_fill_probability: f64,
    ack_delay_ms: u64,
    cancel_success_rate: f64,
    fee_rate: Decimal,
    /// External signals to emit on connect (timing-independent, all sent
    /// before the receiver is returned). Tests use this to verify the
    /// engine's signal-channel pump.
    pre_load_signals: Vec<ExternalSignal>,
}

impl MockExchange {
    pub fn new(quotes: Vec<Quote>, fill_delay_ms: u64, fill_rate: f64) -> Self {
        Self {
            quotes,
            quote_interval_ms: 10,
            orders: Arc::new(Mutex::new(HashMap::new())),
            next_order_id: AtomicU64::new(0),
            positions: Arc::new(Mutex::new(HashMap::new())),
            fill_delay_ms,
            fill_rate,
            fills_tx: None,
            updates_tx: None,
            fail_submit: false,
            cancels: Arc::new(Mutex::new(Vec::new())),
            partial_fill_probability: 0.0,
            ack_delay_ms: 0,
            cancel_success_rate: 1.0,
            fee_rate: Decimal::ZERO,
            pre_load_signals: Vec::new(),
        }
    }

    /// Pre-load signals to emit on connect. The signal channel is then closed
    /// (sender dropped) so receivers won't block waiting for more.
    pub fn with_signals(mut self, signals: Vec<ExternalSignal>) -> Self {
        self.pre_load_signals = signals;
        self
    }

    pub fn with_quote_interval(mut self, ms: u64) -> Self {
        self.quote_interval_ms = ms;
        self
    }

    /// Causes submit_order to return Err every time — lets tests drive the
    /// CircuitBreaker via record_failure without needing real exchange down-time.
    pub fn with_submit_failure(mut self) -> Self {
        self.fail_submit = true;
        self
    }

    /// Fraction of fills that are partial (50% of requested qty). Default 0.0.
    pub fn with_partial_fill_probability(mut self, prob: f64) -> Self {
        self.partial_fill_probability = prob;
        self
    }

    /// Simulated network latency before returning from submit_order. Default 0.
    pub fn with_ack_delay_ms(mut self, ms: u64) -> Self {
        self.ack_delay_ms = ms;
        self
    }

    /// Fraction of cancel_order calls that succeed. Default 1.0.
    pub fn with_cancel_success_rate(mut self, rate: f64) -> Self {
        self.cancel_success_rate = rate;
        self
    }

    /// Fee as fraction of notional applied to each fill. Default ZERO.
    pub fn with_fee_rate(mut self, rate: Decimal) -> Self {
        self.fee_rate = rate;
        self
    }

    pub fn positions_handle(&self) -> Arc<Mutex<HashMap<String, Position>>> {
        self.positions.clone()
    }

    /// Handle for tests to inspect which order_ids were cancelled.
    pub fn cancels_handle(&self) -> Arc<Mutex<Vec<String>>> {
        self.cancels.clone()
    }

    /// Handle for tests to inspect which orders were submitted (keyed by
    /// generated venue order_id). Useful for verifying multi-executor
    /// routing — each per-venue executor only sees orders for its venue.
    pub fn orders_handle(&self) -> Arc<Mutex<HashMap<String, Order>>> {
        self.orders.clone()
    }
}

#[async_trait]
impl MarketDataFeed for MockExchange {
    async fn connect(&mut self) -> anyhow::Result<MarketDataReceivers> {
        let (quotes_tx, quotes_rx) = mpsc::unbounded_channel::<Quote>();
        let (_books_tx, books_rx) = mpsc::unbounded_channel::<OrderBook>();

        let quotes = self.quotes.clone();
        let interval_ms = self.quote_interval_ms;

        tokio::spawn(async move {
            for q in quotes {
                if quotes_tx.send(q).is_err() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
            }
        });

        let signals_rx = if self.pre_load_signals.is_empty() {
            None
        } else {
            let (signal_tx, signal_rx) = mpsc::unbounded_channel::<ExternalSignal>();
            for s in self.pre_load_signals.drain(..) {
                let _ = signal_tx.send(s);
            }
            Some(signal_rx)
        };

        Ok(MarketDataReceivers {
            quotes: quotes_rx,
            order_books: books_rx,
            fills: None,
            signals: signals_rx,
        })
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn subscribe(&mut self, _symbols: &[String]) -> anyhow::Result<()> {
        Ok(())
    }

    async fn unsubscribe(&mut self, _symbols: &[String]) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl OrderExecutor for MockExchange {
    async fn connect(&mut self) -> anyhow::Result<OrderReceivers> {
        let (fills_tx, fills_rx) = mpsc::unbounded_channel::<Fill>();
        let (updates_tx, updates_rx) = mpsc::unbounded_channel::<OrderUpdate>();
        self.fills_tx = Some(fills_tx);
        self.updates_tx = Some(updates_tx);
        Ok(OrderReceivers {
            fills: fills_rx,
            updates: updates_rx,
        })
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.fills_tx = None;
        self.updates_tx = None;
        Ok(())
    }

    async fn submit_order(&self, order: &Order) -> anyhow::Result<String> {
        if self.fail_submit {
            anyhow::bail!("mock submit forced-fail");
        }

        if self.ack_delay_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(self.ack_delay_ms)).await;
        }

        let order_id = format!(
            "mock-{}",
            self.next_order_id.fetch_add(1, Ordering::Relaxed)
        );

        {
            let mut orders = self.orders.lock().await;
            let mut tracked = order.clone();
            tracked.id = order_id.clone();
            orders.insert(order_id.clone(), tracked);
        }

        let should_fill = rand_fill(self.fill_rate);

        if should_fill {
            let fill_price = order.price.unwrap_or(Decimal::ONE);
            let is_partial = rand_fill(self.partial_fill_probability);
            let fill_qty = if is_partial {
                order.quantity / Decimal::from(2)
            } else {
                order.quantity
            };
            let remaining = order.quantity - fill_qty;
            let status = if remaining > Decimal::ZERO {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::Filled
            };
            let fee = fill_price * fill_qty * self.fee_rate;

            if self.fill_delay_ms > 0 {
                let delay = self.fill_delay_ms;
                let fills_tx = self.fills_tx.clone();
                let updates_tx = self.updates_tx.clone();
                let oid = order_id.clone();
                let client_oid = order.client_order_id.clone();
                let venue = order.venue;
                let instrument = order.instrument.clone();
                let side = order.side;
                let quote_ccy = order.instrument.quote.clone();

                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    if let Some(tx) = &fills_tx {
                        let _ = tx.send(Fill {
                            order_id: oid.clone(),
                            client_order_id: Some(client_oid.clone()),
                            venue,
                            instrument: instrument.clone(),
                            side,
                            price: fill_price,
                            quantity: fill_qty,
                            fee,
                            fee_currency: quote_ccy,
                            filled_at: chrono::Utc::now(),
                        });
                    }
                    if let Some(tx) = &updates_tx {
                        let _ = tx.send(OrderUpdate {
                            order_id: oid,
                            status,
                            filled_quantity: fill_qty,
                            remaining_quantity: remaining,
                            average_price: Some(fill_price),
                            updated_at: chrono::Utc::now(),
                        });
                    }
                });
            } else {
                if let Some(tx) = &self.fills_tx {
                    let _ = tx.send(Fill {
                        order_id: order_id.clone(),
                        client_order_id: Some(order.client_order_id.clone()),
                        venue: order.venue,
                        instrument: order.instrument.clone(),
                        side: order.side,
                        price: fill_price,
                        quantity: fill_qty,
                        fee,
                        fee_currency: order.instrument.quote.clone(),
                        filled_at: chrono::Utc::now(),
                    });
                }
                if let Some(tx) = &self.updates_tx {
                    let _ = tx.send(OrderUpdate {
                        order_id: order_id.clone(),
                        status,
                        filled_quantity: fill_qty,
                        remaining_quantity: remaining,
                        average_price: Some(fill_price),
                        updated_at: chrono::Utc::now(),
                    });
                }
            }
        }

        Ok(order_id)
    }

    async fn cancel_order(&self, order_id: &str) -> anyhow::Result<bool> {
        self.cancels.lock().await.push(order_id.to_string());
        Ok(rand_fill(self.cancel_success_rate))
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

#[async_trait]
impl PositionManager for MockExchange {
    async fn get_position(&self, symbol: &str) -> anyhow::Result<Option<Position>> {
        let positions = self.positions.lock().await;
        Ok(positions.get(symbol).cloned())
    }

    async fn get_portfolio(&self) -> anyhow::Result<PortfolioSnapshot> {
        let positions = self.positions.lock().await;
        let pos_vec: Vec<Position> = positions.values().cloned().collect();
        let total_unrealized: Decimal = pos_vec.iter().map(|p| p.unrealized_pnl).sum();
        let total_realized: Decimal = pos_vec.iter().map(|p| p.realized_pnl).sum();
        Ok(PortfolioSnapshot {
            venue: crate::models::enums::Venue::Binance,
            positions: pos_vec,
            total_equity: Decimal::ZERO,
            available_balance: Decimal::ZERO,
            unrealized_pnl: total_unrealized,
            realized_pnl: total_realized,
        })
    }

    async fn sync_positions(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn apply_fill(&mut self, fill: &Fill) -> anyhow::Result<()> {
        let key = format!("{:?}:{}", fill.venue, fill.instrument.base).to_lowercase();
        let mut positions = self.positions.lock().await;
        let pos = positions.entry(key).or_insert_with(|| Position {
            venue: fill.venue,
            instrument: fill.instrument.clone(),
            quantity: Decimal::ZERO,
            average_cost: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            settlement_date: None,
        });
        match fill.side {
            Side::Buy => pos.quantity += fill.quantity,
            Side::Sell => pos.quantity -= fill.quantity,
        }
        pos.average_cost = fill.price;
        Ok(())
    }
}

fn rand_fill(fill_rate: f64) -> bool {
    if fill_rate >= 1.0 {
        return true;
    }
    if fill_rate <= 0.0 {
        return false;
    }
    let val = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as f64
        / 1_000_000_000.0;
    val < fill_rate
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::enums::{OrderType, TimeInForce, Venue};
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
            client_order_id: "test-coid-1".into(),
            venue: Venue::Binance,
            instrument: test_instrument(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::Ioc),
            price: Some(dec!(50000)),
            quantity: dec!(2),
            created_at: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn partial_fill_probability_zero_gives_full_fill() {
        let mut mock = MockExchange::new(vec![], 0, 1.0).with_partial_fill_probability(0.0);
        let recvs = OrderExecutor::connect(&mut mock).await.unwrap();
        let mut fills_rx = recvs.fills;

        let _ = mock.submit_order(&test_order()).await.unwrap();
        let fill = fills_rx.recv().await.unwrap();
        assert_eq!(fill.quantity, dec!(2));
    }

    #[tokio::test]
    async fn partial_fill_probability_one_gives_half_fill() {
        let mut mock = MockExchange::new(vec![], 0, 1.0).with_partial_fill_probability(1.0);
        let recvs = OrderExecutor::connect(&mut mock).await.unwrap();
        let mut fills_rx = recvs.fills;
        let mut updates_rx = recvs.updates;

        let _ = mock.submit_order(&test_order()).await.unwrap();
        let fill = fills_rx.recv().await.unwrap();
        assert_eq!(fill.quantity, dec!(1));

        let update = updates_rx.recv().await.unwrap();
        assert_eq!(update.filled_quantity, dec!(1));
        assert_eq!(update.remaining_quantity, dec!(1));
        assert_eq!(update.status, OrderStatus::PartiallyFilled);
    }

    #[tokio::test]
    async fn ack_delay_adds_latency() {
        let mut mock = MockExchange::new(vec![], 0, 1.0).with_ack_delay_ms(50);
        let _recvs = OrderExecutor::connect(&mut mock).await.unwrap();

        let start = std::time::Instant::now();
        let _ = mock.submit_order(&test_order()).await.unwrap();
        let elapsed = start.elapsed();
        assert!(elapsed >= std::time::Duration::from_millis(40));
    }

    #[tokio::test]
    async fn cancel_success_rate_zero_always_fails() {
        let mock = MockExchange::new(vec![], 0, 1.0).with_cancel_success_rate(0.0);
        let result = mock.cancel_order("order-1").await.unwrap();
        assert!(!result);
    }

    #[tokio::test]
    async fn cancel_success_rate_one_always_succeeds() {
        let mock = MockExchange::new(vec![], 0, 1.0).with_cancel_success_rate(1.0);
        let result = mock.cancel_order("order-1").await.unwrap();
        assert!(result);
        // Also check it's still recorded
        let cancels = mock.cancels_handle();
        assert_eq!(cancels.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn fee_rate_applied_to_fill() {
        let mut mock = MockExchange::new(vec![], 0, 1.0).with_fee_rate(dec!(0.001)); // 10 bps
        let recvs = OrderExecutor::connect(&mut mock).await.unwrap();
        let mut fills_rx = recvs.fills;

        let _ = mock.submit_order(&test_order()).await.unwrap();
        let fill = fills_rx.recv().await.unwrap();
        // notional = 50000 * 2 = 100000, fee = 100000 * 0.001 = 100
        assert_eq!(fill.fee, dec!(100));
    }

    #[tokio::test]
    async fn fee_rate_default_zero() {
        let mut mock = MockExchange::new(vec![], 0, 1.0);
        let recvs = OrderExecutor::connect(&mut mock).await.unwrap();
        let mut fills_rx = recvs.fills;

        let _ = mock.submit_order(&test_order()).await.unwrap();
        let fill = fills_rx.recv().await.unwrap();
        assert_eq!(fill.fee, Decimal::ZERO);
    }

    #[tokio::test]
    async fn fee_rate_with_partial_fill() {
        let mut mock = MockExchange::new(vec![], 0, 1.0)
            .with_fee_rate(dec!(0.001))
            .with_partial_fill_probability(1.0);
        let recvs = OrderExecutor::connect(&mut mock).await.unwrap();
        let mut fills_rx = recvs.fills;

        let _ = mock.submit_order(&test_order()).await.unwrap();
        let fill = fills_rx.recv().await.unwrap();
        // partial: qty=1, notional = 50000*1 = 50000, fee = 50
        assert_eq!(fill.quantity, dec!(1));
        assert_eq!(fill.fee, dec!(50));
    }

    #[tokio::test]
    async fn builders_chainable() {
        let mock = MockExchange::new(vec![], 0, 1.0)
            .with_partial_fill_probability(0.5)
            .with_ack_delay_ms(10)
            .with_cancel_success_rate(0.8)
            .with_fee_rate(dec!(0.0005))
            .with_quote_interval(100);
        // Just verify it compiles and the struct is usable
        assert_eq!(mock.partial_fill_probability, 0.5);
        assert_eq!(mock.ack_delay_ms, 10);
        assert_eq!(mock.cancel_success_rate, 0.8);
        assert_eq!(mock.fee_rate, dec!(0.0005));
    }

    #[tokio::test]
    async fn fill_delay_with_partial_fill_and_fee() {
        let mut mock = MockExchange::new(vec![], 10, 1.0)
            .with_partial_fill_probability(1.0)
            .with_fee_rate(dec!(0.002));
        let recvs = OrderExecutor::connect(&mut mock).await.unwrap();
        let mut fills_rx = recvs.fills;

        let _ = mock.submit_order(&test_order()).await.unwrap();
        let fill = tokio::time::timeout(std::time::Duration::from_millis(500), fills_rx.recv())
            .await
            .unwrap()
            .unwrap();
        // partial: qty=1, notional=50000, fee=50000*0.002=100
        assert_eq!(fill.quantity, dec!(1));
        assert_eq!(fill.fee, dec!(100));
    }
}
