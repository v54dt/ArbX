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
        }
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

    pub fn positions_handle(&self) -> Arc<Mutex<HashMap<String, Position>>> {
        self.positions.clone()
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

        Ok(MarketDataReceivers {
            quotes: quotes_rx,
            order_books: books_rx,
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

            if self.fill_delay_ms > 0 {
                let delay = self.fill_delay_ms;
                let fills_tx = self.fills_tx.clone();
                let updates_tx = self.updates_tx.clone();
                let oid = order_id.clone();
                let venue = order.venue;
                let instrument = order.instrument.clone();
                let side = order.side;
                let quantity = order.quantity;
                let quote_ccy = order.instrument.quote.clone();

                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    if let Some(tx) = &fills_tx {
                        let _ = tx.send(Fill {
                            order_id: oid.clone(),
                            venue,
                            instrument: instrument.clone(),
                            side,
                            price: fill_price,
                            quantity,
                            fee: Decimal::ZERO,
                            fee_currency: quote_ccy,
                            filled_at: chrono::Utc::now(),
                        });
                    }
                    if let Some(tx) = &updates_tx {
                        let _ = tx.send(OrderUpdate {
                            order_id: oid,
                            status: OrderStatus::Filled,
                            filled_quantity: quantity,
                            remaining_quantity: Decimal::ZERO,
                            average_price: Some(fill_price),
                            updated_at: chrono::Utc::now(),
                        });
                    }
                });
            } else {
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
            }
        }

        Ok(order_id)
    }

    async fn cancel_order(&self, _order_id: &str) -> anyhow::Result<bool> {
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
