use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use super::market_data::BinanceMarket;
use crate::adapters::order_executor::{OrderExecutor, OrderReceivers};
use crate::models::enums::{OrderStatus, OrderType, Side};
use crate::models::order::{Fill, Order, OrderUpdate};

/// Scaffold `OrderExecutor` implementation for Binance.
pub struct BinanceOrderExecutor {
    market: BinanceMarket,
    api_key: String,
    api_secret: String,
    base_url: String,
    fills_tx: Option<mpsc::UnboundedSender<Fill>>,
    updates_tx: Option<mpsc::UnboundedSender<OrderUpdate>>,
}

impl BinanceOrderExecutor {
    pub fn new(market: BinanceMarket) -> Self {
        let base_url = match market {
            BinanceMarket::Spot => "https://api.binance.com",
            BinanceMarket::UsdtFutures => "https://fapi.binance.com",
            BinanceMarket::CoinFutures => "https://dapi.binance.com",
        }
        .to_string();

        Self {
            market,
            api_key: String::new(),
            api_secret: String::new(),
            base_url,
            fills_tx: None,
            updates_tx: None,
        }
    }

    fn market_label(&self) -> &'static str {
        match self.market {
            BinanceMarket::Spot => "spot",
            BinanceMarket::UsdtFutures => "usdt-futures",
            BinanceMarket::CoinFutures => "coin-futures",
        }
    }

    fn side_str(side: Side) -> &'static str {
        match side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }
    }

    fn order_type_str(ot: OrderType) -> &'static str {
        match ot {
            OrderType::Limit => "LIMIT",
            OrderType::Market => "MARKET",
        }
    }
}

#[async_trait]
impl OrderExecutor for BinanceOrderExecutor {
    async fn connect(&mut self) -> anyhow::Result<OrderReceivers> {
        let (fills_tx, fills) = mpsc::unbounded_channel::<Fill>();
        let (updates_tx, updates) = mpsc::unbounded_channel::<OrderUpdate>();
        self.fills_tx = Some(fills_tx);
        self.updates_tx = Some(updates_tx);
        tracing::info!(
            market = self.market_label(),
            "binance executor connected (stub)"
        );
        Ok(OrderReceivers { fills, updates })
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.fills_tx = None;
        self.updates_tx = None;
        tracing::info!("binance executor disconnected");
        Ok(())
    }

    async fn submit_order(&self, order: &Order) -> anyhow::Result<String> {
        tracing::info!(
            venue = ?order.venue,
            side = ?order.side,
            qty = %order.quantity,
            "submit_order (stub)"
        );
        // TODO: real Binance REST POST /fapi/v1/order
        Ok(format!(
            "stub-{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ))
    }

    async fn cancel_order(&self, order_id: &str) -> anyhow::Result<bool> {
        tracing::info!(order_id, "cancel_order (stub)");
        // TODO: real Binance REST DELETE /fapi/v1/order
        Ok(true)
    }

    async fn get_order_status(&self, order_id: &str) -> anyhow::Result<OrderUpdate> {
        // TODO: real Binance REST GET /fapi/v1/order
        Ok(OrderUpdate {
            order_id: order_id.to_string(),
            status: OrderStatus::Pending,
            filled_quantity: Decimal::ZERO,
            remaining_quantity: Decimal::ZERO,
            average_price: None,
            updated_at: chrono::Utc::now(),
        })
    }
}
