use std::collections::HashMap;

use anyhow::Result;
use futures_util::future::join_all;
use tokio::sync::mpsc;
use tracing::{info, warn};

use chrono::Utc;
use smallvec::SmallVec;

use std::sync::Arc;

use crate::adapters::market_data::{MarketDataFeed, MarketDataReceivers};
use crate::adapters::order_executor::OrderExecutor;
use crate::adapters::position_manager::PositionManager;
use crate::adapters::private_stream::PrivateStream;
use crate::ipc::IpcPublisher;
use crate::models::market::{BookMap, OrderBook, OrderBookLevel, Quote, book_key};
use crate::models::order::Fill;
use crate::models::position::PortfolioSnapshot;
use crate::models::trade_log::{TradeLeg, TradeLog, TradeLogWriter, TradeOutcome};
use crate::risk::circuit_breaker::CircuitBreaker;
use crate::risk::manager::RiskManager;
use crate::risk::state::RiskState;
use crate::strategy::base::ArbitrageStrategy;
use rust_decimal::Decimal;

struct IntendedFill {
    side: crate::models::enums::Side,
    intended_price: Decimal,
}

const FILL_DEDUP_CAPACITY: usize = 1024;

fn fill_fingerprint(fill: &Fill) -> String {
    format!(
        "{}:{}:{}:{}",
        fill.order_id,
        fill.filled_at.timestamp_millis(),
        fill.price,
        fill.quantity
    )
}

/// Main loop: receive quote → update local book → evaluate strategy → log opportunity.
pub struct ArbitrageEngine {
    feeds: Vec<Box<dyn MarketDataFeed>>,
    strategy: Box<dyn ArbitrageStrategy>,
    risk_manager: RiskManager,
    risk_state: RiskState,
    circuit_breaker: CircuitBreaker,
    executor: Box<dyn OrderExecutor>,
    position_manager: Box<dyn PositionManager>,
    private_streams: Vec<Box<dyn PrivateStream>>,
    quote_publishers: Vec<Arc<dyn IpcPublisher>>,
    books: BookMap,
    portfolios: HashMap<String, PortfolioSnapshot>,
    trade_logs: Vec<TradeLog>,
    intended_fills: HashMap<String, IntendedFill>,
    pending_cancels: Vec<(chrono::DateTime<Utc>, String)>,
    seen_fills: std::collections::VecDeque<String>,
    seen_fills_set: std::collections::HashSet<String>,
    reconcile_interval_secs: u64,
    order_ttl_secs: u64,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    /// Optional append-only audit-trail writer. If Some, every TradeLog is
    /// flushed to disk on creation; logs survive engine crash.
    trade_log_writer: Option<TradeLogWriter>,
}

impl ArbitrageEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        feeds: Vec<Box<dyn MarketDataFeed>>,
        strategy: Box<dyn ArbitrageStrategy>,
        risk_manager: RiskManager,
        risk_state: RiskState,
        circuit_breaker: CircuitBreaker,
        executor: Box<dyn OrderExecutor>,
        position_manager: Box<dyn PositionManager>,
        private_streams: Vec<Box<dyn PrivateStream>>,
        quote_publishers: Vec<Arc<dyn IpcPublisher>>,
        reconcile_interval_secs: u64,
        order_ttl_secs: u64,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        Self {
            feeds,
            strategy,
            risk_manager,
            risk_state,
            circuit_breaker,
            executor,
            position_manager,
            private_streams,
            quote_publishers,
            books: BookMap::default(),
            portfolios: HashMap::new(),
            trade_logs: vec![],
            intended_fills: HashMap::new(),
            pending_cancels: Vec::new(),
            seen_fills: std::collections::VecDeque::with_capacity(FILL_DEDUP_CAPACITY),
            seen_fills_set: std::collections::HashSet::with_capacity(FILL_DEDUP_CAPACITY),
            reconcile_interval_secs,
            order_ttl_secs,
            shutdown_rx,
            trade_log_writer: None,
        }
    }

    /// Attach an append-only TradeLogWriter so every recorded TradeLog is also
    /// flushed to disk for audit / crash recovery.
    pub fn with_trade_log_writer(mut self, writer: TradeLogWriter) -> Self {
        self.trade_log_writer = Some(writer);
        self
    }

    pub fn trade_logs(&self) -> &[TradeLog] {
        &self.trade_logs
    }

    pub async fn run(&mut self) -> Result<()> {
        info!(
            strategy = self.strategy.name(),
            feeds = self.feeds.len(),
            "starting arbitrage engine"
        );

        let (merged_tx, mut merged_rx) = mpsc::unbounded_channel::<Quote>();
        let (merged_book_tx, mut merged_book_rx) = mpsc::unbounded_channel::<OrderBook>();

        for feed in self.feeds.iter_mut() {
            let MarketDataReceivers {
                mut quotes,
                mut order_books,
            } = feed.connect().await?;
            let tx = merged_tx.clone();
            let btx = merged_book_tx.clone();
            let publishers = self.quote_publishers.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        Some(q) = quotes.recv() => {
                            for pub_ in publishers.iter() {
                                let payload = crate::ipc::flatbuf_codec::encode_quote(&q);
                                let mut tagged = Vec::with_capacity(payload.len() + 1);
                                tagged.push(crate::ipc::flatbuf_codec::MSG_TAG_QUOTE);
                                tagged.extend_from_slice(&payload);
                                if let Err(e) = pub_.publish(&tagged).await {
                                    tracing::debug!(error = %e, "ipc quote publish failed");
                                }
                            }
                            if tx.send(q).is_err() { break; }
                        }
                        Some(ob) = order_books.recv() => {
                            for pub_ in publishers.iter() {
                                let payload = crate::ipc::flatbuf_codec::encode_order_book(&ob);
                                let mut tagged = Vec::with_capacity(payload.len() + 1);
                                tagged.push(crate::ipc::flatbuf_codec::MSG_TAG_ORDER_BOOK);
                                tagged.extend_from_slice(&payload);
                                if let Err(e) = pub_.publish(&tagged).await {
                                    tracing::debug!(error = %e, "ipc order_book publish failed");
                                }
                            }
                            if btx.send(ob).is_err() { break; }
                        }
                        else => break,
                    }
                }
            });
        }

        drop(merged_tx);
        drop(merged_book_tx);

        // Connect executor and forward fills into a merged channel.
        let exec_receivers = self.executor.connect().await?;
        let (fill_tx, mut fill_rx) = mpsc::unbounded_channel::<Fill>();
        {
            let tx = fill_tx.clone();
            let mut fills = exec_receivers.fills;
            tokio::spawn(async move {
                while let Some(f) = fills.recv().await {
                    if tx.send(f).is_err() {
                        break;
                    }
                }
            });
        }

        for stream in self.private_streams.iter_mut() {
            match stream.connect().await {
                Ok(ps_receivers) => {
                    let tx = fill_tx.clone();
                    let mut fills = ps_receivers.fills;
                    tokio::spawn(async move {
                        while let Some(f) = fills.recv().await {
                            if tx.send(f).is_err() {
                                break;
                            }
                        }
                    });
                    let mut updates = ps_receivers.order_updates;
                    tokio::spawn(async move {
                        while let Some(u) = updates.recv().await {
                            tracing::debug!(order_id = u.order_id.as_str(), status = ?u.status, "private stream order update");
                        }
                    });
                }
                Err(e) => {
                    warn!(error = %e, "private stream connect failed, skipping");
                }
            }
        }
        drop(fill_tx);

        let mut reconcile_interval =
            tokio::time::interval(std::time::Duration::from_secs(self.reconcile_interval_secs));
        let mut cancel_check_interval =
            tokio::time::interval(std::time::Duration::from_millis(500));

        loop {
            tokio::select! {
                Some(quote) = merged_rx.recv() => {
                    self.handle_quote(quote).await?;
                }
                Some(book) = merged_book_rx.recv() => {
                    let key = book_key(book.venue, &book.instrument);
                    tracing::debug!(key = key.as_str(), levels = book.bids.len(), "L2 order book update");
                    self.books.insert(key, book);
                }
                Some(fill) = fill_rx.recv() => {
                    let fp = fill_fingerprint(&fill);
                    if !self.seen_fills_set.insert(fp.clone()) {
                        tracing::debug!(
                            order_id = fill.order_id.as_str(),
                            "duplicate fill suppressed (likely WS reconnect replay)"
                        );
                        continue;
                    }
                    self.seen_fills.push_back(fp);
                    while self.seen_fills.len() > FILL_DEDUP_CAPACITY
                        && let Some(old) = self.seen_fills.pop_front()
                    {
                        self.seen_fills_set.remove(&old);
                    }

                    let fill_key = book_key(fill.venue, &fill.instrument);
                    let signed_qty = match fill.side {
                        crate::models::enums::Side::Buy => fill.quantity,
                        crate::models::enums::Side::Sell => -fill.quantity,
                    };
                    self.risk_state.apply_fill(fill_key.as_str(), signed_qty, fill.price, Decimal::ZERO);
                    self.circuit_breaker.check_drawdown(self.risk_state.realized_pnl_today);
                    crate::metrics::record_fill_received(self.strategy.name());

                    if let Some(intended) = self.intended_fills.remove(&fill.order_id)
                        && !intended.intended_price.is_zero()
                    {
                        let raw = (fill.price - intended.intended_price)
                            / intended.intended_price
                            * Decimal::from(10_000);
                        let signed_bps = match intended.side {
                            crate::models::enums::Side::Buy => raw,
                            crate::models::enums::Side::Sell => -raw,
                        };
                        let venue_label = format!("{:?}", fill.venue).to_lowercase();
                        crate::metrics::record_slippage_bps(
                            &venue_label,
                            self.strategy.name(),
                            signed_bps.to_string().parse::<f64>().unwrap_or(0.0),
                        );
                    }
                    let pos = self.risk_state.position_by_instrument.get(fill_key.as_str()).copied().unwrap_or(Decimal::ZERO);
                    crate::metrics::set_position(fill_key.as_str(), pos.to_string().parse::<f64>().unwrap_or(0.0));
                    crate::metrics::set_realized_pnl(self.risk_state.realized_pnl_today.to_string().parse::<f64>().unwrap_or(0.0));

                    self.position_manager.apply_fill(&fill).await?;
                    let key = format!("{:?}", fill.venue).to_lowercase();
                    if let Ok(snapshot) = self.position_manager.get_portfolio().await {
                        self.portfolios.insert(key, snapshot);
                    }
                }
                _ = reconcile_interval.tick() => {
                    if let Err(e) = self.position_manager.sync_positions().await {
                        warn!(error = %e, "position reconciliation failed");
                    } else {
                        if let Ok(snapshot) = self.position_manager.get_portfolio().await {
                            let key = "reconciled".to_string();
                            self.portfolios.insert(key, snapshot);
                        }
                        info!("position reconciliation completed");
                    }
                }
                _ = cancel_check_interval.tick() => {
                    let now = Utc::now();
                    let mut still_pending: Vec<(chrono::DateTime<Utc>, String)> = Vec::new();
                    for (deadline, order_id) in self.pending_cancels.drain(..) {
                        if now < deadline {
                            still_pending.push((deadline, order_id));
                            continue;
                        }
                        if !self.intended_fills.contains_key(&order_id) {
                            continue;
                        }
                        match self.executor.cancel_order(&order_id).await {
                            Ok(true) => {
                                info!(order_id = order_id.as_str(), "order cancelled after TTL");
                                self.intended_fills.remove(&order_id);
                            }
                            Ok(false) => {
                                tracing::debug!(order_id = order_id.as_str(), "TTL cancel: already filled or unknown");
                                self.intended_fills.remove(&order_id);
                            }
                            Err(e) => {
                                warn!(error = %e, order_id = order_id.as_str(), "TTL cancel failed");
                            }
                        }
                    }
                    self.pending_cancels = still_pending;
                }
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        info!("shutdown signal received, stopping engine");
                        break;
                    }
                }
                else => break,
            }
        }

        info!(
            trade_logs = self.trade_logs.len(),
            "engine shutting down, {} trades recorded",
            self.trade_logs.len()
        );
        Ok(())
    }

    async fn handle_quote(&mut self, quote: Quote) -> Result<()> {
        let key = book_key(quote.venue, &quote.instrument);
        let quote_age_ms = (chrono::Utc::now() - quote.timestamp).num_milliseconds();
        tracing::debug!(key = key.as_str(), quote_age_ms, "quote received");
        crate::metrics::record_quote_received();
        crate::metrics::record_quote_age_ms(quote_age_ms as f64);
        // In-place update reuses existing SmallVec allocation — no heap alloc on the hot path.
        self.books
            .entry(key)
            .and_modify(|b| b.update_from_quote(&quote))
            .or_insert_with(|| quote_to_book(&quote));

        let eval_start = std::time::Instant::now();
        let eval_result = self.strategy.evaluate(&self.books, &self.portfolios).await;
        let eval_us = eval_start.elapsed().as_micros();

        crate::metrics::record_eval_latency_us(self.strategy.name(), eval_us as f64);

        if let Some(opp) = eval_result {
            tracing::info!(eval_latency_us = eval_us, "strategy evaluation");
            crate::metrics::record_opportunity_detected(self.strategy.name());
            let direction = opp
                .legs
                .iter()
                .map(|leg| {
                    format!(
                        "{:?} {:?} {:?}@{}x{}",
                        leg.side,
                        leg.venue,
                        leg.instrument.instrument_type,
                        leg.order_price,
                        leg.quantity
                    )
                })
                .collect::<Vec<_>>()
                .join(" | ");

            info!(
                id = opp.id.as_str(),
                direction = direction.as_str(),
                gross = %opp.economics.gross_profit,
                fees = %opp.economics.fees_total,
                net = %opp.economics.net_profit,
                net_bps = %opp.economics.net_profit_bps,
                notional = %opp.economics.notional,
                "opportunity detected"
            );

            let orders = self.strategy.compute_hedge_orders(&opp);

            let submit_start = std::time::Instant::now();
            let mut trade_legs: SmallVec<[TradeLeg; 4]> = SmallVec::new();
            let mut submitted_count: usize = 0;
            let mut risk_rejected_count: usize = 0;
            let total_orders = orders.len();

            let portfolio = self
                .position_manager
                .get_portfolio()
                .await
                .unwrap_or_default();

            let mut approved_orders: Vec<crate::models::order::Order> = Vec::new();

            for mut req in orders {
                if self.circuit_breaker.is_tripped() {
                    warn!(
                        reason = self.circuit_breaker.trip_reason().unwrap_or("unknown"),
                        "order skipped: circuit breaker tripped"
                    );
                    crate::metrics::record_circuit_breaker_trip();
                    crate::metrics::record_order_rejected(self.strategy.name());
                    risk_rejected_count += 1;
                    trade_legs.push(TradeLeg {
                        venue: req.venue,
                        instrument: req.instrument.clone(),
                        side: req.side,
                        intended_price: req.price.unwrap_or(Decimal::ZERO),
                        intended_quantity: req.quantity,
                        order_id: None,
                        submitted_at: Utc::now(),
                    });
                    continue;
                }

                let inst_key = book_key(req.venue, &req.instrument);
                let order_notional = req.quantity * req.price.unwrap_or(Decimal::ZERO);
                let fast_verdict =
                    self.risk_state
                        .check_order(inst_key.as_str(), req.quantity, order_notional);

                if !fast_verdict.approved {
                    warn!(
                        reason = fast_verdict.reason.as_deref().unwrap_or("unknown"),
                        "order rejected by risk state (O(1))"
                    );
                    crate::metrics::record_order_rejected(self.strategy.name());
                    risk_rejected_count += 1;
                    trade_legs.push(TradeLeg {
                        venue: req.venue,
                        instrument: req.instrument.clone(),
                        side: req.side,
                        intended_price: req.price.unwrap_or(Decimal::ZERO),
                        intended_quantity: req.quantity,
                        order_id: None,
                        submitted_at: Utc::now(),
                    });
                    continue;
                }

                if let Some(adj_qty) = fast_verdict.adjusted_qty {
                    req.quantity = adj_qty;
                }

                let verdict = self.risk_manager.check_pre_trade(&req, &portfolio);

                if !verdict.approved {
                    warn!(
                        reason = verdict.reason.as_deref().unwrap_or("unknown"),
                        "order rejected by risk manager"
                    );
                    crate::metrics::record_order_rejected(self.strategy.name());
                    risk_rejected_count += 1;
                    trade_legs.push(TradeLeg {
                        venue: req.venue,
                        instrument: req.instrument.clone(),
                        side: req.side,
                        intended_price: req.price.unwrap_or(Decimal::ZERO),
                        intended_quantity: req.quantity,
                        order_id: None,
                        submitted_at: Utc::now(),
                    });
                    continue;
                }

                if let Some(adj_qty) = verdict.adjusted_qty {
                    req.quantity = adj_qty;
                }

                approved_orders.push(req.into_order());
            }

            let futures: Vec<_> = approved_orders
                .iter()
                .map(|order| self.executor.submit_order(order))
                .collect();
            let results = join_all(futures).await;

            for (order, result) in approved_orders.iter().zip(results.iter()) {
                match result {
                    Ok(order_id) => {
                        info!(
                            order_id = order_id.as_str(),
                            side = ?order.side,
                            qty = %order.quantity,
                            "order submitted"
                        );
                        crate::metrics::record_order_submitted(self.strategy.name());
                        self.circuit_breaker.record_success();
                        self.circuit_breaker.record_order();
                        submitted_count += 1;
                        let intended_price = order.price.unwrap_or(Decimal::ZERO);
                        if !intended_price.is_zero() {
                            self.intended_fills.insert(
                                order_id.clone(),
                                IntendedFill {
                                    side: order.side,
                                    intended_price,
                                },
                            );
                        }
                        if self.order_ttl_secs > 0 {
                            let deadline =
                                Utc::now() + chrono::Duration::seconds(self.order_ttl_secs as i64);
                            self.pending_cancels.push((deadline, order_id.clone()));
                        }
                        trade_legs.push(TradeLeg {
                            venue: order.venue,
                            instrument: order.instrument.clone(),
                            side: order.side,
                            intended_price,
                            intended_quantity: order.quantity,
                            order_id: Some(order_id.clone()),
                            submitted_at: Utc::now(),
                        });
                    }
                    Err(e) => {
                        warn!(error = %e, "order submission failed");
                        crate::metrics::record_order_failed(self.strategy.name());
                        self.circuit_breaker.record_failure();
                        self.circuit_breaker.record_order();
                        trade_legs.push(TradeLeg {
                            venue: order.venue,
                            instrument: order.instrument.clone(),
                            side: order.side,
                            intended_price: order.price.unwrap_or(Decimal::ZERO),
                            intended_quantity: order.quantity,
                            order_id: None,
                            submitted_at: Utc::now(),
                        });
                    }
                }
            }

            let submit_us = submit_start.elapsed().as_micros();
            crate::metrics::record_submit_latency_us(self.strategy.name(), submit_us as f64);
            tracing::info!(
                submit_latency_us = submit_us,
                orders = total_orders,
                "order submission complete"
            );

            let outcome = if risk_rejected_count == total_orders {
                TradeOutcome::RiskRejected
            } else if submitted_count == total_orders {
                TradeOutcome::AllSubmitted
            } else {
                TradeOutcome::PartialFailure
            };

            let trade_log = TradeLog {
                id: opp.id.clone(),
                strategy_id: opp.meta.strategy_id.clone(),
                outcome,
                legs: trade_legs,
                expected_gross_profit: opp.economics.gross_profit,
                expected_fees: opp.economics.fees_total,
                expected_net_profit: opp.economics.net_profit,
                expected_net_profit_bps: opp.economics.net_profit_bps,
                notional: opp.economics.notional,
                created_at: Utc::now(),
            };

            info!(
                trade_id = trade_log.id.as_str(),
                outcome = ?trade_log.outcome,
                legs = trade_log.legs.len(),
                expected_net = %trade_log.expected_net_profit,
                "trade log recorded"
            );

            if let Some(writer) = self.trade_log_writer.as_mut()
                && let Err(e) = writer.append(&trade_log)
            {
                warn!(error = %e, "trade log writer append failed");
            }

            self.trade_logs.push(trade_log);
        }

        Ok(())
    }
}

/// Convert a top-of-book Quote into a minimal OrderBook (single level each side).
/// Used only on first-seen instruments; hot-path uses update_from_quote instead.
pub(crate) fn quote_to_book(q: &Quote) -> OrderBook {
    OrderBook {
        venue: q.venue,
        instrument: q.instrument.clone(),
        bids: smallvec::smallvec![OrderBookLevel {
            price: q.bid,
            size: q.bid_size,
        }],
        asks: smallvec::smallvec![OrderBookLevel {
            price: q.ask,
            size: q.ask_size,
        }],
        timestamp: q.timestamp,
        local_timestamp: chrono::Utc::now(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::enums::Venue;
    use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
    use crate::models::market::Quote;
    use chrono::Utc;
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

    fn test_quote(bid: Decimal, ask: Decimal) -> Quote {
        Quote {
            venue: Venue::Binance,
            instrument: test_instrument(),
            bid,
            ask,
            bid_size: dec!(1.5),
            ask_size: dec!(2.0),
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn quote_to_book_creates_single_level_book() {
        let q = test_quote(dec!(50000), dec!(50010));
        let book = quote_to_book(&q);

        assert_eq!(book.bids.len(), 1);
        assert_eq!(book.asks.len(), 1);
        assert_eq!(book.bids[0].price, dec!(50000));
        assert_eq!(book.bids[0].size, dec!(1.5));
        assert_eq!(book.asks[0].price, dec!(50010));
        assert_eq!(book.asks[0].size, dec!(2.0));
    }

    #[test]
    fn quote_to_book_preserves_venue_and_instrument() {
        let q = test_quote(dec!(100), dec!(101));
        let book = quote_to_book(&q);

        assert_eq!(book.venue, Venue::Binance);
        assert_eq!(book.instrument.base, "BTC");
        assert_eq!(book.instrument.quote, "USDT");
        assert_eq!(book.instrument.instrument_type, InstrumentType::Spot);
    }

    #[test]
    fn quote_to_book_preserves_timestamp() {
        let ts = Utc::now();
        let q = Quote {
            venue: Venue::Binance,
            instrument: test_instrument(),
            bid: dec!(100),
            ask: dec!(101),
            bid_size: dec!(1),
            ask_size: dec!(1),
            timestamp: ts,
        };
        let book = quote_to_book(&q);
        assert_eq!(book.timestamp, ts);
    }
}
