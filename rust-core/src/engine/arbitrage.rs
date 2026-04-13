use std::collections::HashMap;

use anyhow::Result;
use futures_util::future::join_all;
use tokio::sync::mpsc;
use tracing::{info, warn};

use chrono::Utc;
use smallvec::SmallVec;

use crate::adapters::market_data::{MarketDataFeed, MarketDataReceivers};
use crate::adapters::order_executor::OrderExecutor;
use crate::adapters::position_manager::PositionManager;
use crate::models::market::book_key as make_book_key;
use crate::models::market::{BookMap, OrderBook, OrderBookLevel, Quote, book_key};
use crate::models::order::Fill;
use crate::models::position::PortfolioSnapshot;
use crate::models::trade_log::{TradeLeg, TradeLog, TradeOutcome};
use crate::risk::circuit_breaker::CircuitBreaker;
use crate::risk::manager::RiskManager;
use crate::risk::state::RiskState;
use crate::strategy::base::ArbitrageStrategy;
use rust_decimal::Decimal;

/// Main loop: receive quote → update local book → evaluate strategy → log opportunity.
pub struct ArbitrageEngine {
    feeds: Vec<Box<dyn MarketDataFeed>>,
    strategy: Box<dyn ArbitrageStrategy>,
    risk_manager: RiskManager,
    risk_state: RiskState,
    circuit_breaker: CircuitBreaker,
    executor: Box<dyn OrderExecutor>,
    position_manager: Box<dyn PositionManager>,
    books: BookMap,
    portfolios: HashMap<String, PortfolioSnapshot>,
    trade_logs: Vec<TradeLog>,
    reconcile_interval_secs: u64,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
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
        reconcile_interval_secs: u64,
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
            books: BookMap::default(),
            portfolios: HashMap::new(),
            trade_logs: vec![],
            reconcile_interval_secs,
            shutdown_rx,
        }
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
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        Some(q) = quotes.recv() => {
                            if tx.send(q).is_err() { break; }
                        }
                        Some(ob) = order_books.recv() => {
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
            let tx = fill_tx;
            let mut fills = exec_receivers.fills;
            tokio::spawn(async move {
                while let Some(f) = fills.recv().await {
                    if tx.send(f).is_err() {
                        break;
                    }
                }
            });
        }

        let mut reconcile_interval =
            tokio::time::interval(std::time::Duration::from_secs(self.reconcile_interval_secs));

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
                    let fill_key = make_book_key(fill.venue, &fill.instrument);
                    let signed_qty = match fill.side {
                        crate::models::enums::Side::Buy => fill.quantity,
                        crate::models::enums::Side::Sell => -fill.quantity,
                    };
                    let notional = fill.price * fill.quantity;
                    self.risk_state.apply_fill(&fill_key, signed_qty, notional, Decimal::ZERO);
                    self.circuit_breaker.check_drawdown(self.risk_state.realized_pnl_today);
                    crate::metrics::record_fill_received();
                    let pos = self.risk_state.position_by_instrument.get(&fill_key).copied().unwrap_or(Decimal::ZERO);
                    crate::metrics::set_position(&fill_key, pos.to_string().parse::<f64>().unwrap_or(0.0));
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
        self.books.insert(key, quote_to_book(&quote));

        let eval_start = std::time::Instant::now();
        let eval_result = self.strategy.evaluate(&self.books, &self.portfolios).await;
        let eval_us = eval_start.elapsed().as_micros();

        crate::metrics::record_eval_latency_us(eval_us as f64);

        if let Some(opp) = eval_result {
            tracing::info!(eval_latency_us = eval_us, "strategy evaluation");
            crate::metrics::record_opportunity_detected();
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
                    crate::metrics::record_order_rejected();
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

                let inst_key = make_book_key(req.venue, &req.instrument);
                let order_notional = req.quantity * req.price.unwrap_or(Decimal::ZERO);
                let fast_verdict =
                    self.risk_state
                        .check_order(&inst_key, req.quantity, order_notional);

                if !fast_verdict.approved {
                    warn!(
                        reason = fast_verdict.reason.as_deref().unwrap_or("unknown"),
                        "order rejected by risk state (O(1))"
                    );
                    crate::metrics::record_order_rejected();
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
                    crate::metrics::record_order_rejected();
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
                        crate::metrics::record_order_submitted();
                        self.circuit_breaker.record_success();
                        self.circuit_breaker.record_order();
                        submitted_count += 1;
                        trade_legs.push(TradeLeg {
                            venue: order.venue,
                            instrument: order.instrument.clone(),
                            side: order.side,
                            intended_price: order.price.unwrap_or(Decimal::ZERO),
                            intended_quantity: order.quantity,
                            order_id: Some(order_id.clone()),
                            submitted_at: Utc::now(),
                        });
                    }
                    Err(e) => {
                        warn!(error = %e, "order submission failed");
                        crate::metrics::record_order_failed();
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
            crate::metrics::record_submit_latency_us(submit_us as f64);
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

            self.trade_logs.push(trade_log);
        }

        Ok(())
    }
}

/// Convert a top-of-book Quote into a minimal OrderBook (single level each side).
pub(crate) fn quote_to_book(q: &Quote) -> OrderBook {
    OrderBook {
        venue: q.venue,
        instrument: q.instrument.clone(),
        bids: vec![OrderBookLevel {
            price: q.bid,
            size: q.bid_size,
        }],
        asks: vec![OrderBookLevel {
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
    use crate::models::market::{Quote, book_key};
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

    #[test]
    fn book_key_format_is_consistent() {
        let inst = test_instrument();
        let key = book_key(Venue::Binance, &inst);
        assert_eq!(key, "binance:btc-usdt:spot");
    }

    #[test]
    fn book_key_different_venues_produce_different_keys() {
        let inst = test_instrument();
        let k1 = book_key(Venue::Binance, &inst);
        let k2 = book_key(Venue::Okx, &inst);
        assert_ne!(k1, k2);
        assert!(k1.starts_with("binance:"));
        assert!(k2.starts_with("okx:"));
    }

    #[test]
    fn book_key_different_instrument_types_produce_different_keys() {
        let spot = test_instrument();
        let mut swap = test_instrument();
        swap.instrument_type = InstrumentType::Swap;

        let k1 = book_key(Venue::Binance, &spot);
        let k2 = book_key(Venue::Binance, &swap);
        assert_ne!(k1, k2);
        assert!(k1.ends_with(":spot"));
        assert!(k2.ends_with(":swap"));
    }
}
