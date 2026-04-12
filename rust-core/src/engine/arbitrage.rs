use std::collections::HashMap;

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::{info, warn};

use chrono::Utc;
use smallvec::SmallVec;

use crate::adapters::market_data::{MarketDataFeed, MarketDataReceivers};
use crate::adapters::order_executor::OrderExecutor;
use crate::models::enums::Venue;
use crate::models::market::{OrderBook, OrderBookLevel, Quote, book_key};
use crate::models::position::PortfolioSnapshot;
use crate::models::trade_log::{TradeLeg, TradeLog, TradeOutcome};
use crate::risk::manager::RiskManager;
use crate::strategy::base::ArbitrageStrategy;
use rust_decimal::Decimal;

/// Main loop: receive quote → update local book → evaluate strategy → log opportunity.
pub struct ArbitrageEngine {
    feeds: Vec<Box<dyn MarketDataFeed>>,
    strategy: Box<dyn ArbitrageStrategy>,
    risk_manager: RiskManager,
    executor: Box<dyn OrderExecutor>,
    books: HashMap<String, OrderBook>,
    portfolios: HashMap<String, PortfolioSnapshot>,
    trade_logs: Vec<TradeLog>,
}

impl ArbitrageEngine {
    pub fn new(
        feeds: Vec<Box<dyn MarketDataFeed>>,
        strategy: Box<dyn ArbitrageStrategy>,
        risk_manager: RiskManager,
        executor: Box<dyn OrderExecutor>,
    ) -> Self {
        Self {
            feeds,
            strategy,
            risk_manager,
            executor,
            books: HashMap::new(),
            portfolios: HashMap::new(),
            trade_logs: vec![],
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        info!(
            strategy = self.strategy.name(),
            feeds = self.feeds.len(),
            "starting arbitrage engine"
        );

        // Merge all feed quote streams into a single channel.
        let (merged_tx, mut merged_rx) = mpsc::unbounded_channel::<Quote>();

        for feed in self.feeds.iter_mut() {
            let MarketDataReceivers {
                mut quotes,
                order_books: _,
            } = feed.connect().await?;
            let tx = merged_tx.clone();
            tokio::spawn(async move {
                while let Some(q) = quotes.recv().await {
                    if tx.send(q).is_err() {
                        break;
                    }
                }
            });
        }

        // Drop our own sender so the channel closes when all feeds end.
        drop(merged_tx);

        while let Some(quote) = merged_rx.recv().await {
            let key = book_key(quote.venue, &quote.instrument);
            self.books.insert(key, quote_to_book(&quote));

            if let Some(opp) = self.strategy.evaluate(&self.books, &self.portfolios).await {
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

                // Compute hedge orders from the opportunity
                let orders = self.strategy.compute_hedge_orders(&opp);

                // Track trade legs for the TradeLog
                let mut trade_legs: SmallVec<[TradeLeg; 4]> = SmallVec::new();
                let mut submitted_count: usize = 0;
                let mut risk_rejected_count: usize = 0;
                let total_orders = orders.len();

                // Risk-check and submit each order
                for mut order in orders {
                    let empty_portfolio = PortfolioSnapshot {
                        venue: Venue::Binance,
                        positions: vec![],
                        total_equity: Decimal::ZERO,
                        available_balance: Decimal::ZERO,
                        unrealized_pnl: Decimal::ZERO,
                        realized_pnl: Decimal::ZERO,
                    };
                    let verdict = self.risk_manager.check_pre_trade(&order, &empty_portfolio);

                    if !verdict.approved {
                        warn!(
                            reason = verdict.reason.as_deref().unwrap_or("unknown"),
                            "order rejected by risk manager"
                        );
                        risk_rejected_count += 1;
                        trade_legs.push(TradeLeg {
                            venue: order.venue,
                            instrument: order.instrument.clone(),
                            side: order.side,
                            intended_price: order.price.unwrap_or(Decimal::ZERO),
                            intended_quantity: order.quantity,
                            order_id: None,
                            submitted_at: Utc::now(),
                        });
                        continue;
                    }

                    // Apply adjusted quantity if risk manager capped it
                    if let Some(adj_qty) = verdict.adjusted_qty {
                        order.quantity = adj_qty;
                    }

                    match self.executor.submit_order(&order).await {
                        Ok(order_id) => {
                            info!(
                                order_id = order_id.as_str(),
                                side = ?order.side,
                                qty = %order.quantity,
                                "order submitted"
                            );
                            submitted_count += 1;
                            trade_legs.push(TradeLeg {
                                venue: order.venue,
                                instrument: order.instrument.clone(),
                                side: order.side,
                                intended_price: order.price.unwrap_or(Decimal::ZERO),
                                intended_quantity: order.quantity,
                                order_id: Some(order_id),
                                submitted_at: Utc::now(),
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "order submission failed");
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

                // Determine outcome and record the trade log
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
        }

        warn!("all quote streams ended");
        Ok(())
    }
}

/// Convert a top-of-book Quote into a minimal OrderBook (single level each side).
fn quote_to_book(q: &Quote) -> OrderBook {
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
