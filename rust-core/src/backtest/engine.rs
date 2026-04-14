use anyhow::Result;
use rust_decimal::Decimal;

use crate::adapters::market_data::MarketDataFeed;
use crate::adapters::paper_executor::PaperExecutor;
use crate::config::RiskConfig;
use crate::engine::arbitrage::ArbitrageEngine;
use crate::models::trade_log::{TradeLog, TradeOutcome};
use crate::risk::circuit_breaker::CircuitBreaker;
use crate::risk::limits::{MaxDailyLoss, MaxNotionalExposure, MaxPositionSize};
use crate::risk::manager::RiskManager;
use crate::risk::state::RiskState;
use crate::strategy::base::ArbitrageStrategy;

pub struct BacktestResult {
    pub trade_logs: Vec<TradeLog>,
    pub total_trades: usize,
    pub profitable_trades: usize,
    pub total_pnl: Decimal,
    pub max_drawdown: Decimal,
    pub sharpe_ratio: f64,
    pub duration_ms: u64,
}

pub async fn run_backtest(
    feeds: Vec<Box<dyn MarketDataFeed>>,
    strategy: Box<dyn ArbitrageStrategy>,
    risk_config: RiskConfig,
) -> Result<BacktestResult> {
    let start = std::time::Instant::now();

    let risk_manager = RiskManager::new(vec![
        Box::new(MaxPositionSize {
            max_quantity: risk_config.max_position_size,
        }),
        Box::new(MaxDailyLoss {
            max_loss: risk_config.max_daily_loss,
        }),
        Box::new(MaxNotionalExposure {
            max_notional: risk_config.max_notional_exposure,
        }),
    ]);

    let risk_state = RiskState::new(
        risk_config.max_position_size,
        risk_config.max_notional_exposure,
        risk_config.max_daily_loss,
    );

    let circuit_breaker = CircuitBreaker::new(
        risk_config.circuit_breaker.max_drawdown,
        risk_config.circuit_breaker.max_orders_per_minute,
        risk_config.circuit_breaker.max_consecutive_failures,
    );

    let executor = PaperExecutor::new();
    let position_manager = NullPositionManager;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let mut engine = ArbitrageEngine::new(
        feeds,
        strategy,
        risk_manager,
        risk_state,
        circuit_breaker,
        Box::new(executor),
        Box::new(position_manager),
        Vec::new(),
        86400,
        shutdown_rx,
    );

    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        let _ = shutdown_tx.send(true);
    });

    engine.run().await?;

    let duration_ms = start.elapsed().as_millis() as u64;
    let trade_logs = engine.trade_logs().to_vec();
    let result = compute_result(trade_logs, duration_ms);
    Ok(result)
}

fn compute_result(trade_logs: Vec<TradeLog>, duration_ms: u64) -> BacktestResult {
    let total_trades = trade_logs.len();
    let profitable_trades = trade_logs
        .iter()
        .filter(|t| {
            t.outcome != TradeOutcome::RiskRejected && t.expected_net_profit > Decimal::ZERO
        })
        .count();
    let total_pnl: Decimal = trade_logs.iter().map(|t| t.expected_net_profit).sum();

    let mut running_pnl = Decimal::ZERO;
    let mut peak = Decimal::ZERO;
    let mut max_drawdown = Decimal::ZERO;
    let mut returns: Vec<f64> = Vec::new();

    for log in &trade_logs {
        let ret = log.expected_net_profit;
        running_pnl += ret;
        if running_pnl > peak {
            peak = running_pnl;
        }
        let dd = running_pnl - peak;
        if dd < max_drawdown {
            max_drawdown = dd;
        }
        if let Some(r) = decimal_to_f64(ret) {
            returns.push(r);
        }
    }

    let sharpe_ratio = if returns.len() > 1 {
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance =
            returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (returns.len() - 1) as f64;
        let std_dev = variance.sqrt();
        if std_dev > 0.0 { mean / std_dev } else { 0.0 }
    } else {
        0.0
    };

    BacktestResult {
        trade_logs,
        total_trades,
        profitable_trades,
        total_pnl,
        max_drawdown,
        sharpe_ratio,
        duration_ms,
    }
}

fn decimal_to_f64(d: Decimal) -> Option<f64> {
    d.to_string().parse::<f64>().ok()
}

use async_trait::async_trait;

use crate::adapters::position_manager::PositionManager;
use crate::models::order::Fill;
use crate::models::position::{PortfolioSnapshot, Position};

struct NullPositionManager;

#[async_trait]
impl PositionManager for NullPositionManager {
    async fn get_position(&self, _symbol: &str) -> Result<Option<Position>> {
        Ok(None)
    }
    async fn get_portfolio(&self) -> Result<PortfolioSnapshot> {
        Ok(PortfolioSnapshot::default())
    }
    async fn sync_positions(&mut self) -> Result<()> {
        Ok(())
    }
    async fn apply_fill(&mut self, _fill: &Fill) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::data_feed::HistoricalDataFeed;
    use crate::config::CircuitBreakerConfig;
    use crate::models::enums::Venue;
    use crate::models::fee::FeeSchedule;
    use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
    use crate::models::market::Quote;
    use crate::strategy::cross_exchange::CrossExchangeStrategy;
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;

    fn test_instrument(inst_type: InstrumentType) -> Instrument {
        Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: inst_type,
            base: "BTC".into(),
            quote: "USDT".into(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        }
    }

    fn make_risk_config() -> RiskConfig {
        RiskConfig {
            max_position_size: dec!(100),
            max_daily_loss: dec!(100000),
            max_notional_exposure: dec!(10000000),
            circuit_breaker: CircuitBreakerConfig::default(),
        }
    }

    fn make_strategy() -> CrossExchangeStrategy {
        CrossExchangeStrategy {
            venue_a: Venue::Binance,
            venue_b: Venue::Bybit,
            instrument_a: test_instrument(InstrumentType::Spot),
            instrument_b: test_instrument(InstrumentType::Spot),
            min_net_profit_bps: dec!(1),
            max_quantity: dec!(1),
            fee_a: FeeSchedule::new(Venue::Binance, dec!(0.001), dec!(0.001)),
            fee_b: FeeSchedule::new(Venue::Bybit, dec!(0.001), dec!(0.001)),
            max_quote_age_ms: 60_000,
            tick_size_a: dec!(0.01),
            tick_size_b: dec!(0.01),
            lot_size_a: dec!(0.001),
            lot_size_b: dec!(0.001),
            max_book_depth: 10,
        }
    }

    fn profitable_quotes(count: usize) -> Vec<Quote> {
        let inst = test_instrument(InstrumentType::Spot);
        let mut quotes = Vec::new();
        let base_ts = Utc::now().timestamp_millis();
        for i in 0..count {
            let ts = Utc.timestamp_millis_opt(base_ts + i as i64 * 100).unwrap();
            quotes.push(Quote {
                venue: Venue::Binance,
                instrument: inst.clone(),
                bid: dec!(49990),
                ask: dec!(50000),
                bid_size: dec!(10),
                ask_size: dec!(10),
                timestamp: ts,
            });
            quotes.push(Quote {
                venue: Venue::Bybit,
                instrument: inst.clone(),
                bid: dec!(50200),
                ask: dec!(50210),
                bid_size: dec!(10),
                ask_size: dec!(10),
                timestamp: ts,
            });
        }
        quotes
    }

    fn no_opportunity_quotes(count: usize) -> Vec<Quote> {
        let inst = test_instrument(InstrumentType::Spot);
        let mut quotes = Vec::new();
        let base_ts = Utc::now().timestamp_millis();
        for i in 0..count {
            let ts = Utc.timestamp_millis_opt(base_ts + i as i64 * 100).unwrap();
            quotes.push(Quote {
                venue: Venue::Binance,
                instrument: inst.clone(),
                bid: dec!(50000),
                ask: dec!(50001),
                bid_size: dec!(1),
                ask_size: dec!(1),
                timestamp: ts,
            });
            quotes.push(Quote {
                venue: Venue::Bybit,
                instrument: inst.clone(),
                bid: dec!(50000),
                ask: dec!(50001),
                bid_size: dec!(1),
                ask_size: dec!(1),
                timestamp: ts,
            });
        }
        quotes
    }

    #[tokio::test]
    async fn backtest_with_profitable_spread() {
        let quotes = profitable_quotes(100);
        let feed = HistoricalDataFeed::from_quotes(quotes);
        let strategy = make_strategy();
        let result = run_backtest(vec![Box::new(feed)], Box::new(strategy), make_risk_config())
            .await
            .unwrap();

        assert!(!result.trade_logs.is_empty());
        assert!(result.total_pnl > Decimal::ZERO);
        assert!(result.profitable_trades > 0);
    }

    #[tokio::test]
    async fn backtest_with_no_opportunity() {
        let quotes = no_opportunity_quotes(50);
        let feed = HistoricalDataFeed::from_quotes(quotes);
        let strategy = make_strategy();
        let result = run_backtest(vec![Box::new(feed)], Box::new(strategy), make_risk_config())
            .await
            .unwrap();

        assert_eq!(result.total_trades, 0);
        assert_eq!(result.total_pnl, Decimal::ZERO);
    }

    #[tokio::test]
    async fn backtest_result_calculates_drawdown() {
        let trade_logs = vec![
            make_trade_log("t1", dec!(100)),
            make_trade_log("t2", dec!(-200)),
            make_trade_log("t3", dec!(50)),
            make_trade_log("t4", dec!(-150)),
            make_trade_log("t5", dec!(300)),
        ];
        let result = compute_result(trade_logs, 1000);
        assert!(result.max_drawdown < Decimal::ZERO);
        // PnL path: 100, -100, -50, -200, 100
        // Peak:     100,  100, 100,  100, 100
        // DD:         0, -200, -150, -300,   0
        assert_eq!(result.max_drawdown, dec!(-300));
    }

    fn make_trade_log(id: &str, net_profit: Decimal) -> TradeLog {
        TradeLog {
            id: id.into(),
            strategy_id: "test".into(),
            outcome: TradeOutcome::AllSubmitted,
            legs: smallvec::smallvec![],
            expected_gross_profit: net_profit,
            expected_fees: Decimal::ZERO,
            expected_net_profit: net_profit,
            expected_net_profit_bps: Decimal::ZERO,
            notional: dec!(50000),
            created_at: chrono::Utc::now(),
        }
    }
}
