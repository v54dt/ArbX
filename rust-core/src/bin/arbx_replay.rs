//! Offline analyzer for TradeLog JSONL files produced by the engine's
//! append-only audit writer (`engine.trade_log_file`). Phase 1 reads the
//! log, computes summary statistics, and prints a per-strategy breakdown.
//! Phase 2 (follow-up) will add quote-CSV replay + strategy re-evaluation
//! diffing.
//!
//! Usage:
//!   arbx-replay <trade_log.jsonl>
//!   arbx-replay --json <trade_log.jsonl>   # machine-readable output

use std::collections::BTreeMap;
use std::io::BufRead;

use anyhow::Context;
use arbx_core::models::trade_log::{TradeLog, TradeOutcome};
use clap::Parser;
use rust_decimal::Decimal;
use serde::Serialize;

#[derive(Parser, Debug)]
#[command(
    name = "arbx-replay",
    version,
    about = "Offline analyzer for arbx-core TradeLog JSONL files"
)]
struct Cli {
    /// Path to a TradeLog JSONL file (one JSON object per line, as written
    /// by `engine.trade_log_file`).
    trade_log: String,

    /// Emit analysis as JSON instead of human-readable text.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Default, Serialize)]
struct StrategyStats {
    trades: u64,
    all_submitted: u64,
    partial_failure: u64,
    risk_rejected: u64,
    expected_gross_profit_sum: Decimal,
    expected_fees_sum: Decimal,
    expected_net_profit_sum: Decimal,
    notional_sum: Decimal,
    min_net_bps: Option<Decimal>,
    max_net_bps: Option<Decimal>,
}

impl StrategyStats {
    fn ingest(&mut self, log: &TradeLog) {
        self.trades += 1;
        match log.outcome {
            TradeOutcome::AllSubmitted => self.all_submitted += 1,
            TradeOutcome::PartialFailure => self.partial_failure += 1,
            TradeOutcome::RiskRejected => self.risk_rejected += 1,
        }
        self.expected_gross_profit_sum += log.expected_gross_profit;
        self.expected_fees_sum += log.expected_fees;
        self.expected_net_profit_sum += log.expected_net_profit;
        self.notional_sum += log.notional;
        self.min_net_bps = Some(match self.min_net_bps {
            None => log.expected_net_profit_bps,
            Some(cur) => cur.min(log.expected_net_profit_bps),
        });
        self.max_net_bps = Some(match self.max_net_bps {
            None => log.expected_net_profit_bps,
            Some(cur) => cur.max(log.expected_net_profit_bps),
        });
    }
}

#[derive(Debug, Default, Serialize)]
struct Report {
    total_trades: u64,
    total_expected_gross_profit: Decimal,
    total_expected_fees: Decimal,
    total_expected_net_profit: Decimal,
    total_notional: Decimal,
    per_strategy: BTreeMap<String, StrategyStats>,
}

fn load_trade_log(path: &str) -> anyhow::Result<Vec<TradeLog>> {
    let file =
        std::fs::File::open(path).with_context(|| format!("opening trade_log file {path}"))?;
    let reader = std::io::BufReader::new(file);
    let mut logs = Vec::new();
    for (n, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("reading line {n} of {path}"))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let log: TradeLog = serde_json::from_str(trimmed)
            .with_context(|| format!("parsing JSONL at line {n} of {path}"))?;
        logs.push(log);
    }
    Ok(logs)
}

fn analyze(logs: &[TradeLog]) -> Report {
    let mut report = Report::default();
    for log in logs {
        report.total_trades += 1;
        report.total_expected_gross_profit += log.expected_gross_profit;
        report.total_expected_fees += log.expected_fees;
        report.total_expected_net_profit += log.expected_net_profit;
        report.total_notional += log.notional;
        report
            .per_strategy
            .entry(log.strategy_id.clone())
            .or_default()
            .ingest(log);
    }
    report
}

fn print_text(report: &Report) {
    println!("=== arbx-replay Phase 1 analyzer ===");
    println!();
    println!("Total trades logged: {}", report.total_trades);
    println!(
        "Expected gross profit: {}",
        report.total_expected_gross_profit
    );
    println!("Expected fees:         {}", report.total_expected_fees);
    println!(
        "Expected net profit:   {}",
        report.total_expected_net_profit
    );
    println!("Total notional:        {}", report.total_notional);
    println!();
    println!("Per-strategy breakdown:");
    if report.per_strategy.is_empty() {
        println!("  (no trades)");
    }
    for (strategy, stats) in &report.per_strategy {
        println!("  [{strategy}]");
        println!(
            "    trades={}  all_submitted={}  partial_failure={}  risk_rejected={}",
            stats.trades, stats.all_submitted, stats.partial_failure, stats.risk_rejected
        );
        println!(
            "    net_profit_sum={}  notional_sum={}",
            stats.expected_net_profit_sum, stats.notional_sum
        );
        match (stats.min_net_bps, stats.max_net_bps) {
            (Some(lo), Some(hi)) => println!("    net_profit_bps: min={lo}  max={hi}"),
            _ => println!("    net_profit_bps: (no data)"),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let logs = load_trade_log(&cli.trade_log)?;
    let report = analyze(&logs);

    if cli.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_text(&report);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arbx_core::models::enums::{Side, Venue};
    use arbx_core::models::instrument::{AssetClass, Instrument, InstrumentType};
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use smallvec::smallvec;

    fn sample_log(strategy: &str, net: Decimal, bps: Decimal, outcome: TradeOutcome) -> TradeLog {
        TradeLog {
            id: format!("id-{strategy}-{bps}"),
            strategy_id: strategy.to_string(),
            outcome,
            legs: smallvec![arbx_core::models::trade_log::TradeLeg {
                venue: Venue::Binance,
                instrument: Instrument {
                    asset_class: AssetClass::Crypto,
                    instrument_type: InstrumentType::Spot,
                    base: "BTC".into(),
                    quote: "USDT".into(),
                    settle_currency: None,
                    expiry: None,
                    last_trade_time: None,
                    settlement_time: None,
                },
                side: Side::Buy,
                intended_price: dec!(50000),
                intended_quantity: dec!(1),
                order_id: None,
                submitted_at: Utc::now(),
            }],
            expected_gross_profit: net + dec!(2),
            expected_fees: dec!(2),
            expected_net_profit: net,
            expected_net_profit_bps: bps,
            notional: dec!(50000),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn analyze_empty_returns_zero_totals() {
        let report = analyze(&[]);
        assert_eq!(report.total_trades, 0);
        assert_eq!(report.total_expected_net_profit, Decimal::ZERO);
        assert!(report.per_strategy.is_empty());
    }

    #[test]
    fn analyze_aggregates_per_strategy() {
        let logs = vec![
            sample_log(
                "cross_exchange",
                dec!(10),
                dec!(5),
                TradeOutcome::AllSubmitted,
            ),
            sample_log(
                "cross_exchange",
                dec!(-2),
                dec!(-1),
                TradeOutcome::PartialFailure,
            ),
            sample_log("funding_rate", dec!(7), dec!(3), TradeOutcome::RiskRejected),
        ];
        let report = analyze(&logs);

        assert_eq!(report.total_trades, 3);
        assert_eq!(report.total_expected_net_profit, dec!(15));

        let ce = &report.per_strategy["cross_exchange"];
        assert_eq!(ce.trades, 2);
        assert_eq!(ce.all_submitted, 1);
        assert_eq!(ce.partial_failure, 1);
        assert_eq!(ce.risk_rejected, 0);
        assert_eq!(ce.expected_net_profit_sum, dec!(8));
        assert_eq!(ce.min_net_bps, Some(dec!(-1)));
        assert_eq!(ce.max_net_bps, Some(dec!(5)));

        let fr = &report.per_strategy["funding_rate"];
        assert_eq!(fr.trades, 1);
        assert_eq!(fr.risk_rejected, 1);
    }

    #[test]
    fn load_trade_log_skips_blank_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trades.jsonl");
        let a = sample_log("a", dec!(1), dec!(1), TradeOutcome::AllSubmitted);
        let b = sample_log("b", dec!(2), dec!(2), TradeOutcome::AllSubmitted);
        let body = format!(
            "{}\n\n{}\n",
            serde_json::to_string(&a).unwrap(),
            serde_json::to_string(&b).unwrap()
        );
        std::fs::write(&path, body).unwrap();
        let logs = load_trade_log(path.to_str().unwrap()).unwrap();
        assert_eq!(logs.len(), 2);
    }

    #[test]
    fn load_trade_log_reports_malformed_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trades.jsonl");
        std::fs::write(&path, "{not json}\n").unwrap();
        let err = load_trade_log(path.to_str().unwrap()).unwrap_err();
        assert!(
            format!("{err:#}").contains("parsing JSONL"),
            "expected parse error, got: {err:#}"
        );
    }
}
