//! Offline analyzer for TradeLog JSONL files produced by the engine's
//! append-only audit writer (`engine.trade_log_file`).
//!
//! Phase 1: read a TradeLog JSONL file and print per-strategy summary stats.
//! Phase 2: replay a JSONL Quote stream against `CrossExchangeStrategy` and
//! diff the replayed opportunities against the originally logged trades
//! (matched / replay_only / trade_log_only buckets).
//!
//! Usage:
//!   arbx-replay <trade_log.jsonl>                     # Phase 1 analyzer
//!   arbx-replay --json <trade_log.jsonl>              # machine-readable
//!   arbx-replay --quotes <quotes.jsonl>               # replay only
//!   arbx-replay --quotes <quotes.jsonl> <trade_log>   # replay + diff

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::BufRead;

use anyhow::Context;
use arbx_core::models::enums::Venue;
use arbx_core::models::fee::FeeSchedule;
use arbx_core::models::instrument::{AssetClass, Instrument, InstrumentType};
use arbx_core::models::market::{BookMap, OrderBook, OrderBookLevel, Quote, book_key};
use arbx_core::models::trade_log::{TradeLog, TradeOutcome};
use arbx_core::strategy::Opportunity;
use arbx_core::strategy::base::ArbitrageStrategy;
use arbx_core::strategy::cross_exchange::CrossExchangeStrategy;
use chrono::{DateTime, Utc};
use clap::Parser;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;
use smallvec::smallvec;

#[derive(Parser, Debug)]
#[command(
    name = "arbx-replay",
    version,
    about = "Offline analyzer + quote-replay differ for arbx-core"
)]
struct Cli {
    /// Optional TradeLog JSONL file. Positional for Phase 1 compatibility.
    /// When `--quotes` is also set, this becomes the diff target; omit it to
    /// print a flat report of replayed opportunities.
    trade_log: Option<String>,

    /// Emit analysis as JSON instead of human-readable text.
    #[arg(long)]
    json: bool,

    /// Path to a JSONL Quote file (one `arbx_core::models::market::Quote`
    /// per line). When set, Phase 2 mode is enabled: replay the stream
    /// through `CrossExchangeStrategy` and report the opportunities
    /// produced, optionally diffed against `trade_log`.
    #[arg(long)]
    quotes: Option<String>,

    /// Overwrite every replayed quote's `timestamp` with `Utc::now()` before
    /// updating the book. Defaults to true so `max_quote_age_ms` filters
    /// don't drop stale fixture data.
    #[arg(long, default_value_t = true)]
    re_stamp: bool,
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

#[derive(Debug, Serialize)]
struct OppSummary {
    id: String,
    strategy_id: String,
    net_profit: Decimal,
    net_profit_bps: Decimal,
    notional: Decimal,
    detected_at: DateTime<Utc>,
}

impl From<&Opportunity> for OppSummary {
    fn from(o: &Opportunity) -> Self {
        Self {
            id: o.id.clone(),
            strategy_id: o.meta.strategy_id.clone(),
            net_profit: o.economics.net_profit,
            net_profit_bps: o.economics.net_profit_bps,
            notional: o.economics.notional,
            detected_at: o.meta.detected_at,
        }
    }
}

#[derive(Debug, Serialize)]
struct TradeLogSummary {
    id: String,
    strategy_id: String,
    outcome: TradeOutcome,
    net_profit: Decimal,
    net_profit_bps: Decimal,
    notional: Decimal,
}

impl From<&TradeLog> for TradeLogSummary {
    fn from(l: &TradeLog) -> Self {
        Self {
            id: l.id.clone(),
            strategy_id: l.strategy_id.clone(),
            outcome: l.outcome,
            net_profit: l.expected_net_profit,
            net_profit_bps: l.expected_net_profit_bps,
            notional: l.notional,
        }
    }
}

#[derive(Debug, Serialize)]
struct DiffReport {
    matched: Vec<OppSummary>,
    replay_only: Vec<OppSummary>,
    trade_log_only: Vec<TradeLogSummary>,
}

#[derive(Debug, Serialize)]
struct ReplayReport {
    quotes_loaded: usize,
    opportunities_detected: usize,
    opportunities: Vec<OppSummary>,
    diff: Option<DiffReport>,
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

fn load_quotes(path: &str) -> anyhow::Result<Vec<Quote>> {
    let file = std::fs::File::open(path).with_context(|| format!("opening quotes file {path}"))?;
    let reader = std::io::BufReader::new(file);
    let mut quotes = Vec::new();
    for (n, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("reading line {n} of {path}"))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let q: Quote = serde_json::from_str(trimmed)
            .with_context(|| format!("parsing quote JSONL at line {n} of {path}"))?;
        quotes.push(q);
    }
    Ok(quotes)
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

fn spot_btc_usdt() -> Instrument {
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

fn default_replay_strategy() -> CrossExchangeStrategy {
    CrossExchangeStrategy {
        venue_a: Venue::Binance,
        venue_b: Venue::Bybit,
        instrument_a: spot_btc_usdt(),
        instrument_b: spot_btc_usdt(),
        min_net_profit_bps: dec!(1),
        max_quantity: dec!(1),
        fee_a: FeeSchedule::new(Venue::Binance, dec!(0.0001), dec!(0.0001)),
        fee_b: FeeSchedule::new(Venue::Bybit, dec!(0.0001), dec!(0.0001)),
        max_quote_age_ms: 5000,
        tick_size_a: dec!(0.01),
        tick_size_b: dec!(0.01),
        lot_size_a: dec!(0.001),
        lot_size_b: dec!(0.001),
        max_book_depth: 5,
    }
}

fn quote_to_book(q: &Quote) -> OrderBook {
    OrderBook {
        venue: q.venue,
        instrument: q.instrument.clone(),
        bids: smallvec![OrderBookLevel {
            price: q.bid,
            size: q.bid_size,
        }],
        asks: smallvec![OrderBookLevel {
            price: q.ask,
            size: q.ask_size,
        }],
        timestamp: q.timestamp,
        local_timestamp: Utc::now(),
    }
}

async fn replay_quotes<S: ArbitrageStrategy>(
    strategy: &S,
    quotes: Vec<Quote>,
    re_stamp: bool,
) -> Vec<Opportunity> {
    let mut books: BookMap = BookMap::default();
    let portfolios: HashMap<String, arbx_core::models::position::PortfolioSnapshot> =
        HashMap::new();
    let mut opportunities = Vec::new();

    for mut q in quotes {
        if re_stamp {
            q.timestamp = Utc::now();
        }
        let key = book_key(q.venue, &q.instrument);
        books
            .entry(key)
            .and_modify(|b| b.update_from_quote(&q))
            .or_insert_with(|| quote_to_book(&q));

        if let Some(opp) = strategy.evaluate(&books, &portfolios).await {
            opportunities.push(opp);
        }
    }

    opportunities
}

fn diff_opportunities(opps: &[Opportunity], logs: &[TradeLog]) -> DiffReport {
    let log_ids: HashSet<&str> = logs.iter().map(|l| l.id.as_str()).collect();
    let opp_ids: HashSet<&str> = opps.iter().map(|o| o.id.as_str()).collect();

    let mut matched = Vec::new();
    let mut replay_only = Vec::new();
    for o in opps {
        if log_ids.contains(o.id.as_str()) {
            matched.push(OppSummary::from(o));
        } else {
            replay_only.push(OppSummary::from(o));
        }
    }
    let trade_log_only: Vec<TradeLogSummary> = logs
        .iter()
        .filter(|l| !opp_ids.contains(l.id.as_str()))
        .map(TradeLogSummary::from)
        .collect();

    DiffReport {
        matched,
        replay_only,
        trade_log_only,
    }
}

fn print_replay_text(report: &ReplayReport) {
    println!("=== arbx-replay Phase 2 quote replay ===");
    println!();
    println!("Quotes loaded:            {}", report.quotes_loaded);
    println!(
        "Opportunities detected:   {}",
        report.opportunities_detected
    );

    if let Some(diff) = &report.diff {
        println!();
        println!("Diff vs trade_log:");
        println!("  matched:        {}", diff.matched.len());
        println!("  replay_only:    {}", diff.replay_only.len());
        println!("  trade_log_only: {}", diff.trade_log_only.len());
        if !diff.replay_only.is_empty() {
            println!();
            println!("Replay-only opportunities (not in trade_log):");
            for o in &diff.replay_only {
                println!(
                    "  [{}] strategy={} net={} bps={} notional={}",
                    o.id, o.strategy_id, o.net_profit, o.net_profit_bps, o.notional
                );
            }
        }
        if !diff.trade_log_only.is_empty() {
            println!();
            println!("Trade-log-only entries (replay did not produce):");
            for l in &diff.trade_log_only {
                println!(
                    "  [{}] strategy={} outcome={:?} net={} bps={} notional={}",
                    l.id, l.strategy_id, l.outcome, l.net_profit, l.net_profit_bps, l.notional
                );
            }
        }
    } else {
        println!();
        println!("Replayed opportunities (no trade_log supplied):");
        if report.opportunities.is_empty() {
            println!("  (none)");
        }
        for o in &report.opportunities {
            println!(
                "  [{}] strategy={} net={} bps={} notional={} at {}",
                o.id, o.strategy_id, o.net_profit, o.net_profit_bps, o.notional, o.detected_at
            );
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if let Some(quotes_path) = cli.quotes.as_deref() {
        let quotes = load_quotes(quotes_path)?;
        let quotes_loaded = quotes.len();
        let strategy = default_replay_strategy();
        let opps = replay_quotes(&strategy, quotes, cli.re_stamp).await;

        let diff = if let Some(tl_path) = cli.trade_log.as_deref() {
            let logs = load_trade_log(tl_path)?;
            Some(diff_opportunities(&opps, &logs))
        } else {
            None
        };

        let report = ReplayReport {
            quotes_loaded,
            opportunities_detected: opps.len(),
            opportunities: opps.iter().map(OppSummary::from).collect(),
            diff,
        };

        if cli.json {
            println!("{}", serde_json::to_string_pretty(&report)?);
        } else {
            print_replay_text(&report);
        }
        return Ok(());
    }

    // Phase 1: analyzer-only path.
    let path = cli
        .trade_log
        .as_deref()
        .context("trade_log path is required when --quotes is not set")?;
    let logs = load_trade_log(path)?;
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
    use arbx_core::models::enums::Side;
    use chrono::Duration;
    use smallvec::smallvec;

    fn sample_log(strategy: &str, net: Decimal, bps: Decimal, outcome: TradeOutcome) -> TradeLog {
        sample_log_with_id(&format!("id-{strategy}-{bps}"), strategy, net, bps, outcome)
    }

    fn sample_log_with_id(
        id: &str,
        strategy: &str,
        net: Decimal,
        bps: Decimal,
        outcome: TradeOutcome,
    ) -> TradeLog {
        TradeLog {
            id: id.into(),
            strategy_id: strategy.to_string(),
            outcome,
            legs: smallvec![arbx_core::models::trade_log::TradeLeg {
                venue: Venue::Binance,
                instrument: spot_btc_usdt(),
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

    fn sample_opportunity(id: &str, net: Decimal, bps: Decimal) -> Opportunity {
        use arbx_core::strategy::{Economics, Leg, OpportunityKind, OpportunityMeta};
        Opportunity {
            id: id.into(),
            kind: OpportunityKind::CrossExchange,
            legs: smallvec![Leg {
                venue: Venue::Binance,
                instrument: spot_btc_usdt(),
                side: Side::Buy,
                quote_price: dec!(50000),
                order_price: dec!(50000),
                quantity: dec!(1),
                fee_estimate: dec!(5),
            }],
            economics: Economics {
                gross_profit: net + dec!(2),
                fees_total: dec!(2),
                net_profit: net,
                net_profit_bps: bps,
                notional: dec!(50000),
            },
            meta: OpportunityMeta {
                detected_at: Utc::now(),
                quote_ts_per_leg: smallvec![Utc::now()],
                ttl: Duration::milliseconds(500),
                strategy_id: "cross_exchange".into(),
            },
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

    #[test]
    fn diff_partitions_by_id_membership() {
        let opps = vec![
            sample_opportunity("shared-1", dec!(10), dec!(5)),
            sample_opportunity("replay-only-1", dec!(8), dec!(4)),
            sample_opportunity("shared-2", dec!(12), dec!(6)),
        ];
        let logs = vec![
            sample_log_with_id(
                "shared-1",
                "cross_exchange",
                dec!(10),
                dec!(5),
                TradeOutcome::AllSubmitted,
            ),
            sample_log_with_id(
                "shared-2",
                "cross_exchange",
                dec!(12),
                dec!(6),
                TradeOutcome::AllSubmitted,
            ),
            sample_log_with_id(
                "log-only-1",
                "cross_exchange",
                dec!(3),
                dec!(2),
                TradeOutcome::RiskRejected,
            ),
        ];
        let diff = diff_opportunities(&opps, &logs);
        assert_eq!(diff.matched.len(), 2);
        assert_eq!(diff.replay_only.len(), 1);
        assert_eq!(diff.trade_log_only.len(), 1);
        assert_eq!(diff.replay_only[0].id, "replay-only-1");
        assert_eq!(diff.trade_log_only[0].id, "log-only-1");
    }

    #[test]
    fn diff_empty_opps_puts_all_logs_in_trade_log_only() {
        let logs = vec![sample_log_with_id(
            "only-log",
            "cross_exchange",
            dec!(1),
            dec!(1),
            TradeOutcome::AllSubmitted,
        )];
        let diff = diff_opportunities(&[], &logs);
        assert!(diff.matched.is_empty());
        assert!(diff.replay_only.is_empty());
        assert_eq!(diff.trade_log_only.len(), 1);
    }

    #[test]
    fn diff_empty_logs_puts_all_opps_in_replay_only() {
        let opps = vec![sample_opportunity("o1", dec!(10), dec!(5))];
        let diff = diff_opportunities(&opps, &[]);
        assert!(diff.matched.is_empty());
        assert!(diff.trade_log_only.is_empty());
        assert_eq!(diff.replay_only.len(), 1);
    }

    fn profitable_quote_jsonl() -> String {
        // Binance cheap, Bybit expensive: cross-exchange arb exists.
        let quotes = [
            Quote {
                venue: Venue::Binance,
                instrument: spot_btc_usdt(),
                bid: dec!(49900),
                ask: dec!(49950),
                bid_size: dec!(10),
                ask_size: dec!(10),
                timestamp: Utc::now(),
            },
            Quote {
                venue: Venue::Bybit,
                instrument: spot_btc_usdt(),
                bid: dec!(50200),
                ask: dec!(50250),
                bid_size: dec!(10),
                ask_size: dec!(10),
                timestamp: Utc::now(),
            },
        ];
        quotes
            .iter()
            .map(|q| serde_json::to_string(q).unwrap())
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[tokio::test]
    async fn replay_produces_opportunities_on_profitable_fixture() {
        let strategy = default_replay_strategy();
        let quotes: Vec<Quote> = profitable_quote_jsonl()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        let opps = replay_quotes(&strategy, quotes, true).await;
        assert!(
            !opps.is_empty(),
            "expected at least one opportunity from profitable fixture"
        );
        assert!(opps[0].economics.net_profit > Decimal::ZERO);
    }

    #[tokio::test]
    async fn replay_produces_nothing_on_unprofitable_spread() {
        let strategy = default_replay_strategy();
        // Same price both venues → no arbitrage.
        let q = Quote {
            venue: Venue::Binance,
            instrument: spot_btc_usdt(),
            bid: dec!(50000),
            ask: dec!(50010),
            bid_size: dec!(10),
            ask_size: dec!(10),
            timestamp: Utc::now(),
        };
        let mut q2 = q.clone();
        q2.venue = Venue::Bybit;
        let opps = replay_quotes(&strategy, vec![q, q2], true).await;
        assert!(opps.is_empty(), "no opp expected, got {}", opps.len());
    }

    #[tokio::test]
    async fn replay_end_to_end_writes_jsonl_loads_and_diffs() {
        // Whole pipeline: JSONL quotes on disk → load_quotes → replay →
        // diff vs hand-written trade_log on disk.
        let dir = tempfile::tempdir().unwrap();
        let quotes_path = dir.path().join("quotes.jsonl");
        std::fs::write(&quotes_path, profitable_quote_jsonl()).unwrap();

        let loaded = load_quotes(quotes_path.to_str().unwrap()).unwrap();
        assert_eq!(loaded.len(), 2);

        let strategy = default_replay_strategy();
        let opps = replay_quotes(&strategy, loaded, true).await;
        assert!(!opps.is_empty());

        // Write a trade_log where only one id overlaps with replay output.
        let tl_path = dir.path().join("trades.jsonl");
        let overlap = sample_log_with_id(
            &opps[0].id,
            "cross_exchange",
            dec!(10),
            dec!(5),
            TradeOutcome::AllSubmitted,
        );
        let stale = sample_log_with_id(
            "unseen-id",
            "cross_exchange",
            dec!(1),
            dec!(1),
            TradeOutcome::RiskRejected,
        );
        let body = format!(
            "{}\n{}\n",
            serde_json::to_string(&overlap).unwrap(),
            serde_json::to_string(&stale).unwrap()
        );
        std::fs::write(&tl_path, body).unwrap();

        let logs = load_trade_log(tl_path.to_str().unwrap()).unwrap();
        let diff = diff_opportunities(&opps, &logs);
        assert_eq!(diff.matched.len(), 1, "one id should overlap");
        assert_eq!(
            diff.trade_log_only.len(),
            1,
            "the stale log should show up as trade_log_only"
        );
    }
}
