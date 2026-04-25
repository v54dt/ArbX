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
use arbx_core::strategy::cross_venue_funding::CrossVenueFundingStrategy;
use arbx_core::strategy::ewma_spread::EwmaSpreadStrategy;
use arbx_core::strategy::funding_rate::FundingRateStrategy;
use arbx_core::strategy::multi_pair_cross_exchange::{MultiPairCrossExchangeStrategy, PairConfig};
use arbx_core::strategy::signal_momentum::SignalMomentumStrategy;
use arbx_core::strategy::triangular_arb::{TriangleCycle, TriangleLeg, TriangularArbStrategy};
use arbx_core::strategy::tw_etf_futures::TwEtfFuturesStrategy;
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

    /// Re-stamp every replayed quote's `timestamp` to the latest fixture
    /// timestamp seen so far, so `max_quote_age_ms` filters don't drop stale
    /// fixture data. Defaults to true.
    #[arg(long, default_value_t = true)]
    re_stamp: bool,

    /// Path to a config YAML (same format as arbx-core). When set, the
    /// strategy is built from the config instead of the hardcoded default.
    /// Enables replay with any strategy type (cross_exchange, ewma_spread,
    /// triangular_arb, etc.).
    #[arg(long, short)]
    config: Option<String>,
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

fn build_strategy_from_config(config_path: &str) -> anyhow::Result<Box<dyn ArbitrageStrategy>> {
    let cfg = arbx_core::config::load(config_path)?;
    let venue_a = match cfg.venues[0].name.to_lowercase().as_str() {
        "binance" => Venue::Binance,
        "bybit" => Venue::Bybit,
        "okx" => Venue::Okx,
        other => anyhow::bail!("unsupported venue: {other}"),
    };
    let venue_b = match cfg.venues.get(1).map(|v| v.name.to_lowercase()) {
        Some(ref n) if n == "binance" => Venue::Binance,
        Some(ref n) if n == "bybit" => Venue::Bybit,
        Some(ref n) if n == "okx" => Venue::Okx,
        _ => venue_a,
    };
    let instrument_a = Instrument {
        asset_class: AssetClass::Crypto,
        instrument_type: InstrumentType::Spot,
        base: cfg.strategy.instrument_a.base.clone(),
        quote: cfg.strategy.instrument_a.quote.clone(),
        settle_currency: cfg.strategy.instrument_a.settle_currency.clone(),
        expiry: None,
        last_trade_time: None,
        settlement_time: None,
    };
    let instrument_b = Instrument {
        asset_class: AssetClass::Crypto,
        instrument_type: InstrumentType::Spot,
        base: cfg.strategy.instrument_b.base.clone(),
        quote: cfg.strategy.instrument_b.quote.clone(),
        settle_currency: cfg.strategy.instrument_b.settle_currency.clone(),
        expiry: None,
        last_trade_time: None,
        settlement_time: None,
    };
    let fee_default = dec!(0.001);
    let fee_a = FeeSchedule::new(
        venue_a,
        cfg.venues[0].fee_maker_override.unwrap_or(fee_default),
        cfg.venues[0].fee_taker_override.unwrap_or(fee_default),
    );
    let fee_b = FeeSchedule::new(
        venue_b,
        cfg.venues
            .get(1)
            .and_then(|v| v.fee_maker_override)
            .unwrap_or(fee_default),
        cfg.venues
            .get(1)
            .and_then(|v| v.fee_taker_override)
            .unwrap_or(fee_default),
    );
    let tick = cfg.strategy.tick_size_a.unwrap_or(dec!(0.01));
    let lot = cfg.strategy.lot_size_a.unwrap_or(dec!(0.001));

    let tick_b = cfg.strategy.tick_size_b.unwrap_or(tick);
    let lot_b = cfg.strategy.lot_size_b.unwrap_or(lot);

    match cfg.strategy.name.as_str() {
        "cross_exchange" => Ok(Box::new(CrossExchangeStrategy {
            venue_a,
            venue_b,
            instrument_a,
            instrument_b,
            min_net_profit_bps: cfg.strategy.min_net_profit_bps,
            max_quantity: cfg.strategy.max_quantity,
            fee_a,
            fee_b,
            max_quote_age_ms: cfg.strategy.max_quote_age_ms,
            tick_size_a: tick,
            tick_size_b: tick_b,
            lot_size_a: lot,
            lot_size_b: lot_b,
            max_book_depth: cfg.strategy.max_book_depth,
        })),
        "ewma_spread" => Ok(Box::new(EwmaSpreadStrategy::new(
            venue_a,
            venue_b,
            instrument_a,
            instrument_b,
            fee_a,
            fee_b,
            cfg.strategy.ewma_alpha.unwrap_or(dec!(0.05)),
            cfg.strategy.ewma_entry_sigma.unwrap_or(dec!(2.0)),
            cfg.strategy.max_quantity,
            cfg.strategy.min_net_profit_bps,
            cfg.strategy.max_quote_age_ms,
            tick,
            tick_b,
            lot,
            cfg.strategy.max_book_depth,
            cfg.strategy.ewma_min_samples.unwrap_or(60),
        ))),
        "funding_rate" => Ok(Box::new(FundingRateStrategy {
            venue: venue_a,
            instrument_perp: instrument_a,
            instrument_spot: instrument_b,
            min_funding_rate_bps: cfg
                .strategy
                .funding_min_bps
                .unwrap_or(cfg.strategy.min_net_profit_bps),
            max_quantity: cfg.strategy.max_quantity,
            fee_perp: fee_a,
            fee_spot: fee_b,
            max_quote_age_ms: cfg.strategy.max_quote_age_ms,
            funding_interval_hours: cfg.strategy.funding_interval_hours.unwrap_or(8),
        })),
        "cross_venue_funding" => Ok(Box::new(CrossVenueFundingStrategy {
            venue_a,
            venue_b,
            instrument_a,
            instrument_b,
            min_funding_diff_bps: cfg.strategy.min_net_profit_bps,
            max_quantity: cfg.strategy.max_quantity,
            fee_a,
            fee_b,
            max_quote_age_ms: cfg.strategy.max_quote_age_ms,
            funding_interval_hours: cfg.strategy.funding_interval_hours.unwrap_or(8),
            tick_size_a: tick,
            tick_size_b: tick_b,
            lot_size_a: lot,
            lot_size_b: lot_b,
            holding_intervals: 21,
        })),
        "multi_pair_cross_exchange" => {
            // Build a single pair from instrument_a/b in the config; multi-pair
            // tuning is best done in the live config (extra_strategies). Replay
            // is a single-pair check.
            let pairs = vec![PairConfig {
                venue_a,
                venue_b,
                instrument_a: instrument_a.clone(),
                instrument_b: instrument_b.clone(),
                max_quantity: cfg.strategy.max_quantity,
                tick_size_a: tick,
                tick_size_b: tick_b,
                lot_size_a: lot,
                lot_size_b: lot_b,
                fee_a: fee_a.clone(),
                fee_b: fee_b.clone(),
            }];
            Ok(Box::new(MultiPairCrossExchangeStrategy {
                pairs,
                min_net_profit_bps: cfg.strategy.min_net_profit_bps,
                max_quote_age_ms: cfg.strategy.max_quote_age_ms,
                max_book_depth: cfg.strategy.max_book_depth,
            }))
        }
        "triangular_arb" => {
            if cfg.strategy.triangle_cycles.is_empty() {
                anyhow::bail!("triangular_arb config has no triangle_cycles");
            }
            let mut cycles: Vec<TriangleCycle> = Vec::new();
            for c in &cfg.strategy.triangle_cycles {
                let mk_inst = |base: &str, quote_cur: &str| Instrument {
                    asset_class: AssetClass::Crypto,
                    instrument_type: InstrumentType::Spot,
                    base: base.into(),
                    quote: quote_cur.into(),
                    settle_currency: None,
                    expiry: None,
                    last_trade_time: None,
                    settlement_time: None,
                };
                let parse_side = |s: &str| match s.to_lowercase().as_str() {
                    "buy" => arbx_core::models::enums::Side::Buy,
                    _ => arbx_core::models::enums::Side::Sell,
                };
                cycles.push(TriangleCycle {
                    venue: venue_a,
                    leg_a: TriangleLeg {
                        instrument: mk_inst(&c.leg_a.base, &c.leg_a.quote),
                        side: parse_side(&c.leg_a.side),
                    },
                    leg_b: TriangleLeg {
                        instrument: mk_inst(&c.leg_b.base, &c.leg_b.quote),
                        side: parse_side(&c.leg_b.side),
                    },
                    leg_c: TriangleLeg {
                        instrument: mk_inst(&c.leg_c.base, &c.leg_c.quote),
                        side: parse_side(&c.leg_c.side),
                    },
                    fee: fee_a.clone(),
                    max_notional_usdt: c.max_notional_usdt,
                    min_net_profit_bps: c
                        .min_net_profit_bps
                        .unwrap_or(cfg.strategy.min_net_profit_bps),
                    tick_size: c.tick_size,
                    lot_size: c.lot_size,
                });
            }
            Ok(Box::new(TriangularArbStrategy {
                cycles,
                max_quote_age_ms: cfg.strategy.max_quote_age_ms,
                max_book_depth: cfg.strategy.max_book_depth,
            }))
        }
        "tw_etf_futures" => Ok(Box::new(TwEtfFuturesStrategy {
            venue: venue_a,
            etf_instrument: instrument_a,
            futures_instrument: instrument_b,
            hedge_ratio: cfg.strategy.tw_hedge_ratio.unwrap_or(dec!(1.0)),
            min_net_profit_bps: cfg.strategy.min_net_profit_bps,
            max_quantity: cfg.strategy.max_quantity,
            fee_etf: fee_a,
            fee_futures: fee_b,
            max_quote_age_ms: cfg.strategy.max_quote_age_ms,
            cost_of_carry_bps: cfg.strategy.tw_cost_of_carry_bps.unwrap_or(dec!(0)),
            days_to_expiry: cfg.strategy.tw_days_to_expiry.unwrap_or(30),
        })),
        "signal_momentum" => Ok(Box::new(SignalMomentumStrategy {
            venue: venue_a,
            instrument: instrument_a,
            fee: fee_a,
            min_signal_strength: dec!(0.2),
            max_quantity: cfg.strategy.max_quantity,
            max_quote_age_ms: cfg.strategy.max_quote_age_ms,
            ttl_ms: 3000,
            alpha_bps_at_full_signal: dec!(20),
            min_net_profit_bps: cfg.strategy.min_net_profit_bps,
        })),
        other => anyhow::bail!(
            "unsupported strategy '{}' in --config; supported: cross_exchange, \
             multi_pair_cross_exchange, ewma_spread, funding_rate, cross_venue_funding, \
             triangular_arb, tw_etf_futures, signal_momentum",
            other
        ),
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
        local_timestamp: q.timestamp, // OK: replay uses fixture timestamp for determinism
    }
}

async fn replay_quotes(
    strategy: &dyn ArbitrageStrategy,
    quotes: Vec<Quote>,
    re_stamp: bool,
) -> Vec<Opportunity> {
    let mut books: BookMap = BookMap::default();
    let portfolios: HashMap<String, arbx_core::models::position::PortfolioSnapshot> =
        HashMap::new();
    let mut opportunities = Vec::new();
    // Monotonic replay clock: each quote advances to the latest timestamp seen.
    let mut replay_now: Option<DateTime<Utc>> = None;

    for mut q in quotes {
        if re_stamp {
            // Advance the replay clock to the latest fixture timestamp, then
            // re-stamp the quote so staleness filters see it as "just arrived".
            let now = match replay_now {
                Some(prev) => prev.max(q.timestamp),
                None => q.timestamp,
            };
            replay_now = Some(now);
            q.timestamp = now;
        } else {
            // Even without re-stamp, track the latest quote time for evaluate().
            replay_now = Some(match replay_now {
                Some(prev) => prev.max(q.timestamp),
                None => q.timestamp,
            });
        }
        let key = book_key(q.venue, &q.instrument);
        books
            .entry(key)
            .and_modify(|b| b.update_from_quote(&q))
            .or_insert_with(|| quote_to_book(&q));

        // Use the replay clock (fixture-derived) instead of wall time so
        // strategy staleness checks are deterministic across runs.
        let eval_now = replay_now.unwrap_or_else(Utc::now);
        if let Some(opp) = strategy
            .evaluate(
                &books,
                &portfolios,
                eval_now,
                &arbx_core::engine::signal::SignalCache::new(),
            )
            .await
        {
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
        let strategy: Box<dyn ArbitrageStrategy> = if let Some(cfg_path) = cli.config.as_deref() {
            build_strategy_from_config(cfg_path)?
        } else {
            Box::new(default_replay_strategy())
        };
        let opps = replay_quotes(strategy.as_ref(), quotes, cli.re_stamp).await;

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
                submitted_at: Utc::now(), // OK: test fixture only
            }],
            expected_gross_profit: net + dec!(2),
            expected_fees: dec!(2),
            expected_net_profit: net,
            expected_net_profit_bps: bps,
            notional: dec!(50000),
            created_at: Utc::now(), // OK: test fixture only
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
                detected_at: Utc::now(),                 // OK: test fixture only
                quote_ts_per_leg: smallvec![Utc::now()], // OK: test fixture only
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
                timestamp: Utc::now(), // OK: test fixture, re-stamped by replay
            },
            Quote {
                venue: Venue::Bybit,
                instrument: spot_btc_usdt(),
                bid: dec!(50200),
                ask: dec!(50250),
                bid_size: dec!(10),
                ask_size: dec!(10),
                timestamp: Utc::now(), // OK: test fixture, re-stamped by replay
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
            timestamp: Utc::now(), // OK: test fixture, re-stamped by replay
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
