#[allow(dead_code)]
mod adapters;
#[allow(dead_code)]
mod backtest;
mod config;
mod engine;
#[allow(dead_code)]
mod ipc;
mod metrics;
#[allow(dead_code)]
mod models;
#[allow(dead_code)]
mod risk;
#[allow(dead_code)]
mod strategy;

use adapters::binance::fee_provider::BinanceFeeProvider;
use adapters::binance::market_data::{BinanceMarket, BinanceMarketData};
use adapters::binance::order_executor::BinanceOrderExecutor;
use adapters::binance::position_manager::BinancePositionManager;
use adapters::binance::private_stream::BinancePrivateStream;
use adapters::binance::rest_client::BinanceRestClient;
use adapters::bybit::fee_provider::BybitFeeProvider;
use adapters::bybit::market_data::{BybitMarket, BybitMarketData};
use adapters::bybit::order_executor::BybitOrderExecutor;
use adapters::bybit::position_manager::BybitPositionManager;
use adapters::bybit::private_stream::BybitPrivateStream;
use adapters::bybit::rest_client::BybitRestClient;
use adapters::fee_provider::FeeProvider;
use adapters::market_data::MarketDataFeed;
use adapters::okx::fee_provider::OkxFeeProvider;
use adapters::okx::market_data::OkxMarketData;
use adapters::okx::order_executor::OkxOrderExecutor;
use adapters::okx::position_manager::OkxPositionManager;
use adapters::okx::private_stream::OkxPrivateStream;
use adapters::okx::rest_client::OkxRestClient;
use adapters::paper_executor::PaperExecutor;
use adapters::private_stream::PrivateStream;
use engine::arbitrage::ArbitrageEngine;
use models::enums::{Side, Venue};
use models::fee::FeeSchedule;
use models::instrument::{AssetClass, Instrument, InstrumentType};
use risk::circuit_breaker::CircuitBreaker;
use risk::limits::{MaxDailyLoss, MaxNotionalExposure, MaxPositionPerVenue, MaxPositionSize};
use risk::manager::RiskManager;
use risk::state::RiskState;
use strategy::base::ArbitrageStrategy;
use strategy::cross_exchange::CrossExchangeStrategy;
use strategy::ewma_spread::EwmaSpreadStrategy;
use strategy::funding_rate::FundingRateStrategy;
use strategy::multi_pair_cross_exchange::{MultiPairCrossExchangeStrategy, PairConfig};
use strategy::triangular_arb::{TriangleCycle, TriangleLeg, TriangularArbStrategy};
use strategy::tw_etf_futures::TwEtfFuturesStrategy;

use clap::Parser;
use config::{InstrumentConfig, VenueConfig};

#[derive(Parser)]
#[command(name = "arbx", about = "Cross-market arbitrage engine")]
struct Cli {
    #[arg(short, long, default_value = "config/default.yaml")]
    config: String,

    #[arg(long)]
    dry_run: bool,

    #[arg(long)]
    log_level: Option<String>,

    /// Run offline backtest over the given quote CSV and exit.
    #[arg(long, value_name = "CSV")]
    backtest: Option<String>,

    /// When --backtest is set, also write per-trade rows to this CSV.
    #[arg(long, value_name = "CSV")]
    backtest_csv_out: Option<String>,

    /// When --backtest is set, split quotes into windows of this size and
    /// report per-window stats (walk-forward style).
    #[arg(long, value_name = "N")]
    backtest_window_size: Option<usize>,

    /// When --backtest is set, also append every TradeLog to this JSONL file.
    #[arg(long, value_name = "JSONL")]
    backtest_trade_log: Option<String>,

    /// Publish each Quote / OrderBook to the default Aeron IPC stream (requires media driver).
    #[arg(long)]
    aeron_publish: bool,

    /// Replace WS feeds with an Aeron Subscriber on the given stream id (requires media driver).
    #[arg(long, value_name = "STREAM_ID")]
    aeron_subscribe: Option<i32>,

    /// Load + validate config, log resolved venues / strategy / pairs, exit 0.
    /// No network, no threads. Pre-deploy sanity check.
    #[arg(long)]
    dry_run_validate: bool,
}

fn parse_venue(name: &str) -> anyhow::Result<Venue> {
    match name.to_lowercase().as_str() {
        "binance" => Ok(Venue::Binance),
        "bybit" => Ok(Venue::Bybit),
        "okx" => Ok(Venue::Okx),
        "fubon" => Ok(Venue::Fubon),
        "shioaji" => Ok(Venue::Shioaji),
        other => anyhow::bail!("unknown venue: {}", other),
    }
}

fn parse_binance_market(market: &str) -> anyhow::Result<BinanceMarket> {
    match market.to_lowercase().as_str() {
        "spot" => Ok(BinanceMarket::Spot),
        "usdt_futures" => Ok(BinanceMarket::UsdtFutures),
        "coin_futures" => Ok(BinanceMarket::CoinFutures),
        other => anyhow::bail!("unknown binance market: {}", other),
    }
}

fn parse_bybit_market(market: &str) -> anyhow::Result<BybitMarket> {
    match market.to_lowercase().as_str() {
        "spot" => Ok(BybitMarket::Spot),
        "linear" | "usdt_futures" => Ok(BybitMarket::Linear),
        "inverse" | "coin_futures" => Ok(BybitMarket::Inverse),
        other => anyhow::bail!("unknown bybit market: {}", other),
    }
}

fn parse_instrument(cfg: &InstrumentConfig) -> anyhow::Result<Instrument> {
    let instrument_type = match cfg.instrument_type.to_lowercase().as_str() {
        "spot" => InstrumentType::Spot,
        "swap" => InstrumentType::Swap,
        "futures" | "future" => InstrumentType::Futures,
        "option" => InstrumentType::Option,
        other => anyhow::bail!("unknown instrument type: {}", other),
    };
    Ok(Instrument {
        asset_class: AssetClass::Crypto,
        instrument_type,
        base: cfg.base.clone(),
        quote: cfg.quote.clone(),
        settle_currency: cfg.settle_currency.clone(),
        expiry: None,
        last_trade_time: None,
        settlement_time: None,
    })
}

fn format_symbol(venue_name: &str, base: &str, quote: &str) -> String {
    match venue_name.to_lowercase().as_str() {
        "okx" => format!("{}-{}", base, quote).to_uppercase(),
        _ => format!("{}{}", base, quote).to_uppercase(),
    }
}

fn register_venue_instruments(
    venue_cfg: &VenueConfig,
    feed: &mut dyn RegisterInstrument,
) -> anyhow::Result<()> {
    if let Some(ref instruments) = venue_cfg.instruments {
        for icfg in instruments {
            let inst = parse_instrument(icfg)?;
            let sym = format_symbol(&venue_cfg.name, &icfg.base, &icfg.quote);
            feed.register_instrument(&sym, inst);
        }
    }
    Ok(())
}

trait RegisterInstrument {
    fn register_instrument(&mut self, symbol: &str, instrument: Instrument);
}

impl RegisterInstrument for BinanceMarketData {
    fn register_instrument(&mut self, symbol: &str, instrument: Instrument) {
        BinanceMarketData::register_instrument(self, symbol, instrument);
    }
}

impl RegisterInstrument for OkxMarketData {
    fn register_instrument(&mut self, symbol: &str, instrument: Instrument) {
        OkxMarketData::register_instrument(self, symbol, instrument);
    }
}

impl RegisterInstrument for BybitMarketData {
    fn register_instrument(&mut self, symbol: &str, instrument: Instrument) {
        BybitMarketData::register_instrument(self, symbol, instrument);
    }
}

fn build_market_data(
    venue_cfg: &VenueConfig,
    symbol: &str,
    instrument: Instrument,
) -> anyhow::Result<Box<dyn MarketDataFeed>> {
    match venue_cfg.name.to_lowercase().as_str() {
        "binance" => {
            let market = parse_binance_market(&venue_cfg.market)?;
            let mut feed = BinanceMarketData::new(market);
            feed.register_instrument(symbol, instrument);
            register_venue_instruments(venue_cfg, &mut feed)?;
            Ok(Box::new(feed))
        }
        "okx" => {
            let mut feed = OkxMarketData::new();
            feed.register_instrument(symbol, instrument);
            register_venue_instruments(venue_cfg, &mut feed)?;
            Ok(Box::new(feed))
        }
        "bybit" => {
            let market = parse_bybit_market(&venue_cfg.market)?;
            let mut feed = BybitMarketData::new(market);
            feed.register_instrument(symbol, instrument);
            register_venue_instruments(venue_cfg, &mut feed)?;
            Ok(Box::new(feed))
        }
        other => anyhow::bail!("unsupported venue for market data: {}", other),
    }
}

fn build_executor(
    venue_cfg: &VenueConfig,
) -> anyhow::Result<Box<dyn adapters::order_executor::OrderExecutor>> {
    match venue_cfg.name.to_lowercase().as_str() {
        "binance" => {
            let market = parse_binance_market(&venue_cfg.market)?;
            let exec = BinanceOrderExecutor::new(
                market,
                venue_cfg.api_key.clone(),
                venue_cfg.api_secret.clone(),
            )?;
            Ok(Box::new(exec))
        }
        "okx" => {
            let passphrase = venue_cfg.passphrase.clone().unwrap_or_default();
            let exec = OkxOrderExecutor::new(
                venue_cfg.api_key.clone(),
                venue_cfg.api_secret.clone(),
                passphrase,
            )?;
            Ok(Box::new(exec))
        }
        "bybit" => {
            let market = parse_bybit_market(&venue_cfg.market)?;
            let exec = BybitOrderExecutor::new(
                market,
                venue_cfg.api_key.clone(),
                venue_cfg.api_secret.clone(),
            )?;
            Ok(Box::new(exec))
        }
        other => anyhow::bail!("unsupported venue for executor: {}", other),
    }
}

fn build_position_manager(
    venue_cfg: &VenueConfig,
) -> anyhow::Result<Box<dyn adapters::position_manager::PositionManager>> {
    match venue_cfg.name.to_lowercase().as_str() {
        "binance" => {
            let market = parse_binance_market(&venue_cfg.market)?;
            let pm =
                BinancePositionManager::new(market, &venue_cfg.api_key, &venue_cfg.api_secret)?;
            Ok(Box::new(pm))
        }
        "okx" => {
            let passphrase = venue_cfg.passphrase.as_deref().unwrap_or_default();
            let pm =
                OkxPositionManager::new(&venue_cfg.api_key, &venue_cfg.api_secret, passphrase)?;
            Ok(Box::new(pm))
        }
        "bybit" => {
            let market = parse_bybit_market(&venue_cfg.market)?;
            let pm = BybitPositionManager::new(market, &venue_cfg.api_key, &venue_cfg.api_secret)?;
            Ok(Box::new(pm))
        }
        other => anyhow::bail!("unsupported venue for position manager: {}", other),
    }
}

fn build_private_streams(venue_cfg: &VenueConfig) -> Vec<Box<dyn PrivateStream>> {
    if venue_cfg.api_key.is_empty() || venue_cfg.api_secret.is_empty() {
        tracing::warn!(
            venue = venue_cfg.name.as_str(),
            "no API credentials; skipping private stream (fills from engine-owned executor only)"
        );
        return Vec::new();
    }
    match venue_cfg.name.to_lowercase().as_str() {
        "binance" => match parse_binance_market(&venue_cfg.market) {
            Ok(market) => {
                let rest_base = market.rest_base_url();
                match BinancePrivateStream::new(
                    market,
                    rest_base,
                    &venue_cfg.api_key,
                    &venue_cfg.api_secret,
                ) {
                    Ok(s) => vec![Box::new(s)],
                    Err(e) => {
                        tracing::warn!(error = %e, "failed to build Binance private stream");
                        Vec::new()
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "skipping Binance private stream");
                Vec::new()
            }
        },
        "okx" => {
            let passphrase = venue_cfg.passphrase.as_deref().unwrap_or_default();
            vec![Box::new(OkxPrivateStream::new(
                &venue_cfg.api_key,
                &venue_cfg.api_secret,
                passphrase,
            ))]
        }
        "bybit" => vec![Box::new(BybitPrivateStream::new(
            &venue_cfg.api_key,
            &venue_cfg.api_secret,
        ))],
        _ => Vec::new(),
    }
}

async fn fetch_fee_schedule(venue_cfg: &VenueConfig, venue: Venue) -> anyhow::Result<FeeSchedule> {
    if let (Some(maker), Some(taker)) = (venue_cfg.fee_maker_override, venue_cfg.fee_taker_override)
    {
        tracing::info!(?venue, %maker, %taker, "using fee overrides from config");
        return Ok(FeeSchedule::new(venue, maker, taker));
    }

    if venue_cfg.api_key.is_empty() || venue_cfg.api_secret.is_empty() {
        tracing::warn!(?venue, "no API credentials, using default fee schedule");
        return Ok(FeeSchedule::new(
            venue,
            rust_decimal_macros::dec!(0.001),
            rust_decimal_macros::dec!(0.001),
        ));
    }

    match venue_cfg.name.to_lowercase().as_str() {
        "binance" => {
            let market = parse_binance_market(&venue_cfg.market)?;
            let rest = BinanceRestClient::new(
                market.rest_base_url(),
                &venue_cfg.api_key,
                &venue_cfg.api_secret,
            )?;
            let provider = BinanceFeeProvider::new(rest, market);
            match provider.get_fee_schedule().await {
                Ok(fee) => Ok(fee),
                Err(e) => {
                    tracing::warn!(%e, ?venue, "fee provider failed, using defaults");
                    Ok(FeeSchedule::new(
                        venue,
                        rust_decimal_macros::dec!(0.001),
                        rust_decimal_macros::dec!(0.001),
                    ))
                }
            }
        }
        "okx" => {
            let passphrase = venue_cfg.passphrase.as_deref().unwrap_or_default();
            let rest = OkxRestClient::new(
                "https://www.okx.com",
                &venue_cfg.api_key,
                &venue_cfg.api_secret,
                passphrase,
            )?;
            let inst_type = match venue_cfg.market.to_lowercase().as_str() {
                "spot" => "SPOT",
                _ => "SWAP",
            };
            let provider = OkxFeeProvider::new(rest, inst_type);
            match provider.get_fee_schedule().await {
                Ok(fee) => Ok(fee),
                Err(e) => {
                    tracing::warn!(%e, ?venue, "fee provider failed, using defaults");
                    Ok(FeeSchedule::new(
                        venue,
                        rust_decimal_macros::dec!(0.001),
                        rust_decimal_macros::dec!(0.001),
                    ))
                }
            }
        }
        "bybit" => {
            let market = parse_bybit_market(&venue_cfg.market)?;
            let rest = BybitRestClient::new(
                "https://api.bybit.com",
                &venue_cfg.api_key,
                &venue_cfg.api_secret,
            )?;
            let provider = BybitFeeProvider::new(rest, market);
            match provider.get_fee_schedule().await {
                Ok(fee) => Ok(fee),
                Err(e) => {
                    tracing::warn!(%e, ?venue, "fee provider failed, using defaults");
                    Ok(FeeSchedule::new(
                        venue,
                        rust_decimal_macros::dec!(0.001),
                        rust_decimal_macros::dec!(0.001),
                    ))
                }
            }
        }
        other => anyhow::bail!("unsupported venue for fees: {}", other),
    }
}

fn build_triangle_leg(cfg: &config::TriangleLegConfig) -> anyhow::Result<TriangleLeg> {
    let side = match cfg.side.to_lowercase().as_str() {
        "buy" => Side::Buy,
        "sell" => Side::Sell,
        other => anyhow::bail!("triangle leg side must be 'buy' or 'sell', got {}", other),
    };
    Ok(TriangleLeg {
        instrument: Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: InstrumentType::Spot,
            base: cfg.base.clone(),
            quote: cfg.quote.clone(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        },
        side,
    })
}

#[allow(clippy::too_many_arguments)]
fn build_strategy_from_config(
    cfg: &config::AppConfig,
    strategy_cfg: &config::StrategyConfig,
    venue_a: Venue,
    venue_b: Venue,
    instrument_a: Instrument,
    instrument_b: Instrument,
    fee_a: FeeSchedule,
    fee_b: FeeSchedule,
) -> anyhow::Result<Box<dyn ArbitrageStrategy>> {
    let tick_a = strategy_cfg
        .tick_size_a
        .unwrap_or(rust_decimal_macros::dec!(0.01));
    let tick_b = strategy_cfg
        .tick_size_b
        .unwrap_or(rust_decimal_macros::dec!(0.01));
    let lot_a = strategy_cfg
        .lot_size_a
        .unwrap_or(rust_decimal_macros::dec!(0.00001));
    let lot_b = strategy_cfg
        .lot_size_b
        .unwrap_or(rust_decimal_macros::dec!(0.00001));

    let strategy: Box<dyn ArbitrageStrategy> = match strategy_cfg.name.as_str() {
        "cross_exchange" => {
            tracing::info!(venue_a = ?venue_a, venue_b = ?venue_b, "using CrossExchangeStrategy");
            Box::new(CrossExchangeStrategy {
                venue_a,
                venue_b,
                instrument_a,
                instrument_b,
                min_net_profit_bps: strategy_cfg.min_net_profit_bps,
                max_quantity: strategy_cfg.max_quantity,
                fee_a,
                fee_b,
                max_quote_age_ms: strategy_cfg.max_quote_age_ms,
                tick_size_a: tick_a,
                tick_size_b: tick_b,
                lot_size_a: lot_a,
                lot_size_b: lot_b,
                max_book_depth: strategy_cfg.max_book_depth,
            })
        }
        "multi_pair" => {
            let extra_instruments: Vec<_> = cfg.venues[0]
                .instruments
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .filter_map(|icfg| parse_instrument(icfg).ok())
                .collect();
            if extra_instruments.len() < 2 {
                anyhow::bail!(
                    "strategy.name=multi_pair requires venues[0].instruments to list ≥ 2 \
                     entries; found {}",
                    extra_instruments.len()
                );
            }
            let pairs: Vec<PairConfig> = extra_instruments
                .iter()
                .map(|inst| {
                    let mut inst_b = inst.clone();
                    inst_b.instrument_type = instrument_b.instrument_type;
                    inst_b.settle_currency = instrument_b.settle_currency.clone();
                    PairConfig {
                        venue_a,
                        venue_b,
                        instrument_a: inst.clone(),
                        instrument_b: inst_b,
                        max_quantity: strategy_cfg.max_quantity,
                        tick_size_a: tick_a,
                        tick_size_b: tick_b,
                        lot_size_a: lot_a,
                        lot_size_b: lot_b,
                        fee_a: fee_a.clone(),
                        fee_b: fee_b.clone(),
                    }
                })
                .collect();
            tracing::info!(
                pairs = pairs.len(),
                venue_a = ?venue_a,
                venue_b = ?venue_b,
                "using MultiPairCrossExchangeStrategy"
            );
            Box::new(MultiPairCrossExchangeStrategy {
                pairs,
                min_net_profit_bps: strategy_cfg.min_net_profit_bps,
                max_quote_age_ms: strategy_cfg.max_quote_age_ms,
                max_book_depth: strategy_cfg.max_book_depth,
            })
        }
        "ewma_spread" => {
            let alpha = cfg
                .strategy
                .ewma_alpha
                .unwrap_or(rust_decimal_macros::dec!(0.05));
            let entry_sigma = cfg
                .strategy
                .ewma_entry_sigma
                .unwrap_or(rust_decimal_macros::dec!(2.0));
            let min_samples = strategy_cfg.ewma_min_samples.unwrap_or(60);
            tracing::info!(
                %alpha, %entry_sigma, min_samples,
                venue_a = ?venue_a, venue_b = ?venue_b,
                "using EwmaSpreadStrategy"
            );
            Box::new(EwmaSpreadStrategy::new(
                venue_a,
                venue_b,
                instrument_a,
                instrument_b,
                fee_a,
                fee_b,
                alpha,
                entry_sigma,
                strategy_cfg.max_quantity,
                strategy_cfg.min_net_profit_bps,
                strategy_cfg.max_quote_age_ms,
                tick_a,
                tick_b,
                lot_a,
                strategy_cfg.max_book_depth,
                min_samples,
            ))
        }
        "triangular_arb" => {
            if strategy_cfg.triangle_cycles.is_empty() {
                anyhow::bail!(
                    "strategy.name=triangular_arb requires at least one entry in \
                     strategy.triangle_cycles"
                );
            }
            let cycles: Vec<TriangleCycle> = cfg
                .strategy
                .triangle_cycles
                .iter()
                .map(|c| {
                    Ok(TriangleCycle {
                        venue: venue_a,
                        leg_a: build_triangle_leg(&c.leg_a)?,
                        leg_b: build_triangle_leg(&c.leg_b)?,
                        leg_c: build_triangle_leg(&c.leg_c)?,
                        fee: fee_a.clone(),
                        max_notional_usdt: c.max_notional_usdt,
                        min_net_profit_bps: c
                            .min_net_profit_bps
                            .unwrap_or(strategy_cfg.min_net_profit_bps),
                        tick_size: c.tick_size,
                        lot_size: c.lot_size,
                    })
                })
                .collect::<anyhow::Result<_>>()?;
            tracing::info!(
                cycles = cycles.len(),
                venue = ?venue_a,
                "using TriangularArbStrategy"
            );
            Box::new(TriangularArbStrategy {
                cycles,
                max_quote_age_ms: strategy_cfg.max_quote_age_ms,
                max_book_depth: strategy_cfg.max_book_depth,
            })
        }
        "funding_rate" => {
            let min_funding_rate_bps = cfg
                .strategy
                .funding_min_bps
                .unwrap_or(strategy_cfg.min_net_profit_bps);
            tracing::info!(
                venue = ?venue_a,
                %min_funding_rate_bps,
                "using FundingRateStrategy"
            );
            Box::new(FundingRateStrategy {
                venue: venue_a,
                instrument_perp: instrument_a,
                instrument_spot: instrument_b,
                min_funding_rate_bps,
                max_quantity: strategy_cfg.max_quantity,
                fee_perp: fee_a,
                fee_spot: fee_b,
                max_quote_age_ms: strategy_cfg.max_quote_age_ms,
                funding_interval_hours: cfg
                    .strategy
                    .funding_interval_hours
                    .unwrap_or(strategy::funding_rate::DEFAULT_FUNDING_INTERVAL_HOURS),
            })
        }
        "tw_etf_futures" => {
            let hedge_ratio = cfg
                .strategy
                .tw_hedge_ratio
                .unwrap_or(rust_decimal_macros::dec!(1));
            let cost_of_carry_bps = cfg
                .strategy
                .tw_cost_of_carry_bps
                .unwrap_or(rust_decimal_macros::dec!(0));
            let days_to_expiry = strategy_cfg.tw_days_to_expiry.unwrap_or(30);
            tracing::info!(
                venue = ?venue_a,
                %hedge_ratio,
                %cost_of_carry_bps,
                days_to_expiry,
                "using TwEtfFuturesStrategy"
            );
            Box::new(TwEtfFuturesStrategy {
                venue: venue_a,
                etf_instrument: instrument_a,
                futures_instrument: instrument_b,
                hedge_ratio,
                min_net_profit_bps: strategy_cfg.min_net_profit_bps,
                max_quantity: strategy_cfg.max_quantity,
                fee_etf: fee_a,
                fee_futures: fee_b,
                max_quote_age_ms: strategy_cfg.max_quote_age_ms,
                cost_of_carry_bps,
                days_to_expiry,
            })
        }
        other => anyhow::bail!("unknown strategy.name: {other}"),
    };
    Ok(strategy)
}

async fn run_backtest_mode(
    csv_path: &str,
    csv_out: Option<&str>,
    trade_log_out: Option<&str>,
    window_size: Option<usize>,
    cfg: &config::AppConfig,
) -> anyhow::Result<()> {
    use backtest::data_feed::HistoricalDataFeed;
    use backtest::engine::run_backtest;

    tracing::info!(csv = csv_path, "loading historical quotes");
    let feed = HistoricalDataFeed::from_csv(csv_path)?;
    let quotes = feed.into_quotes();

    let venue_a = parse_venue(&cfg.venues[0].name)?;
    let venue_b = parse_venue(&cfg.venues[1 % cfg.venues.len()].name)?;
    let instrument_a = parse_instrument(&cfg.strategy.instrument_a)?;
    let instrument_b = parse_instrument(&cfg.strategy.instrument_b)?;

    let fee_default = rust_decimal_macros::dec!(0.001);
    let fee_a = FeeSchedule::new(
        venue_a,
        cfg.venues[0].fee_maker_override.unwrap_or(fee_default),
        cfg.venues[0].fee_taker_override.unwrap_or(fee_default),
    );
    let fee_b = FeeSchedule::new(
        venue_b,
        cfg.venues[1 % cfg.venues.len()]
            .fee_maker_override
            .unwrap_or(fee_default),
        cfg.venues[1 % cfg.venues.len()]
            .fee_taker_override
            .unwrap_or(fee_default),
    );

    let make_risk_cfg = || config::RiskConfig {
        max_position_size: cfg.risk.max_position_size,
        max_daily_loss: cfg.risk.max_daily_loss,
        max_notional_exposure: cfg.risk.max_notional_exposure,
        circuit_breaker: config::CircuitBreakerConfig {
            max_drawdown: cfg.risk.circuit_breaker.max_drawdown,
            max_orders_per_minute: cfg.risk.circuit_breaker.max_orders_per_minute,
            max_consecutive_failures: cfg.risk.circuit_breaker.max_consecutive_failures,
        },
        max_position_per_venue: cfg.risk.max_position_per_venue.clone(),
        backtest_fill_delay_ms: cfg.risk.backtest_fill_delay_ms,
        backtest_slippage_bps: cfg.risk.backtest_slippage_bps,
    };

    match window_size {
        None => {
            let strategy = build_strategy_from_config(
                cfg,
                &cfg.strategy,
                venue_a,
                venue_b,
                instrument_a.clone(),
                instrument_b.clone(),
                fee_a.clone(),
                fee_b.clone(),
            )?;
            let feed = HistoricalDataFeed::from_quotes(quotes);
            let result = run_backtest(vec![Box::new(feed)], strategy, make_risk_cfg()).await?;

            println!("─── Backtest result ───");
            println!("total_trades:      {}", result.total_trades);
            println!("profitable_trades: {}", result.profitable_trades);
            println!("total_pnl:         {}", result.total_pnl);
            println!("max_drawdown:      {}", result.max_drawdown);
            println!("sharpe_ratio:      {:.4}", result.sharpe_ratio);
            println!("duration_ms:       {}", result.duration_ms);

            if let Some(path) = csv_out {
                backtest::report::write_trade_csv(path, &result.trade_logs)?;
                println!("trade rows written: {}", path);
            }
            if let Some(path) = trade_log_out {
                let mut w = models::trade_log::TradeLogWriter::create(path)?;
                for log in &result.trade_logs {
                    w.append(log)?;
                }
                println!(
                    "trade_log JSONL written: {} ({} entries)",
                    path,
                    result.trade_logs.len()
                );
            }
        }
        Some(0) => anyhow::bail!("--backtest-window-size must be > 0"),
        Some(n) => {
            println!(
                "─── Walk-forward backtest: window={} quotes, total_quotes={} ───",
                n,
                quotes.len()
            );
            println!(
                "window,start_ts,end_ts,quotes,total_trades,profitable,total_pnl,max_drawdown,sharpe"
            );
            let mut all_trade_logs = Vec::new();
            for (i, chunk) in quotes.chunks(n).enumerate() {
                let start_ts = chunk
                    .first()
                    .map(|q| q.timestamp.timestamp_millis())
                    .unwrap_or(0);
                let end_ts = chunk
                    .last()
                    .map(|q| q.timestamp.timestamp_millis())
                    .unwrap_or(0);
                let window_feed = HistoricalDataFeed::from_quotes(chunk.to_vec());
                let window_strategy = build_strategy_from_config(
                    cfg,
                    &cfg.strategy,
                    venue_a,
                    venue_b,
                    instrument_a.clone(),
                    instrument_b.clone(),
                    fee_a.clone(),
                    fee_b.clone(),
                )?;
                let result = run_backtest(
                    vec![Box::new(window_feed)],
                    window_strategy,
                    make_risk_cfg(),
                )
                .await?;
                println!(
                    "{},{},{},{},{},{},{},{},{:.4}",
                    i,
                    start_ts,
                    end_ts,
                    chunk.len(),
                    result.total_trades,
                    result.profitable_trades,
                    result.total_pnl,
                    result.max_drawdown,
                    result.sharpe_ratio,
                );
                // Prefix ids with window index to avoid collisions when two windows
                // emit opportunities on the same nanosecond.
                all_trade_logs.extend(result.trade_logs.into_iter().map(|mut log| {
                    log.id = format!("w{}-{}", i, log.id);
                    log
                }));
            }

            if let Some(path) = csv_out {
                backtest::report::write_trade_csv(path, &all_trade_logs)?;
                println!("trade rows written: {}", path);
            }
            if let Some(path) = trade_log_out {
                let mut w = models::trade_log::TradeLogWriter::create(path)?;
                for log in &all_trade_logs {
                    w.append(log)?;
                }
                println!(
                    "trade_log JSONL written: {} ({} entries)",
                    path,
                    all_trade_logs.len()
                );
            }
        }
    }
    Ok(())
}

fn run_dry_validate(cfg: &config::AppConfig) -> anyhow::Result<()> {
    println!("─── Config dry-run validation ───");
    println!("Loaded {} venue(s):", cfg.venues.len());
    for v in &cfg.venues {
        let nin = v.instruments.as_ref().map(|x| x.len()).unwrap_or(0);
        println!(
            "  • {:?} market={} paper_trading={} testnet={} instruments={} fee_overrides={}",
            v.name,
            v.market,
            v.paper_trading,
            v.testnet,
            nin,
            v.fee_maker_override.is_some() || v.fee_taker_override.is_some(),
        );
        if let Some(ref insts) = v.instruments {
            for i in insts {
                println!(
                    "      ↳ {} {}-{} settle={:?}",
                    i.instrument_type, i.base, i.quote, i.settle_currency
                );
            }
        }
    }
    println!();
    println!("Strategy: {}", cfg.strategy.name);
    println!(
        "  instrument_a: {} {}-{}",
        cfg.strategy.instrument_a.instrument_type,
        cfg.strategy.instrument_a.base,
        cfg.strategy.instrument_a.quote,
    );
    println!(
        "  instrument_b: {} {}-{}",
        cfg.strategy.instrument_b.instrument_type,
        cfg.strategy.instrument_b.base,
        cfg.strategy.instrument_b.quote,
    );
    println!(
        "  min_net_profit_bps={}  max_quantity={}  max_quote_age_ms={}",
        cfg.strategy.min_net_profit_bps, cfg.strategy.max_quantity, cfg.strategy.max_quote_age_ms,
    );
    if !cfg.strategy.triangle_cycles.is_empty() {
        println!(
            "  triangle_cycles: {} cycle(s)",
            cfg.strategy.triangle_cycles.len()
        );
    }
    println!();
    println!("Risk:");
    println!(
        "  max_position_size={}  max_daily_loss={}  max_notional_exposure={}",
        cfg.risk.max_position_size, cfg.risk.max_daily_loss, cfg.risk.max_notional_exposure,
    );
    println!(
        "  circuit_breaker: drawdown={}  orders/min={}  consec_failures={}",
        cfg.risk.circuit_breaker.max_drawdown,
        cfg.risk.circuit_breaker.max_orders_per_minute,
        cfg.risk.circuit_breaker.max_consecutive_failures,
    );
    if let Some(ref pv) = cfg.risk.max_position_per_venue {
        println!("  per-venue caps: {} entries", pv.len());
        for (k, v) in pv {
            println!("    {} → {}", k, v);
        }
    }
    println!();
    println!("Engine:");
    println!(
        "  reconcile_interval_secs={}  order_ttl_secs={}",
        cfg.engine.reconcile_interval_secs, cfg.engine.order_ttl_secs
    );
    if let Some(ref f) = cfg.engine.trade_log_file {
        println!("  trade_log_file: {}", f);
    }
    if let Some(p) = cfg.engine.admin_port {
        println!("  admin_port: {}", p);
    }
    if let Some(ms) = cfg.engine.heartbeat_stall_ms {
        println!("  heartbeat_stall_ms: {}", ms);
    }
    println!();
    println!("Validation passed. Exiting without starting any networking.");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let cfg = config::load(&cli.config)?;

    {
        use tracing_subscriber::prelude::*;

        let log_level = cli.log_level.as_deref().unwrap_or(&cfg.logging.level);
        let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level));

        let file_appender = cfg.logging.log_file.as_ref().map(|path| {
            let parent = std::path::Path::new(path)
                .parent()
                .unwrap_or(std::path::Path::new("."));
            let filename = std::path::Path::new(path)
                .file_name()
                .unwrap_or(std::ffi::OsStr::new("arbx.log"));
            tracing_appender::rolling::daily(parent, filename.to_string_lossy().to_string())
        });

        let (non_blocking, _guard) = match file_appender {
            Some(appender) => {
                let (nb, guard) = tracing_appender::non_blocking(appender);
                (Some(nb), Some(guard))
            }
            None => (None, None),
        };

        if cfg.logging.json_output {
            let stdout_layer = tracing_subscriber::fmt::layer().json();
            let file_layer =
                non_blocking.map(|nb| tracing_subscriber::fmt::layer().json().with_writer(nb));
            tracing_subscriber::registry()
                .with(env_filter)
                .with(stdout_layer)
                .with(file_layer)
                .init();
        } else {
            let stdout_layer = tracing_subscriber::fmt::layer();
            let file_layer =
                non_blocking.map(|nb| tracing_subscriber::fmt::layer().with_writer(nb));
            tracing_subscriber::registry()
                .with(env_filter)
                .with(stdout_layer)
                .with(file_layer)
                .init();
        }
    }

    if cli.dry_run_validate {
        return run_dry_validate(&cfg);
    }

    metrics::setup_metrics_server(9090);

    if let Some(csv_path) = cli.backtest.as_ref() {
        return run_backtest_mode(
            csv_path,
            cli.backtest_csv_out.as_deref(),
            cli.backtest_trade_log.as_deref(),
            cli.backtest_window_size,
            &cfg,
        )
        .await;
    }

    // Pin main thread to reduce OS context-switch jitter; leave core 0 to the OS.
    if let Some(cores) = core_affinity::get_core_ids() {
        let target = if cores.len() > 1 { cores[1] } else { cores[0] };
        if core_affinity::set_for_current(target) {
            tracing::info!(core = target.id, "engine thread pinned to CPU core");
        }
    }

    let instrument_a = parse_instrument(&cfg.strategy.instrument_a)?;
    let instrument_b = parse_instrument(&cfg.strategy.instrument_b)?;

    let venue_a = parse_venue(&cfg.venues[0].name)?;
    let venue_b = parse_venue(&cfg.venues[1 % cfg.venues.len()].name)?;

    let idx_b = 1 % cfg.venues.len();

    let symbol_a = format_symbol(
        &cfg.venues[0].name,
        &cfg.strategy.instrument_a.base,
        &cfg.strategy.instrument_a.quote,
    );
    let symbol_b = format_symbol(
        &cfg.venues[idx_b].name,
        &cfg.strategy.instrument_b.base,
        &cfg.strategy.instrument_b.quote,
    );

    let (feed_a, feed_b): (Box<dyn MarketDataFeed>, Option<Box<dyn MarketDataFeed>>) =
        if let Some(stream_id) = cli.aeron_subscribe {
            tracing::info!(stream_id, "Aeron subscriber feed (replaces WS feeds)");
            (
                Box::new(adapters::aeron_feed::AeronMarketDataFeed::new(stream_id)),
                None,
            )
        } else {
            (
                build_market_data(&cfg.venues[0], &symbol_a, instrument_a.clone())?,
                Some(build_market_data(
                    &cfg.venues[idx_b],
                    &symbol_b,
                    instrument_b.clone(),
                )?),
            )
        };

    let fee_a = fetch_fee_schedule(&cfg.venues[0], venue_a).await?;
    let fee_b = fetch_fee_schedule(&cfg.venues[idx_b], venue_b).await?;

    let strategy = build_strategy_from_config(
        &cfg,
        &cfg.strategy,
        venue_a,
        venue_b,
        instrument_a.clone(),
        instrument_b.clone(),
        fee_a.clone(),
        fee_b.clone(),
    )?;

    let mut feeds: Vec<Box<dyn MarketDataFeed>> = vec![feed_a];
    if let Some(b) = feed_b {
        feeds.push(b);
    }

    let mut risk_limits: Vec<Box<dyn risk::limits::RiskLimit>> = vec![
        Box::new(MaxPositionSize {
            max_quantity: cfg.risk.max_position_size,
        }),
        Box::new(MaxDailyLoss {
            max_loss: cfg.risk.max_daily_loss,
        }),
        Box::new(MaxNotionalExposure {
            max_notional: cfg.risk.max_notional_exposure,
        }),
    ];
    if let Some(per_venue) = cfg.risk.max_position_per_venue.as_ref()
        && !per_venue.is_empty()
    {
        let mut caps = std::collections::HashMap::new();
        for (name, cap) in per_venue {
            caps.insert(parse_venue(name)?, *cap);
        }
        risk_limits.push(Box::new(MaxPositionPerVenue { caps }));
    }
    let risk_manager = RiskManager::new(risk_limits);

    let risk_state = RiskState::new(
        cfg.risk.max_position_size,
        cfg.risk.max_notional_exposure,
        cfg.risk.max_daily_loss,
    );

    let circuit_breaker = CircuitBreaker::new(
        cfg.risk.circuit_breaker.max_drawdown,
        cfg.risk.circuit_breaker.max_orders_per_minute,
        cfg.risk.circuit_breaker.max_consecutive_failures,
    );

    let venue_cfg = &cfg.venues[idx_b];
    let executor: Box<dyn adapters::order_executor::OrderExecutor> =
        if venue_cfg.paper_trading || cli.dry_run {
            Box::new(PaperExecutor::new())
        } else {
            build_executor(venue_cfg)?
        };
    let position_manager = build_position_manager(&cfg.venues[idx_b])?;

    let private_streams = if venue_cfg.paper_trading || cli.dry_run {
        Vec::new()
    } else {
        build_private_streams(venue_cfg)
    };

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let quote_publishers: Vec<std::sync::Arc<dyn ipc::IpcPublisher>> = if cli.aeron_publish {
        match ipc::aeron::AeronPublisher::with_default_stream() {
            Ok(p) => {
                tracing::info!("Aeron publisher attached to quote/order_book stream");
                vec![std::sync::Arc::new(p)]
            }
            Err(e) => {
                tracing::warn!(error = %e, "AeronPublisher init failed; running without IPC publish");
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let mut engine = ArbitrageEngine::new(
        feeds,
        strategy,
        risk_manager,
        risk_state,
        circuit_breaker,
        executor,
        position_manager,
        private_streams,
        quote_publishers,
        cfg.engine.reconcile_interval_secs,
        cfg.engine.order_ttl_secs,
        shutdown_rx,
    );
    if let Some(ref path) = cfg.engine.trade_log_file {
        match models::trade_log::TradeLogWriter::create(path) {
            Ok(writer) => {
                tracing::info!(path = path.as_str(), "TradeLog audit writer attached");
                engine = engine.with_trade_log_writer(writer);
            }
            Err(e) => {
                tracing::warn!(path = path.as_str(), error = %e, "failed to open trade_log_file; running without audit writer");
            }
        }
    }

    // D-3 PR2: register per-venue executor + position manager for every
    // venue except the legacy one (cfg.venues[idx_b], already passed to
    // engine.new() above). Paper / dry-run venues stay on the legacy
    // venue-agnostic PaperExecutor — venue-specific routing only matters
    // when real REST endpoints diverge per venue.
    if !cli.dry_run {
        for (i, vc) in cfg.venues.iter().enumerate() {
            if i == idx_b || vc.paper_trading {
                continue;
            }
            let v = parse_venue(&vc.name)?;
            let venue_exec = build_executor(vc)?;
            let venue_pm = build_position_manager(vc)?;
            tracing::info!(
                venue = vc.name.as_str(),
                market = vc.market.as_str(),
                "registering per-venue executor + position manager"
            );
            engine = engine
                .with_executor_for(v, venue_exec)
                .with_position_manager_for(v, venue_pm);
        }
    }

    // D-8: strict routing validation — every strategy-referenced venue must
    // have a registered executor (per-venue map or legacy). Prevents silent
    // fallback to the wrong venue's executor, which corrupts position accounting.
    if !cli.dry_run {
        let legacy_venue = venue_b;
        for v in [venue_a, venue_b] {
            if v == legacy_venue {
                continue;
            }
            let has_paper = cfg
                .venues
                .iter()
                .any(|vc| parse_venue(&vc.name).ok() == Some(v) && vc.paper_trading);
            if has_paper {
                continue;
            }
            if !engine.has_executor_for(v) {
                anyhow::bail!(
                    "venue {:?} referenced by strategy but has no registered executor \
                     — check cfg.venues or set paper_trading: true",
                    v
                );
            }
        }
    }

    // C5 PR2: build each cfg.extra_strategies entry and register on the engine.
    // All extras share the primary strategy's venue + instrument + fee context;
    // cross-strategy heterogeneity (different venues / instruments per strategy)
    // is a future schema change.
    for extra_cfg in cfg.extra_strategies.iter() {
        let extra = build_strategy_from_config(
            &cfg,
            extra_cfg,
            venue_a,
            venue_b,
            instrument_a.clone(),
            instrument_b.clone(),
            fee_a.clone(),
            fee_b.clone(),
        )?;
        tracing::info!(
            extra_strategy = extra_cfg.name.as_str(),
            "registering extra strategy"
        );
        engine = engine.with_extra_strategy(extra);
        if let Some(ref budget_cfg) = extra_cfg.risk_budget {
            let budget = risk::strategy_budget::StrategyRiskBudget::new(budget_cfg.clone());
            tracing::info!(
                strategy = extra_cfg.name.as_str(),
                ?budget_cfg,
                "registering per-strategy risk budget"
            );
            engine = engine.with_strategy_budget(&extra_cfg.name, budget);
        }
    }

    // Primary strategy budget (if configured).
    if let Some(ref budget_cfg) = cfg.strategy.risk_budget {
        let budget = risk::strategy_budget::StrategyRiskBudget::new(budget_cfg.clone());
        tracing::info!(
            strategy = cfg.strategy.name.as_str(),
            ?budget_cfg,
            "registering per-strategy risk budget (primary)"
        );
        engine = engine.with_strategy_budget(&cfg.strategy.name, budget);
    }

    let admin_port = cfg.engine.admin_port.unwrap_or(9091);
    let admin_bind = cfg
        .engine
        .admin_bind
        .as_deref()
        .unwrap_or("127.0.0.1")
        .to_string();
    let admin_token = std::env::var("ARBX_ADMIN_TOKEN").ok();
    let admin_handle =
        engine::admin::EngineHandle::new(shutdown_tx.clone()).with_token(admin_token);
    engine = engine.with_admin(admin_handle.clone());
    tokio::spawn(async move {
        if let Err(e) = engine::admin::serve(admin_handle, admin_port, &admin_bind).await {
            tracing::warn!(error = %e, "admin HTTP server exited");
        }
    });

    // Dead-man's-switch: if the engine loop stops stamping heartbeat within
    // `engine.heartbeat_stall_ms`, fire shutdown_tx so supervisors can restart.
    if let Some(stall_ms) = cfg.engine.heartbeat_stall_ms.filter(|&ms| ms > 0) {
        let hb = std::sync::Arc::new(engine::watchdog::Heartbeat::new());
        engine = engine.with_heartbeat(hb.clone());
        let wd_tx = shutdown_tx.clone();
        let wd_rx = shutdown_tx.subscribe();
        tokio::spawn(async move {
            engine::watchdog::run_watchdog(hb, stall_ms, wd_tx, wd_rx).await;
        });
    }

    // Cert/credential expiry watchdog. Spawns with an empty provider list for now;
    // real providers (PFX expiry reader, Binance ban detector) plug in via future
    // PRs. The gauge `arbx_cert_seconds_until_expiry` is emitted per provider.
    {
        let providers: Vec<std::sync::Arc<dyn engine::cert_watchdog::CertExpiryProvider>> =
            Vec::new();
        let wd_rx = shutdown_tx.subscribe();
        tokio::spawn(async move {
            engine::cert_watchdog::run_cert_watchdog(providers, 300, wd_rx).await;
        });
    }

    // Spawn engine on its own task so Ctrl+C drives shutdown_tx and the engine's
    // internal select! observes shutdown_rx; wrapping the engine in an outer
    // select! cancelled run() before pending_cancels could flush.
    let mut engine_handle = tokio::spawn(async move {
        let result = engine.run().await;
        (result, engine)
    });

    let outcome = tokio::select! {
        biased;
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Ctrl+C received, signalling shutdown...");
            let _ = shutdown_tx.send(true);
            engine_handle.await
        }
        joined = &mut engine_handle => {
            tracing::info!("engine task exited on its own");
            joined
        }
    };

    match outcome {
        Ok((result, engine)) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "engine error");
            }
            tracing::info!(
                total_trades = engine.trade_logs().len(),
                "shutdown complete"
            );
        }
        Err(e) => {
            tracing::error!(error = %e, "engine task panicked");
        }
    }

    Ok(())
}
