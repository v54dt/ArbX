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
use adapters::binance::rest_client::BinanceRestClient;
use adapters::bybit::fee_provider::BybitFeeProvider;
use adapters::bybit::market_data::{BybitMarket, BybitMarketData};
use adapters::bybit::order_executor::BybitOrderExecutor;
use adapters::bybit::position_manager::BybitPositionManager;
use adapters::bybit::rest_client::BybitRestClient;
use adapters::fee_provider::FeeProvider;
use adapters::market_data::MarketDataFeed;
use adapters::okx::fee_provider::OkxFeeProvider;
use adapters::okx::market_data::OkxMarketData;
use adapters::okx::order_executor::OkxOrderExecutor;
use adapters::okx::position_manager::OkxPositionManager;
use adapters::okx::rest_client::OkxRestClient;
use adapters::paper_executor::PaperExecutor;
use engine::arbitrage::ArbitrageEngine;
use models::enums::Venue;
use models::fee::FeeSchedule;
use models::instrument::{AssetClass, Instrument, InstrumentType};
use risk::circuit_breaker::CircuitBreaker;
use risk::limits::{MaxDailyLoss, MaxNotionalExposure, MaxPositionSize};
use risk::manager::RiskManager;
use risk::state::RiskState;
use strategy::cross_exchange::CrossExchangeStrategy;

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

    metrics::setup_metrics_server(9090);

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

    let feed_a = build_market_data(&cfg.venues[0], &symbol_a, instrument_a.clone())?;
    let feed_b = build_market_data(&cfg.venues[idx_b], &symbol_b, instrument_b.clone())?;

    let fee_a = fetch_fee_schedule(&cfg.venues[0], venue_a).await?;
    let fee_b = fetch_fee_schedule(&cfg.venues[idx_b], venue_b).await?;

    let strategy = CrossExchangeStrategy {
        venue_a,
        venue_b,
        instrument_a,
        instrument_b,
        min_net_profit_bps: cfg.strategy.min_net_profit_bps,
        max_quantity: cfg.strategy.max_quantity,
        fee_a,
        fee_b,
        max_quote_age_ms: cfg.strategy.max_quote_age_ms,
        tick_size_a: cfg
            .strategy
            .tick_size_a
            .unwrap_or(rust_decimal_macros::dec!(0.01)),
        tick_size_b: cfg
            .strategy
            .tick_size_b
            .unwrap_or(rust_decimal_macros::dec!(0.01)),
        lot_size_a: cfg
            .strategy
            .lot_size_a
            .unwrap_or(rust_decimal_macros::dec!(0.00001)),
        lot_size_b: cfg
            .strategy
            .lot_size_b
            .unwrap_or(rust_decimal_macros::dec!(0.00001)),
        max_book_depth: cfg.strategy.max_book_depth,
    };

    let feeds: Vec<Box<dyn MarketDataFeed>> = vec![feed_a, feed_b];

    let risk_manager = RiskManager::new(vec![
        Box::new(MaxPositionSize {
            max_quantity: cfg.risk.max_position_size,
        }),
        Box::new(MaxDailyLoss {
            max_loss: cfg.risk.max_daily_loss,
        }),
        Box::new(MaxNotionalExposure {
            max_notional: cfg.risk.max_notional_exposure,
        }),
    ]);

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
    let executor = build_executor(venue_cfg)?;
    let executor: Box<dyn adapters::order_executor::OrderExecutor> =
        if venue_cfg.paper_trading || cli.dry_run {
            Box::new(PaperExecutor::new(executor))
        } else {
            executor
        };
    let position_manager = build_position_manager(&cfg.venues[idx_b])?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let mut engine = ArbitrageEngine::new(
        feeds,
        Box::new(strategy),
        risk_manager,
        risk_state,
        circuit_breaker,
        executor,
        position_manager,
        cfg.engine.reconcile_interval_secs,
        shutdown_rx,
    );

    tokio::select! {
        result = engine.run() => {
            if let Err(e) = result {
                tracing::error!(error = %e, "engine error");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Ctrl+C received, initiating shutdown...");
            let _ = shutdown_tx.send(true);
        }
    }

    tracing::info!(
        total_trades = engine.trade_logs().len(),
        "shutdown complete"
    );

    Ok(())
}
