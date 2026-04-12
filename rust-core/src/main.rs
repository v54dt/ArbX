// TODO: remove once scaffold phase is complete and all modules are wired up
#![allow(dead_code)]

mod adapters;
mod config;
mod engine;
mod models;
mod risk;
mod strategy;

use adapters::binance::fee_provider::BinanceFeeProvider;
use adapters::binance::market_data::{BinanceMarket, BinanceMarketData};
use adapters::binance::order_executor::BinanceOrderExecutor;
use adapters::binance::position_manager::BinancePositionManager;
use adapters::binance::rest_client::BinanceRestClient;
use adapters::fee_provider::FeeProvider;
use adapters::market_data::MarketDataFeed;
use engine::arbitrage::ArbitrageEngine;
use models::enums::Venue;
use models::fee::FeeSchedule;
use models::instrument::{AssetClass, Instrument, InstrumentType};
use risk::limits::{MaxDailyLoss, MaxNotionalExposure, MaxPositionSize};
use risk::manager::RiskManager;
use strategy::cross_exchange::CrossExchangeStrategy;

use config::InstrumentConfig;

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

fn parse_market(market: &str) -> anyhow::Result<BinanceMarket> {
    match market.to_lowercase().as_str() {
        "spot" => Ok(BinanceMarket::Spot),
        "usdt_futures" => Ok(BinanceMarket::UsdtFutures),
        "coin_futures" => Ok(BinanceMarket::CoinFutures),
        other => anyhow::bail!("unknown market: {}", other),
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
    })
}

async fn fetch_fee_schedule(
    venue_cfg: &config::VenueConfig,
    market: BinanceMarket,
    venue: Venue,
) -> FeeSchedule {
    if venue_cfg.api_key.is_empty() || venue_cfg.api_secret.is_empty() {
        tracing::warn!(?venue, "no API credentials, using default fee schedule");
        return FeeSchedule::new(
            venue,
            rust_decimal_macros::dec!(0.001),
            rust_decimal_macros::dec!(0.001),
        );
    }
    let rest = BinanceRestClient::new(
        market.rest_base_url(),
        &venue_cfg.api_key,
        &venue_cfg.api_secret,
    );
    let provider = BinanceFeeProvider::new(rest, market);
    match provider.get_fee_schedule().await {
        Ok(fee) => fee,
        Err(e) => {
            tracing::warn!(%e, ?venue, "fee provider failed, using defaults");
            FeeSchedule::new(
                venue,
                rust_decimal_macros::dec!(0.001),
                rust_decimal_macros::dec!(0.001),
            )
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config/default.yaml".into());
    let cfg = config::load(&config_path)?;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cfg.logging.level)),
        )
        .init();

    let instrument_a = parse_instrument(&cfg.strategy.instrument_a)?;
    let instrument_b = parse_instrument(&cfg.strategy.instrument_b)?;

    let venue_a = parse_venue(&cfg.venues[0].name)?;
    let venue_b = parse_venue(&cfg.venues[1 % cfg.venues.len()].name)?;

    let idx_b = 1 % cfg.venues.len();

    let symbol_a = format!(
        "{}{}",
        cfg.strategy.instrument_a.base, cfg.strategy.instrument_a.quote
    );
    let symbol_b = format!(
        "{}{}",
        cfg.strategy.instrument_b.base, cfg.strategy.instrument_b.quote
    );

    let mut feed_a = BinanceMarketData::new(parse_market(&cfg.venues[0].market)?);
    feed_a.register_instrument(&symbol_a, instrument_a.clone());

    let mut feed_b = BinanceMarketData::new(parse_market(&cfg.venues[idx_b].market)?);
    feed_b.register_instrument(&symbol_b, instrument_b.clone());

    let market_a = parse_market(&cfg.venues[0].market)?;
    let market_b = parse_market(&cfg.venues[idx_b].market)?;

    let fee_a = fetch_fee_schedule(&cfg.venues[0], market_a, venue_a).await;
    let fee_b = fetch_fee_schedule(&cfg.venues[idx_b], market_b, venue_b).await;

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
    };

    let feeds: Vec<Box<dyn MarketDataFeed>> = vec![Box::new(feed_a), Box::new(feed_b)];

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

    let executor = BinanceOrderExecutor::new(parse_market(&cfg.venues[idx_b].market)?);
    let position_manager = BinancePositionManager::new(parse_market(&cfg.venues[idx_b].market)?);

    let mut engine = ArbitrageEngine::new(
        feeds,
        Box::new(strategy),
        risk_manager,
        Box::new(executor),
        Box::new(position_manager),
    );
    engine.run().await?;

    Ok(())
}
