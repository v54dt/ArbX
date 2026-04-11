mod adapters;
mod engine;
mod models;
mod risk;
mod strategy;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use adapters::binance::market_data::{BinanceMarket, BinanceMarketData};
use adapters::market_data::MarketDataFeed;
use engine::arbitrage::ArbitrageEngine;
use models::enums::Venue;
use models::instrument::{AssetClass, Instrument, InstrumentType};
use strategy::cross_exchange::CrossExchangeStrategy;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Spot form on Binance spot, perpetual swap form on Binance USDT futures.
    let spot_instrument = Instrument {
        asset_class: AssetClass::Crypto,
        instrument_type: InstrumentType::Spot,
        base: "BTC".into(),
        quote: "USDT".into(),
        settle_currency: None,
        expiry: None,
    };

    let perp_instrument = Instrument {
        asset_class: AssetClass::Crypto,
        instrument_type: InstrumentType::Swap,
        base: "BTC".into(),
        quote: "USDT".into(),
        settle_currency: Some("USDT".into()),
        expiry: None,
    };

    // Binance spot adapter
    let mut spot_feed = BinanceMarketData::new(BinanceMarket::Spot);
    spot_feed.register_instrument("BTCUSDT", spot_instrument.clone());

    // Binance USDT-M futures adapter
    let mut futures_feed = BinanceMarketData::new(BinanceMarket::UsdtFutures);
    futures_feed.register_instrument("BTCUSDT", perp_instrument.clone());

    // Strategy: cross between spot and perp on the same venue
    let strategy = CrossExchangeStrategy {
        venue_a: Venue::Binance,
        venue_b: Venue::Binance,
        instrument_a: spot_instrument,
        instrument_b: perp_instrument,
        min_net_profit_bps: dec!(1),
        max_quantity: dec!(0.01),
        fee_rate_a: dec!(0.001),
        fee_rate_b: dec!(0.0004),
    };

    let feeds: Vec<Box<dyn MarketDataFeed>> = vec![Box::new(spot_feed), Box::new(futures_feed)];

    let mut engine = ArbitrageEngine::new(feeds, Box::new(strategy));
    engine.run().await?;

    Ok(())
}
