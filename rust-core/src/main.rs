mod adapters;
mod models;
mod risk;
mod strategy;

use adapters::binance::market_data::{BinanceMarketData, BinanceMarket};

fn main() {
    let _feed = BinanceMarketData::new(BinanceMarket::Spot);
    println!("ArbX engine starting");
}
