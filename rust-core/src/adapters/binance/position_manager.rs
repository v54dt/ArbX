use std::collections::HashMap;

use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;

use super::market_data::BinanceMarket;
use super::rest_client::BinanceRestClient;
use crate::adapters::position_manager::PositionManager;
use crate::adapters::rest_client::{HttpMethod, RestRequest};
use crate::models::enums::{Side, Venue};
use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
use crate::models::order::Fill;
use crate::models::position::{PortfolioSnapshot, Position};

#[derive(Debug, Deserialize)]
struct FuturesPositionRisk {
    symbol: String,
    #[serde(rename = "positionAmt")]
    position_amt: String,
    #[serde(rename = "entryPrice")]
    entry_price: String,
    #[serde(rename = "unRealizedProfit")]
    unrealized_profit: String,
}

#[derive(Debug, Deserialize)]
struct SpotBalance {
    asset: String,
    free: String,
    locked: String,
}

#[derive(Debug, Deserialize)]
struct SpotAccountInfo {
    balances: Vec<SpotBalance>,
}

pub struct BinancePositionManager {
    market: BinanceMarket,
    base_url: String,
    rest_client: Option<BinanceRestClient>,
    positions: HashMap<String, Position>,
}

impl BinancePositionManager {
    pub fn new(market: BinanceMarket, api_key: &str, api_secret: &str) -> anyhow::Result<Self> {
        let base_url = market.rest_base_url().to_string();

        let rest_client = if api_key.is_empty() || api_secret.is_empty() {
            None
        } else {
            Some(BinanceRestClient::new(&base_url, api_key, api_secret)?)
        };

        Ok(Self {
            market,
            base_url,
            rest_client,
            positions: HashMap::new(),
        })
    }

    /// Produce a deterministic key for an instrument, e.g. `"BTC-USDT"`.
    fn position_key(instrument: &Instrument) -> String {
        format!("{}-{}", instrument.base, instrument.quote)
    }

    fn market_label(&self) -> &'static str {
        match self.market {
            BinanceMarket::Spot => "spot",
            BinanceMarket::UsdtFutures => "usdt-futures",
            BinanceMarket::CoinFutures => "coin-futures",
        }
    }

    pub fn apply_fill_inner(&mut self, fill: &Fill) {
        let key = Self::position_key(&fill.instrument);
        let market_label = self.market_label();

        let position = self
            .positions
            .entry(key.clone())
            .or_insert_with(|| Position {
                venue: Venue::Binance,
                instrument: fill.instrument.clone(),
                quantity: Decimal::ZERO,
                average_cost: Decimal::ZERO,
                unrealized_pnl: Decimal::ZERO,
                realized_pnl: Decimal::ZERO,
                settlement_date: None,
            });

        match fill.side {
            Side::Buy => {
                let old_cost = position.quantity * position.average_cost;
                let fill_cost = fill.quantity * fill.price;
                let new_qty = position.quantity + fill.quantity;
                position.average_cost = if new_qty > Decimal::ZERO {
                    (old_cost + fill_cost) / new_qty
                } else {
                    Decimal::ZERO
                };
                position.quantity = new_qty;
            }
            Side::Sell => {
                if fill.quantity > position.quantity {
                    tracing::warn!(
                        fill_qty = %fill.quantity,
                        pos_qty = %position.quantity,
                        "sell quantity exceeds position, resetting to zero"
                    );
                }
                let pnl = (fill.price - position.average_cost) * fill.quantity;
                position.realized_pnl += pnl;
                position.quantity -= fill.quantity;

                if position.quantity <= Decimal::ZERO {
                    position.quantity = Decimal::ZERO;
                    position.average_cost = Decimal::ZERO;
                }
            }
        }

        tracing::info!(
            market = market_label,
            key = key.as_str(),
            side = ?fill.side,
            fill_qty = %fill.quantity,
            fill_price = %fill.price,
            pos_qty = %position.quantity,
            avg_cost = %position.average_cost,
            realized_pnl = %position.realized_pnl,
            "apply_fill: position updated"
        );
    }
}

#[async_trait]
impl PositionManager for BinancePositionManager {
    async fn get_position(&self, symbol: &str) -> anyhow::Result<Option<Position>> {
        let position = self.positions.get(symbol).cloned();
        tracing::info!(
            market = self.market_label(),
            symbol,
            found = position.is_some(),
            "get_position"
        );
        Ok(position)
    }

    async fn get_portfolio(&self) -> anyhow::Result<PortfolioSnapshot> {
        let positions: Vec<Position> = self.positions.values().cloned().collect();
        let unrealized_pnl = positions.iter().map(|p| p.unrealized_pnl).sum();
        let realized_pnl = positions.iter().map(|p| p.realized_pnl).sum();

        tracing::info!(
            market = self.market_label(),
            num_positions = positions.len(),
            "get_portfolio (stub equity/balance)"
        );

        Ok(PortfolioSnapshot {
            venue: Venue::Binance,
            positions,
            total_equity: Decimal::ZERO,
            available_balance: Decimal::ZERO,
            unrealized_pnl,
            realized_pnl,
        })
    }

    async fn sync_positions(&mut self) -> anyhow::Result<()> {
        let rest = match &self.rest_client {
            Some(r) => r,
            None => {
                tracing::warn!(
                    market = self.market_label(),
                    "no credentials, skipping sync"
                );
                return Ok(());
            }
        };

        use crate::adapters::rest_client::ExchangeRestClient;

        match self.market {
            BinanceMarket::UsdtFutures | BinanceMarket::CoinFutures => {
                let req = RestRequest {
                    method: HttpMethod::Get,
                    path: "/fapi/v2/positionRisk".to_string(),
                    params: HashMap::new(),
                };
                let resp = rest.send(req).await?;
                if resp.status != 200 {
                    anyhow::bail!("positionRisk returned {}: {}", resp.status, resp.body);
                }
                let risks: Vec<FuturesPositionRisk> = serde_json::from_str(&resp.body)?;
                for r in risks {
                    let amt: Decimal = r.position_amt.parse().unwrap_or(Decimal::ZERO);
                    if amt == Decimal::ZERO {
                        continue;
                    }
                    let entry: Decimal = r.entry_price.parse().unwrap_or(Decimal::ZERO);
                    let upnl: Decimal = r.unrealized_profit.parse().unwrap_or(Decimal::ZERO);
                    let key = r.symbol.clone();
                    let pos = self.positions.entry(key).or_insert_with(|| Position {
                        venue: Venue::Binance,
                        instrument: Instrument {
                            asset_class: AssetClass::Crypto,
                            instrument_type: InstrumentType::Swap,
                            base: r.symbol.clone(),
                            quote: "USDT".into(),
                            settle_currency: Some("USDT".into()),
                            expiry: None,
                            last_trade_time: None,
                            settlement_time: None,
                        },
                        quantity: Decimal::ZERO,
                        average_cost: Decimal::ZERO,
                        unrealized_pnl: Decimal::ZERO,
                        realized_pnl: Decimal::ZERO,
                        settlement_date: None,
                    });
                    pos.quantity = amt;
                    pos.average_cost = entry;
                    pos.unrealized_pnl = upnl;
                }
            }
            BinanceMarket::Spot => {
                let req = RestRequest {
                    method: HttpMethod::Get,
                    path: "/api/v3/account".to_string(),
                    params: HashMap::new(),
                };
                let resp = rest.send(req).await?;
                if resp.status != 200 {
                    anyhow::bail!("account returned {}: {}", resp.status, resp.body);
                }
                let info: SpotAccountInfo = serde_json::from_str(&resp.body)?;
                for b in info.balances {
                    let free: Decimal = b.free.parse().unwrap_or(Decimal::ZERO);
                    let locked: Decimal = b.locked.parse().unwrap_or(Decimal::ZERO);
                    let total = free + locked;
                    if total == Decimal::ZERO {
                        continue;
                    }
                    let key = format!("{}-USDT", b.asset);
                    let pos = self.positions.entry(key).or_insert_with(|| Position {
                        venue: Venue::Binance,
                        instrument: Instrument {
                            asset_class: AssetClass::Crypto,
                            instrument_type: InstrumentType::Spot,
                            base: b.asset.clone(),
                            quote: "USDT".into(),
                            settle_currency: None,
                            expiry: None,
                            last_trade_time: None,
                            settlement_time: None,
                        },
                        quantity: Decimal::ZERO,
                        average_cost: Decimal::ZERO,
                        unrealized_pnl: Decimal::ZERO,
                        realized_pnl: Decimal::ZERO,
                        settlement_date: None,
                    });
                    pos.quantity = total;
                }
            }
        }

        tracing::info!(
            market = self.market_label(),
            positions = self.positions.len(),
            "sync_positions completed"
        );
        Ok(())
    }

    async fn apply_fill(&mut self, fill: &Fill) -> anyhow::Result<()> {
        self.apply_fill_inner(fill);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::instrument::{AssetClass, InstrumentType};
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn test_instrument() -> Instrument {
        Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: InstrumentType::Swap,
            base: "BTC".to_string(),
            quote: "USDT".to_string(),
            settle_currency: Some("USDT".to_string()),
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        }
    }

    fn make_fill(instrument: &Instrument, side: Side, price: Decimal, qty: Decimal) -> Fill {
        Fill {
            order_id: "test-order".to_string(),
            venue: Venue::Binance,
            instrument: instrument.clone(),
            side,
            price,
            quantity: qty,
            fee: Decimal::ZERO,
            fee_currency: "USDT".to_string(),
            filled_at: Utc::now(),
        }
    }

    #[test]
    fn test_position_key() {
        let inst = test_instrument();
        assert_eq!(BinancePositionManager::position_key(&inst), "BTC-USDT");
    }

    #[test]
    fn test_apply_fill_buy() {
        let mut pm = BinancePositionManager::new(BinanceMarket::UsdtFutures, "", "").unwrap();
        let inst = test_instrument();

        // First buy: 1 BTC @ 50000
        pm.apply_fill_inner(&make_fill(&inst, Side::Buy, dec!(50000), dec!(1)));
        let pos = pm.positions.get("BTC-USDT").unwrap();
        assert_eq!(pos.quantity, dec!(1));
        assert_eq!(pos.average_cost, dec!(50000));

        // Second buy: 1 BTC @ 60000 -> avg = (50000 + 60000) / 2 = 55000
        pm.apply_fill_inner(&make_fill(&inst, Side::Buy, dec!(60000), dec!(1)));
        let pos = pm.positions.get("BTC-USDT").unwrap();
        assert_eq!(pos.quantity, dec!(2));
        assert_eq!(pos.average_cost, dec!(55000));
    }

    #[test]
    fn test_apply_fill_sell_with_pnl() {
        let mut pm = BinancePositionManager::new(BinanceMarket::UsdtFutures, "", "").unwrap();
        let inst = test_instrument();

        // Buy 2 BTC @ 50000
        pm.apply_fill_inner(&make_fill(&inst, Side::Buy, dec!(50000), dec!(2)));

        // Sell 1 BTC @ 55000 -> realized PnL = (55000 - 50000) * 1 = 5000
        pm.apply_fill_inner(&make_fill(&inst, Side::Sell, dec!(55000), dec!(1)));
        let pos = pm.positions.get("BTC-USDT").unwrap();
        assert_eq!(pos.quantity, dec!(1));
        assert_eq!(pos.average_cost, dec!(50000));
        assert_eq!(pos.realized_pnl, dec!(5000));
    }

    #[test]
    fn test_apply_fill_full_close() {
        let mut pm = BinancePositionManager::new(BinanceMarket::UsdtFutures, "", "").unwrap();
        let inst = test_instrument();

        pm.apply_fill_inner(&make_fill(&inst, Side::Buy, dec!(50000), dec!(1)));
        pm.apply_fill_inner(&make_fill(&inst, Side::Sell, dec!(50000), dec!(1)));
        let pos = pm.positions.get("BTC-USDT").unwrap();
        assert_eq!(pos.quantity, dec!(0));
        assert_eq!(pos.average_cost, dec!(0));
    }

    #[tokio::test]
    async fn test_get_position_missing() {
        let pm = BinancePositionManager::new(BinanceMarket::Spot, "", "").unwrap();
        let result = pm.get_position("ETH-USDT").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_get_portfolio_empty() {
        let pm = BinancePositionManager::new(BinanceMarket::Spot, "", "").unwrap();
        let snap = pm.get_portfolio().await.unwrap();
        assert!(snap.positions.is_empty());
        assert_eq!(snap.total_equity, Decimal::ZERO);
    }
}
