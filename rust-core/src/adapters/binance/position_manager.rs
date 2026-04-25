use std::collections::HashMap;

use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;

use super::market_data::BinanceMarket;
use super::rest_client::BinanceRestClient;
use crate::adapters::position_manager::PositionManager;
use crate::adapters::rest_client::{ExchangeRestClient, HttpMethod, RestRequest};
use crate::models::enums::{Side, Venue};
use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
use crate::models::order::Fill;
use crate::models::position::{PortfolioSnapshot, Position};

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

#[derive(Debug, Deserialize)]
struct FuturesPosition {
    symbol: String,
    #[serde(rename = "positionAmt")]
    position_amt: String,
    #[serde(rename = "entryPrice")]
    entry_price: String,
    #[serde(rename = "unrealizedProfit")]
    unrealized_profit: String,
}

#[derive(Debug, Deserialize)]
struct FuturesAccountInfo {
    #[serde(rename = "totalWalletBalance")]
    total_wallet_balance: String,
    #[serde(rename = "availableBalance")]
    available_balance: String,
    #[serde(rename = "totalUnrealizedProfit")]
    total_unrealized_profit: String,
    positions: Vec<FuturesPosition>,
}

pub struct BinancePositionManager {
    market: BinanceMarket,
    rest_client: Option<BinanceRestClient>,
    positions: HashMap<String, Position>,
}

impl BinancePositionManager {
    pub fn new(market: BinanceMarket, api_key: &str, api_secret: &str) -> anyhow::Result<Self> {
        let rest_client = if api_key.is_empty() || api_secret.is_empty() {
            None
        } else {
            Some(BinanceRestClient::new(
                market.rest_base_url(),
                api_key,
                api_secret,
            )?)
        };

        Ok(Self {
            market,
            rest_client,
            positions: HashMap::new(),
        })
    }

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

    pub(crate) fn parse_spot_portfolio(
        json: &str,
        venue: Venue,
    ) -> anyhow::Result<PortfolioSnapshot> {
        let info: SpotAccountInfo =
            serde_json::from_str(json).map_err(|e| anyhow::anyhow!("spot parse error: {e}"))?;

        let mut positions = Vec::new();
        let mut usdt_free = Decimal::ZERO;
        let mut usdt_total = Decimal::ZERO;

        for b in info.balances {
            let free: Decimal = b.free.parse().unwrap_or(Decimal::ZERO);
            let locked: Decimal = b.locked.parse().unwrap_or(Decimal::ZERO);
            let total = free + locked;
            if total == Decimal::ZERO {
                continue;
            }
            if b.asset == "USDT" {
                usdt_free = free;
                usdt_total = total;
            }
            positions.push(Position {
                venue,
                instrument: Instrument {
                    asset_class: AssetClass::Crypto,
                    instrument_type: InstrumentType::Spot,
                    base: b.asset,
                    quote: "USDT".into(),
                    settle_currency: None,
                    expiry: None,
                    last_trade_time: None,
                    settlement_time: None,
                },
                quantity: total,
                average_cost: Decimal::ZERO,
                unrealized_pnl: Decimal::ZERO,
                realized_pnl: Decimal::ZERO,
                settlement_date: None,
            });
        }

        Ok(PortfolioSnapshot {
            venue,
            positions,
            total_equity: usdt_total,
            available_balance: usdt_free,
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
        })
    }

    pub(crate) fn parse_futures_portfolio(
        json: &str,
        venue: Venue,
    ) -> anyhow::Result<PortfolioSnapshot> {
        let info: FuturesAccountInfo =
            serde_json::from_str(json).map_err(|e| anyhow::anyhow!("futures parse error: {e}"))?;

        let total_equity: Decimal = info.total_wallet_balance.parse().unwrap_or(Decimal::ZERO);
        let available_balance: Decimal = info.available_balance.parse().unwrap_or(Decimal::ZERO);
        let unrealized_pnl: Decimal = info
            .total_unrealized_profit
            .parse()
            .unwrap_or(Decimal::ZERO);

        let mut positions = Vec::new();
        for p in info.positions {
            let amt: Decimal = p.position_amt.parse().unwrap_or(Decimal::ZERO);
            if amt == Decimal::ZERO {
                continue;
            }
            let entry: Decimal = p.entry_price.parse().unwrap_or(Decimal::ZERO);
            let upnl: Decimal = p.unrealized_profit.parse().unwrap_or(Decimal::ZERO);
            positions.push(Position {
                venue,
                instrument: Instrument {
                    asset_class: AssetClass::Crypto,
                    instrument_type: InstrumentType::Swap,
                    base: p.symbol.clone(),
                    quote: "USDT".into(),
                    settle_currency: Some("USDT".into()),
                    expiry: None,
                    last_trade_time: None,
                    settlement_time: None,
                },
                quantity: amt.abs(),
                average_cost: entry,
                unrealized_pnl: upnl,
                realized_pnl: Decimal::ZERO,
                settlement_date: None,
            });
        }

        Ok(PortfolioSnapshot {
            venue,
            positions,
            total_equity,
            available_balance,
            unrealized_pnl,
            realized_pnl: Decimal::ZERO,
        })
    }

    fn diff_check(&self, remote: &PortfolioSnapshot) {
        for remote_pos in &remote.positions {
            let key = Self::position_key(&remote_pos.instrument);
            match self.positions.get(&key) {
                None => {
                    tracing::warn!(
                        key,
                        remote_qty = %remote_pos.quantity,
                        "position on exchange not tracked locally"
                    );
                }
                Some(local_pos) => {
                    let diff = (remote_pos.quantity - local_pos.quantity).abs();
                    let threshold = local_pos.quantity * rust_decimal_macros::dec!(0.01);
                    if diff > threshold {
                        tracing::warn!(
                            key,
                            local_qty = %local_pos.quantity,
                            remote_qty = %remote_pos.quantity,
                            diff = %diff,
                            "position mismatch detected"
                        );
                    }
                }
            }
        }
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
        let rest = match &self.rest_client {
            Some(r) => r,
            None => {
                tracing::warn!(
                    market = self.market_label(),
                    "no credentials, returning local state"
                );
                let positions: Vec<Position> = self.positions.values().cloned().collect();
                let unrealized_pnl = positions.iter().map(|p| p.unrealized_pnl).sum();
                let realized_pnl = positions.iter().map(|p| p.realized_pnl).sum();
                return Ok(PortfolioSnapshot {
                    venue: Venue::Binance,
                    positions,
                    total_equity: Decimal::ZERO,
                    available_balance: Decimal::ZERO,
                    unrealized_pnl,
                    realized_pnl,
                });
            }
        };

        let (path, parser): (&str, fn(&str, Venue) -> anyhow::Result<PortfolioSnapshot>) =
            match self.market {
                BinanceMarket::Spot => ("/api/v3/account", Self::parse_spot_portfolio),
                BinanceMarket::UsdtFutures => ("/fapi/v2/account", Self::parse_futures_portfolio),
                BinanceMarket::CoinFutures => ("/dapi/v2/account", Self::parse_futures_portfolio),
            };

        let req = RestRequest {
            method: HttpMethod::Get,
            path: path.to_string(),
            params: HashMap::new(),
            raw_body: None,
        };

        match rest.send(req).await {
            Err(e) => {
                tracing::warn!(
                    market = self.market_label(),
                    error = %e,
                    "get_portfolio REST call failed, returning local state"
                );
                let positions: Vec<Position> = self.positions.values().cloned().collect();
                let unrealized_pnl = positions.iter().map(|p| p.unrealized_pnl).sum();
                let realized_pnl = positions.iter().map(|p| p.realized_pnl).sum();
                Ok(PortfolioSnapshot {
                    venue: Venue::Binance,
                    positions,
                    total_equity: Decimal::ZERO,
                    available_balance: Decimal::ZERO,
                    unrealized_pnl,
                    realized_pnl,
                })
            }
            Ok(resp) => {
                if resp.status != 200 {
                    tracing::warn!(
                        market = self.market_label(),
                        status = resp.status,
                        "get_portfolio returned non-200, returning local state"
                    );
                    let positions: Vec<Position> = self.positions.values().cloned().collect();
                    let unrealized_pnl = positions.iter().map(|p| p.unrealized_pnl).sum();
                    let realized_pnl = positions.iter().map(|p| p.realized_pnl).sum();
                    return Ok(PortfolioSnapshot {
                        venue: Venue::Binance,
                        positions,
                        total_equity: Decimal::ZERO,
                        available_balance: Decimal::ZERO,
                        unrealized_pnl,
                        realized_pnl,
                    });
                }
                let snapshot = parser(&resp.body, Venue::Binance)?;
                tracing::info!(
                    market = self.market_label(),
                    num_positions = snapshot.positions.len(),
                    total_equity = %snapshot.total_equity,
                    "get_portfolio"
                );
                Ok(snapshot)
            }
        }
    }

    async fn sync_positions(&mut self) -> anyhow::Result<()> {
        if self.rest_client.is_none() {
            tracing::warn!(
                market = self.market_label(),
                "no credentials, skipping sync"
            );
            return Ok(());
        }

        match self.get_portfolio().await {
            Err(e) => {
                tracing::warn!(
                    market = self.market_label(),
                    error = %e,
                    "sync_positions failed to fetch remote state"
                );
            }
            Ok(remote) => {
                self.diff_check(&remote);
            }
        }

        tracing::info!(venue = ?Venue::Binance, "position reconciliation complete");
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
            client_order_id: None,
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

        pm.apply_fill_inner(&make_fill(&inst, Side::Buy, dec!(50000), dec!(1)));
        let pos = pm.positions.get("BTC-USDT").unwrap();
        assert_eq!(pos.quantity, dec!(1));
        assert_eq!(pos.average_cost, dec!(50000));

        pm.apply_fill_inner(&make_fill(&inst, Side::Buy, dec!(60000), dec!(1)));
        let pos = pm.positions.get("BTC-USDT").unwrap();
        assert_eq!(pos.quantity, dec!(2));
        assert_eq!(pos.average_cost, dec!(55000));
    }

    #[test]
    fn test_apply_fill_sell_with_pnl() {
        let mut pm = BinancePositionManager::new(BinanceMarket::UsdtFutures, "", "").unwrap();
        let inst = test_instrument();

        pm.apply_fill_inner(&make_fill(&inst, Side::Buy, dec!(50000), dec!(2)));
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

    // ── New tests ─────────────────────────────────────────────────────────────

    #[test]
    fn get_portfolio_parses_spot_balance() {
        let json = r#"{
            "balances": [
                {"asset": "BTC", "free": "0.5", "locked": "0.1"},
                {"asset": "USDT", "free": "1000.0", "locked": "50.0"},
                {"asset": "ETH", "free": "0.0", "locked": "0.0"}
            ]
        }"#;
        let snap = BinancePositionManager::parse_spot_portfolio(json, Venue::Binance).unwrap();
        // ETH with zero balance should be filtered out
        assert_eq!(snap.positions.len(), 2);
        assert_eq!(snap.available_balance, dec!(1000));
        assert_eq!(snap.total_equity, dec!(1050));
        // verify BTC position quantity
        let btc = snap
            .positions
            .iter()
            .find(|p| p.instrument.base == "BTC")
            .unwrap();
        assert_eq!(btc.quantity, dec!(0.6));
    }

    #[test]
    fn get_portfolio_parses_futures_account() {
        let json = r#"{
            "totalWalletBalance": "5000.0",
            "availableBalance": "4500.0",
            "totalUnrealizedProfit": "123.45",
            "positions": [
                {"symbol": "BTCUSDT", "positionAmt": "0.5", "entryPrice": "30000.0", "unrealizedProfit": "100.0"},
                {"symbol": "ETHUSDT", "positionAmt": "0.0", "entryPrice": "0.0", "unrealizedProfit": "0.0"}
            ]
        }"#;
        let snap = BinancePositionManager::parse_futures_portfolio(json, Venue::Binance).unwrap();
        // ETH with zero positionAmt filtered out
        assert_eq!(snap.positions.len(), 1);
        assert_eq!(snap.total_equity, dec!(5000));
        assert_eq!(snap.available_balance, dec!(4500));
        assert_eq!(snap.unrealized_pnl, dec!(123.45));
        let btc = &snap.positions[0];
        assert_eq!(btc.quantity, dec!(0.5));
        assert_eq!(btc.average_cost, dec!(30000));
        assert_eq!(btc.unrealized_pnl, dec!(100));
    }

    #[tokio::test]
    async fn sync_positions_logs_mismatch_when_diff_exceeds_threshold() {
        // Local: 1.0 BTC; diff_check against remote 1.5 BTC — >1%, should warn but not error.
        let mut pm = BinancePositionManager::new(BinanceMarket::Spot, "", "").unwrap();
        let inst = test_instrument();
        pm.apply_fill_inner(&make_fill(&inst, Side::Buy, dec!(50000), dec!(1)));

        let remote_snap = PortfolioSnapshot {
            venue: Venue::Binance,
            positions: vec![Position {
                venue: Venue::Binance,
                instrument: inst.clone(),
                quantity: dec!(1.5),
                average_cost: dec!(50000),
                unrealized_pnl: Decimal::ZERO,
                realized_pnl: Decimal::ZERO,
                settlement_date: None,
            }],
            total_equity: dec!(75000),
            available_balance: dec!(10000),
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
        };
        // diff_check should not panic
        pm.diff_check(&remote_snap);

        // full sync with no credentials still returns Ok
        let result = pm.sync_positions().await;
        assert!(result.is_ok());
    }

    #[test]
    fn position_key_format_consistent() {
        let inst = Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: InstrumentType::Spot,
            base: "ETH".to_string(),
            quote: "USDT".to_string(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        };
        assert_eq!(BinancePositionManager::position_key(&inst), "ETH-USDT");
    }

    #[test]
    fn empty_portfolio_returns_valid_snapshot() {
        let json = r#"{"balances": []}"#;
        let snap = BinancePositionManager::parse_spot_portfolio(json, Venue::Binance).unwrap();
        assert!(snap.positions.is_empty());
        assert_eq!(snap.total_equity, Decimal::ZERO);
        assert_eq!(snap.available_balance, Decimal::ZERO);
        assert_eq!(snap.unrealized_pnl, Decimal::ZERO);
    }
}
