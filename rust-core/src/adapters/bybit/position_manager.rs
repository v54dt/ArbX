use std::collections::HashMap;

use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;

use super::market_data::BybitMarket;
use super::rest_client::BybitRestClient;
use crate::adapters::position_manager::PositionManager;
use crate::adapters::rest_client::{ExchangeRestClient, HttpMethod, RestRequest};
use crate::models::enums::{Side, Venue};
use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
use crate::models::order::Fill;
use crate::models::position::{PortfolioSnapshot, Position};

#[derive(Debug, Deserialize)]
struct BybitCoin {
    coin: String,
    #[serde(rename = "walletBalance")]
    wallet_balance: String,
    #[serde(rename = "unrealisedPnl", default)]
    unrealised_pnl: String,
}

#[derive(Debug, Deserialize)]
struct BybitWallet {
    #[serde(rename = "totalEquity")]
    total_equity: String,
    #[serde(rename = "availableToWithdraw")]
    available_to_withdraw: String,
    coin: Vec<BybitCoin>,
}

#[derive(Debug, Deserialize)]
struct BybitWalletResult {
    list: Vec<BybitWallet>,
}

#[derive(Debug, Deserialize)]
struct BybitWalletResponse {
    result: BybitWalletResult,
}

#[derive(Debug, Deserialize)]
struct BybitPositionEntry {
    symbol: String,
    size: String,
    #[serde(rename = "avgPrice")]
    avg_price: String,
    #[serde(rename = "unrealisedPnl")]
    unrealised_pnl: String,
}

#[derive(Debug, Deserialize)]
struct BybitPositionList {
    list: Vec<BybitPositionEntry>,
}

#[derive(Debug, Deserialize)]
struct BybitPositionResponse {
    result: BybitPositionList,
}

pub struct BybitPositionManager {
    market: BybitMarket,
    rest_client: Option<BybitRestClient>,
    positions: HashMap<String, Position>,
}

fn split_bybit_symbol(symbol: &str) -> (String, String) {
    const KNOWN_QUOTES: &[&str] = &["USDT", "USDC", "BTC", "ETH", "USD"];
    for q in KNOWN_QUOTES {
        if let Some(base) = symbol.strip_suffix(q)
            && !base.is_empty()
        {
            return (base.to_string(), q.to_string());
        }
    }
    (symbol.to_string(), String::new())
}

impl BybitPositionManager {
    pub fn new(market: BybitMarket, api_key: &str, api_secret: &str) -> anyhow::Result<Self> {
        let rest_client = if api_key.is_empty() || api_secret.is_empty() {
            None
        } else {
            Some(BybitRestClient::new(
                "https://api.bybit.com",
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
            BybitMarket::Spot => "spot",
            BybitMarket::Linear => "linear",
            BybitMarket::Inverse => "inverse",
        }
    }

    fn category(&self) -> &'static str {
        match self.market {
            BybitMarket::Spot => "spot",
            BybitMarket::Linear => "linear",
            BybitMarket::Inverse => "inverse",
        }
    }

    pub fn apply_fill_inner(&mut self, fill: &Fill) {
        let key = Self::position_key(&fill.instrument);
        let market_label = self.market_label();

        let position = self
            .positions
            .entry(key.clone())
            .or_insert_with(|| Position {
                venue: Venue::Bybit,
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

    pub(crate) fn parse_wallet_and_positions(
        wallet_json: &str,
        positions_json: &str,
        venue: Venue,
        instrument_type: InstrumentType,
    ) -> anyhow::Result<PortfolioSnapshot> {
        let wallet_resp: BybitWalletResponse = serde_json::from_str(wallet_json)
            .map_err(|e| anyhow::anyhow!("bybit wallet parse error: {e}"))?;
        let pos_resp: BybitPositionResponse = serde_json::from_str(positions_json)
            .map_err(|e| anyhow::anyhow!("bybit positions parse error: {e}"))?;

        let (total_equity, available_balance, coin_positions) =
            if let Some(wallet) = wallet_resp.result.list.into_iter().next() {
                let equity: Decimal = wallet.total_equity.parse().unwrap_or(Decimal::ZERO);
                let available: Decimal = wallet
                    .available_to_withdraw
                    .parse()
                    .unwrap_or(Decimal::ZERO);

                let mut coin_pos = Vec::new();
                for c in wallet.coin {
                    let bal: Decimal = c.wallet_balance.parse().unwrap_or(Decimal::ZERO);
                    if bal <= Decimal::ZERO {
                        continue;
                    }
                    let upnl: Decimal = c.unrealised_pnl.parse().unwrap_or(Decimal::ZERO);
                    coin_pos.push(Position {
                        venue,
                        instrument: Instrument {
                            asset_class: AssetClass::Crypto,
                            instrument_type: InstrumentType::Spot,
                            base: c.coin,
                            quote: "USDT".into(),
                            settle_currency: Some("USDT".into()),
                            expiry: None,
                            last_trade_time: None,
                            settlement_time: None,
                        },
                        quantity: bal,
                        average_cost: Decimal::ZERO,
                        unrealized_pnl: upnl,
                        realized_pnl: Decimal::ZERO,
                        settlement_date: None,
                    });
                }
                (equity, available, coin_pos)
            } else {
                (Decimal::ZERO, Decimal::ZERO, Vec::new())
            };

        let mut positions = coin_positions;
        for entry in pos_resp.result.list {
            let size: Decimal = entry.size.parse().unwrap_or(Decimal::ZERO);
            if size == Decimal::ZERO {
                continue;
            }
            let avg: Decimal = entry.avg_price.parse().unwrap_or(Decimal::ZERO);
            let upnl: Decimal = entry.unrealised_pnl.parse().unwrap_or(Decimal::ZERO);
            let (base, quote) = split_bybit_symbol(&entry.symbol);
            let settle = if quote.is_empty() {
                "USDT".to_string()
            } else {
                quote.clone()
            };

            positions.push(Position {
                venue,
                instrument: Instrument {
                    asset_class: AssetClass::Crypto,
                    instrument_type,
                    base,
                    quote: settle.clone(),
                    settle_currency: Some(settle),
                    expiry: None,
                    last_trade_time: None,
                    settlement_time: None,
                },
                quantity: size,
                average_cost: avg,
                unrealized_pnl: upnl,
                realized_pnl: Decimal::ZERO,
                settlement_date: None,
            });
        }

        let unrealized_pnl = positions.iter().map(|p| p.unrealized_pnl).sum();

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

    fn local_snapshot(&self) -> PortfolioSnapshot {
        let positions: Vec<Position> = self.positions.values().cloned().collect();
        let unrealized_pnl = positions.iter().map(|p| p.unrealized_pnl).sum();
        let realized_pnl = positions.iter().map(|p| p.realized_pnl).sum();
        PortfolioSnapshot {
            venue: Venue::Bybit,
            positions,
            total_equity: Decimal::ZERO,
            available_balance: Decimal::ZERO,
            unrealized_pnl,
            realized_pnl,
        }
    }

    fn instrument_type(&self) -> InstrumentType {
        match self.market {
            BybitMarket::Spot => InstrumentType::Spot,
            BybitMarket::Linear | BybitMarket::Inverse => InstrumentType::Swap,
        }
    }
}

#[async_trait]
impl PositionManager for BybitPositionManager {
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
                return Ok(self.local_snapshot());
            }
        };

        let mut wallet_params = HashMap::new();
        wallet_params.insert("accountType".to_string(), "UNIFIED".to_string());
        let wallet_req = RestRequest {
            method: HttpMethod::Get,
            path: "/v5/account/wallet-balance".to_string(),
            params: wallet_params,
        };

        let mut pos_params = HashMap::new();
        pos_params.insert("category".to_string(), self.category().to_string());
        pos_params.insert("settleCoin".to_string(), "USDT".to_string());
        let pos_req = RestRequest {
            method: HttpMethod::Get,
            path: "/v5/position/list".to_string(),
            params: pos_params,
        };

        let wallet_resp = match rest.send(wallet_req).await {
            Err(e) => {
                tracing::warn!(
                    market = self.market_label(),
                    error = %e,
                    "get_portfolio wallet call failed, returning local state"
                );
                return Ok(self.local_snapshot());
            }
            Ok(r) => r,
        };
        if wallet_resp.status != 200 {
            tracing::warn!(
                market = self.market_label(),
                status = wallet_resp.status,
                "wallet-balance returned non-200, returning local state"
            );
            return Ok(self.local_snapshot());
        }

        let pos_resp = match rest.send(pos_req).await {
            Err(e) => {
                tracing::warn!(
                    market = self.market_label(),
                    error = %e,
                    "get_portfolio positions call failed, returning local state"
                );
                return Ok(self.local_snapshot());
            }
            Ok(r) => r,
        };
        if pos_resp.status != 200 {
            tracing::warn!(
                market = self.market_label(),
                status = pos_resp.status,
                "position/list returned non-200, returning local state"
            );
            return Ok(self.local_snapshot());
        }

        let snapshot = Self::parse_wallet_and_positions(
            &wallet_resp.body,
            &pos_resp.body,
            Venue::Bybit,
            self.instrument_type(),
        )?;
        tracing::info!(
            market = self.market_label(),
            num_positions = snapshot.positions.len(),
            total_equity = %snapshot.total_equity,
            "get_portfolio"
        );
        Ok(snapshot)
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

        tracing::info!(venue = ?Venue::Bybit, "position reconciliation complete");
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
            venue: Venue::Bybit,
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
        assert_eq!(BybitPositionManager::position_key(&inst), "BTC-USDT");
    }

    #[test]
    fn test_apply_fill_buy() {
        let mut pm = BybitPositionManager::new(BybitMarket::Linear, "", "").unwrap();
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
        let mut pm = BybitPositionManager::new(BybitMarket::Linear, "", "").unwrap();
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
        let mut pm = BybitPositionManager::new(BybitMarket::Linear, "", "").unwrap();
        let inst = test_instrument();

        pm.apply_fill_inner(&make_fill(&inst, Side::Buy, dec!(50000), dec!(1)));
        pm.apply_fill_inner(&make_fill(&inst, Side::Sell, dec!(50000), dec!(1)));
        let pos = pm.positions.get("BTC-USDT").unwrap();
        assert_eq!(pos.quantity, dec!(0));
        assert_eq!(pos.average_cost, dec!(0));
    }

    #[tokio::test]
    async fn test_get_position_missing() {
        let pm = BybitPositionManager::new(BybitMarket::Spot, "", "").unwrap();
        let result = pm.get_position("ETH-USDT").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_get_portfolio_empty() {
        let pm = BybitPositionManager::new(BybitMarket::Spot, "", "").unwrap();
        let snap = pm.get_portfolio().await.unwrap();
        assert!(snap.positions.is_empty());
        assert_eq!(snap.total_equity, Decimal::ZERO);
    }

    // ── New tests ─────────────────────────────────────────────────────────────

    #[test]
    fn get_portfolio_parses_spot_balance() {
        let wallet_json = r#"{
            "result": {
                "list": [{
                    "totalEquity": "10000.0",
                    "availableToWithdraw": "8000.0",
                    "coin": [
                        {"coin": "BTC", "walletBalance": "0.1", "unrealisedPnl": "50.0"},
                        {"coin": "USDT", "walletBalance": "5000.0", "unrealisedPnl": "0.0"},
                        {"coin": "ETH", "walletBalance": "0.0", "unrealisedPnl": "0.0"}
                    ]
                }]
            }
        }"#;
        let positions_json = r#"{"result": {"list": []}}"#;

        let snap = BybitPositionManager::parse_wallet_and_positions(
            wallet_json,
            positions_json,
            Venue::Bybit,
            InstrumentType::Spot,
        )
        .unwrap();

        // ETH with zero walletBalance filtered out
        assert_eq!(snap.positions.len(), 2);
        assert_eq!(snap.total_equity, dec!(10000));
        assert_eq!(snap.available_balance, dec!(8000));

        let btc = snap
            .positions
            .iter()
            .find(|p| p.instrument.base == "BTC")
            .unwrap();
        assert_eq!(btc.quantity, dec!(0.1));
        assert_eq!(btc.unrealized_pnl, dec!(50));
    }

    #[tokio::test]
    async fn sync_positions_logs_mismatch_when_diff_exceeds_threshold() {
        // Local: 1.0 BTC; diff_check against remote 1.5 BTC — >1%, should warn but not error.
        let mut pm = BybitPositionManager::new(BybitMarket::Linear, "", "").unwrap();
        let inst = test_instrument();
        pm.apply_fill_inner(&make_fill(&inst, Side::Buy, dec!(50000), dec!(1)));

        let remote_snap = PortfolioSnapshot {
            venue: Venue::Bybit,
            positions: vec![Position {
                venue: Venue::Bybit,
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
            instrument_type: InstrumentType::Swap,
            base: "ETH".to_string(),
            quote: "USDT".to_string(),
            settle_currency: Some("USDT".to_string()),
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        };
        assert_eq!(BybitPositionManager::position_key(&inst), "ETH-USDT");
    }

    #[test]
    fn empty_portfolio_returns_valid_snapshot() {
        let wallet_json = r#"{"result": {"list": []}}"#;
        let positions_json = r#"{"result": {"list": []}}"#;
        let snap = BybitPositionManager::parse_wallet_and_positions(
            wallet_json,
            positions_json,
            Venue::Bybit,
            InstrumentType::Spot,
        )
        .unwrap();
        assert!(snap.positions.is_empty());
        assert_eq!(snap.total_equity, Decimal::ZERO);
        assert_eq!(snap.available_balance, Decimal::ZERO);
        assert_eq!(snap.unrealized_pnl, Decimal::ZERO);
    }
}
