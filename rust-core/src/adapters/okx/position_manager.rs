use std::collections::HashMap;

use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;

use super::rest_client::OkxRestClient;
use crate::adapters::position_manager::PositionManager;
use crate::adapters::rest_client::{ExchangeRestClient, HttpMethod, RestRequest};
use crate::models::enums::{Side, Venue};
use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
use crate::models::order::Fill;
use crate::models::position::{PortfolioSnapshot, Position};

// ── Balance deserialization ───────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct OkxBalanceDetail {
    ccy: String,
    #[serde(rename = "cashBal")]
    cash_bal: String,
    #[serde(default)]
    upl: String,
}

#[derive(Debug, Deserialize)]
struct OkxBalanceData {
    #[serde(rename = "totalEq")]
    total_eq: String,
    #[serde(rename = "availEq")]
    avail_eq: String,
    details: Vec<OkxBalanceDetail>,
}

#[derive(Debug, Deserialize)]
struct OkxBalanceResponse {
    data: Vec<OkxBalanceData>,
}

// ── Positions deserialization ─────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct OkxPositionEntry {
    #[serde(rename = "instId")]
    inst_id: String,
    pos: String,
    #[serde(rename = "avgPx")]
    avg_px: String,
    upl: String,
    #[serde(rename = "instType")]
    inst_type: String,
}

#[derive(Debug, Deserialize)]
struct OkxPositionResponse {
    data: Vec<OkxPositionEntry>,
}

// ── Manager ───────────────────────────────────────────────────────────────────

pub struct OkxPositionManager {
    rest_client: Option<OkxRestClient>,
    positions: HashMap<String, Position>,
}

impl OkxPositionManager {
    pub fn new(api_key: &str, api_secret: &str, passphrase: &str) -> anyhow::Result<Self> {
        let rest_client = if api_key.is_empty() || api_secret.is_empty() {
            None
        } else {
            Some(OkxRestClient::new(
                "https://www.okx.com",
                api_key,
                api_secret,
                passphrase,
            )?)
        };

        Ok(Self {
            rest_client,
            positions: HashMap::new(),
        })
    }

    fn position_key(instrument: &Instrument) -> String {
        format!("{}-{}", instrument.base, instrument.quote)
    }

    pub fn apply_fill_inner(&mut self, fill: &Fill) {
        let key = Self::position_key(&fill.instrument);

        let position = self
            .positions
            .entry(key.clone())
            .or_insert_with(|| Position {
                venue: Venue::Okx,
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
            key = key.as_str(),
            side = ?fill.side,
            fill_qty = %fill.quantity,
            fill_price = %fill.price,
            pos_qty = %position.quantity,
            avg_cost = %position.average_cost,
            realized_pnl = %position.realized_pnl,
            "okx apply_fill: position updated"
        );
    }

    // ── Parsing helpers (pub(crate) for unit tests) ───────────────────────────

    pub(crate) fn parse_balance_response(
        balance_json: &str,
        positions_json: &str,
        venue: Venue,
    ) -> anyhow::Result<PortfolioSnapshot> {
        let bal_resp: OkxBalanceResponse = serde_json::from_str(balance_json)
            .map_err(|e| anyhow::anyhow!("okx balance parse error: {e}"))?;
        let pos_resp: OkxPositionResponse = serde_json::from_str(positions_json)
            .map_err(|e| anyhow::anyhow!("okx positions parse error: {e}"))?;

        let (total_equity, available_balance, balance_positions) =
            if let Some(data) = bal_resp.data.into_iter().next() {
                let total_eq: Decimal = data.total_eq.parse().unwrap_or(Decimal::ZERO);
                let avail_eq: Decimal = data.avail_eq.parse().unwrap_or(Decimal::ZERO);

                let mut bal_positions = Vec::new();
                for d in data.details {
                    let cash: Decimal = d.cash_bal.parse().unwrap_or(Decimal::ZERO);
                    if cash <= Decimal::ZERO {
                        continue;
                    }
                    let upnl: Decimal = d.upl.parse().unwrap_or(Decimal::ZERO);
                    bal_positions.push(Position {
                        venue,
                        instrument: Instrument {
                            asset_class: AssetClass::Crypto,
                            instrument_type: InstrumentType::Spot,
                            base: d.ccy,
                            quote: "USDT".into(),
                            settle_currency: None,
                            expiry: None,
                            last_trade_time: None,
                            settlement_time: None,
                        },
                        quantity: cash,
                        average_cost: Decimal::ZERO,
                        unrealized_pnl: upnl,
                        realized_pnl: Decimal::ZERO,
                        settlement_date: None,
                    });
                }
                (total_eq, avail_eq, bal_positions)
            } else {
                (Decimal::ZERO, Decimal::ZERO, Vec::new())
            };

        // Merge in open derivative positions from /account/positions
        let mut positions = balance_positions;
        for p in pos_resp.data {
            let amt: Decimal = p.pos.parse().unwrap_or(Decimal::ZERO);
            if amt == Decimal::ZERO {
                continue;
            }
            let avg: Decimal = p.avg_px.parse().unwrap_or(Decimal::ZERO);
            let upnl: Decimal = p.upl.parse().unwrap_or(Decimal::ZERO);

            let parts: Vec<&str> = p.inst_id.split('-').collect();
            let (base, quote) = if parts.len() >= 2 {
                (parts[0].to_string(), parts[1].to_string())
            } else {
                (p.inst_id.clone(), "USDT".to_string())
            };

            let instrument_type = if p.inst_type == "SPOT" {
                InstrumentType::Spot
            } else {
                InstrumentType::Swap
            };

            positions.push(Position {
                venue,
                instrument: Instrument {
                    asset_class: AssetClass::Crypto,
                    instrument_type,
                    base,
                    quote,
                    settle_currency: Some("USDT".into()),
                    expiry: None,
                    last_trade_time: None,
                    settlement_time: None,
                },
                quantity: amt.abs(),
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
            venue: Venue::Okx,
            positions,
            total_equity: Decimal::ZERO,
            available_balance: Decimal::ZERO,
            unrealized_pnl,
            realized_pnl,
        }
    }
}

#[async_trait]
impl PositionManager for OkxPositionManager {
    async fn get_position(&self, symbol: &str) -> anyhow::Result<Option<Position>> {
        let position = self.positions.get(symbol).cloned();
        tracing::info!(symbol, found = position.is_some(), "okx get_position");
        Ok(position)
    }

    async fn get_portfolio(&self) -> anyhow::Result<PortfolioSnapshot> {
        let rest = match &self.rest_client {
            Some(r) => r,
            None => {
                tracing::warn!("okx: no credentials, returning local state");
                return Ok(self.local_snapshot());
            }
        };

        let bal_req = RestRequest {
            method: HttpMethod::Get,
            path: "/api/v5/account/balance".to_string(),
            params: HashMap::new(),
        };
        let pos_req = RestRequest {
            method: HttpMethod::Get,
            path: "/api/v5/account/positions".to_string(),
            params: HashMap::new(),
        };

        let bal_resp = match rest.send(bal_req).await {
            Err(e) => {
                tracing::warn!(error = %e, "okx get_portfolio balance call failed, returning local state");
                return Ok(self.local_snapshot());
            }
            Ok(r) => r,
        };
        if bal_resp.status != 200 {
            tracing::warn!(
                status = bal_resp.status,
                "okx balance returned non-200, returning local state"
            );
            return Ok(self.local_snapshot());
        }

        let pos_resp = match rest.send(pos_req).await {
            Err(e) => {
                tracing::warn!(error = %e, "okx get_portfolio positions call failed, returning local state");
                return Ok(self.local_snapshot());
            }
            Ok(r) => r,
        };
        if pos_resp.status != 200 {
            tracing::warn!(
                status = pos_resp.status,
                "okx positions returned non-200, returning local state"
            );
            return Ok(self.local_snapshot());
        }

        let snapshot =
            Self::parse_balance_response(&bal_resp.body, &pos_resp.body, Venue::Okx)?;
        tracing::info!(
            num_positions = snapshot.positions.len(),
            total_equity = %snapshot.total_equity,
            "okx get_portfolio"
        );
        Ok(snapshot)
    }

    async fn sync_positions(&mut self) -> anyhow::Result<()> {
        if self.rest_client.is_none() {
            tracing::warn!("okx: no credentials, skipping sync");
            return Ok(());
        }

        match self.get_portfolio().await {
            Err(e) => {
                tracing::warn!(error = %e, "okx sync_positions failed to fetch remote state");
            }
            Ok(remote) => {
                self.diff_check(&remote);
            }
        }

        tracing::info!(venue = ?Venue::Okx, "position reconciliation complete");
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
            venue: Venue::Okx,
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
        assert_eq!(OkxPositionManager::position_key(&inst), "BTC-USDT");
    }

    #[test]
    fn test_apply_fill_buy() {
        let mut pm = OkxPositionManager::new("", "", "").unwrap();
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
        let mut pm = OkxPositionManager::new("", "", "").unwrap();
        let inst = test_instrument();

        pm.apply_fill_inner(&make_fill(&inst, Side::Buy, dec!(50000), dec!(2)));
        pm.apply_fill_inner(&make_fill(&inst, Side::Sell, dec!(55000), dec!(1)));
        let pos = pm.positions.get("BTC-USDT").unwrap();
        assert_eq!(pos.quantity, dec!(1));
        assert_eq!(pos.realized_pnl, dec!(5000));
    }

    #[test]
    fn test_apply_fill_full_close() {
        let mut pm = OkxPositionManager::new("", "", "").unwrap();
        let inst = test_instrument();

        pm.apply_fill_inner(&make_fill(&inst, Side::Buy, dec!(50000), dec!(1)));
        pm.apply_fill_inner(&make_fill(&inst, Side::Sell, dec!(50000), dec!(1)));
        let pos = pm.positions.get("BTC-USDT").unwrap();
        assert_eq!(pos.quantity, dec!(0));
        assert_eq!(pos.average_cost, dec!(0));
    }

    #[tokio::test]
    async fn test_get_position_missing() {
        let pm = OkxPositionManager::new("", "", "").unwrap();
        let result = pm.get_position("ETH-USDT").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_get_portfolio_empty() {
        let pm = OkxPositionManager::new("", "", "").unwrap();
        let snap = pm.get_portfolio().await.unwrap();
        assert!(snap.positions.is_empty());
        assert_eq!(snap.total_equity, Decimal::ZERO);
    }

    // ── New tests ─────────────────────────────────────────────────────────────

    #[test]
    fn get_portfolio_parses_spot_balance() {
        let balance_json = r#"{
            "data": [{
                "totalEq": "2000.5",
                "availEq": "1500.0",
                "details": [
                    {"ccy": "BTC", "cashBal": "0.05", "frozenBal": "0.0", "upl": "10.0"},
                    {"ccy": "USDT", "cashBal": "1000.0", "frozenBal": "0.0", "upl": "0.0"},
                    {"ccy": "ETH", "cashBal": "0.0", "frozenBal": "0.0", "upl": "0.0"}
                ]
            }]
        }"#;
        let positions_json = r#"{"data": []}"#;

        let snap = OkxPositionManager::parse_balance_response(
            balance_json,
            positions_json,
            Venue::Okx,
        )
        .unwrap();

        // ETH with zero cashBal filtered out
        assert_eq!(snap.positions.len(), 2);
        assert_eq!(snap.total_equity, dec!(2000.5));
        assert_eq!(snap.available_balance, dec!(1500));

        let btc = snap
            .positions
            .iter()
            .find(|p| p.instrument.base == "BTC")
            .unwrap();
        assert_eq!(btc.quantity, dec!(0.05));
        assert_eq!(btc.unrealized_pnl, dec!(10));
    }

    #[tokio::test]
    async fn sync_positions_logs_mismatch_when_diff_exceeds_threshold() {
        // Local: 1.0 BTC; diff_check against remote 1.5 BTC — >1%, should warn but not error.
        let mut pm = OkxPositionManager::new("", "", "").unwrap();
        let inst = test_instrument();
        pm.apply_fill_inner(&make_fill(&inst, Side::Buy, dec!(50000), dec!(1)));

        let remote_snap = PortfolioSnapshot {
            venue: Venue::Okx,
            positions: vec![Position {
                venue: Venue::Okx,
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
            base: "SOL".to_string(),
            quote: "USDT".to_string(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        };
        assert_eq!(OkxPositionManager::position_key(&inst), "SOL-USDT");
    }

    #[test]
    fn empty_portfolio_returns_valid_snapshot() {
        let balance_json = r#"{"data": []}"#;
        let positions_json = r#"{"data": []}"#;
        let snap = OkxPositionManager::parse_balance_response(
            balance_json,
            positions_json,
            Venue::Okx,
        )
        .unwrap();
        assert!(snap.positions.is_empty());
        assert_eq!(snap.total_equity, Decimal::ZERO);
        assert_eq!(snap.available_balance, Decimal::ZERO);
        assert_eq!(snap.unrealized_pnl, Decimal::ZERO);
    }
}
