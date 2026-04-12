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

#[derive(Debug, Deserialize)]
struct OkxPosition {
    #[serde(rename = "instId")]
    inst_id: String,
    pos: String,
    #[serde(rename = "avgPx")]
    avg_px: String,
    upl: String,
}

#[derive(Debug, Deserialize)]
struct OkxPositionResponse {
    data: Vec<OkxPosition>,
}

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
}

#[async_trait]
impl PositionManager for OkxPositionManager {
    async fn get_position(&self, symbol: &str) -> anyhow::Result<Option<Position>> {
        let position = self.positions.get(symbol).cloned();
        tracing::info!(symbol, found = position.is_some(), "okx get_position");
        Ok(position)
    }

    async fn get_portfolio(&self) -> anyhow::Result<PortfolioSnapshot> {
        let positions: Vec<Position> = self.positions.values().cloned().collect();
        let unrealized_pnl = positions.iter().map(|p| p.unrealized_pnl).sum();
        let realized_pnl = positions.iter().map(|p| p.realized_pnl).sum();

        Ok(PortfolioSnapshot {
            venue: Venue::Okx,
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
                tracing::warn!("okx: no credentials, skipping sync");
                return Ok(());
            }
        };

        let req = RestRequest {
            method: HttpMethod::Get,
            path: "/api/v5/account/positions".to_string(),
            params: HashMap::new(),
        };
        let resp = rest.send(req).await?;
        if resp.status != 200 {
            anyhow::bail!("okx positions returned {}: {}", resp.status, resp.body);
        }

        let parsed: OkxPositionResponse = serde_json::from_str(&resp.body)?;
        for p in parsed.data {
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

            let key = format!("{}-{}", base, quote);
            let pos = self.positions.entry(key).or_insert_with(|| Position {
                venue: Venue::Okx,
                instrument: Instrument {
                    asset_class: AssetClass::Crypto,
                    instrument_type: InstrumentType::Swap,
                    base: base.clone(),
                    quote: quote.clone(),
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
            pos.average_cost = avg;
            pos.unrealized_pnl = upnl;
        }

        tracing::info!(
            positions = self.positions.len(),
            "okx sync_positions completed"
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
}
