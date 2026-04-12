use std::collections::HashMap;

use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;

use super::rest_client::BybitRestClient;
use crate::adapters::position_manager::PositionManager;
use crate::adapters::rest_client::{ExchangeRestClient, HttpMethod, RestRequest};
use crate::models::enums::{Side, Venue};
use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
use crate::models::order::Fill;
use crate::models::position::{PortfolioSnapshot, Position};

use super::market_data::BybitMarket;

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

impl BybitPositionManager {
    pub fn new(market: BybitMarket, api_key: &str, api_secret: &str) -> Self {
        let rest_client = if api_key.is_empty() || api_secret.is_empty() {
            None
        } else {
            Some(BybitRestClient::new(
                "https://api.bybit.com",
                api_key,
                api_secret,
            ))
        };

        Self {
            market,
            rest_client,
            positions: HashMap::new(),
        }
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
        let positions: Vec<Position> = self.positions.values().cloned().collect();
        let unrealized_pnl = positions.iter().map(|p| p.unrealized_pnl).sum();
        let realized_pnl = positions.iter().map(|p| p.realized_pnl).sum();

        tracing::info!(
            market = self.market_label(),
            num_positions = positions.len(),
            "get_portfolio"
        );

        Ok(PortfolioSnapshot {
            venue: Venue::Bybit,
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

        let mut params = HashMap::new();
        params.insert("category".to_string(), self.category().to_string());

        let req = RestRequest {
            method: HttpMethod::Get,
            path: "/v5/position/list".to_string(),
            params,
        };
        let resp = rest.send(req).await?;
        if resp.status != 200 {
            anyhow::bail!("position/list returned {}: {}", resp.status, resp.body);
        }

        let parsed: BybitPositionResponse = serde_json::from_str(&resp.body)?;
        for entry in parsed.result.list {
            let size: Decimal = entry.size.parse().unwrap_or(Decimal::ZERO);
            if size == Decimal::ZERO {
                continue;
            }
            let avg: Decimal = entry.avg_price.parse().unwrap_or(Decimal::ZERO);
            let upnl: Decimal = entry.unrealised_pnl.parse().unwrap_or(Decimal::ZERO);
            let key = entry.symbol.clone();

            let instrument_type = match self.market {
                BybitMarket::Spot => InstrumentType::Spot,
                BybitMarket::Linear | BybitMarket::Inverse => InstrumentType::Swap,
            };

            let pos = self.positions.entry(key).or_insert_with(|| Position {
                venue: Venue::Bybit,
                instrument: Instrument {
                    asset_class: AssetClass::Crypto,
                    instrument_type,
                    base: entry.symbol.clone(),
                    quote: "USDT".into(),
                    settle_currency: Some("USDT".into()),
                    expiry: None,
                },
                quantity: Decimal::ZERO,
                average_cost: Decimal::ZERO,
                unrealized_pnl: Decimal::ZERO,
                realized_pnl: Decimal::ZERO,
            });
            pos.quantity = size;
            pos.average_cost = avg;
            pos.unrealized_pnl = upnl;
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
        let mut pm = BybitPositionManager::new(BybitMarket::Linear, "", "");
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
        let mut pm = BybitPositionManager::new(BybitMarket::Linear, "", "");
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
        let mut pm = BybitPositionManager::new(BybitMarket::Linear, "", "");
        let inst = test_instrument();

        pm.apply_fill_inner(&make_fill(&inst, Side::Buy, dec!(50000), dec!(1)));
        pm.apply_fill_inner(&make_fill(&inst, Side::Sell, dec!(50000), dec!(1)));
        let pos = pm.positions.get("BTC-USDT").unwrap();
        assert_eq!(pos.quantity, dec!(0));
        assert_eq!(pos.average_cost, dec!(0));
    }

    #[tokio::test]
    async fn test_get_position_missing() {
        let pm = BybitPositionManager::new(BybitMarket::Spot, "", "");
        let result = pm.get_position("ETH-USDT").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_get_portfolio_empty() {
        let pm = BybitPositionManager::new(BybitMarket::Spot, "", "");
        let snap = pm.get_portfolio().await.unwrap();
        assert!(snap.positions.is_empty());
        assert_eq!(snap.total_equity, Decimal::ZERO);
    }
}
