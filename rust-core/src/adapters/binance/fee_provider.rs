use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;

use crate::adapters::binance::market_data::BinanceMarket;
use crate::adapters::binance::rest_client::BinanceRestClient;
use crate::adapters::fee_provider::FeeProvider;
use crate::adapters::rest_client::{ExchangeRestClient, HttpMethod, RestRequest};
use crate::models::enums::Venue;
use crate::models::fee::FeeSchedule;

pub struct BinanceFeeProvider {
    rest_client: BinanceRestClient,
    venue: Venue,
    market: BinanceMarket,
    /// Symbol to fetch the commission rate for (Futures only — spot uses the
    /// account-wide rate). Different VIP tiers can apply per-symbol on Binance
    /// Futures, so always fetch the symbol the strategy will trade against.
    symbol: String,
}

fn decimal_from_str<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Decimal::from_str(&s).map_err(serde::de::Error::custom)
}

#[derive(Deserialize)]
struct FuturesCommissionRate {
    #[serde(rename = "makerCommissionRate", deserialize_with = "decimal_from_str")]
    maker_commission_rate: Decimal,
    #[serde(rename = "takerCommissionRate", deserialize_with = "decimal_from_str")]
    taker_commission_rate: Decimal,
}

#[derive(Deserialize)]
struct SpotAccount {
    #[serde(rename = "makerCommission")]
    maker_commission: Decimal,
    #[serde(rename = "takerCommission")]
    taker_commission: Decimal,
}

impl BinanceFeeProvider {
    pub fn new(rest_client: BinanceRestClient, market: BinanceMarket, symbol: &str) -> Self {
        let venue = Venue::Binance;
        Self {
            rest_client,
            venue,
            market,
            symbol: symbol.to_string(),
        }
    }

    fn default_futures() -> FeeSchedule {
        FeeSchedule::new(
            Venue::Binance,
            rust_decimal_macros::dec!(0.0002),
            rust_decimal_macros::dec!(0.0004),
        )
    }

    fn default_spot() -> FeeSchedule {
        FeeSchedule::new(
            Venue::Binance,
            rust_decimal_macros::dec!(0.001),
            rust_decimal_macros::dec!(0.001),
        )
    }
}

#[async_trait]
impl FeeProvider for BinanceFeeProvider {
    async fn get_fee_schedule(&self) -> anyhow::Result<FeeSchedule> {
        match self.market {
            BinanceMarket::UsdtFutures | BinanceMarket::CoinFutures => {
                let mut params = HashMap::new();
                params.insert("symbol".to_string(), self.symbol.clone());
                let request = RestRequest {
                    method: HttpMethod::Get,
                    path: "/fapi/v1/commissionRate".to_string(),
                    params,
                    raw_body: None,
                };
                match self.rest_client.send(request).await {
                    Ok(resp) if resp.status == 200 => {
                        let rate: FuturesCommissionRate = serde_json::from_str(&resp.body)?;
                        Ok(FeeSchedule::new(
                            self.venue,
                            rate.maker_commission_rate,
                            rate.taker_commission_rate,
                        ))
                    }
                    Ok(resp) => {
                        tracing::warn!(
                            status = resp.status,
                            "failed to fetch futures commission rate, using defaults"
                        );
                        Ok(Self::default_futures())
                    }
                    Err(e) => {
                        tracing::warn!(%e, "failed to fetch futures commission rate, using defaults");
                        Ok(Self::default_futures())
                    }
                }
            }
            BinanceMarket::Spot => {
                let request = RestRequest {
                    method: HttpMethod::Get,
                    path: "/api/v3/account".to_string(),
                    params: HashMap::new(),
                    raw_body: None,
                };
                match self.rest_client.send(request).await {
                    Ok(resp) if resp.status == 200 => {
                        let acct: SpotAccount = serde_json::from_str(&resp.body)?;
                        let basis = rust_decimal_macros::dec!(10000);
                        Ok(FeeSchedule::new(
                            self.venue,
                            acct.maker_commission / basis,
                            acct.taker_commission / basis,
                        ))
                    }
                    Ok(resp) => {
                        tracing::warn!(
                            status = resp.status,
                            "failed to fetch spot account fees, using defaults"
                        );
                        Ok(Self::default_spot())
                    }
                    Err(e) => {
                        tracing::warn!(%e, "failed to fetch spot account fees, using defaults");
                        Ok(Self::default_spot())
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_futures_fees_are_sensible() {
        let fee = BinanceFeeProvider::default_futures();
        assert_eq!(fee.maker_rate, rust_decimal_macros::dec!(0.0002));
        assert_eq!(fee.taker_rate, rust_decimal_macros::dec!(0.0004));
    }

    #[test]
    fn default_spot_fees_are_sensible() {
        let fee = BinanceFeeProvider::default_spot();
        assert_eq!(fee.maker_rate, rust_decimal_macros::dec!(0.001));
        assert_eq!(fee.taker_rate, rust_decimal_macros::dec!(0.001));
    }

    #[test]
    fn parse_futures_commission_response() {
        let json = r#"{"makerCommissionRate":"0.0002","takerCommissionRate":"0.0004"}"#;
        let rate: FuturesCommissionRate = serde_json::from_str(json).unwrap();
        assert_eq!(
            rate.maker_commission_rate,
            rust_decimal_macros::dec!(0.0002)
        );
        assert_eq!(
            rate.taker_commission_rate,
            rust_decimal_macros::dec!(0.0004)
        );
    }

    #[test]
    fn parse_spot_account_response() {
        let json = r#"{"makerCommission":10,"takerCommission":10,"buyerCommission":0,"sellerCommission":0}"#;
        let acct: SpotAccount = serde_json::from_str(json).unwrap();
        assert_eq!(acct.maker_commission, rust_decimal_macros::dec!(10));
        assert_eq!(acct.taker_commission, rust_decimal_macros::dec!(10));
    }
}
