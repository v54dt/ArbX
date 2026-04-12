use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;

use crate::adapters::bybit::rest_client::BybitRestClient;
use crate::adapters::fee_provider::FeeProvider;
use crate::adapters::rest_client::{ExchangeRestClient, HttpMethod, RestRequest};
use crate::models::enums::Venue;
use crate::models::fee::FeeSchedule;

use super::market_data::BybitMarket;

fn decimal_from_str<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Decimal::from_str(&s).map_err(serde::de::Error::custom)
}

#[derive(Deserialize)]
struct FeeRateEntry {
    #[serde(rename = "makerFeeRate", deserialize_with = "decimal_from_str")]
    maker_fee_rate: Decimal,
    #[serde(rename = "takerFeeRate", deserialize_with = "decimal_from_str")]
    taker_fee_rate: Decimal,
}

#[derive(Deserialize)]
struct FeeRateList {
    list: Vec<FeeRateEntry>,
}

#[derive(Deserialize)]
struct FeeRateResponse {
    result: FeeRateList,
}

pub struct BybitFeeProvider {
    rest_client: BybitRestClient,
    market: BybitMarket,
}

impl BybitFeeProvider {
    pub fn new(rest_client: BybitRestClient, market: BybitMarket) -> Self {
        Self {
            rest_client,
            market,
        }
    }

    fn default_linear() -> FeeSchedule {
        FeeSchedule::new(
            Venue::Bybit,
            rust_decimal_macros::dec!(0.0002),
            rust_decimal_macros::dec!(0.00055),
        )
    }

    fn default_spot() -> FeeSchedule {
        FeeSchedule::new(
            Venue::Bybit,
            rust_decimal_macros::dec!(0.001),
            rust_decimal_macros::dec!(0.001),
        )
    }

    fn category(&self) -> &'static str {
        match self.market {
            BybitMarket::Spot => "spot",
            BybitMarket::Linear => "linear",
            BybitMarket::Inverse => "inverse",
        }
    }

    fn default_schedule(&self) -> FeeSchedule {
        match self.market {
            BybitMarket::Spot => Self::default_spot(),
            BybitMarket::Linear | BybitMarket::Inverse => Self::default_linear(),
        }
    }
}

#[async_trait]
impl FeeProvider for BybitFeeProvider {
    async fn get_fee_schedule(&self) -> anyhow::Result<FeeSchedule> {
        let mut params = HashMap::new();
        params.insert("category".to_string(), self.category().to_string());
        params.insert("symbol".to_string(), "BTCUSDT".to_string());

        let request = RestRequest {
            method: HttpMethod::Get,
            path: "/v5/account/fee-rate".to_string(),
            params,
        };

        match self.rest_client.send(request).await {
            Ok(resp) if resp.status == 200 => {
                match serde_json::from_str::<FeeRateResponse>(&resp.body) {
                    Ok(parsed) if !parsed.result.list.is_empty() => {
                        let entry = &parsed.result.list[0];
                        Ok(FeeSchedule::new(
                            Venue::Bybit,
                            entry.maker_fee_rate,
                            entry.taker_fee_rate,
                        ))
                    }
                    _ => {
                        tracing::warn!("failed to parse fee-rate response, using defaults");
                        Ok(self.default_schedule())
                    }
                }
            }
            Ok(resp) => {
                tracing::warn!(
                    status = resp.status,
                    "failed to fetch fee-rate, using defaults"
                );
                Ok(self.default_schedule())
            }
            Err(e) => {
                tracing::warn!(%e, "failed to fetch fee-rate, using defaults");
                Ok(self.default_schedule())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_linear_fees_are_sensible() {
        let fee = BybitFeeProvider::default_linear();
        assert_eq!(fee.maker_rate, rust_decimal_macros::dec!(0.0002));
        assert_eq!(fee.taker_rate, rust_decimal_macros::dec!(0.00055));
    }

    #[test]
    fn default_spot_fees_are_sensible() {
        let fee = BybitFeeProvider::default_spot();
        assert_eq!(fee.maker_rate, rust_decimal_macros::dec!(0.001));
        assert_eq!(fee.taker_rate, rust_decimal_macros::dec!(0.001));
    }

    #[test]
    fn parse_fee_rate_response() {
        let json = r#"{"retCode":0,"result":{"list":[{"makerFeeRate":"0.0002","takerFeeRate":"0.00055"}]}}"#;
        let parsed: FeeRateResponse = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.result.list.len(), 1);
        assert_eq!(
            parsed.result.list[0].maker_fee_rate,
            rust_decimal_macros::dec!(0.0002)
        );
        assert_eq!(
            parsed.result.list[0].taker_fee_rate,
            rust_decimal_macros::dec!(0.00055)
        );
    }
}
