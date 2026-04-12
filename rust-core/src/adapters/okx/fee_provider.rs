use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;

use crate::adapters::fee_provider::FeeProvider;
use crate::adapters::okx::rest_client::OkxRestClient;
use crate::adapters::rest_client::{ExchangeRestClient, HttpMethod, RestRequest};
use crate::models::enums::Venue;
use crate::models::fee::FeeSchedule;

fn decimal_from_str<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Decimal::from_str(&s).map_err(serde::de::Error::custom)
}

#[derive(Deserialize)]
struct OkxFeeRate {
    #[serde(deserialize_with = "decimal_from_str")]
    maker: Decimal,
    #[serde(deserialize_with = "decimal_from_str")]
    taker: Decimal,
}

#[derive(Deserialize)]
struct OkxFeeResponse {
    data: Vec<OkxFeeRate>,
}

pub struct OkxFeeProvider {
    rest_client: OkxRestClient,
    inst_type: String,
}

impl OkxFeeProvider {
    pub fn new(rest_client: OkxRestClient, inst_type: &str) -> Self {
        Self {
            rest_client,
            inst_type: inst_type.to_string(),
        }
    }

    fn default_fees() -> FeeSchedule {
        FeeSchedule::new(
            Venue::Okx,
            rust_decimal_macros::dec!(0.0008),
            rust_decimal_macros::dec!(0.001),
        )
    }
}

#[async_trait]
impl FeeProvider for OkxFeeProvider {
    async fn get_fee_schedule(&self) -> anyhow::Result<FeeSchedule> {
        let mut params = HashMap::new();
        params.insert("instType".to_string(), self.inst_type.clone());

        let request = RestRequest {
            method: HttpMethod::Get,
            path: "/api/v5/account/trade-fee".to_string(),
            params,
        };

        match self.rest_client.send(request).await {
            Ok(resp) if resp.status == 200 => {
                let parsed: OkxFeeResponse = serde_json::from_str(&resp.body)?;
                if let Some(rate) = parsed.data.first() {
                    if rate.maker < Decimal::ZERO || rate.taker < Decimal::ZERO {
                        tracing::debug!(
                            maker = %rate.maker,
                            taker = %rate.taker,
                            "OKX rebate detected, using absolute value"
                        );
                    }
                    Ok(FeeSchedule::new(
                        Venue::Okx,
                        rate.maker.abs(),
                        rate.taker.abs(),
                    ))
                } else {
                    tracing::warn!("okx fee response empty, using defaults");
                    Ok(Self::default_fees())
                }
            }
            Ok(resp) => {
                tracing::warn!(
                    status = resp.status,
                    "failed to fetch okx fee schedule, using defaults"
                );
                Ok(Self::default_fees())
            }
            Err(e) => {
                tracing::warn!(%e, "failed to fetch okx fee schedule, using defaults");
                Ok(Self::default_fees())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_fees_are_sensible() {
        let fee = OkxFeeProvider::default_fees();
        assert_eq!(fee.maker_rate, rust_decimal_macros::dec!(0.0008));
        assert_eq!(fee.taker_rate, rust_decimal_macros::dec!(0.001));
    }

    #[test]
    fn parse_fee_response() {
        let json = r#"{"code":"0","data":[{"maker":"-0.0008","taker":"-0.001"}]}"#;
        let parsed: OkxFeeResponse = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.data[0].maker, rust_decimal_macros::dec!(-0.0008));
        assert_eq!(parsed.data[0].taker, rust_decimal_macros::dec!(-0.001));
    }
}
