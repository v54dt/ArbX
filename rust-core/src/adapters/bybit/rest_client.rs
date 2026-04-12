use async_trait::async_trait;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;

use crate::adapters::rest_client::{ExchangeRestClient, HttpMethod, RestRequest, RestResponse};

type HmacSha256 = Hmac<Sha256>;

const RECV_WINDOW: &str = "5000";

pub struct BybitRestClient {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    api_secret: String,
}

impl BybitRestClient {
    pub fn new(base_url: &str, api_key: &str, api_secret: &str) -> Self {
        let http = reqwest::Client::builder()
            .default_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert("Content-Type", "application/json".parse().unwrap());
                headers
            })
            .build()
            .expect("failed to build reqwest client");

        Self {
            http,
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
        }
    }

    fn sign(&self, payload: &str) -> String {
        let mut mac =
            HmacSha256::new_from_slice(self.api_secret.as_bytes()).expect("HMAC accepts any key");
        mac.update(payload.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    fn timestamp_ms() -> String {
        chrono::Utc::now().timestamp_millis().to_string()
    }

    fn build_query_string(params: &HashMap<String, String>) -> String {
        let mut sorted: Vec<(&str, &str)> = params
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        sorted.sort_by(|a, b| a.0.cmp(b.0));
        sorted
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&")
    }
}

#[async_trait]
impl ExchangeRestClient for BybitRestClient {
    async fn send(&self, request: RestRequest) -> anyhow::Result<RestResponse> {
        let ts = Self::timestamp_ms();

        match request.method {
            HttpMethod::Get | HttpMethod::Delete => {
                let qs = Self::build_query_string(&request.params);
                let sign_payload = format!("{}{}{}{}", ts, self.api_key, RECV_WINDOW, qs);
                let signature = self.sign(&sign_payload);

                let url = if qs.is_empty() {
                    format!("{}{}", self.base_url, request.path)
                } else {
                    format!("{}{}?{}", self.base_url, request.path, qs)
                };

                let builder = match request.method {
                    HttpMethod::Get => self.http.get(&url),
                    HttpMethod::Delete => self.http.delete(&url),
                    _ => unreachable!(),
                };

                let resp = builder
                    .header("X-BAPI-API-KEY", &self.api_key)
                    .header("X-BAPI-SIGN", &signature)
                    .header("X-BAPI-TIMESTAMP", &ts)
                    .header("X-BAPI-RECV-WINDOW", RECV_WINDOW)
                    .send()
                    .await?;

                Ok(RestResponse {
                    status: resp.status().as_u16(),
                    body: resp.text().await?,
                })
            }
            HttpMethod::Post => {
                let body = serde_json::to_string(&request.params)?;
                let sign_payload = format!("{}{}{}{}", ts, self.api_key, RECV_WINDOW, body);
                let signature = self.sign(&sign_payload);

                let url = format!("{}{}", self.base_url, request.path);

                let resp = self
                    .http
                    .post(&url)
                    .header("X-BAPI-API-KEY", &self.api_key)
                    .header("X-BAPI-SIGN", &signature)
                    .header("X-BAPI-TIMESTAMP", &ts)
                    .header("X-BAPI-RECV-WINDOW", RECV_WINDOW)
                    .body(body)
                    .send()
                    .await?;

                Ok(RestResponse {
                    status: resp.status().as_u16(),
                    body: resp.text().await?,
                })
            }
        }
    }

    async fn send_public(&self, request: RestRequest) -> anyhow::Result<RestResponse> {
        let qs = Self::build_query_string(&request.params);
        let url = if qs.is_empty() {
            format!("{}{}", self.base_url, request.path)
        } else {
            format!("{}{}?{}", self.base_url, request.path, qs)
        };

        let req = match request.method {
            HttpMethod::Get => self.http.get(&url),
            HttpMethod::Post => self.http.post(&url),
            HttpMethod::Delete => self.http.delete(&url),
        };

        let resp = req.send().await?;

        Ok(RestResponse {
            status: resp.status().as_u16(),
            body: resp.text().await?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_client() -> BybitRestClient {
        BybitRestClient::new("https://api.bybit.com", "test-key", "test-secret")
    }

    #[test]
    fn sign_produces_correct_hmac() {
        let client = test_client();
        let sig1 = client.sign("1234567890test-key5000symbol=BTCUSDT");
        let sig2 = client.sign("1234567890test-key5000symbol=BTCUSDT");
        assert_eq!(sig1, sig2);
        assert_eq!(sig1.len(), 64);

        let sig3 = client.sign("1234567890test-key5000symbol=ETHUSDT");
        assert_ne!(sig1, sig3);
    }

    #[test]
    fn build_query_string_sorts_keys() {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), "BTCUSDT".to_string());
        params.insert("category".to_string(), "linear".to_string());
        let qs = BybitRestClient::build_query_string(&params);
        assert!(qs.starts_with("category="));
        assert!(qs.contains("&symbol="));
    }
}
