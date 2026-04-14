use async_trait::async_trait;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;
use std::time::Duration;
use tracing::warn;

use crate::adapters::rate_limiter::RateLimiter;
use crate::adapters::rest_client::{ExchangeRestClient, HttpMethod, RestRequest, RestResponse};

type HmacSha256 = Hmac<Sha256>;

pub struct BinanceRestClient {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    api_secret: String,
    rate_limiter: RateLimiter,
}

impl BinanceRestClient {
    pub fn new(base_url: &str, api_key: &str, api_secret: &str) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .default_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert(
                    "Content-Type",
                    "application/x-www-form-urlencoded".parse().unwrap(),
                );
                headers
            })
            .build()?;

        Ok(Self {
            http,
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            rate_limiter: RateLimiter::new(20),
        })
    }

    fn sign(&self, query_string: &str) -> String {
        let mut mac =
            HmacSha256::new_from_slice(self.api_secret.as_bytes()).expect("HMAC accepts any key");
        mac.update(query_string.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    fn build_signed_params(&self, params: &HashMap<String, String>) -> String {
        let ts = chrono::Utc::now().timestamp_millis().to_string();
        let mut sorted: Vec<(String, String)> =
            params.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        sorted.push(("recvWindow".to_string(), "5000".to_string()));
        sorted.push(("timestamp".to_string(), ts));
        sorted.sort_by(|a, b| a.0.cmp(&b.0));

        let qs: String = sorted
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");

        let sig = self.sign(&qs);
        format!("{qs}&signature={sig}")
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
impl ExchangeRestClient for BinanceRestClient {
    async fn send(&self, request: RestRequest) -> anyhow::Result<RestResponse> {
        self.rate_limiter.acquire().await;

        let qs = self.build_signed_params(&request.params);
        let url = format!("{}{}?{}", self.base_url, request.path, qs);

        let req = match request.method {
            HttpMethod::Get => self.http.get(&url),
            HttpMethod::Post => self.http.post(&url),
            HttpMethod::Put => self.http.put(&url),
            HttpMethod::Delete => self.http.delete(&url),
        };

        let resp = req.header("X-MBX-APIKEY", &self.api_key).send().await?;

        if resp.status() == 429 {
            let retry_after = resp
                .headers()
                .get("Retry-After")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(5);
            warn!(retry_after_secs = retry_after, "rate limited by exchange");
            tokio::time::sleep(Duration::from_secs(retry_after)).await;
            return self.send(request).await;
        }

        Ok(RestResponse {
            status: resp.status().as_u16(),
            body: resp.text().await?,
        })
    }

    async fn send_public(&self, request: RestRequest) -> anyhow::Result<RestResponse> {
        self.rate_limiter.acquire().await;

        let qs = Self::build_query_string(&request.params);
        let url = if qs.is_empty() {
            format!("{}{}", self.base_url, request.path)
        } else {
            format!("{}{}?{}", self.base_url, request.path, qs)
        };

        let req = match request.method {
            HttpMethod::Get => self.http.get(&url),
            HttpMethod::Post => self.http.post(&url),
            HttpMethod::Put => self.http.put(&url),
            HttpMethod::Delete => self.http.delete(&url),
        };

        let resp = req.send().await?;

        if resp.status() == 429 {
            let retry_after = resp
                .headers()
                .get("Retry-After")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(5);
            warn!(retry_after_secs = retry_after, "rate limited by exchange");
            tokio::time::sleep(Duration::from_secs(retry_after)).await;
            return self.send_public(request).await;
        }

        Ok(RestResponse {
            status: resp.status().as_u16(),
            body: resp.text().await?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_client() -> BinanceRestClient {
        BinanceRestClient::new("https://api.binance.com", "test-key", "test-secret").unwrap()
    }

    #[test]
    fn sign_produces_correct_hmac() {
        let client = test_client();
        let sig1 = client.sign("symbol=BTCUSDT&side=BUY");
        let sig2 = client.sign("symbol=BTCUSDT&side=BUY");
        assert_eq!(sig1, sig2);
        assert_eq!(sig1.len(), 64);

        let sig3 = client.sign("symbol=ETHUSDT&side=BUY");
        assert_ne!(sig1, sig3);
    }

    #[test]
    fn build_signed_params_includes_timestamp_and_signature() {
        let client = test_client();
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), "BTCUSDT".to_string());
        let qs = client.build_signed_params(&params);
        assert!(qs.contains("timestamp="));
        assert!(qs.contains("signature="));
        assert!(qs.contains("recvWindow=5000"));
    }

    #[test]
    fn build_signed_params_sorts_keys() {
        let client = test_client();
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), "BTCUSDT".to_string());
        params.insert("side".to_string(), "BUY".to_string());
        params.insert("amount".to_string(), "1.0".to_string());
        let qs = client.build_signed_params(&params);

        let before_sig = qs.split("&signature=").next().unwrap();
        let parts: Vec<&str> = before_sig.split('&').collect();
        let keys: Vec<&str> = parts.iter().map(|p| p.split('=').next().unwrap()).collect();
        for i in 0..keys.len() - 1 {
            assert!(
                keys[i] <= keys[i + 1],
                "keys not sorted: {} > {}",
                keys[i],
                keys[i + 1]
            );
        }
    }
}
