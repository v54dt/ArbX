use async_trait::async_trait;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;

use crate::adapters::rest_client::{ExchangeRestClient, HttpMethod, RestRequest, RestResponse};

type HmacSha256 = Hmac<Sha256>;

pub struct OkxRestClient {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    api_secret: String,
    passphrase: String,
}

impl OkxRestClient {
    pub fn new(base_url: &str, api_key: &str, api_secret: &str, passphrase: &str) -> Self {
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
            passphrase: passphrase.to_string(),
        }
    }

    fn timestamp() -> String {
        chrono::Utc::now()
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string()
    }

    fn sign(&self, timestamp: &str, method: &str, request_path: &str, body: &str) -> String {
        let prehash = format!("{timestamp}{method}{request_path}{body}");
        let mut mac =
            HmacSha256::new_from_slice(self.api_secret.as_bytes()).expect("HMAC accepts any key");
        mac.update(prehash.as_bytes());
        base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
    }

    fn method_str(method: HttpMethod) -> &'static str {
        match method {
            HttpMethod::Get => "GET",
            HttpMethod::Post => "POST",
            HttpMethod::Delete => "DELETE",
        }
    }

    fn build_query_string(params: &HashMap<String, String>) -> String {
        if params.is_empty() {
            return String::new();
        }
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
impl ExchangeRestClient for OkxRestClient {
    async fn send(&self, request: RestRequest) -> anyhow::Result<RestResponse> {
        let method_str = Self::method_str(request.method);
        let qs = Self::build_query_string(&request.params);
        let request_path = if qs.is_empty() {
            request.path.clone()
        } else {
            format!("{}?{}", request.path, qs)
        };

        let body = if matches!(request.method, HttpMethod::Post) && !request.params.is_empty() {
            serde_json::to_string(&request.params)?
        } else {
            String::new()
        };

        let timestamp = Self::timestamp();
        let signature = self.sign(&timestamp, method_str, &request_path, &body);

        let url = format!("{}{}", self.base_url, request_path);

        let mut req = match request.method {
            HttpMethod::Get => self.http.get(&url),
            HttpMethod::Post => self.http.post(&url).body(body),
            HttpMethod::Delete => self.http.delete(&url),
        };

        req = req
            .header("OK-ACCESS-KEY", &self.api_key)
            .header("OK-ACCESS-SIGN", &signature)
            .header("OK-ACCESS-TIMESTAMP", &timestamp)
            .header("OK-ACCESS-PASSPHRASE", &self.passphrase);

        let resp = req.send().await?;

        Ok(RestResponse {
            status: resp.status().as_u16(),
            body: resp.text().await?,
        })
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

    fn test_client() -> OkxRestClient {
        OkxRestClient::new(
            "https://www.okx.com",
            "test-key",
            "test-secret",
            "test-pass",
        )
    }

    #[test]
    fn sign_produces_consistent_hmac() {
        let client = test_client();
        let ts = "2023-01-01T00:00:00.000Z";
        let sig1 = client.sign(ts, "GET", "/api/v5/account/balance", "");
        let sig2 = client.sign(ts, "GET", "/api/v5/account/balance", "");
        assert_eq!(sig1, sig2);

        let sig3 = client.sign(ts, "POST", "/api/v5/trade/order", "{}");
        assert_ne!(sig1, sig3);
    }

    #[test]
    fn sign_is_base64_encoded() {
        let client = test_client();
        let sig = client.sign("2023-01-01T00:00:00.000Z", "GET", "/api/v5/test", "");
        assert!(
            base64::engine::general_purpose::STANDARD
                .decode(&sig)
                .is_ok()
        );
    }

    #[test]
    fn build_query_string_sorts_keys() {
        let mut params = HashMap::new();
        params.insert("instType".to_string(), "SPOT".to_string());
        params.insert("instId".to_string(), "BTC-USDT".to_string());
        let qs = OkxRestClient::build_query_string(&params);
        assert!(qs.starts_with("instId="));
    }

    #[test]
    fn build_query_string_empty() {
        let params = HashMap::new();
        let qs = OkxRestClient::build_query_string(&params);
        assert!(qs.is_empty());
    }
}
