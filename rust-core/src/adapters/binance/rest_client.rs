use async_trait::async_trait;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::{debug, warn};

use crate::adapters::rate_limiter::RateLimiter;
use crate::adapters::rest_client::{ExchangeRestClient, HttpMethod, RestRequest, RestResponse};

type HmacSha256 = Hmac<Sha256>;

/// Binance-advertised default 1-minute request-weight quota (spot `api/v3`).
/// Futures `fapi/v1` is 2400/min; pass an explicit override if you're on fUSDM/fCOIN.
const DEFAULT_WEIGHT_LIMIT_1M: u64 = 1200;

/// When used-weight climbs past this fraction of the quota, we start slowing
/// outbound requests to avoid tripping a 429. 0.80 leaves headroom for bursts.
const WEIGHT_BACKOFF_THRESHOLD: f64 = 0.80;

/// Minimum extra sleep injected before a call when above the threshold.
/// At threshold we wait this long; at 100% we wait ~5x this long.
const WEIGHT_BACKOFF_BASE_MS: u64 = 200;

/// Tracks last-observed `X-MBX-USED-WEIGHT-1M` so concurrent requests share one
/// view of quota consumption. Updates are best-effort (non-atomic RMW is fine —
/// header reads are monotonic within a 1-minute window from Binance's side).
#[derive(Debug)]
pub(crate) struct WeightGuard {
    used_1m: AtomicU64,
    limit_1m: AtomicU64,
}

impl WeightGuard {
    fn new(limit: u64) -> Self {
        Self {
            used_1m: AtomicU64::new(0),
            limit_1m: AtomicU64::new(limit),
        }
    }

    /// Update the last-observed used weight (from an HTTP response header).
    pub(crate) fn observe(&self, used: u64) {
        self.used_1m.store(used, Ordering::SeqCst);
    }

    /// How long to sleep before the next outbound request given current usage.
    /// Returns zero below the threshold; scales linearly up to ~5x base at 100%.
    pub(crate) fn adaptive_delay(&self) -> Duration {
        let used = self.used_1m.load(Ordering::SeqCst);
        let limit = self.limit_1m.load(Ordering::SeqCst).max(1);
        let ratio = used as f64 / limit as f64;
        if ratio < WEIGHT_BACKOFF_THRESHOLD {
            return Duration::ZERO;
        }
        // at threshold → 1x base; at 100% → 5x base; above 100% → capped at 10x
        let over =
            ((ratio - WEIGHT_BACKOFF_THRESHOLD) / (1.0 - WEIGHT_BACKOFF_THRESHOLD)).clamp(0.0, 2.0);
        let multiplier = 1.0 + 4.0 * over;
        Duration::from_millis((WEIGHT_BACKOFF_BASE_MS as f64 * multiplier) as u64)
    }
}

pub struct BinanceRestClient {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    api_secret: String,
    rate_limiter: RateLimiter,
    weight_guard: Arc<WeightGuard>,
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
            weight_guard: Arc::new(WeightGuard::new(DEFAULT_WEIGHT_LIMIT_1M)),
        })
    }

    fn observe_weight_header(&self, resp: &reqwest::Response) {
        if let Some(used) = resp
            .headers()
            .get("x-mbx-used-weight-1m")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
        {
            self.weight_guard.observe(used);
            let limit = self.weight_guard.limit_1m.load(Ordering::SeqCst);
            if limit > 0 && used as f64 / limit as f64 >= WEIGHT_BACKOFF_THRESHOLD {
                debug!(
                    used_weight_1m = used,
                    limit_1m = limit,
                    "Binance weight budget approaching limit — adaptive backoff engaged"
                );
            }
        }
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

/// Cap on consecutive 429 retries before bailing — prevents unbounded recursion
/// if the venue keeps returning Retry-After (e.g. IP banned for the day).
const MAX_429_RETRIES: u32 = 3;

#[async_trait]
impl ExchangeRestClient for BinanceRestClient {
    async fn send(&self, request: RestRequest) -> anyhow::Result<RestResponse> {
        for attempt in 0..=MAX_429_RETRIES {
            let delay = self.weight_guard.adaptive_delay();
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
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
            self.observe_weight_header(&resp);

            if resp.status() == 429 && attempt < MAX_429_RETRIES {
                let retry_after = resp
                    .headers()
                    .get("Retry-After")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(5);
                warn!(
                    retry_after_secs = retry_after,
                    attempt = attempt + 1,
                    "rate limited by exchange, retrying"
                );
                tokio::time::sleep(Duration::from_secs(retry_after)).await;
                continue;
            }

            return Ok(RestResponse {
                status: resp.status().as_u16(),
                body: resp.text().await?,
            });
        }
        anyhow::bail!("exhausted {} retries on 429", MAX_429_RETRIES)
    }

    async fn send_public(&self, request: RestRequest) -> anyhow::Result<RestResponse> {
        for attempt in 0..=MAX_429_RETRIES {
            let delay = self.weight_guard.adaptive_delay();
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
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
            self.observe_weight_header(&resp);

            if resp.status() == 429 && attempt < MAX_429_RETRIES {
                let retry_after = resp
                    .headers()
                    .get("Retry-After")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(5);
                warn!(
                    retry_after_secs = retry_after,
                    attempt = attempt + 1,
                    "rate limited by exchange (public), retrying"
                );
                tokio::time::sleep(Duration::from_secs(retry_after)).await;
                continue;
            }

            return Ok(RestResponse {
                status: resp.status().as_u16(),
                body: resp.text().await?,
            });
        }
        anyhow::bail!("exhausted {} retries on 429 (public)", MAX_429_RETRIES)
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

    #[test]
    fn weight_guard_zero_delay_below_threshold() {
        let guard = WeightGuard::new(DEFAULT_WEIGHT_LIMIT_1M);
        guard.observe(0);
        assert_eq!(guard.adaptive_delay(), Duration::ZERO);

        // 70% of 1200 = 840 < 80% threshold
        guard.observe(840);
        assert_eq!(guard.adaptive_delay(), Duration::ZERO);
    }

    #[test]
    fn weight_guard_nonzero_delay_at_threshold() {
        let guard = WeightGuard::new(DEFAULT_WEIGHT_LIMIT_1M);
        // exactly 80% → base multiplier (1x)
        guard.observe((DEFAULT_WEIGHT_LIMIT_1M as f64 * WEIGHT_BACKOFF_THRESHOLD) as u64);
        let delay = guard.adaptive_delay();
        assert!(!delay.is_zero(), "delay should be non-zero at threshold");
        assert!(
            delay.as_millis() >= WEIGHT_BACKOFF_BASE_MS as u128,
            "at threshold should be at least base backoff"
        );
    }

    #[test]
    fn weight_guard_scales_with_usage() {
        let guard = WeightGuard::new(DEFAULT_WEIGHT_LIMIT_1M);
        // at threshold
        guard.observe(960);
        let low = guard.adaptive_delay();
        // near 100%
        guard.observe(1200);
        let high = guard.adaptive_delay();
        assert!(
            high > low,
            "higher usage must produce longer delay: low={low:?}, high={high:?}"
        );
    }

    #[test]
    fn weight_guard_caps_above_100_percent() {
        let guard = WeightGuard::new(DEFAULT_WEIGHT_LIMIT_1M);
        guard.observe(DEFAULT_WEIGHT_LIMIT_1M * 5);
        let delay = guard.adaptive_delay();
        // capped at 10x base → ~2000ms
        assert!(
            delay.as_millis() <= (WEIGHT_BACKOFF_BASE_MS as u128) * 10,
            "delay {:?} exceeded 10x cap",
            delay
        );
    }
}
