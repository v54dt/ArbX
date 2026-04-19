use async_trait::async_trait;

use crate::models::fee::FeeSchedule;

#[async_trait]
pub trait FeeProvider: Send + Sync {
    async fn get_fee_schedule(&self) -> anyhow::Result<FeeSchedule>;
}

/// Caching decorator for any `FeeProvider`. Returns the cached schedule when
/// fresh (< `ttl`). On fetch failure, returns the stale cached value if one
/// exists (stale-on-error). Thread-safe via `tokio::sync::RwLock`.
#[allow(dead_code)]
pub struct CachedFeeProvider {
    inner: Box<dyn FeeProvider>,
    cache: tokio::sync::RwLock<Option<(FeeSchedule, std::time::Instant)>>,
    ttl: std::time::Duration,
}

impl CachedFeeProvider {
    #[allow(dead_code)]
    pub fn new(inner: Box<dyn FeeProvider>, ttl: std::time::Duration) -> Self {
        Self {
            inner,
            cache: tokio::sync::RwLock::new(None),
            ttl,
        }
    }
}

#[async_trait]
impl FeeProvider for CachedFeeProvider {
    async fn get_fee_schedule(&self) -> anyhow::Result<FeeSchedule> {
        // Fast path: cache hit within TTL.
        {
            let guard = self.cache.read().await;
            if let Some((ref schedule, fetched_at)) = *guard {
                if fetched_at.elapsed() < self.ttl {
                    return Ok(schedule.clone());
                }
            }
        }

        // Cache miss or stale — try to refresh.
        match self.inner.get_fee_schedule().await {
            Ok(schedule) => {
                let mut guard = self.cache.write().await;
                *guard = Some((schedule.clone(), std::time::Instant::now()));
                Ok(schedule)
            }
            Err(e) => {
                // Stale-on-error: return cached value if available.
                let guard = self.cache.read().await;
                if let Some((ref schedule, _)) = *guard {
                    tracing::warn!(
                        error = %e,
                        "fee fetch failed, returning stale cached schedule"
                    );
                    return Ok(schedule.clone());
                }
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::enums::Venue;
    use rust_decimal_macros::dec;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct CountingProvider {
        call_count: Arc<AtomicU32>,
        fail_after: Option<u32>,
    }

    #[async_trait]
    impl FeeProvider for CountingProvider {
        async fn get_fee_schedule(&self) -> anyhow::Result<FeeSchedule> {
            let n = self.call_count.fetch_add(1, Ordering::SeqCst);
            if let Some(limit) = self.fail_after {
                if n >= limit {
                    anyhow::bail!("simulated failure");
                }
            }
            Ok(FeeSchedule::new(Venue::Binance, dec!(0.0002), dec!(0.0004)))
        }
    }

    #[tokio::test]
    async fn cache_hit_avoids_refetch() {
        let calls = Arc::new(AtomicU32::new(0));
        let provider = CachedFeeProvider::new(
            Box::new(CountingProvider {
                call_count: calls.clone(),
                fail_after: None,
            }),
            std::time::Duration::from_secs(3600),
        );
        let _ = provider.get_fee_schedule().await.unwrap();
        let _ = provider.get_fee_schedule().await.unwrap();
        let _ = provider.get_fee_schedule().await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn stale_on_error_returns_cached() {
        let calls = Arc::new(AtomicU32::new(0));
        let provider = CachedFeeProvider::new(
            Box::new(CountingProvider {
                call_count: calls.clone(),
                fail_after: Some(1),
            }),
            std::time::Duration::from_millis(0), // expire immediately
        );
        // First call succeeds and populates cache.
        let s = provider.get_fee_schedule().await.unwrap();
        assert_eq!(s.maker_rate, dec!(0.0002));
        // Second call: cache expired, inner fails → stale cache returned.
        let s2 = provider.get_fee_schedule().await.unwrap();
        assert_eq!(s2.taker_rate, dec!(0.0004));
    }

    #[tokio::test]
    async fn propagates_error_when_no_cache() {
        let calls = Arc::new(AtomicU32::new(0));
        let provider = CachedFeeProvider::new(
            Box::new(CountingProvider {
                call_count: calls.clone(),
                fail_after: Some(0),
            }),
            std::time::Duration::from_secs(3600),
        );
        assert!(provider.get_fee_schedule().await.is_err());
    }
}
