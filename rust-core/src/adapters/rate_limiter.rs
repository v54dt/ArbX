use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter as GovernorLimiter};
use std::num::NonZeroU32;
use std::sync::Arc;

pub struct RateLimiter {
    limiter: Arc<GovernorLimiter<NotKeyed, InMemoryState, DefaultClock>>,
}

impl RateLimiter {
    pub fn new(requests_per_second: u32) -> Self {
        let quota = Quota::per_second(NonZeroU32::new(requests_per_second).unwrap());
        Self {
            limiter: Arc::new(GovernorLimiter::direct(quota)),
        }
    }

    pub async fn acquire(&self) {
        self.limiter.until_ready().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rate_limiter_allows_burst() {
        let limiter = RateLimiter::new(100);
        let start = tokio::time::Instant::now();
        for _ in 0..10 {
            limiter.acquire().await;
        }
        assert!(start.elapsed().as_millis() < 500);
    }

    #[tokio::test]
    async fn rate_limiter_throttles() {
        let limiter = RateLimiter::new(1);
        limiter.acquire().await;
        let start = tokio::time::Instant::now();
        limiter.acquire().await;
        assert!(start.elapsed().as_millis() >= 900);
    }
}
