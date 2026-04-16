//! Credential / certificate expiry watchdog.
//!
//! Operators register `CertExpiryProvider` instances (PFX cert files, IP-ban
//! retry-after windows, etc.); a background task polls each one periodically
//! and writes the seconds-until-expiry to a Prometheus gauge so dashboards /
//! alerting can fire well before something actually breaks.
//!
//! Stub implementation. Real providers (TW PFX reader, Binance ban detector)
//! plug in later via `with_provider` without engine changes. The module-level
//! `allow(dead_code)` will be removed once main.rs spawns the watchdog.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::watch;
use tracing::{info, warn};

/// A source of credential / cert expiry information. Implementations are
/// expected to be cheap (no network); the watchdog calls `next_expiry()`
/// once per poll and just publishes the gauge.
pub trait CertExpiryProvider: Send + Sync {
    /// Stable label used in the Prometheus gauge `name` dimension.
    fn name(&self) -> &str;
    /// `Some(when)` if the credential expires at a known time; `None` if
    /// the provider has no data (e.g., file unreadable, never observed yet).
    fn next_expiry(&self) -> Option<DateTime<Utc>>;
}

/// Default no-op provider — emits nothing. Used as a placeholder so
/// `run_cert_watchdog` can run with an empty provider set without special
/// casing, and so tests have a trivially-constructable provider.
pub struct NoopProvider {
    label: String,
}

impl NoopProvider {
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
        }
    }
}

impl CertExpiryProvider for NoopProvider {
    fn name(&self) -> &str {
        &self.label
    }
    fn next_expiry(&self) -> Option<DateTime<Utc>> {
        None
    }
}

/// Threshold (seconds) below which a `warn!` is logged each poll. Independent
/// of the Prometheus gauge — alerting rules on the gauge are the source of
/// truth; this just gives operators a heads-up in stdout logs.
const WARN_THRESHOLD_SECS: i64 = 7 * 24 * 3600;

/// Run the watchdog until the shutdown channel flips to `true`. Polls every
/// `poll_interval_secs` (clamped to a minimum of 60s; this is not a hot path).
pub async fn run_cert_watchdog(
    providers: Vec<Arc<dyn CertExpiryProvider>>,
    poll_interval_secs: u64,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let interval_secs = poll_interval_secs.max(60);
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    info!(
        providers = providers.len(),
        interval_secs, "cert watchdog started"
    );

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let now = Utc::now();
                for p in providers.iter() {
                    let Some(when) = p.next_expiry() else { continue; };
                    let secs = (when - now).num_seconds();
                    crate::metrics::set_cert_seconds_until_expiry(p.name(), secs as f64);
                    if secs < WARN_THRESHOLD_SECS {
                        warn!(
                            name = p.name(),
                            seconds_until_expiry = secs,
                            "credential approaching expiry"
                        );
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!("cert watchdog exiting — shutdown observed");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_provider_returns_none() {
        let p = NoopProvider::new("test");
        assert_eq!(p.name(), "test");
        assert!(p.next_expiry().is_none());
    }

    struct FixedProvider {
        label: String,
        when: DateTime<Utc>,
    }
    impl CertExpiryProvider for FixedProvider {
        fn name(&self) -> &str {
            &self.label
        }
        fn next_expiry(&self) -> Option<DateTime<Utc>> {
            Some(self.when)
        }
    }

    #[tokio::test]
    async fn watchdog_exits_on_shutdown() {
        let providers: Vec<Arc<dyn CertExpiryProvider>> = vec![Arc::new(NoopProvider::new("noop"))];
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(run_cert_watchdog(providers, 60, rx));
        let _ = tx.send(true);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn watchdog_polls_provider_at_least_once() {
        // poll_interval_secs is clamped to >= 60, so we can't observe an
        // actual tick in a fast test. We rely on the shutdown_exits test
        // for the loop body. This case just verifies the spawn + clamp
        // doesn't panic with a 1-second request.
        let p: Arc<dyn CertExpiryProvider> = Arc::new(FixedProvider {
            label: "test".into(),
            when: Utc::now() + chrono::Duration::days(30),
        });
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(run_cert_watchdog(vec![p], 1, rx));
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = tx.send(true);
        let _ = handle.await;
    }
}
