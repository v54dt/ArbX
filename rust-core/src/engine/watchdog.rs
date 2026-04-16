//! Dead-man's switch for the engine main loop.
//!
//! The engine stamps [`Heartbeat::beat`] every iteration of its `select!`
//! loop; a separate watchdog task polls the stamp and, if it hasn't advanced
//! within `stall_threshold_ms`, pushes `true` into the shared shutdown
//! channel so the rest of the process can drain cleanly.
//!
//! Scope: this catches *stuck* loops (blocked future, deadlocked mutex,
//! runaway compute) — it does not protect against a complete process hang
//! (tokio runtime wedged), which needs an external supervisor (systemd /
//! docker restart policy / k8s liveness probe).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::watch;
use tracing::{error, info};

/// Shared heartbeat stamp. Updated by the engine via [`Heartbeat::beat`];
/// read by the watchdog via [`Heartbeat::last_ms`].
#[derive(Debug)]
pub struct Heartbeat {
    last_tick_ms: AtomicU64,
}

impl Heartbeat {
    pub fn new() -> Self {
        Self {
            last_tick_ms: AtomicU64::new(now_ms()),
        }
    }

    /// Stamp current monotonic-ish time. Called from the engine's main loop.
    pub fn beat(&self) {
        self.last_tick_ms.store(now_ms(), Ordering::Relaxed);
    }

    /// Last-stamped unix millis. `0` is never returned for a fresh Heartbeat
    /// because `new()` stamps on construction.
    pub fn last_ms(&self) -> u64 {
        self.last_tick_ms.load(Ordering::Relaxed)
    }
}

impl Default for Heartbeat {
    fn default() -> Self {
        Self::new()
    }
}

fn now_ms() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}

/// Run the watchdog until the shutdown channel flips to `true`.
///
/// Polls the heartbeat every `stall_threshold_ms / 2` (clamped to a minimum
/// of 200 ms). If the gap since the last beat exceeds `stall_threshold_ms`,
/// logs an error and sends `true` through `shutdown_tx`. Exits promptly
/// when shutdown is signalled from elsewhere.
pub async fn run_watchdog(
    heartbeat: Arc<Heartbeat>,
    stall_threshold_ms: u64,
    shutdown_tx: watch::Sender<bool>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let poll_ms = (stall_threshold_ms / 2).max(200);
    let mut ticker = tokio::time::interval(Duration::from_millis(poll_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    info!(
        stall_threshold_ms,
        poll_ms, "engine watchdog started (dead-man's switch)"
    );

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let last = heartbeat.last_ms();
                let now = now_ms();
                if now.saturating_sub(last) > stall_threshold_ms {
                    error!(
                        stall_ms = now.saturating_sub(last),
                        stall_threshold_ms,
                        "engine heartbeat stalled — firing dead-man's shutdown"
                    );
                    let _ = shutdown_tx.send(true);
                    break;
                }
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!("watchdog exiting — shutdown observed");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    // Tests use real time deliberately: the watchdog reads wall-clock via
    // chrono::Utc::now(), which tokio's start_paused doesn't override. Keeping
    // thresholds in the low-hundreds-of-ms range keeps the suite fast.

    #[tokio::test]
    async fn fires_shutdown_when_heartbeat_stalls() {
        let hb = Arc::new(Heartbeat::new());
        let (tx, rx) = watch::channel(false);
        let watcher_rx = tx.subscribe();

        // stall_threshold 200ms, poll clamped to 200ms → first fire check at ~200ms.
        let handle = tokio::spawn(run_watchdog(hb.clone(), 200, tx, watcher_rx));

        // wait long enough for at least two poll cycles to catch the stall
        tokio::time::sleep(Duration::from_millis(700)).await;

        handle.await.unwrap();
        assert!(*rx.borrow(), "shutdown signal must be true after stall");
    }

    #[tokio::test]
    async fn does_not_fire_while_heartbeat_is_fresh() {
        let hb = Arc::new(Heartbeat::new());
        let (tx, _rx) = watch::channel(false);
        let watcher_rx = tx.subscribe();

        // beat every 50ms for 400ms — well under the 300ms stall threshold
        let stop = Arc::new(AtomicBool::new(false));
        let hb_c = hb.clone();
        let stop_c = stop.clone();
        let beat_handle = tokio::spawn(async move {
            while !stop_c.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(50)).await;
                hb_c.beat();
            }
        });

        let wd_rx = tx.subscribe();
        let tx_c = tx.clone();
        let watch_handle = tokio::spawn(run_watchdog(hb, 300, tx_c, wd_rx));

        tokio::time::sleep(Duration::from_millis(400)).await;
        // Watchdog should NOT have fired on its own yet.
        assert!(
            !*watcher_rx.borrow(),
            "watchdog should not fire while heartbeat is fresh"
        );

        // Clean shutdown
        stop.store(true, Ordering::Relaxed);
        let _ = tx.send(true);
        let _ = beat_handle.await;
        let _ = watch_handle.await;
    }

    #[tokio::test]
    async fn heartbeat_beat_advances_timestamp() {
        let hb = Heartbeat::new();
        let t0 = hb.last_ms();
        tokio::time::sleep(Duration::from_millis(10)).await;
        hb.beat();
        let t1 = hb.last_ms();
        assert!(t1 > t0, "beat() must advance last_ms (t0={t0}, t1={t1})");
    }
}
