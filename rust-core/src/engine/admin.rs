//! Admin HTTP endpoint for ops levers — health probes, status snapshots,
//! pause / resume / kill. Default port 9091 (one above metrics' 9090).
//!
//! Routes:
//!   GET  /healthz   → 200 always
//!   GET  /status    → JSON snapshot of EngineHandle state
//!   POST /pause     → set paused=true
//!   POST /resume    → set paused=false
//!   POST /kill      → trigger shutdown_tx send(true)

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use serde::Serialize;
use tokio::sync::Mutex;
use tracing::{info, warn};

/// Shared state the engine updates and the admin server reads. Cloning the
/// outer `Arc` is cheap; all interior mutability is atomic / locked.
#[derive(Clone)]
pub struct EngineHandle {
    pub paused: Arc<AtomicBool>,
    pub trade_log_count: Arc<AtomicUsize>,
    pub position_count: Arc<AtomicUsize>,
    pub cb_state: Arc<Mutex<CircuitBreakerState>>,
    pub shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl EngineHandle {
    pub fn new(shutdown_tx: tokio::sync::watch::Sender<bool>) -> Self {
        Self {
            paused: Arc::new(AtomicBool::new(false)),
            trade_log_count: Arc::new(AtomicUsize::new(0)),
            position_count: Arc::new(AtomicUsize::new(0)),
            cb_state: Arc::new(Mutex::new(CircuitBreakerState {
                tripped: false,
                reason: None,
            })),
            shutdown_tx,
        }
    }
}

#[derive(Clone, Default, Debug)]
pub struct CircuitBreakerState {
    pub tripped: bool,
    pub reason: Option<String>,
}

#[derive(Serialize)]
struct StatusBody {
    paused: bool,
    trade_log_count: usize,
    position_count: usize,
    cb_tripped: bool,
    cb_reason: Option<String>,
}

async fn healthz() -> &'static str {
    "ok"
}

async fn status(State(handle): State<EngineHandle>) -> impl IntoResponse {
    let cb = handle.cb_state.lock().await.clone();
    Json(StatusBody {
        paused: handle.paused.load(Ordering::Relaxed),
        trade_log_count: handle.trade_log_count.load(Ordering::Relaxed),
        position_count: handle.position_count.load(Ordering::Relaxed),
        cb_tripped: cb.tripped,
        cb_reason: cb.reason,
    })
}

async fn pause(State(handle): State<EngineHandle>) -> impl IntoResponse {
    handle.paused.store(true, Ordering::Relaxed);
    info!("admin: engine paused");
    (StatusCode::OK, "paused")
}

async fn resume(State(handle): State<EngineHandle>) -> impl IntoResponse {
    handle.paused.store(false, Ordering::Relaxed);
    info!("admin: engine resumed");
    (StatusCode::OK, "resumed")
}

async fn kill(State(handle): State<EngineHandle>) -> impl IntoResponse {
    info!("admin: kill received, sending shutdown");
    if handle.shutdown_tx.send(true).is_err() {
        warn!("admin: shutdown channel already closed");
        return (StatusCode::SERVICE_UNAVAILABLE, "no shutdown receivers");
    }
    (StatusCode::OK, "shutting down")
}

/// Build the Router. Exposed for tests; main.rs uses `serve` instead.
pub fn router(handle: EngineHandle) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/status", get(status))
        .route("/pause", post(pause))
        .route("/resume", post(resume))
        .route("/kill", post(kill))
        .with_state(handle)
}

/// Serve the admin endpoint on `port`, bound to all interfaces. Returns when
/// the listener errors out — typically not before shutdown.
pub async fn serve(handle: EngineHandle, port: u16) -> anyhow::Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "admin HTTP listening");
    axum::serve(listener, router(handle)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handle() -> EngineHandle {
        let (tx, _rx) = tokio::sync::watch::channel(false);
        EngineHandle::new(tx)
    }

    #[tokio::test]
    async fn pause_then_resume_toggles_atomic() {
        let h = handle();
        assert!(!h.paused.load(Ordering::Relaxed));
        let _ = pause(State(h.clone())).await.into_response();
        assert!(h.paused.load(Ordering::Relaxed));
        let _ = resume(State(h.clone())).await.into_response();
        assert!(!h.paused.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn kill_sends_shutdown_true() {
        let (tx, mut rx) = tokio::sync::watch::channel(false);
        let h = EngineHandle::new(tx);
        let _ = kill(State(h)).await.into_response();
        // Receiver should observe the change.
        rx.changed().await.unwrap();
        assert!(*rx.borrow());
    }

    #[tokio::test]
    async fn status_reflects_atomic_writes() {
        let h = handle();
        h.trade_log_count.store(7, Ordering::Relaxed);
        h.position_count.store(3, Ordering::Relaxed);
        {
            let mut cb = h.cb_state.lock().await;
            cb.tripped = true;
            cb.reason = Some("test".into());
        }
        let resp = status(State(h)).await.into_response();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
