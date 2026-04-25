use metrics::{counter, gauge, histogram};
use std::net::SocketAddr;

pub fn try_setup_metrics_server(bind: &str, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = format!("{bind}:{port}").parse()?;
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    builder
        .with_http_listener(addr)
        .install()
        .map_err(|e| e.into())
}

pub fn setup_metrics_server(bind: &str, port: u16) {
    if let Err(e) = try_setup_metrics_server(bind, port) {
        tracing::warn!(error = %e, "metrics server install failed (may already be running)");
    }
}

pub fn record_quote_received() {
    counter!("arbx_quotes_received_total").increment(1);
}
pub fn record_opportunity_detected(strategy: &str) {
    counter!("arbx_opportunities_detected_total", "strategy" => strategy.to_string()).increment(1);
}
pub fn record_order_submitted(strategy: &str) {
    counter!("arbx_orders_submitted_total", "strategy" => strategy.to_string()).increment(1);
}
pub fn record_order_failed(strategy: &str) {
    counter!("arbx_orders_failed_total", "strategy" => strategy.to_string()).increment(1);
}
pub fn record_order_rejected(strategy: &str) {
    counter!("arbx_orders_risk_rejected_total", "strategy" => strategy.to_string()).increment(1);
}
pub fn record_fill_received(strategy: &str) {
    counter!("arbx_fills_received_total", "strategy" => strategy.to_string()).increment(1);
}
pub fn record_circuit_breaker_trip() {
    counter!("arbx_circuit_breaker_trips_total").increment(1);
}

pub fn set_position(instrument: &str, qty: f64) {
    gauge!("arbx_position_quantity", "instrument" => instrument.to_string()).set(qty);
}
pub fn set_realized_pnl(pnl: f64) {
    gauge!("arbx_realized_pnl").set(pnl);
}
#[allow(dead_code)]
pub fn set_unrealized_pnl(pnl: f64) {
    gauge!("arbx_unrealized_pnl").set(pnl);
}

pub fn record_eval_latency_us(strategy: &str, us: f64) {
    histogram!("arbx_strategy_eval_latency_us", "strategy" => strategy.to_string()).record(us);
}
pub fn record_submit_latency_us(strategy: &str, us: f64) {
    histogram!("arbx_order_submit_latency_us", "strategy" => strategy.to_string()).record(us);
}
pub fn record_quote_age_ms(ms: f64) {
    histogram!("arbx_quote_age_ms").record(ms);
}
pub fn record_slippage_bps(venue: &str, strategy: &str, slippage_bps: f64) {
    histogram!(
        "arbx_slippage_bps",
        "venue" => venue.to_string(),
        "strategy" => strategy.to_string()
    )
    .record(slippage_bps);
}

pub fn record_ws_reconnect(venue: &str) {
    counter!("arbx_ws_reconnects_total", "venue" => venue.to_string()).increment(1);
}
pub fn set_ws_connected(venue: &str, connected: bool) {
    gauge!("arbx_ws_connected", "venue" => venue.to_string()).set(if connected {
        1.0
    } else {
        0.0
    });
}
pub fn record_ws_message(venue: &str) {
    counter!("arbx_ws_messages_received_total", "venue" => venue.to_string()).increment(1);
}

#[allow(dead_code)]
pub fn record_ws_private_reconnect(venue: &str) {
    counter!("arbx_ws_private_reconnects_total", "venue" => venue.to_string()).increment(1);
}
pub fn set_ws_private_connected(venue: &str, connected: bool) {
    gauge!("arbx_ws_private_connected", "venue" => venue.to_string()).set(if connected {
        1.0
    } else {
        0.0
    });
}
pub fn record_ws_private_message(venue: &str) {
    counter!("arbx_ws_private_messages_received_total", "venue" => venue.to_string()).increment(1);
}

pub fn record_tca_fill_delay_ms(venue: &str, ms: f64) {
    histogram!(
        "arbx_tca_fill_delay_ms",
        "venue" => venue.to_string()
    )
    .record(ms);
}

pub fn record_quote_dropped_paused() {
    counter!("arbx_quotes_dropped_paused_total").increment(1);
}

pub fn record_opportunity_reverified(strategy: &str, accepted: bool) {
    let result = if accepted { "accept" } else { "reject" };
    counter!(
        "arbx_opportunity_reverified_total",
        "strategy" => strategy.to_string(),
        "result" => result.to_string()
    )
    .increment(1);
}

pub fn set_cert_seconds_until_expiry(name: &str, secs: f64) {
    gauge!("arbx_cert_seconds_until_expiry", "name" => name.to_string()).set(secs);
}

pub fn set_orders_pending(venue: &str, strategy: &str, count: f64) {
    gauge!(
        "arbx_orders_pending_total",
        "venue" => venue.to_string(),
        "strategy" => strategy.to_string()
    )
    .set(count);
}

pub fn record_order_ttl_expired(strategy: &str) {
    counter!("arbx_order_ttl_expired_total", "strategy" => strategy.to_string()).increment(1);
}

pub fn record_send_to_ack_latency_us(venue: &str, us: f64) {
    histogram!(
        "arbx_send_to_ack_latency_us",
        "venue" => venue.to_string()
    )
    .record(us);
}

pub fn record_fees_paid(venue: &str, strategy: &str, maker_taker: &str, amount: f64) {
    counter!(
        "arbx_fees_paid_total",
        "venue" => venue.to_string(),
        "strategy" => strategy.to_string(),
        "type" => maker_taker.to_string()
    )
    .increment(amount as u64);
}

pub fn record_event_bus_dropped(count: u64) {
    counter!("arbx_event_bus_dropped_total").increment(count);
}

pub fn record_aeron_backpressure() {
    counter!("arbx_aeron_publish_backpressure_total").increment(1);
}

/// Depth (queued items) of a bounded engine channel. Surfaces sustained
/// backpressure as a high gauge value; transient bursts barely show up.
pub fn set_channel_depth(channel: &str, depth: f64) {
    gauge!("arbx_channel_depth", "channel" => channel.to_string()).set(depth);
}
