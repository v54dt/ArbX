use metrics::{counter, gauge, histogram};

pub fn setup_metrics_server(port: u16) {
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    builder
        .with_http_listener(([0, 0, 0, 0], port))
        .install()
        .expect("failed to install Prometheus exporter");
}

pub fn record_quote_received() {
    counter!("arbx_quotes_received_total").increment(1);
}
pub fn record_opportunity_detected() {
    counter!("arbx_opportunities_detected_total").increment(1);
}
pub fn record_order_submitted() {
    counter!("arbx_orders_submitted_total").increment(1);
}
pub fn record_order_failed() {
    counter!("arbx_orders_failed_total").increment(1);
}
pub fn record_order_rejected() {
    counter!("arbx_orders_risk_rejected_total").increment(1);
}
pub fn record_fill_received() {
    counter!("arbx_fills_received_total").increment(1);
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

pub fn record_eval_latency_us(us: f64) {
    histogram!("arbx_strategy_eval_latency_us").record(us);
}
pub fn record_submit_latency_us(us: f64) {
    histogram!("arbx_order_submit_latency_us").record(us);
}
pub fn record_quote_age_ms(ms: f64) {
    histogram!("arbx_quote_age_ms").record(ms);
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
