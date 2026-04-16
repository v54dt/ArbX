# Observability stack (Prometheus + Grafana)

Docker-compose stack to scrape the arbx-core engine's Prometheus exporter and
render it in a pre-provisioned Grafana dashboard.

## Topology

- **Engine** runs on the host, exposes Prometheus on `:9090` (see
  `rust-core/src/metrics.rs::setup_metrics_server`).
- **Prometheus** container scrapes `host.docker.internal:9090` every 5s and is
  exposed on the host at `:9099` (to avoid colliding with the engine's own
  Prometheus listener on `:9090`).
- **Grafana** container at `:3000`, auto-provisioned with the Prometheus
  datasource and the `arbx.json` dashboard.

## Usage

```bash
# From repo root
docker compose -f docker/observability/docker-compose.yml up -d

# Open:
#   Prometheus UI:  http://localhost:9099
#   Grafana UI:     http://localhost:3000  (anonymous viewer; admin/admin for edit)
#
# In Grafana → Dashboards → "ArbX Core"

# Stop:
docker compose -f docker/observability/docker-compose.yml down

# Reset all volumes (wipe metric history + Grafana state):
docker compose -f docker/observability/docker-compose.yml down -v
```

## Dashboard panels

`grafana/dashboards/arbx.json` ships eight panels backed by the engine's
existing metrics:

1. **Opportunity detection rate** — `rate(arbx_opportunities_detected_total[1m])`, split by strategy.
2. **Orders submitted / failed / rejected** — `rate(arbx_orders_*_total[1m])`.
3. **Evaluation latency p50/p99** — `histogram_quantile` over `arbx_eval_latency_us`.
4. **Submit latency p50/p99** — `histogram_quantile` over `arbx_submit_latency_us`.
5. **Slippage (bps) median** — `histogram_quantile(0.50, arbx_slippage_bps)` by venue/strategy.
6. **WS connectivity** — `arbx_ws_connected` (public) + `arbx_ws_private_connected`.
7. **PnL** — `arbx_realized_pnl`, `arbx_unrealized_pnl`.
8. **Circuit-breaker trips** — `arbx_circuit_breaker_trips_total` (stat).

Add more panels freely — anonymous Grafana users are `Viewer`, but logging in
as `admin/admin` gives full edit rights. Dashboard JSON is reloaded from disk
every 30s, so editing `arbx.json` and saving picks up automatically.

## Notes

- On Linux, `host.docker.internal` resolves to the host via the `host-gateway`
  alias in `docker-compose.yml`. No changes needed for the default Docker
  install.
- Prometheus retention is 7d; bump `--storage.tsdb.retention.time` in the
  compose file for longer.
- If the engine binds Prometheus to a non-default port via `engine.metrics_port`,
  update the target in `prometheus.yml` accordingly.
