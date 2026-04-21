#!/usr/bin/env bash
set -euo pipefail

echo "=== ArbX Smoke Test ==="

echo "1. Running cargo test (integration + unit)..."
cargo test --all 2>&1 | tail -5

echo "2. Running backtest..."
cargo run --release -- \
  --backtest rust-core/tests/fixtures/backtest_quotes.jsonl \
  --config config/default.yaml 2>&1 | tail -10

echo "=== Smoke test complete ==="
