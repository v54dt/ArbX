#!/bin/bash
set -e

echo "=== Setting up ArbX development environment ==="

# --- Rust setup ---
echo "[1/4] Installing Rust tools..."
rustup component add clippy rustfmt
cargo install cargo-watch 2>/dev/null || true

# --- Python setup ---
echo "[2/4] Setting up Python environment with uv..."
cd /workspaces/ArbX
if [ -f "python-sidecar/pyproject.toml" ]; then
    cd python-sidecar
    uv sync
    cd ..
fi

# --- FlatBuffers codegen ---
echo "[3/4] Generating FlatBuffers stubs..."
if [ -f "schemas/messages.fbs" ]; then
    mkdir -p rust-core/src/generated
    mkdir -p python-sidecar/src/generated
    flatc --rust -o rust-core/src/generated/ schemas/messages.fbs
    flatc --python -o python-sidecar/src/generated/ schemas/messages.fbs
fi

# --- Rust build check ---
echo "[4/4] Checking Rust build..."
cargo check 2>/dev/null || echo "Cargo check skipped (dependencies may need network)"

echo "=== Setup complete ==="
echo ""
echo "Quick start:"
echo "  Rust engine:    cargo run -- config/crypto.yaml"
echo "  Python sidecar: python -m src.main config/crypto.yaml"
echo "  Aeron driver:   aeronmd"
