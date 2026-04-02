#!/bin/bash
set -e

echo "=== Setting up Arbitrageur development environment ==="

# --- Rust setup ---
echo "[1/4] Installing Rust tools..."
rustup component add clippy rustfmt
cargo install cargo-watch 2>/dev/null || true

# --- Python setup ---
echo "[2/4] Setting up Python environment..."
cd /workspaces/Arbitrageur
python -m venv .venv
source .venv/bin/activate

pip install --upgrade pip
pip install -r python-sidecar/requirements.txt 2>/dev/null || echo "requirements.txt not found, skipping"

# --- FlatBuffers codegen ---
echo "[3/4] Generating FlatBuffers stubs..."
if [ -f "schemas/messages.fbs" ]; then
    mkdir -p rust-core/src/generated
    mkdir -p python-sidecar/src/generated
    flatc --rust -o rust-core/src/generated/ schemas/messages.fbs 2>/dev/null || echo "flatc Rust generation skipped"
    flatc --python -o python-sidecar/src/generated/ schemas/messages.fbs 2>/dev/null || echo "flatc Python generation skipped"
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
