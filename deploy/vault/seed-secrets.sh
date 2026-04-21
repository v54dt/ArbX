#!/usr/bin/env bash
# Run after vault-init.sh to seed secrets.
# Fill in actual values before running.
set -euo pipefail
export VAULT_ADDR=http://127.0.0.1:8200
export VAULT_TOKEN=$(grep ROOT_TOKEN /etc/arbx/vault-keys | cut -d= -f2)

vault kv put secret/arbx/crypto/binance \
  api_key="FILL_ME" \
  api_secret="FILL_ME"

vault kv put secret/arbx/crypto/bybit \
  api_key="FILL_ME" \
  api_secret="FILL_ME"

vault kv put secret/arbx/crypto/okx \
  api_key="FILL_ME" \
  api_secret="FILL_ME" \
  passphrase="FILL_ME"

vault kv put secret/arbx/tw/shioaji \
  api_key="FILL_ME" \
  secret_key="FILL_ME" \
  cert_password="FILL_ME"

vault kv put secret/arbx/tw/fubon \
  user_id="FILL_ME" \
  password="FILL_ME" \
  pfx_password="FILL_ME"

vault kv put secret/arbx/engine/admin \
  admin_token="$(uuidgen)"

echo "Secrets seeded. Remember to upload PFX certs separately."
