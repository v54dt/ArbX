#!/usr/bin/env bash
set -euo pipefail
export VAULT_ADDR=http://127.0.0.1:8200

echo "=== Vault Bootstrap ==="

# Init (single key share for solo operator)
vault operator init -key-shares=1 -key-threshold=1 -format=json > /tmp/vault-init.json
UNSEAL_KEY=$(jq -r '.unseal_keys_b64[0]' /tmp/vault-init.json)
ROOT_TOKEN=$(jq -r '.root_token' /tmp/vault-init.json)
vault operator unseal "$UNSEAL_KEY"
export VAULT_TOKEN="$ROOT_TOKEN"

# Enable KV v2
vault secrets enable -path=secret kv-v2

# Policies
vault policy write arbx-core - <<'POLICY'
path "secret/data/arbx/crypto/*" { capabilities = ["read"] }
path "secret/data/arbx/engine/*" { capabilities = ["read"] }
POLICY

vault policy write sidecar-shioaji - <<'POLICY'
path "secret/data/arbx/tw/shioaji" { capabilities = ["read"] }
POLICY

vault policy write sidecar-fubon - <<'POLICY'
path "secret/data/arbx/tw/fubon" { capabilities = ["read"] }
POLICY

# AppRoles
vault auth enable approle

for role in arbx-core sidecar-shioaji sidecar-fubon; do
  vault write auth/approle/role/$role \
    token_policies="$role" \
    token_ttl=24h \
    token_max_ttl=48h \
    secret_id_ttl=0
  ROLE_ID=$(vault read -field=role_id auth/approle/role/$role/role-id)
  SECRET_ID=$(vault write -f -field=secret_id auth/approle/role/$role/secret-id)
  echo "$role: role_id=$ROLE_ID secret_id=$SECRET_ID"
done

# Save keys
mkdir -p /etc/arbx
cat > /etc/arbx/vault-keys <<EOF
UNSEAL_KEY=$UNSEAL_KEY
ROOT_TOKEN=$ROOT_TOKEN
EOF
chmod 600 /etc/arbx/vault-keys
rm /tmp/vault-init.json

echo "=== Vault bootstrap complete ==="
echo "Store unseal key securely. Root token in /etc/arbx/vault-keys"
