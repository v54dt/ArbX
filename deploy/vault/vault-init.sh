#!/usr/bin/env bash
set -euo pipefail
export VAULT_ADDR=${VAULT_ADDR:-http://127.0.0.1:8200}

echo "=== Vault Bootstrap ==="

# Idempotent guard (review §3.10): `vault operator init` errors hard on a
# second run, so wrap it with a status check first. If Vault is already
# initialized, skip init/unseal and pick up policies / approles below if
# the operator wants to re-run them.
if vault status -format=json 2>/dev/null | jq -e '.initialized' >/dev/null; then
  echo "Vault is already initialized; skipping operator init."
  if [[ -f /etc/arbx/vault-keys ]]; then
    # Load existing root token so policy/approle steps can run.
    # shellcheck disable=SC1091
    source /etc/arbx/vault-keys
    export VAULT_TOKEN="$ROOT_TOKEN"
    if vault status -format=json | jq -e '.sealed' >/dev/null; then
      vault operator unseal "$UNSEAL_KEY"
    fi
  else
    echo "ERROR: Vault initialized but /etc/arbx/vault-keys missing." >&2
    echo "       Cannot proceed without the existing root token." >&2
    exit 1
  fi
else
  vault operator init -key-shares=1 -key-threshold=1 -format=json > /tmp/vault-init.json
  UNSEAL_KEY=$(jq -r '.unseal_keys_b64[0]' /tmp/vault-init.json)
  ROOT_TOKEN=$(jq -r '.root_token' /tmp/vault-init.json)
  vault operator unseal "$UNSEAL_KEY"
  export VAULT_TOKEN="$ROOT_TOKEN"
fi

# Enable KV v2 (idempotent — `vault secrets enable` errors if already enabled,
# so check first).
if ! vault secrets list -format=json | jq -e '."secret/" | .type == "kv"' >/dev/null; then
  vault secrets enable -path=secret kv-v2
fi

# Policies (always overwrite — they're declarative).
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

# AppRole auth backend (idempotent — only enable when not already present).
if ! vault auth list -format=json | jq -e '."approle/" | .type == "approle"' >/dev/null; then
  vault auth enable approle
fi

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

# Save keys (only on first init — already-initialised case loads the existing
# file). Persistent disk path; review §1.8 recommended tmpfs but that's a
# separate operational change because operators reboot and need to unseal.
if [[ -f /tmp/vault-init.json ]]; then
  mkdir -p /etc/arbx
  cat > /etc/arbx/vault-keys <<EOF
UNSEAL_KEY=$UNSEAL_KEY
ROOT_TOKEN=$ROOT_TOKEN
EOF
  chmod 600 /etc/arbx/vault-keys
  rm /tmp/vault-init.json
fi

echo "=== Vault bootstrap complete ==="
echo "Store unseal key securely. Root token in /etc/arbx/vault-keys"
