"""Vault AppRole client for reading secrets.

Tokens auto-refresh: the client tracks the lease's expiration time and
re-authenticates with AppRole when ~80% of the TTL has elapsed. A long-running
sidecar that originally had a 32-day token won't silently start failing reads
with 403 once the token expires (review §1.8).

Usage:
    client = VaultClient.from_env()
    secrets = client.read_secret("arbx/tw/shioaji")
    api_key = secrets["api_key"]
"""

import json
import logging
import os
import time
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

# Re-auth when this fraction of the original lease has elapsed. 0.8 leaves
# enough headroom for clock skew + the AppRole login itself to complete.
REFRESH_AT_FRACTION = 0.8


class VaultClient:
    def __init__(
        self,
        addr: str,
        token: str,
        role_id: str,
        secret_id: str,
        lease_duration: int,
    ):
        self.addr = addr.rstrip("/")
        self.token = token
        self._role_id = role_id
        self._secret_id = secret_id
        # When `time.monotonic()` reaches this value, we re-login before the
        # next read. Stored as a monotonic deadline so wall-clock changes can't
        # confuse it.
        self._refresh_at = time.monotonic() + lease_duration * REFRESH_AT_FRACTION

    @classmethod
    def from_env(cls) -> "VaultClient":
        addr = os.environ.get("VAULT_ADDR", "http://127.0.0.1:8200")
        role_id = os.environ.get("VAULT_ROLE_ID", "")
        secret_id = os.environ.get("VAULT_SECRET_ID", "")

        if not role_id or not secret_id:
            raise RuntimeError("VAULT_ROLE_ID and VAULT_SECRET_ID must be set")

        token, lease_duration = cls._approle_login(addr, role_id, secret_id)
        return cls(addr, token, role_id, secret_id, lease_duration)

    @staticmethod
    def _approle_login(addr: str, role_id: str, secret_id: str) -> tuple[str, int]:
        login_url = f"{addr.rstrip('/')}/v1/auth/approle/login"
        payload = json.dumps({"role_id": role_id, "secret_id": secret_id}).encode()
        req = urllib.request.Request(login_url, data=payload, method="POST")
        req.add_header("Content-Type", "application/json")

        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())

        token = data["auth"]["client_token"]
        lease_duration = int(data["auth"].get("lease_duration", 3600))
        logger.info(
            "Vault AppRole login succeeded (lease_duration=%ds)", lease_duration
        )
        return token, lease_duration

    def _maybe_refresh(self) -> None:
        if time.monotonic() < self._refresh_at:
            return
        token, lease_duration = self._approle_login(
            self.addr, self._role_id, self._secret_id
        )
        self.token = token
        self._refresh_at = time.monotonic() + lease_duration * REFRESH_AT_FRACTION
        logger.info("Vault token refreshed proactively (next refresh in %ds)",
                    int(lease_duration * REFRESH_AT_FRACTION))

    def read_secret(self, path: str) -> dict[str, Any]:
        """Read a KV v2 secret. Path should NOT include 'secret/data/' prefix."""
        self._maybe_refresh()
        url = f"{self.addr}/v1/secret/data/{path}"
        req = urllib.request.Request(url)
        req.add_header("X-Vault-Token", self.token)

        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            # 403 most likely means the token expired between our refresh
            # check and now (clock skew, very long polling gap). Re-auth
            # eagerly and retry once.
            if e.code in (401, 403):
                logger.warning(
                    "Vault read got %s for %s; forcing token refresh and retrying",
                    e.code, path,
                )
                token, lease_duration = self._approle_login(
                    self.addr, self._role_id, self._secret_id
                )
                self.token = token
                self._refresh_at = (
                    time.monotonic() + lease_duration * REFRESH_AT_FRACTION
                )
                req = urllib.request.Request(url)
                req.add_header("X-Vault-Token", self.token)
                with urllib.request.urlopen(req, timeout=10) as resp:
                    data = json.loads(resp.read())
            else:
                raise

        return data["data"]["data"]
