"""Vault AppRole client for reading secrets at startup.

Usage:
    client = VaultClient.from_env()  # reads VAULT_ADDR, VAULT_ROLE_ID, VAULT_SECRET_ID
    secrets = client.read_secret("arbx/tw/shioaji")
    api_key = secrets["api_key"]
"""

import json
import logging
import os
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)


class VaultClient:
    def __init__(self, addr: str, token: str):
        self.addr = addr.rstrip("/")
        self.token = token

    @classmethod
    def from_env(cls) -> "VaultClient":
        addr = os.environ.get("VAULT_ADDR", "http://127.0.0.1:8200")
        role_id = os.environ.get("VAULT_ROLE_ID", "")
        secret_id = os.environ.get("VAULT_SECRET_ID", "")

        if not role_id or not secret_id:
            raise RuntimeError("VAULT_ROLE_ID and VAULT_SECRET_ID must be set")

        # AppRole login
        login_url = f"{addr}/v1/auth/approle/login"
        payload = json.dumps({"role_id": role_id, "secret_id": secret_id}).encode()
        req = urllib.request.Request(login_url, data=payload, method="POST")
        req.add_header("Content-Type", "application/json")

        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())

        token = data["auth"]["client_token"]
        logger.info("Vault AppRole login succeeded (ttl=%s)", data["auth"]["lease_duration"])
        return cls(addr, token)

    def read_secret(self, path: str) -> dict[str, Any]:
        """Read a KV v2 secret. Path should NOT include 'secret/data/' prefix."""
        url = f"{self.addr}/v1/secret/data/{path}"
        req = urllib.request.Request(url)
        req.add_header("X-Vault-Token", self.token)

        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())

        return data["data"]["data"]
