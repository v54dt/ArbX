import logging
import os
import re
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


def _expand_env_vars(text: str) -> str:
    """Replace ${VAR_NAME} patterns with environment variable values."""
    return re.sub(r"\$\{(\w+)\}", lambda m: os.environ.get(m.group(1), ""), text)


def inject_vault_secrets(cfg: dict) -> dict:
    """If VAULT_ROLE_ID is set, read venue secrets from Vault and inject into config."""
    if not os.environ.get("VAULT_ROLE_ID"):
        return cfg

    from src.secrets.vault_client import VaultClient

    client = VaultClient.from_env()

    for venue_cfg in cfg.get("venues", []):
        vault_path = venue_cfg.get("vault_path")
        if not vault_path:
            continue
        try:
            secrets = client.read_secret(vault_path)
            venue_cfg.update(secrets)
            logger.info("Injected Vault secrets for venue %s", venue_cfg.get("venue"))
        except Exception:
            logger.exception("Failed to read Vault secrets for %s", vault_path)

    return cfg


def load_config(path: str) -> dict:
    raw = Path(path).read_text()
    cfg = yaml.safe_load(_expand_env_vars(raw))
    return inject_vault_secrets(cfg)
