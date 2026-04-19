import os
import re
from pathlib import Path

import yaml


def _expand_env_vars(text: str) -> str:
    """Replace ${VAR_NAME} patterns with environment variable values."""
    return re.sub(r"\$\{(\w+)\}", lambda m: os.environ.get(m.group(1), ""), text)


def load_config(path: str) -> dict:
    raw = Path(path).read_text()
    return yaml.safe_load(_expand_env_vars(raw))
