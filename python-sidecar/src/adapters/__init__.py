from src.adapters.base import BaseAdapter

# Optional heavyweight adapters (shioaji / fubon_neo) are imported lazily
# by main.build_adapter so users don't need the TW broker SDKs installed
# just to run the crypto / mock paths.

__all__ = ["BaseAdapter"]
