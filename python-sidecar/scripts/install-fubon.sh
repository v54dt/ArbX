#!/usr/bin/env bash
# fubon_neo is not on PyPI. Download the .whl from Fubon's developer portal
# and install it manually:
#
#   pip install fubon_neo-X.Y.Z-cp3xx-cp3xx-linux_x86_64.whl
#
# Place the .whl file in this directory and update the filename below.

set -euo pipefail

WHL_FILE="${1:?Usage: $0 <path-to-fubon_neo.whl>}"

pip install "$WHL_FILE"
