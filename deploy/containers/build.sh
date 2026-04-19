#!/bin/bash
# Build all ArbX container images. Run from repo root.
#
# Usage:
#   deploy/containers/build.sh           # build all
#   deploy/containers/build.sh core      # build arbx-core only
#   deploy/containers/build.sh shioaji   # arbx-core + sidecar-shioaji
#   deploy/containers/build.sh fubon     # arbx-core + sidecar-fubon

set -euo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel)
cd "$REPO_ROOT"

TAG="${TAG:-latest}"
ENGINE="${CONTAINER_ENGINE:-podman}"  # podman or docker

build_core() {
    echo "==> Building arbx-core:$TAG"
    $ENGINE build \
        -f deploy/containers/arbx-core.Containerfile \
        -t "arbx-core:$TAG" \
        .
}

build_shioaji() {
    echo "==> Building sidecar-shioaji:$TAG"
    $ENGINE build \
        -f deploy/containers/sidecar-shioaji.Containerfile \
        --build-arg AERON_PUB_FROM="arbx-core:$TAG" \
        -t "sidecar-shioaji:$TAG" \
        .
}

build_fubon() {
    if ! ls deploy/containers/wheels/fubon_neo-*.whl >/dev/null 2>&1; then
        echo "ERROR: fubon_neo .whl not found in deploy/containers/wheels/"
        echo "Place the wheel there first (not committed to git)."
        exit 1
    fi
    local wheel_name
    wheel_name=$(basename deploy/containers/wheels/fubon_neo-*.whl)
    echo "==> Building sidecar-fubon:$TAG (wheel=$wheel_name)"
    $ENGINE build \
        -f deploy/containers/sidecar-fubon.Containerfile \
        --build-arg AERON_PUB_FROM="arbx-core:$TAG" \
        --build-arg FUBON_WHEEL="$wheel_name" \
        -t "sidecar-fubon:$TAG" \
        .
}

case "${1:-all}" in
    core)    build_core ;;
    shioaji) build_core && build_shioaji ;;
    fubon)   build_core && build_fubon ;;
    all)     build_core && build_shioaji && build_fubon ;;
    *)       echo "Usage: $0 [core|shioaji|fubon|all]"; exit 1 ;;
esac

echo "==> Done"
