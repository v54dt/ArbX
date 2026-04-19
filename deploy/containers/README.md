# ArbX Container Images

Multi-stage production images for ArbX services. Designed for Podman (rootless + `--network=host --ipc=host` for zero-overhead Aeron IPC) but work with Docker too.

## Images

| Image | Base | Purpose |
|-------|------|---------|
| `arbx-core` | debian:bookworm-slim | Rust engine + aeron_pub helper + libaeron.so |
| `sidecar-shioaji` | python:3.14-slim | 永豐 Shioaji sidecar (Python 3.14, shioaji 1.3.3) |
| `sidecar-fubon` | python:3.13-slim | 富邦 Fubon Neo sidecar (Python 3.13, fubon_neo 2.2.8) |

The sidecar images pull `aeron_pub` + `libaeron.so` from `arbx-core` via multi-stage `FROM`, avoiding a full Rust toolchain rebuild for each.

## Build

From repo root:

```bash
deploy/containers/build.sh           # all
deploy/containers/build.sh core      # arbx-core only
deploy/containers/build.sh shioaji   # core + shioaji
deploy/containers/build.sh fubon     # core + fubon (needs .whl)
```

Environment:
- `TAG=v1.2.3` — image tag (default `latest`)
- `CONTAINER_ENGINE=docker` — use docker instead of podman

### Fubon wheel

`fubon_neo` is not on PyPI. Place the `.whl` in `deploy/containers/wheels/` before building:

```bash
mkdir -p deploy/containers/wheels
cp /path/to/fubon_neo-2.2.8-cp313-cp313-manylinux_2_17_x86_64.whl deploy/containers/wheels/
```

`wheels/` is gitignored.

## Run

```bash
# Aeron media driver (unchanged, still uses docker/aeron/docker-compose.yml)
cd docker/aeron && AERON_UID=$(id -u) AERON_GID=$(id -g) docker compose up -d

# arbx-core
podman run -d --name arbx-core \
    --network=host --ipc=host \
    --ulimit memlock=-1:-1 \
    -e AERON_DIR=/dev/shm/aeron \
    -v /dev/shm:/dev/shm \
    -v /etc/arbx:/etc/arbx:ro \
    arbx-core:latest --config /etc/arbx/engine.yaml --aeron-subscribe 1001 --aeron-subscribe 1002

# Shioaji sidecar
podman run -d --name arbx-sidecar-shioaji \
    --network=host --ipc=host \
    --ulimit memlock=-1:-1 \
    --env-file /etc/arbx/shioaji.env \
    -v /dev/shm:/dev/shm \
    -v /etc/arbx/certs:/certs:ro \
    sidecar-shioaji:latest

# Fubon sidecar
podman run -d --name arbx-sidecar-fubon \
    --network=host --ipc=host \
    --ulimit memlock=-1:-1 \
    --env-file /etc/arbx/fubon.env \
    -v /dev/shm:/dev/shm \
    -v /etc/arbx/certs:/certs:ro \
    sidecar-fubon:latest
```

Service lifecycle is managed by systemd units (see `deploy/systemd/` — coming in next PR).

## Why these flags

| Flag | Why |
|------|-----|
| `--network=host` | Zero-overhead network; admin/prometheus ports bind direct to host |
| `--ipc=host` | Aeron IPC uses shared memory in `/dev/shm/aeron`, must share with host |
| `--ulimit memlock=-1:-1` | Aeron `mlock`s term buffers; unlimited to avoid OOM |
| `-v /dev/shm:/dev/shm` | Redundant with `--ipc=host` but explicit |
| `USER arbx` (uid 1001) | Defense in depth; matches `AERON_UID` on host |

## Non-goals

- **Not pushing to ghcr.io yet** — builds local only; registry push comes later
- **No healthcheck in image** — systemd + admin `/healthz` does that
