# Aeron MediaDriver (dev)

Runs the Aeron media driver in a container so local Rust / Python
clients on the host can use `aeron:ipc` without installing Java or
maintaining the driver process by hand.

## What it gives you

A long-running `arbx-aeron-driver` container that:

- Listens on `aeron.dir=/dev/shm/aeron` (bind-mounted from host).
- Uses `DEDICATED` threading and an 8 MB term buffer — defaults chosen
  for low-latency dev, not production tuning.
- No TCP/UDP ports: pure shared-memory IPC.

## Quick start

```bash
# From repo root
docker compose -f docker/aeron/docker-compose.yml up -d --build
docker compose -f docker/aeron/docker-compose.yml logs -f   # watch startup

# Local Rust client finds the driver automatically via AERON_DIR.
export AERON_DIR=/dev/shm/aeron
cargo run -p arbx-core -- --aeron-publish
```

## Stop / clean up

```bash
docker compose -f docker/aeron/docker-compose.yml down
rm -rf /dev/shm/aeron   # remove stale shared-memory files
```

## Gotchas

- **Permissions**: the first time the driver starts, it creates files
  under `/dev/shm/aeron` owned by the container user. On some hosts you
  may need to `chmod -R go+rw /dev/shm/aeron` so the host cargo user can
  read/write. A fresh `rm -rf /dev/shm/aeron` before `up` usually
  resolves it.
- **Aeron version**: pinned in the Dockerfile (`AERON_VERSION`). Bump in
  lockstep with the `rusteron-client` crate so wire formats stay
  compatible.
- **Only local dev**: this compose is for a single-host IPC setup. For
  multi-host UDP, use `aeron:udp?endpoint=...` channels and a different
  driver config (not provided here).
