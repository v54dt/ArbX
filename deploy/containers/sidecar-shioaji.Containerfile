# Build: podman build -f deploy/containers/sidecar-shioaji.Containerfile \
#          --build-arg AERON_PUB_FROM=arbx-core:latest \
#          -t sidecar-shioaji:latest .
# Run:   podman run --network=host --ipc=host --ulimit memlock=-1:-1 \
#          -e AERON_DIR=/dev/shm/aeron \
#          -e SHIOAJI_API_KEY=... -e SHIOAJI_SECRET_KEY=... \
#          -v /dev/shm:/dev/shm \
#          -v /etc/arbx/certs:/certs:ro \
#          sidecar-shioaji:latest /etc/arbx/shioaji.yaml

# ──────────────────────────────────────────────────────────────
# Stage 1: pull aeron_pub binary + libaeron.so from arbx-core image
# (avoids rebuilding the Rust toolchain in this sidecar image).
# ──────────────────────────────────────────────────────────────
ARG AERON_PUB_FROM=arbx-core:latest
FROM ${AERON_PUB_FROM} AS aeron-bits

# ──────────────────────────────────────────────────────────────
# Stage 2: Python 3.14 runtime + shioaji
# Shioaji 1.3.3 supports 3.7-3.14; we use 3.14.
# ──────────────────────────────────────────────────────────────
FROM docker.io/library/python:3.14-slim-bookworm AS runtime

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Taipei PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

# Shioaji SDK has C extensions → needs libssl, libstdc++.
# TZ=Asia/Taipei required else place_order timeouts (issue #149).
RUN apt-get update && apt-get install -y --no-install-recommends \
        libssl3 tzdata ca-certificates \
    && ln -fs /usr/share/zoneinfo/Asia/Taipei /etc/localtime \
    && dpkg-reconfigure -f noninteractive tzdata \
    && rm -rf /var/lib/apt/lists/*

# Copy aeron bits from arbx-core image.
COPY --from=aeron-bits /usr/local/lib/libaeron.so /usr/local/lib/libaeron.so
COPY --from=aeron-bits /usr/local/bin/aeron_pub   /usr/local/bin/aeron_pub
RUN ldconfig

# Python deps.
WORKDIR /app
COPY python-sidecar/pyproject.toml ./
RUN pip install --no-cache-dir -e . shioaji==1.3.3

COPY python-sidecar/src ./src

ENV AERON_DIR=/dev/shm/aeron \
    SHIOAJI_SIMULATION=false \
    PYTHONPATH=/app

RUN useradd -r -u 1001 -s /bin/false arbx
USER arbx

ENTRYPOINT ["python", "-m", "src.main"]
CMD ["/etc/arbx/shioaji.yaml"]
