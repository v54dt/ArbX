# Build: podman build -f deploy/containers/sidecar-fubon.Containerfile \
#          --build-arg AERON_PUB_FROM=arbx-core:latest \
#          --build-arg FUBON_WHEEL=fubon_neo-2.2.8-cp313-cp313-manylinux_2_17_x86_64.whl \
#          -t sidecar-fubon:latest .
# Place the .whl in deploy/containers/wheels/ before building.
# Run:   podman run --network=host --ipc=host --ulimit memlock=-1:-1 \
#          -e AERON_DIR=/dev/shm/aeron \
#          -e FUBON_USER_ID=... -e FUBON_PASSWORD=... \
#          -v /dev/shm:/dev/shm \
#          -v /etc/arbx/certs:/certs:ro \
#          sidecar-fubon:latest /etc/arbx/fubon.yaml

ARG AERON_PUB_FROM=arbx-core:latest
FROM ${AERON_PUB_FROM} AS aeron-bits

# ──────────────────────────────────────────────────────────────
# Runtime: Python 3.13 (fubon_neo 2.2.8 caps at 3.13)
# ──────────────────────────────────────────────────────────────
FROM docker.io/library/python:3.13-slim-bookworm AS runtime

ARG DEBIAN_FRONTEND=noninteractive
ARG FUBON_WHEEL=fubon_neo-2.2.8-cp313-cp313-manylinux_2_17_x86_64.whl
ENV TZ=Asia/Taipei PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        libssl3 tzdata ca-certificates \
    && ln -fs /usr/share/zoneinfo/Asia/Taipei /etc/localtime \
    && dpkg-reconfigure -f noninteractive tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY --from=aeron-bits /usr/local/lib/libaeron.so /usr/local/lib/libaeron.so
COPY --from=aeron-bits /usr/local/bin/aeron_pub   /usr/local/bin/aeron_pub
RUN ldconfig

WORKDIR /app
COPY python-sidecar/pyproject.toml ./
RUN pip install --no-cache-dir -e .

# fubon_neo is a local wheel (not on PyPI). Context must contain
# deploy/containers/wheels/<wheel file>.
COPY deploy/containers/wheels/${FUBON_WHEEL} /tmp/fubon.whl
RUN pip install --no-cache-dir /tmp/fubon.whl && rm /tmp/fubon.whl

COPY python-sidecar/src ./src

ENV AERON_DIR=/dev/shm/aeron \
    PYTHONPATH=/app

RUN useradd -r -u 1001 -s /bin/false arbx
USER arbx

ENTRYPOINT ["python", "-m", "src.main"]
CMD ["/etc/arbx/fubon.yaml"]
