# Multi-stage build for arbx-core (Rust arbitrage engine).
#
# Build:   podman build -f deploy/containers/arbx-core.Containerfile -t arbx-core:latest .
# Run:     podman run --network=host --ipc=host --ulimit memlock=-1:-1 \
#            -e AERON_DIR=/dev/shm/aeron \
#            -v /dev/shm:/dev/shm \
#            -v /etc/arbx:/etc/arbx:ro \
#            arbx-core:latest --config /etc/arbx/engine.yaml

# ──────────────────────────────────────────────────────────────
# Stage 1: builder
# Compile arbx-core + aeron_pub + embedded libaeron.so via rusteron-client.
# Includes full toolchain (rustc, cargo, cmake, gcc, aeron build deps).
# ──────────────────────────────────────────────────────────────
FROM docker.io/library/rust:1.94.1-bookworm AS builder

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential pkg-config libssl-dev uuid-dev libbsd-dev \
        wget ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# CMake ≥ 3.30 for rusteron-client's Aeron cmake build.
RUN wget -qO- https://github.com/Kitware/CMake/releases/download/v4.3.1/cmake-4.3.1-linux-x86_64.tar.gz \
      | tar xzf - -C /usr/local --strip-components=1

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY rust-core ./rust-core

RUN cargo build --release --bin arbx-core --bin aeron_pub

# ──────────────────────────────────────────────────────────────
# Stage 2: runtime
# Minimal Debian + libaeron.so + arbx-core binary. No toolchain.
# ──────────────────────────────────────────────────────────────
FROM docker.io/library/debian:bookworm-slim AS runtime

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        libssl3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy libaeron.so from builder (built by rusteron-client) into system path.
COPY --from=builder /build/target/release/build/rusteron-client-*/out/build/lib/libaeron.so \
     /usr/local/lib/libaeron.so
RUN ldconfig

# Copy binaries.
COPY --from=builder /build/target/release/arbx-core /usr/local/bin/arbx-core
COPY --from=builder /build/target/release/aeron_pub /usr/local/bin/aeron_pub

# Default Aeron dir (can override via AERON_DIR env).
ENV AERON_DIR=/dev/shm/aeron

# Non-root user for defense in depth.
RUN useradd -r -u 1001 -s /bin/false arbx
USER arbx

EXPOSE 9090 9091
ENTRYPOINT ["/usr/local/bin/arbx-core"]
