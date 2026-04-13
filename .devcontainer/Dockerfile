FROM mcr.microsoft.com/devcontainers/base:ubuntu-24.04

ARG DEBIAN_FRONTEND=noninteractive

# System dependencies for Aeron, FlatBuffers, and build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    libssl-dev \
    uuid-dev \
    libbsd-dev \
    flatbuffers-compiler \
    curl \
    git \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Rust 1.94.1
RUN curl --proto '=https' --tlsv1.2 -sSf https://static.rust-lang.org/rustup/rustup-init.sh | sh -s -- -y --default-toolchain 1.94.1
ENV PATH="/root/.cargo/bin:${PATH}"

# CMake 4.3.1
RUN wget -qO- https://github.com/Kitware/CMake/releases/download/v4.3.1/cmake-4.3.1-linux-x86_64.tar.gz \
    | tar xzf - -C /usr/local --strip-components=1

# Aeron C media driver + client library
RUN git clone --depth 1 https://github.com/real-logic/aeron.git /tmp/aeron && \
    cd /tmp/aeron && \
    mkdir build && cd build && \
    cmake \
      -DCMAKE_BUILD_TYPE=Release \
      -DBUILD_AERON_DRIVER=ON \
      -DAERON_TESTS=OFF \
      -DAERON_SYSTEM_TESTS=OFF \
      -DAERON_BUILD_SAMPLES=OFF \
      -DAERON_BUILD_DOCUMENTATION=OFF \
      -DC_CLIENT_TIMEOUT=OFF \
      -DBUILD_AERON_ARCHIVE_API=OFF \
      .. && \
    make -j$(nproc) aeron aeron_driver aeronmd && \
    make install && \
    ldconfig && \
    rm -rf /tmp/aeron

# uv (fast Python package manager)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Python 3.13 via uv
RUN uv python install 3.13
