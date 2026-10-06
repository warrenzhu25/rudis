# Multi-stage Dockerfile for high-performance io_uring Redis replacement (rudis)
# Stage 1: Build
# Keep in sync with rust-toolchain.toml.
FROM rust:1.99-bookworm AS builder

WORKDIR /usr/src/rudis

# Pre-install native build dependencies (libclang, pkg-config, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    libssl-dev \
    llvm-dev \
    libclang-dev \
    clang \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency manifests for cache optimization
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src

# Build the shipped binary with the fat-LTO `dist` profile (see Cargo.toml)
RUN cargo build --profile dist --bin rudis

# Stage 2: Runtime
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create dedicated rudis system user and group
RUN groupadd -r rudis && useradd -r -g rudis -d /var/lib/rudis -m -s /sbin/nologin rudis

# Copy binary from builder
COPY --from=builder /usr/src/rudis/target/dist/rudis /usr/local/bin/rudis

# Create data directory
RUN mkdir -p /var/lib/rudis && chown -R rudis:rudis /var/lib/rudis
VOLUME ["/var/lib/rudis"]

WORKDIR /var/lib/rudis
USER rudis

# Expose default Redis and TLS ports
EXPOSE 6379 6380

ENTRYPOINT ["/usr/local/bin/rudis"]
# Like the official redis image: protected mode would refuse every client
# arriving over the container network, so it is disabled here. Set a password
# (requirepass) before publishing the port beyond a trusted network.
CMD ["--port", "6379", "--aof", "true", "--aof-dir", "/var/lib/rudis", "--protected-mode", "no"]
