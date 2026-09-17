# Multi-stage Dockerfile for high-performance io_uring Redis replacement (rudis)
# Stage 1: Build
FROM rust:1.85-bookworm AS builder

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

# Build release binary
RUN cargo build --release --bin rudis

# Stage 2: Runtime
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create dedicated rudis system user and group
RUN groupadd -r rudis && useradd -r -g rudis -d /var/lib/rudis -m -s /sbin/nologin rudis

# Copy binary from builder
COPY --from=builder /usr/src/rudis/target/release/rudis /usr/local/bin/rudis

# Create data directory
RUN mkdir -p /var/lib/rudis && chown -R rudis:rudis /var/lib/rudis
VOLUME ["/var/lib/rudis"]

WORKDIR /var/lib/rudis
USER rudis

# Expose default Redis and TLS ports
EXPOSE 6379 6380

ENTRYPOINT ["/usr/local/bin/rudis"]
CMD ["--port", "6379", "--aof", "--aof-dir", "/var/lib/rudis"]
