FROM rust:1.88-bookworm AS builder
WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY gateway-core /build/gateway-core
COPY gateway-rust /build/gateway-rust

WORKDIR /build/gateway-rust
RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    libssl3 \
    librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/gateway-rust/target/release/gateway-rust /app/gateway-rust

EXPOSE 8081

HEALTHCHECK --interval=10s --timeout=2s --start-period=20s --retries=5 \
    CMD curl -fsS http://localhost:8081/health || exit 1

CMD ["/app/gateway-rust"]
