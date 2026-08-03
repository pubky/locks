# ========================
# Build Stage
# ========================
FROM rust:1.89.0-alpine3.20 AS builder

RUN echo "TARGETARCH: $TARGETARCH"

# Build dependencies for Rust crates that need C toolchains or static TLS libs.
RUN apk add --no-cache \
    musl-dev \
    openssl-dev \
    openssl-libs-static \
    pkgconfig \
    build-base \
    cmake \
    perl \
    curl

ENV OPENSSL_STATIC=yes
ENV OPENSSL_LIB_DIR=/usr/lib
ENV OPENSSL_INCLUDE_DIR=/usr/include
ENV SQLX_OFFLINE=true

WORKDIR /usr/src/app

# Copy manifests first for better Docker layer caching.
COPY Cargo.toml Cargo.lock ./
COPY locks-core/Cargo.toml locks-core/Cargo.toml
COPY locks-service/Cargo.toml locks-service/Cargo.toml
COPY locks-server/Cargo.toml locks-server/Cargo.toml
COPY locks-sdk/Cargo.toml locks-sdk/Cargo.toml
COPY locks-sdk/bindings/js/Cargo.toml locks-sdk/bindings/js/Cargo.toml
COPY locks-e2e/Cargo.toml locks-e2e/Cargo.toml

# Copy source after manifests.
COPY . .

# Build only the lock-server binary.
RUN cargo build --release -p locks-server --bin locks-server

RUN strip target/release/locks-server

# ========================
# Runtime Stage
# ========================
FROM alpine:3.20

RUN apk add --no-cache ca-certificates

COPY --from=builder /usr/src/app/target/release/locks-server /usr/local/bin/locks-server
COPY docker/locks-server-compose-entrypoint.sh /usr/local/bin/locks-server-compose-entrypoint.sh
RUN chmod +x /usr/local/bin/locks-server-compose-entrypoint.sh

# The server persists its generated config and key under $HOME/.pubky-lock when
# no --config is provided. Mount /var/lib/pubky-lock or pass --config for managed deployments.
ENV HOME=/var/lib/pubky-lock
WORKDIR /var/lib/pubky-lock

# Default lock-server bind port. Operators can override bind_addr in config.toml.
EXPOSE 3000

CMD ["locks-server"]
