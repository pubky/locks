FROM rust:1.91.1-bookworm AS builder

ARG PUBKY_HOMESERVER_REF=v0.11.0
WORKDIR /usr/src/pubky-homeserver
RUN git clone --branch "${PUBKY_HOMESERVER_REF}" --depth 1 https://github.com/pubky/pubky-homeserver.git .
# The Pubky v0.11.0 lockfile pins quinn-proto 0.11.14. Version 0.11.18 is the
# first release patched for all of RUSTSEC-2026-0338 through RUSTSEC-2026-0342.
# Keep this precise override until its lockfile advances.
RUN cargo update -p quinn-proto --precise 0.11.18 \
    && cargo build --release -p pubky-testnet --bin pubky-testnet

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/pubky-homeserver/target/release/pubky-testnet /usr/local/bin/pubky-testnet
EXPOSE 6881 15411 15412 6288
CMD ["pubky-testnet"]
