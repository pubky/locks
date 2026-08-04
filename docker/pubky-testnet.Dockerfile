FROM rust:1.89.0-bookworm AS builder

ARG PUBKY_CORE_REV=75eb1324f86e8caa16c41f18a2cd6b8e1909ee7b
WORKDIR /usr/src/pubky-core
RUN git clone --filter=blob:none https://github.com/pubky/pubky-core.git . \
    && git checkout --detach "${PUBKY_CORE_REV}"
# The pinned Pubky Core revision locks quinn-proto 0.11.14, affected by
# RUSTSEC-2026-0185. Keep this precise override until its lockfile advances.
RUN cargo update -p quinn-proto --precise 0.11.15 \
    && cargo build --release -p pubky-testnet --bin pubky-testnet

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/pubky-core/target/release/pubky-testnet /usr/local/bin/pubky-testnet
EXPOSE 6881 15411 15412 6288
CMD ["pubky-testnet"]
