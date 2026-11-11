# syntax=docker/dockerfile:1.7
FROM rust:1.91.1-bookworm@sha256:c1e5f19e773b7878c3f7a805dd00a495e747acbdc76fb2337a4ebf0418896b33
ARG PAYKIT_REV=725a4cd6578cdc149b4f38c8577bb9937a81bbb6
RUN apt-get update && apt-get install -y --no-install-recommends clang lld cmake libssl-dev pkg-config git ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /paykit
RUN git clone https://github.com/pubky/paykit-server.git . && git checkout "$PAYKIT_REV" && test "$(git rev-parse HEAD)" = "$PAYKIT_REV" && test "$(git rev-parse HEAD^{tree})" = 460e541fa10c8da8c761f71177407cd0959ca17d
COPY fixtures/paykit-mixed-graph-payment-v2-bridge.rs /bridge.rs
RUN printf '\ninclude!("/bridge.rs");\n' >> paykit-server-e2e/tests/server_workflow.rs
ENV RUSTUP_TOOLCHAIN=1.91.1-x86_64-unknown-linux-gnu CARGO_INCREMENTAL=0
RUN cargo test --locked -p paykit-server-e2e --test server_workflow locks_http_mixed_graph_payment_v2_bridge --no-run
CMD ["cargo", "test", "--locked", "-p", "paykit-server-e2e", "--test", "server_workflow", "locks_http_mixed_graph_payment_v2_bridge", "--", "--exact", "--nocapture"]
