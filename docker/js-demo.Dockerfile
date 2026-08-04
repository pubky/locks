# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
FROM paykit-runtime AS paykit-runtime

FROM node:22-bookworm-slim@sha256:813a7480f28fdadac1f7f5c824bcdad435b5bc1322a5968bbbdef8d058f9dff4
WORKDIR /workspace
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates util-linux \
    && rm -rf /var/lib/apt/lists/*
COPY --chown=node:node examples/js-sdk/package.json examples/js-sdk/package-lock.json /workspace/examples/js-sdk/
RUN npm --prefix examples/js-sdk ci --ignore-scripts \
    && npm cache clean --force
COPY --chown=node:node examples/js-sdk /workspace/examples/js-sdk
COPY --from=paykit-runtime /usr/local/bin/paykit-companion-auth /usr/local/bin/paykit-companion-auth
COPY --from=paykit-runtime /usr/local/bin/paykit-reader-demo /usr/local/bin/paykit-reader-demo
RUN mkdir -p /workspace/locks-sdk/bindings/js/pkg /workspace/.local \
    && chown -R node:node /workspace
USER node:node
