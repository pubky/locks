# SDK release readiness

This document tracks what remains before the JS/WASM browser SDK can be published as a public npm package.

## Current status

The SDK API foundation is implemented and locally verified. The package is not publish-ready because final release policy is not decided.

Run the metadata audit:

```bash
npm --prefix locks-sdk/bindings/js run release:audit
```

The command is intentionally informational and exits 0. It reports blockers without pretending policy decisions are resolved.

## Current publish blockers

### 1. Publishing switch

`locks-sdk/bindings/js/package.json` currently has:

```json
"private": true
```

Do not flip this until the final package name, registry ownership, and dry-run checklist are approved.

### 2. Generated wasm-pack package name

The scaffold package is:

```text
@pubky/locks-sdk
```

The generated package under `locks-sdk/bindings/js/pkg/` is currently:

```text
locks-sdk-wasm
```

because wasm-pack derives it from the Rust crate name. Decide whether to keep that generated name, adjust wasm-pack metadata/build output, or publish from a wrapper package.

## Pre-publish checklist

Once policy is decided:

1. Confirm final npm package name and org ownership.
2. Decide whether `pkg/` is committed or generated during release.
3. Run:

```bash
npm --prefix locks-sdk/bindings/js test
npm --prefix locks-sdk/bindings/js run release:audit
npm --prefix locks-sdk/bindings/js run build
npm --prefix locks-sdk/bindings/js publish --dry-run
```

4. Run workspace gates:

```bash
cargo fmt
TEST_DATABASE_URL='postgres://locks:locks@localhost:55433/locks_test' cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
git diff --check
```

## Explicit non-goals

- Do not add lock-type proof helper APIs as part of publishing.
- Do not require live Pubky/testnet smoke in ordinary local package tests.
- Do not publish generated artifacts until the release flow is explicit.
