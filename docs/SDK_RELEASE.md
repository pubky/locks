# SDK release readiness

This document tracks what remains before the JS/WASM browser SDK can be published as a public npm package.

## Current status

The SDK API foundation is implemented. Public release policy uses `@synonymdev/locks-sdk`, public npm access, and the `rc` dist-tag for release candidates.

Run the metadata audit:

```bash
npm --prefix locks-sdk/bindings/js run release:audit
```

The command is intentionally informational and exits 0. It reports blockers without pretending policy decisions are resolved.

## Package layout

The public package is `@synonymdev/locks-sdk`. Its root manifest exports generated wasm-pack JS, declarations, and WASM from `pkg/`. The internal generated package name remains `locks-sdk-wasm`; it is not published as a separate package.

Release-candidate publication must use npm's `rc` dist-tag so prereleases never replace `latest`.

## Pre-publish checklist

Once policy is decided:

1. Confirm final npm package name and org ownership.
2. Decide whether `pkg/` is committed or generated during release.
3. Run:

```bash
npm --prefix locks-sdk/bindings/js test
npm --prefix locks-sdk/bindings/js run release:audit
npm --prefix locks-sdk/bindings/js run build
npm pack ./locks-sdk/bindings/js --dry-run
```

4. Run workspace gates:

```bash
cargo fmt
TEST_DATABASE_URL='postgres://postgres:postgres@localhost:5433/locks_test' cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
git diff --check
```

5. Publish from an authenticated account with `@synonymdev` package permission:

```bash
npm publish ./locks-sdk/bindings/js --access public --tag rc
```

6. Read back registry metadata and verify package version, public access, and `rc` dist-tag.

## Explicit non-goals

- Do not add lock-type proof helper APIs as part of publishing.
- Do not require live Pubky/testnet smoke in ordinary local package tests.
- Do not publish generated artifacts until the release flow is explicit.
