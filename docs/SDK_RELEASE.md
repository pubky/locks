# SDK release readiness

This document tracks what remains before the JS/WASM browser SDK can be published as a public npm package.

## Current status

The SDK API foundation is implemented. Public release policy uses `@synonymdev/locks-sdk`, public npm access, the `rc` dist-tag for release candidates, and the `latest` dist-tag for stable releases.

Run the metadata audit:

```bash
npm --prefix locks-sdk/bindings/js run release:audit
```

The command is intentionally informational and exits 0. It reports blockers without pretending policy decisions are resolved.

## Package layout

The public package is `@synonymdev/locks-sdk`. Its root manifest exports generated wasm-pack JS, declarations, and WASM from `pkg/`. The internal generated package name remains `locks-sdk-wasm`; it is not published as a separate package.

Release-candidate publication must use npm's `rc` dist-tag so prereleases never replace `latest`. Stable publication uses `latest`.

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

5. Confirm the manifest version and dist-tag match the release type. Release candidates must have a prerelease version and `publishConfig.tag = "rc"`; stable releases must have a stable version and `publishConfig.tag = "latest"`:

```bash
PACKAGE_VERSION=$(node -p "require('./locks-sdk/bindings/js/package.json').version")
DIST_TAG=$(node -p "require('./locks-sdk/bindings/js/package.json').publishConfig.tag")
printf 'Publishing %s with npm dist-tag %s\n' "$PACKAGE_VERSION" "$DIST_TAG"
```

6. Publish from an authenticated account with `@synonymdev` package permission. Do not pass `--tag`; npm must use the reviewed `publishConfig.tag` from the manifest:

```bash
npm publish ./locks-sdk/bindings/js --access public
```

7. Read back registry metadata and verify both the exact package version and the manifest-selected dist-tag:

```bash
PACKAGE_VERSION=$(node -p "require('./locks-sdk/bindings/js/package.json').version")
DIST_TAG=$(node -p "require('./locks-sdk/bindings/js/package.json').publishConfig.tag")
PUBLISHED_VERSION=$(npm view "@synonymdev/locks-sdk@${PACKAGE_VERSION}" version)
TAGGED_VERSION=$(npm view @synonymdev/locks-sdk "dist-tags.${DIST_TAG}")
test "$PUBLISHED_VERSION" = "$PACKAGE_VERSION"
test "$TAGGED_VERSION" = "$PACKAGE_VERSION"
```

## Explicit non-goals

- Do not add lock-type proof helper APIs as part of publishing.
- Do not require live Pubky/testnet smoke in ordinary local package tests.
- Do not publish generated artifacts until the release flow is explicit.
