# Contributing

Pubky Locks is pre-production. Issues and focused pull requests are welcome, but
maintainers may change APIs and persistence contracts without backwards compatibility.

## Development prerequisites

- Rust 1.89.0 with `rustfmt`, `clippy`, and the `wasm32-unknown-unknown` target
- PostgreSQL 16 for persistence and E2E tests
- Node.js 22 and npm
- `cargo-nextest`
- `wasm-pack` 0.13.1
- Docker with Compose for the local integrated demo

## Verification

Before requesting review, run the checks relevant to your change. The complete CI
shape is documented in [`.github/workflows/check.yml`](.github/workflows/check.yml).
The primary gates are:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace --exclude locks-e2e --exclude locks-service --exclude locks-sdk-wasm
cargo nextest run -p locks-service --lib -- --skip infrastructure::postgres
npm --prefix locks-sdk/bindings/js run test
git diff --check
```

CI runs five lanes in parallel:

| Lane | Ownership |
| --- | --- |
| `Style / Compose` | Formatting, Compose bootstrap regression, and whitespace |
| `Clippy` | Workspace lint across all targets and features |
| `Rust tests` | Non-PostgreSQL Rust tests and fake/in-memory E2E tests |
| `JS / WASM` | Native bindings, wasm target, generated package, demo, and example smokes |
| `PostgreSQL` | PostgreSQL-backed service tests and runtime E2E |

The required `Checks` result aggregates all five lanes. `scripts/check` remains the
sequential local umbrella.

PostgreSQL-backed tests and `postgres_runtime` require `TEST_DATABASE_URL`; CI uses an
ephemeral local PostgreSQL service. Do not claim the full suite passed when only a subset
ran.

## Pull requests

- Keep changes focused and explain user-visible and persisted-data implications.
- Add regression tests for bug fixes where practical.
- Update API, runtime, SDK, ADR, and example documentation when contracts change.
- Never commit `.local/`, `.env`, sessions, passphrases, recovery files, keys,
  credentials, wallet material, database dumps, or generated `node_modules`.
- Do not include real identities, addresses, payment requests, or bearer credentials
  in tests, logs, screenshots, issues, or pull requests.
- Do not self-merge. A human reviewer should read and, where relevant, run the change.

Report security issues through [`SECURITY.md`](SECURITY.md), not a public issue.

## Conduct

Be respectful and constructive. Harassment, personal attacks, and publication of
another person's private information are not acceptable. Maintainers may edit,
hide, or reject contributions that violate these expectations.
