#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: ./get_public_key.sh /path/to/secret.sess

Derives the Lock Server public key for a secret file containing:
  keypair-seed:<base64url-no-pad-32-byte-seed>

Prints only the derived public key, suitable for:
  [credentials]
  lock_server_public_key = "<output>"
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -ne 1 ]]; then
  usage
  exit 2
fi

secret_path="$1"
if [[ ! -f "${secret_path}" ]]; then
  echo "secret file not found: ${secret_path}" >&2
  exit 1
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp_dir="$(mktemp -d -t locks-public-key-derive.XXXXXX)"
cleanup() {
  rm -rf "${tmp_dir}"
}
trap cleanup EXIT

cat >"${tmp_dir}/Cargo.toml" <<'EOF'
[package]
name = "locks-public-key-derive"
version = "0.0.0"
edition = "2024"

[dependencies]
base64 = "0.22"
pubky-common = "0.9.0"
EOF

mkdir -p "${tmp_dir}/src"
cat >"${tmp_dir}/src/main.rs" <<'EOF'
use std::{env, fs};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use pubky_common::crypto::Keypair;

fn main() {
    let path = env::args()
        .nth(1)
        .expect("usage: locks-public-key-derive <secret.sess>");
    let secret = fs::read_to_string(path).expect("failed to read secret file");
    let encoded_seed = secret
        .trim()
        .strip_prefix("keypair-seed:")
        .expect("expected secret format: keypair-seed:<base64url-no-pad-32-byte-seed>");
    let seed = URL_SAFE_NO_PAD
        .decode(encoded_seed.as_bytes())
        .expect("failed to decode base64url-no-pad seed");
    let seed: [u8; 32] = seed.try_into().expect("decoded seed must be 32 bytes");
    let keypair = Keypair::from_secret(&seed);
    println!("{}", keypair.public_key());
}
EOF

CARGO_TARGET_DIR="${repo_root}/target" cargo run --quiet --manifest-path "${tmp_dir}/Cargo.toml" -- "${secret_path}"
