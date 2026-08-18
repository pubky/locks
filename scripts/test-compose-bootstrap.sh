#!/bin/sh
set -eu

repo_root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
entrypoint="$repo_root/docker/locks-server-compose-entrypoint.sh"
key_name="PUBKY_LOCK_RUNTIME_MASTER_KEY"

env -u "$key_name" docker compose -f "$repo_root/docker-compose.yml" config --quiet

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
service_home="$tmp/home/.pubky-lock"
bin_dir="$tmp/bin"
capture="$tmp/captured-key"
public_config="$tmp/locks-public/config.toml"
mkdir -p "$service_home" "$bin_dir"

cat > "$service_home/config.toml" <<'EOF'
lock_server_public_key = "test-public-key"
EOF
: > "$service_home/secret.sess"

cat > "$bin_dir/locks-server" <<'EOF'
#!/bin/sh
set -eu
printf '%s' "$PUBKY_LOCK_RUNTIME_MASTER_KEY" > "$LOCKS_TEST_KEY_CAPTURE"
EOF
chmod +x "$bin_dir/locks-server"

run_entrypoint() {
  env -u "$key_name" \
    PATH="$bin_dir:$PATH" \
    LOCKS_SERVICE_HOME="$service_home" \
    LOCKS_COMPOSE_CONFIG="$tmp/config.compose.toml" \
    LOCKS_PUBLIC_CONFIG="$public_config" \
    LOCKS_TEST_KEY_CAPTURE="$capture" \
    sh "$entrypoint"
}

file_mode() {
  if stat -c '%a' "$1" >/dev/null 2>&1; then
    stat -c '%a' "$1"
  else
    stat -f '%Lp' "$1"
  fi
}

run_entrypoint
key_file="$service_home/runtime-master-key"
test -f "$key_file"
test "$(wc -c < "$key_file" | tr -d ' ')" -eq 43
grep -Eq '^[A-Za-z0-9_-]{43}$' "$key_file"
test "$(file_mode "$key_file")" = 600
cmp -s "$key_file" "$capture"
test "$(cat "$public_config")" = 'lock_server_public_key = "test-public-key"'
test "$(file_mode "$public_config")" = 644
first_key="$(cat "$key_file")"

run_entrypoint
test "$(cat "$capture")" = "$first_key"

override='AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
if PUBKY_LOCK_RUNTIME_MASTER_KEY="$override" \
  PATH="$bin_dir:$PATH" \
  LOCKS_SERVICE_HOME="$service_home" \
  LOCKS_COMPOSE_CONFIG="$tmp/config.compose.toml" \
  LOCKS_TEST_KEY_CAPTURE="$capture" \
  sh "$entrypoint" >"$tmp/mismatched-key.stdout" 2>"$tmp/mismatched-key.stderr"; then
  echo "entrypoint replaced an existing runtime master key" >&2
  exit 1
fi
grep -q "does not match the persisted runtime master key" "$tmp/mismatched-key.stderr"
test "$(cat "$key_file")" = "$first_key"

# Explicitly discarding local encrypted state includes discarding its key. A
# valid override may establish the key only once that reset has happened.
rm "$key_file"
PUBKY_LOCK_RUNTIME_MASTER_KEY="$override" \
  PATH="$bin_dir:$PATH" \
  LOCKS_SERVICE_HOME="$service_home" \
  LOCKS_COMPOSE_CONFIG="$tmp/config.compose.toml" \
  LOCKS_PUBLIC_CONFIG="$public_config" \
  LOCKS_TEST_KEY_CAPTURE="$capture" \
  sh "$entrypoint"
test "$(cat "$capture")" = "$override"
test "$(cat "$key_file")" = "$override"
test "$(file_mode "$key_file")" = 600

run_entrypoint
test "$(cat "$capture")" = "$override"

invalid_override='AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
if PUBKY_LOCK_RUNTIME_MASTER_KEY="$invalid_override" \
  PATH="$bin_dir:$PATH" \
  LOCKS_SERVICE_HOME="$service_home" \
  LOCKS_COMPOSE_CONFIG="$tmp/config.compose.toml" \
  LOCKS_TEST_KEY_CAPTURE="$capture" \
  sh "$entrypoint" >"$tmp/invalid-key.stdout" 2>"$tmp/invalid-key.stderr"; then
  echo "entrypoint accepted a padded-or-wrong-length runtime master key" >&2
  exit 1
fi
grep -q "must be an unpadded base64url-encoded 32-byte key" "$tmp/invalid-key.stderr"
test "$(cat "$key_file")" = "$override"

# The final base64url character for 32 bytes carries only two data bits. `B`
# has non-zero trailing bits and must not be accepted as an alias for `A`.
noncanonical_override='AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB'
if PUBKY_LOCK_RUNTIME_MASTER_KEY="$noncanonical_override" \
  PATH="$bin_dir:$PATH" \
  LOCKS_SERVICE_HOME="$service_home" \
  LOCKS_COMPOSE_CONFIG="$tmp/config.compose.toml" \
  LOCKS_TEST_KEY_CAPTURE="$capture" \
  sh "$entrypoint" >"$tmp/noncanonical-key.stdout" 2>"$tmp/noncanonical-key.stderr"; then
  echo "entrypoint accepted a noncanonical runtime master key" >&2
  exit 1
fi
grep -q "must be an unpadded base64url-encoded 32-byte key" \
  "$tmp/noncanonical-key.stderr"
test "$(cat "$key_file")" = "$override"

retired_key_file="$service_home/creator-authority-encryption-key"
: > "$retired_key_file"
if run_entrypoint >"$tmp/retired-key.stdout" 2>"$tmp/retired-key.stderr"; then
  echo "entrypoint accepted retired creator-authority key" >&2
  exit 1
fi
grep -q "retired creator-authority key detected" "$tmp/retired-key.stderr"
grep -q "discard and reacquire creator authority rows or recreate the local database" \
  "$tmp/retired-key.stderr"

printf 'compose bootstrap regression passed\n'
