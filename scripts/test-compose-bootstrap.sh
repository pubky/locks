#!/bin/sh
set -eu

repo_root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
entrypoint="$repo_root/docker/locks-server-compose-entrypoint.sh"
key_name="PUBKY_LOCK_CREATOR_AUTH_ENCRYPTION_KEY"

env -u "$key_name" docker compose -f "$repo_root/docker-compose.yml" config --quiet

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
service_home="$tmp/home/.pubky-lock"
bin_dir="$tmp/bin"
capture="$tmp/captured-key"
mkdir -p "$service_home" "$bin_dir"

cat > "$service_home/config.toml" <<'EOF'
lock_server_public_key = "test-public-key"
EOF
: > "$service_home/secret.sess"

cat > "$bin_dir/locks-server" <<'EOF'
#!/bin/sh
set -eu
printf '%s' "$PUBKY_LOCK_CREATOR_AUTH_ENCRYPTION_KEY" > "$LOCKS_TEST_KEY_CAPTURE"
EOF
chmod +x "$bin_dir/locks-server"

run_entrypoint() {
  env -u "$key_name" \
    PATH="$bin_dir:$PATH" \
    LOCKS_SERVICE_HOME="$service_home" \
    LOCKS_COMPOSE_CONFIG="$tmp/config.compose.toml" \
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
key_file="$service_home/creator-authority-encryption-key"
test -f "$key_file"
test "$(wc -c < "$key_file" | tr -d ' ')" -eq 43
grep -Eq '^[A-Za-z0-9_-]{43}$' "$key_file"
test "$(file_mode "$key_file")" = 600
cmp -s "$key_file" "$capture"
first_key="$(cat "$key_file")"

run_entrypoint
test "$(cat "$capture")" = "$first_key"

override='AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
PUBKY_LOCK_CREATOR_AUTH_ENCRYPTION_KEY="$override" \
  PATH="$bin_dir:$PATH" \
  LOCKS_SERVICE_HOME="$service_home" \
  LOCKS_COMPOSE_CONFIG="$tmp/config.compose.toml" \
  LOCKS_TEST_KEY_CAPTURE="$capture" \
  sh "$entrypoint"
test "$(cat "$capture")" = "$override"
test "$(cat "$key_file")" = "$first_key"

printf 'compose bootstrap regression passed\n'
