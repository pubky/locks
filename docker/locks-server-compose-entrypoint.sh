#!/bin/sh
set -eu

service_home="${LOCKS_SERVICE_HOME:-/var/lib/pubky-lock/.pubky-lock}"
generated_config="$service_home/config.toml"
compose_config="${LOCKS_COMPOSE_CONFIG:-/var/lib/pubky-lock/config.compose.toml}"
secret_path="$service_home/secret.sess"
runtime_master_key_path="$service_home/runtime-master-key"
retired_creator_authority_key_path="$service_home/creator-authority-encryption-key"

mkdir -p "$service_home"

if [ -f "$retired_creator_authority_key_path" ]; then
  echo "[locks-compose] retired creator-authority key detected: $retired_creator_authority_key_path" >&2
  echo "[locks-compose] stop the stack, discard and reacquire creator authority rows or recreate the local database, remove the retired key file, then restart" >&2
  exit 1
fi

if [ -n "${PUBKY_LOCK_RUNTIME_MASTER_KEY:-}" ]; then
  if ! printf '%s' "$PUBKY_LOCK_RUNTIME_MASTER_KEY" | grep -Eq '^[A-Za-z0-9_-]{43}$'; then
    echo "[locks-compose] PUBKY_LOCK_RUNTIME_MASTER_KEY must be an unpadded base64url-encoded 32-byte key" >&2
    exit 1
  fi
  umask 077
  decoded_key_path="$runtime_master_key_path.decoded.$$"
  cleanup_decoded_key() {
    rm -f "$decoded_key_path"
  }
  trap cleanup_decoded_key EXIT HUP INT TERM
  if ! printf '%s=' "$PUBKY_LOCK_RUNTIME_MASTER_KEY" \
    | tr '_-' '/+' \
    | base64 -d > "$decoded_key_path" 2>/dev/null \
    || [ "$(wc -c < "$decoded_key_path" | tr -d ' ')" -ne 32 ]; then
    echo "[locks-compose] PUBKY_LOCK_RUNTIME_MASTER_KEY must be an unpadded base64url-encoded 32-byte key" >&2
    exit 1
  fi
  canonical_runtime_master_key="$(
    base64 < "$decoded_key_path" \
      | tr '+/' '-_' \
      | tr -d '=\n'
  )"
  if [ "$canonical_runtime_master_key" != "$PUBKY_LOCK_RUNTIME_MASTER_KEY" ]; then
    echo "[locks-compose] PUBKY_LOCK_RUNTIME_MASTER_KEY must be an unpadded base64url-encoded 32-byte key" >&2
    exit 1
  fi
  cleanup_decoded_key
  trap - EXIT HUP INT TERM
  if [ -f "$runtime_master_key_path" ]; then
    persisted_runtime_master_key="$(cat "$runtime_master_key_path")"
    if [ "$persisted_runtime_master_key" != "$PUBKY_LOCK_RUNTIME_MASTER_KEY" ]; then
      echo "[locks-compose] PUBKY_LOCK_RUNTIME_MASTER_KEY does not match the persisted runtime master key" >&2
      echo "[locks-compose] rotate only through an explicit data migration or reset that handles encrypted state" >&2
      exit 1
    fi
  else
    temporary_key_path="$runtime_master_key_path.tmp.$$"
    printf '%s' "$PUBKY_LOCK_RUNTIME_MASTER_KEY" > "$temporary_key_path"
    chmod 600 "$temporary_key_path"
    mv "$temporary_key_path" "$runtime_master_key_path"
  fi
else
  if [ ! -f "$runtime_master_key_path" ]; then
    echo "[locks-compose] generating runtime master key"
    umask 077
    temporary_key_path="$runtime_master_key_path.tmp.$$"
    head -c 32 /dev/urandom \
      | base64 \
      | tr '+/' '-_' \
      | tr -d '=\n' > "$temporary_key_path"
    chmod 600 "$temporary_key_path"
    mv "$temporary_key_path" "$runtime_master_key_path"
  fi

  PUBKY_LOCK_RUNTIME_MASTER_KEY="$(cat "$runtime_master_key_path")"
  export PUBKY_LOCK_RUNTIME_MASTER_KEY
fi

if [ ! -f "$generated_config" ] || [ ! -f "$secret_path" ]; then
  echo "[locks-compose] initializing Lock Server identity/config in $service_home"
  timeout 5 locks-server || true
fi

if [ ! -f "$generated_config" ]; then
  echo "[locks-compose] missing generated config: $generated_config" >&2
  exit 1
fi
if [ ! -f "$secret_path" ]; then
  echo "[locks-compose] missing generated secret: $secret_path" >&2
  exit 1
fi

lock_server_public_key="$(grep -E '^lock_server_public_key = ' "$generated_config" | head -n 1 | cut -d '"' -f 2)"
if [ -z "$lock_server_public_key" ] || [ "$lock_server_public_key" = "<derived-on-first-run>" ]; then
  echo "[locks-compose] generated config has no usable lock_server_public_key" >&2
  exit 1
fi

cat > "$compose_config" <<EOF
bind_addr = "0.0.0.0:3000"

[credentials]
lock_server_secret_key = "$secret_path"
lock_server_public_key = "$lock_server_public_key"
max_ttl_seconds = 900

[database]
url_env = "PUBKY_LOCK_DATABASE_URL"
max_connections = 10
run_migrations_on_startup = true

[worker]
enabled = true
poll_interval_ms = 250
claim_timeout_seconds = 60
worker_id = "compose-worker"

[runtime]
environment = "development"

[creator_authority_acquisition]
enabled = true
method = "legacy-connect"
frontend_session_ttl_seconds = 86400
frontend_session_code_ttl_seconds = 120

[creator_authority_acquisition.legacy_connect]
allowed_return_origins = ["http://localhost:8080"]

[secrets]
runtime_master_key_env = "PUBKY_LOCK_RUNTIME_MASTER_KEY"

[deletion]
retry_max_attempts = 10
retry_initial_backoff_seconds = 1
retry_max_backoff_seconds = 300
final_credential_issuance_window_seconds = 900
final_read_window_seconds = 900

[logging]
level = "info"

[pubky]
network = "testnet"

[pkdns]
public_ip = "127.0.0.1"
public_pubky_tls_port = 6287
public_icann_http_port = 3000
icann_domain = "localhost"
pkarr_relays = ["http://localhost:15411"]
key_republisher_interval_seconds = 86400

[rate_limits.verification_submission]
enabled = true
max_requests = 60
window_seconds = 60

[content_locks]
max_resource_bytes = 10000000
max_resources = 10
max_total_resource_bytes = 100000000
EOF

echo "[locks-compose] starting locks-server with $compose_config"
exec locks-server --config "$compose_config"
