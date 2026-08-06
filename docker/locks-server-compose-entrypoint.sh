#!/bin/sh
set -eu

service_home="${LOCKS_SERVICE_HOME:-/var/lib/pubky-lock/.pubky-lock}"
generated_config="$service_home/config.toml"
compose_config="${LOCKS_COMPOSE_CONFIG:-/var/lib/pubky-lock/config.compose.toml}"
secret_path="$service_home/secret.sess"
creator_authority_key_path="$service_home/creator-authority-encryption-key"
public_config="${LOCKS_PUBLIC_CONFIG:-/run/locks-public/config.toml}"

mkdir -p "$service_home"

if [ -z "${PUBKY_LOCK_CREATOR_AUTH_ENCRYPTION_KEY:-}" ]; then
  if [ ! -f "$creator_authority_key_path" ]; then
    echo "[locks-compose] generating creator-authority encryption key"
    umask 077
    temporary_key_path="$creator_authority_key_path.tmp.$$"
    head -c 32 /dev/urandom \
      | base64 \
      | tr '+/' '-_' \
      | tr -d '=\n' > "$temporary_key_path"
    chmod 600 "$temporary_key_path"
    mv "$temporary_key_path" "$creator_authority_key_path"
  fi

  PUBKY_LOCK_CREATOR_AUTH_ENCRYPTION_KEY="$(cat "$creator_authority_key_path")"
  export PUBKY_LOCK_CREATOR_AUTH_ENCRYPTION_KEY
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

mkdir -p "$(dirname "$public_config")"
public_config_tmp="$public_config.tmp.$$"
printf 'lock_server_public_key = "%s"\n' "$lock_server_public_key" > "$public_config_tmp"
chmod 0644 "$public_config_tmp"
mv "$public_config_tmp" "$public_config"

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

[paykit]
server_url = "http://127.0.0.1:3001"
minimum_confirmations = 0

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
creator_authority_key_env = "PUBKY_LOCK_CREATOR_AUTH_ENCRYPTION_KEY"

[logging]
level = "info,pubky::actors::session=warn"

[pubky]
network = "testnet"

[pkdns]
public_ip = "127.0.0.1"
public_pubky_tls_port = 6287
public_icann_http_port = 3000
icann_domain = "localhost"
pkarr_relays = ["http://127.0.0.1:15411"]
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
