#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-}"

if [[ -z "${MODE}" || "${MODE}" == "-h" || "${MODE}" == "--help" ]]; then
  cat >&2 <<'USAGE'
Usage:
  scripts/dev-legacy-connect-testnet.sh auth
  scripts/dev-legacy-connect-testnet.sh locked-content

Environment:
  LOCK_SERVER_URL                Default: http://127.0.0.1:3000
  PUBKY_LOCK_DEV_HOME            Default: .local/pubky-lock-dev
  PUBKY_LOCK_DEV_HOMESERVER      Default: pubky8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo
  PUBKY_LOCK_DEV_RETURN_TO       Default: http://localhost:3000/locks/dev-auth-complete
  PUBKY_LOCK_FIXTURES            Default: locks-server/tests/fixtures
  KEEP_DEMO_TMP                  Set to 1 to preserve generated temp files
  LOCKS_DEV_HTTP_TRACE           Set to 0 to disable Lock Server request/response trace output
  LOCKS_DEV_HTTP_TRACE_SECRETS   Set to 1 to print raw secret-bearing headers/bodies instead of redacted values

Assumptions:
  - Pubky-Core local testnet is already running.
  - Lock Server is already running and configured for [pubky].network = "testnet".
  - legacy-connect JSON routes are mounted.
  - locked-content mode expects dev integration config: mode = "dev", pubky-homeserver backend, legacy-connect enabled, and dev manual completion enabled.
USAGE
  exit 2
fi

case "${MODE}" in
  auth|locked-content) ;;
  *)
    echo "Unsupported mode: ${MODE}" >&2
    exit 2
    ;;
esac

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 127
  fi
}

require_command cargo
require_command curl
require_command jq
require_command python3

LOCK_SERVER_URL="${LOCK_SERVER_URL:-http://127.0.0.1:3000}"
PUBKY_LOCK_DEV_HOME="${PUBKY_LOCK_DEV_HOME:-.local/pubky-lock-dev}"
PUBKY_LOCK_DEV_HOMESERVER="${PUBKY_LOCK_DEV_HOMESERVER:-pubky8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo}"
PUBKY_LOCK_DEV_RETURN_TO="${PUBKY_LOCK_DEV_RETURN_TO:-http://localhost:3000/locks/dev-auth-complete}"
PUBKY_LOCK_FIXTURES="${PUBKY_LOCK_FIXTURES:-locks-server/tests/fixtures}"
KEEP_DEMO_TMP="${KEEP_DEMO_TMP:-0}"
LOCKS_DEV_HTTP_TRACE="${LOCKS_DEV_HTTP_TRACE:-1}"
LOCKS_DEV_HTTP_TRACE_SECRETS="${LOCKS_DEV_HTTP_TRACE_SECRETS:-1}"
STATE="locks-dev-$(date +%s)-$$"
DEMO_TMP_DIR="$(mktemp -d)"

cleanup() {
  if [[ "${KEEP_DEMO_TMP}" != "1" ]]; then
    rm -rf "${DEMO_TMP_DIR}"
  fi
}
trap cleanup EXIT

trace_enabled() {
  [[ "${LOCKS_DEV_HTTP_TRACE}" != "0" ]]
}

redact_header_value() {
  local header="$1"
  if [[ "${LOCKS_DEV_HTTP_TRACE_SECRETS}" == "1" ]]; then
    printf '%s\n' "${header}"
  elif [[ "${header,,}" == authorization:* ]]; then
    printf '%s\n' "${header%%:*}: [REDACTED]"
  else
    printf '%s\n' "${header}"
  fi
}

pretty_trace_body() {
  local body_file="$1"
  if [[ ! -s "${body_file}" ]]; then
    echo "<empty>"
    return
  fi
  if [[ "${LOCKS_DEV_HTTP_TRACE_SECRETS}" == "1" ]]; then
    jq . "${body_file}" 2>/dev/null || cat "${body_file}"
    return
  fi
  jq '
    walk(
      if type == "object" then
        with_entries(
          if (.key | test("(?i)(authorization_url|session_token|credential|code|secret|token)")) then
            .value = "[REDACTED]"
          else
            .
          end
        )
      elif type == "string" and test("(?i)^Bearer ") then
        "Bearer [REDACTED]"
      else
        .
      end
    )
  ' "${body_file}" 2>/dev/null || cat "${body_file}"
}

pretty_trace_body_from_string() {
  local body="$1"
  local tmp_body
  tmp_body="$(mktemp)"
  printf '%s' "${body}" >"${tmp_body}"
  pretty_trace_body "${tmp_body}" >&2
  rm -f "${tmp_body}"
}

trace_request() {
  local method="$1"
  local path="$2"
  local body="$3"
  shift 3
  trace_enabled || return 0
  {
    echo
    echo "<<< LOCKS REQUEST ${method} ${LOCK_SERVER_URL}${path}"
    echo "headers:"
    if [[ "${method}" == "POST" ]]; then
      echo "  content-type: application/json"
    fi
    local previous_was_header=0 arg
    for arg in "$@"; do
      if [[ "${previous_was_header}" == "1" ]]; then
        printf '  %s\n' "$(redact_header_value "${arg}")"
        previous_was_header=0
      elif [[ "${arg}" == "-H" || "${arg}" == "--header" ]]; then
        previous_was_header=1
      fi
    done
    if [[ "${method}" == "POST" ]]; then
      echo "body:"
      pretty_trace_body_from_string "${body}"
    else
      echo "body: <empty>"
    fi
  } >&2
}

trace_response() {
  local method="$1"
  local path="$2"
  local status="$3"
  local headers_file="$4"
  local body_file="$5"
  trace_enabled || return 0
  {
    echo ">>> LOCKS RESPONSE ${method} ${path} HTTP ${status}"
    echo "headers:"
    if [[ -s "${headers_file}" ]]; then
      sed -e 's/\r$//' "${headers_file}" | while IFS= read -r header; do
        [[ -z "${header}" || "${header}" == HTTP/* ]] && continue
        printf '  %s\n' "$(redact_header_value "${header}")"
      done
    else
      echo "  <none>"
    fi
    echo "body:"
    pretty_trace_body "${body_file}"
    echo
  } >&2
}

json_post() {
  local path="$1"
  local body="$2"
  shift 2
  local tmp_body tmp_headers tmp_status
  tmp_body="$(mktemp)"
  tmp_headers="$(mktemp)"
  tmp_status="$(mktemp)"
  trace_request POST "${path}" "${body}" "$@"
  curl -sS \
    -D "${tmp_headers}" \
    -o "${tmp_body}" \
    -w '%{http_code}' \
    -H 'content-type: application/json' \
    "$@" \
    -X POST \
    --data "${body}" \
    "${LOCK_SERVER_URL}${path}" >"${tmp_status}" || {
      trace_response POST "${path}" "curl-error" "${tmp_headers}" "${tmp_body}"
      echo "HTTP request failed: POST ${path}" >&2
      echo "Response body:" >&2
      cat "${tmp_body}" >&2 || true
      rm -f "${tmp_body}" "${tmp_headers}" "${tmp_status}"
      exit 1
    }
  local status
  status="$(cat "${tmp_status}")"
  trace_response POST "${path}" "${status}" "${tmp_headers}" "${tmp_body}"
  if [[ "${status}" != 2* ]]; then
    echo "Unexpected HTTP ${status}: POST ${path}" >&2
    echo "Response body:" >&2
    cat "${tmp_body}" >&2 || true
    rm -f "${tmp_body}" "${tmp_headers}" "${tmp_status}"
    exit 1
  fi
  cat "${tmp_body}"
  rm -f "${tmp_body}" "${tmp_headers}" "${tmp_status}"
}

json_post_capture_status() {
  local path="$1"
  local body="$2"
  local output_file="$3"
  shift 3
  local tmp_headers tmp_status
  tmp_headers="$(mktemp)"
  tmp_status="$(mktemp)"
  trace_request POST "${path}" "${body}" "$@"
  curl -sS \
    -D "${tmp_headers}" \
    -o "${output_file}" \
    -w '%{http_code}' \
    -H 'content-type: application/json' \
    "$@" \
    -X POST \
    --data "${body}" \
    "${LOCK_SERVER_URL}${path}" >"${tmp_status}"
  local status
  status="$(cat "${tmp_status}")"
  trace_response POST "${path}" "${status}" "${tmp_headers}" "${output_file}"
  printf '%s' "${status}"
  rm -f "${tmp_headers}" "${tmp_status}"
}

locks_get() {
  local path="$1"
  local output_file="$2"
  shift 2
  local tmp_headers tmp_status
  tmp_headers="$(mktemp)"
  tmp_status="$(mktemp)"
  trace_request GET "${path}" '' "$@"
  curl -sS \
    -D "${tmp_headers}" \
    -o "${output_file}" \
    -w '%{http_code}' \
    "$@" \
    "${LOCK_SERVER_URL}${path}" >"${tmp_status}" || {
      trace_response GET "${path}" "curl-error" "${tmp_headers}" "${output_file}"
      rm -f "${tmp_headers}" "${tmp_status}"
      return 1
    }
  local status
  status="$(cat "${tmp_status}")"
  trace_response GET "${path}" "${status}" "${tmp_headers}" "${output_file}"
  printf '%s' "${status}"
  rm -f "${tmp_headers}" "${tmp_status}"
}

locks_put_file() {
  local path="$1"
  local input_file="$2"
  local output_file="$3"
  shift 3
  local tmp_headers tmp_status
  tmp_headers="$(mktemp)"
  tmp_status="$(mktemp)"
  trace_request PUT "${path}" "<raw bytes: ${input_file}>" "$@"
  curl -sS \
    -D "${tmp_headers}" \
    -o "${output_file}" \
    -w '%{http_code}' \
    "$@" \
    -X PUT \
    --data-binary "@${input_file}" \
    "${LOCK_SERVER_URL}${path}" >"${tmp_status}" || {
      trace_response PUT "${path}" "curl-error" "${tmp_headers}" "${output_file}"
      rm -f "${tmp_headers}" "${tmp_status}"
      return 1
    }
  local status
  status="$(cat "${tmp_status}")"
  trace_response PUT "${path}" "${status}" "${tmp_headers}" "${output_file}"
  printf '%s' "${status}"
  rm -f "${tmp_headers}" "${tmp_status}"
}

preflight() {
  local mode="$1"
  echo "Preflight: ${LOCK_SERVER_URL}/healthz" >&2
  local health_body health_status ready_body ready_status
  health_body="$(mktemp)"
  health_status="$(locks_get "/healthz" "${health_body}")"
  if [[ "${health_status}" != 2* ]]; then
    echo "Unexpected HTTP ${health_status}: GET /healthz" >&2
    cat "${health_body}" >&2 || true
    rm -f "${health_body}"
    exit 1
  fi
  rm -f "${health_body}"
  echo "Preflight: ${LOCK_SERVER_URL}/readyz" >&2
  ready_body="$(mktemp)"
  ready_status="$(locks_get "/readyz" "${ready_body}")"
  if [[ "${ready_status}" != 2* ]]; then
    echo "Unexpected HTTP ${ready_status}: GET /readyz" >&2
    cat "${ready_body}" >&2 || true
    rm -f "${ready_body}"
    exit 1
  fi
  jq . "${ready_body}" >&2
  rm -f "${ready_body}"
  cat >&2 <<EOF
Expected Lock Server config for ${mode} mode:
  [pubky]
  network = "testnet"

  [runtime]
  mode = "dev"
  expose_dev_completion_route = true
  expose_creator_publishing_routes = false
  expose_creator_connect_routes = false

  [creator_authority_acquisition]
  enabled = true
  method = "legacy-connect"

  [creator_repositories]
  backend = "pubky-homeserver"
EOF
  if [[ "${mode}" == "locked-content" ]]; then
    cat >&2 <<'EOF'

locked-content uses authenticated Pubky-backed creator routes and the dev-only manual verification completion route. It should not require production mode.
EOF
  fi
}

ensure_user() {
  cargo run -p locks-server --example dev_legacy_connect_testnet -- \
    ensure-user \
    --home "${PUBKY_LOCK_DEV_HOME}" \
    --homeserver "${PUBKY_LOCK_DEV_HOMESERVER}"
}

approve_auth() {
  local auth_url="$1"
  cargo run -p locks-server --example dev_legacy_connect_testnet -- \
    approve-auth \
    --home "${PUBKY_LOCK_DEV_HOME}" \
    --homeserver "${PUBKY_LOCK_DEV_HOMESERVER}" \
    --auth-url "${auth_url}"
}

run_auth() {
  preflight auth
  acquire_frontend_session
}

acquire_frontend_session() {
  echo "Ensuring reusable dev Pubky testnet user" >&2
  local user_json creator
  user_json="$(ensure_user)"
  creator="$(jq -r '.creator' <<<"${user_json}")"
  echo "Dev creator: ${creator}" >&2

  echo "Starting Lock Server hosted legacy-connect shell" >&2
  local return_to_q state_q shell_path shell_body flow_id auth_url
  return_to_q="$(jq -nr --arg v "${PUBKY_LOCK_DEV_RETURN_TO}" '$v|@uri')"
  state_q="$(jq -nr --arg v "${STATE}" '$v|@uri')"
  shell_path="/connect?return_to=${return_to_q}&state=${state_q}"
  shell_body="${DEMO_TMP_DIR}/connect_shell.html"
  local shell_status
  shell_status="$(locks_get "${shell_path}" "${shell_body}")"
  if [[ "${shell_status}" != 2* ]]; then
    echo "Unexpected HTTP ${shell_status}: GET ${shell_path}" >&2
    cat "${shell_body}" >&2 || true
    exit 1
  fi
  read -r flow_id auth_url < <(python3 - "${shell_body}" <<'PY'
import html, re, sys
body = open(sys.argv[1], encoding='utf-8').read()
flow = re.search(r'action="/connect/([^"/]+)/complete"', body)
auth = re.search(r'href="([^"]+)"', body)
if not flow or not auth:
    raise SystemExit('connect shell missing flow action or auth href')
print(flow.group(1), html.unescape(auth.group(1)))
PY
  )

  if [[ -z "${flow_id}" || "${flow_id}" == "null" || -z "${auth_url}" || "${auth_url}" == "null" ]]; then
    echo "Connect shell did not contain flow_id and authorization URL" >&2
    cat "${shell_body}" >&2 || true
    exit 1
  fi

  echo "Approving legacy-connect auth URL for flow ${flow_id}" >&2
  approve_auth "${auth_url}" >/dev/null

  echo "Completing Lock Server hosted legacy-connect flow" >&2
  local completion_headers completion_body completion_status location code completed_state
  completion_headers="${DEMO_TMP_DIR}/connect_completion_headers.txt"
  completion_body="${DEMO_TMP_DIR}/connect_completion_body.txt"
  completion_status="$(curl -sS -D "${completion_headers}" -o "${completion_body}" -w '%{http_code}' -X POST "${LOCK_SERVER_URL}/connect/${flow_id}/complete")"
  if [[ "${completion_status}" != "303" ]]; then
    echo "Unexpected HTTP ${completion_status}: POST /connect/${flow_id}/complete" >&2
    cat "${completion_body}" >&2 || true
    exit 1
  fi
  location="$(awk 'tolower($1)=="location:" {sub(/^[^ ]+[ ]*/, ""); sub(/\r$/, ""); print; exit}' "${completion_headers}")"
  code="$(python3 - "${location}" <<'PY'
from urllib.parse import urlparse, parse_qs
import sys
qs = parse_qs(urlparse(sys.argv[1]).query)
print(qs.get('code', [''])[0])
PY
  )"
  completed_state="$(python3 - "${location}" <<'PY'
from urllib.parse import urlparse, parse_qs
import sys
qs = parse_qs(urlparse(sys.argv[1]).query)
print(qs.get('state', [''])[0])
PY
  )"

  echo "Exchanging frontend session code" >&2
  local session_json
  session_json="$(json_post "/frontend-sessions" "$(jq -nc --arg code "${code}" --arg state "${completed_state}" '{code: $code, state: $state}')")"

  jq -n \
    --arg creator "$(jq -r '.creator' <<<"${session_json}")" \
    --arg session_token "$(jq -r '.session_token' <<<"${session_json}")" \
    --arg expires_at "$(jq -r '.expires_at' <<<"${session_json}")" \
    --arg state "${STATE}" \
    '{creator: $creator, session_token: $session_token, expires_at: $expires_at, state: $state}'
}

require_fixture() {
  local path="$1"
  [[ -f "${path}" ]] || {
    echo "missing fixture: ${path}" >&2
    exit 2
  }
}

generate_bundle_id() {
  python3 - <<'PY'
import os
alphabet = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'
data = os.urandom(16)
bits = ''.join(f'{byte:08b}' for byte in data)
out = []
for i in range(0, len(bits), 5):
    chunk = bits[i:i + 5]
    if len(chunk) < 5:
        chunk = chunk.ljust(5, '0')
    out.append(alphabet[int(chunk, 2)])
print(''.join(out))
PY
}

run_locked_content() {
  preflight locked-content

  require_fixture "${PUBKY_LOCK_FIXTURES}/creator_publishing/set_lock_service_config_request.json"
  require_fixture "${PUBKY_LOCK_FIXTURES}/creator_publishing/create_content_lock_request.json"
  require_fixture "${PUBKY_LOCK_FIXTURES}/viewer_access/submit_proof_bundle_request.json"

  local auth_json creator session_token
  auth_json="$(acquire_frontend_session)"
  creator="$(jq -r '.creator' <<<"${auth_json}")"
  session_token="$(jq -r '.session_token' <<<"${auth_json}")"

  if [[ -z "${session_token}" || "${session_token}" == "null" ]]; then
    echo "auth flow did not return a frontend session token" >&2
    jq . <<<"${auth_json}" >&2
    exit 1
  fi

  echo "Creating lock-service pointer for ${creator}" >&2
  local set_config_body set_config_json
  set_config_body="$(jq '.' "${PUBKY_LOCK_FIXTURES}/creator_publishing/set_lock_service_config_request.json")"
  set_config_json="$(json_post "/creator/lock-service-config" "${set_config_body}" -H "authorization: Bearer ${session_token}")"
  jq . <<<"${set_config_json}" >"${DEMO_TMP_DIR}/lock_service_config_response.json"

  echo "Registering guarded private resource" >&2
  local guarded_json guarded_resource_path upload_status
  printf 'guarded bytes' >"${DEMO_TMP_DIR}/guarded_resource_body.txt"
  upload_status="$(locks_put_file "/creator/priv-resources/content/example.txt" "${DEMO_TMP_DIR}/guarded_resource_body.txt" "${DEMO_TMP_DIR}/priv_response.json" -H "authorization: Bearer ${session_token}" -H "content-type: text/plain")"
  if [[ "${upload_status}" != 2* ]]; then
    echo "Unexpected HTTP ${upload_status}: PUT /creator/priv-resources/content/example.txt" >&2
    cat "${DEMO_TMP_DIR}/priv_response.json" >&2 || true
    exit 1
  fi
  guarded_json="$(cat "${DEMO_TMP_DIR}/priv_response.json")"
  guarded_resource_path="$(jq -r '.guarded_resource.path' <<<"${guarded_json}")"

  echo "Creating content lock" >&2
  local create_lock_body content_lock_json content_lock_path pubky_lock_resource
  create_lock_body="$(jq --slurpfile registered "${DEMO_TMP_DIR}/priv_response.json" '.primary_resource = $registered[0].guarded_resource' "${PUBKY_LOCK_FIXTURES}/creator_publishing/create_content_lock_request.json")"
  content_lock_json="$(json_post "/creator/content-locks" "${create_lock_body}" -H "authorization: Bearer ${session_token}")"
  jq . <<<"${content_lock_json}" >"${DEMO_TMP_DIR}/content_lock_response.json"
  content_lock_path="$(jq -r '.content_lock_path' <<<"${content_lock_json}")"
  pubky_lock_resource="${creator}${content_lock_path}"

  echo "Submitting viewer proof bundle" >&2
  local bundle_id submit_body submit_json
  bundle_id="$(generate_bundle_id)"
  submit_body="$(jq --arg bundle_id "${bundle_id}" --arg pubky_lock_resource "${pubky_lock_resource}" '.submitted_proof_bundle.bundle_id = $bundle_id | .submitted_proof_bundle.pubky_lock_resource = $pubky_lock_resource' "${PUBKY_LOCK_FIXTURES}/viewer_access/submit_proof_bundle_request.json")"
  submit_json="$(json_post "/proof-bundles" "${submit_body}")"
  jq . <<<"${submit_json}" >"${DEMO_TMP_DIR}/submit_proof_bundle_response.json"

  local final_status completion_status completion_body credential_json access_credential proxy_body
  final_status="$(jq -r '.status' <<<"${submit_json}")"
  completion_body="$(jq -nc --arg creator "${creator}" --arg bundle_id "${bundle_id}" '{creator: $creator, bundle_id: $bundle_id}')"
  completion_status="$(json_post_capture_status "/verification-task-completions" "${completion_body}" "${DEMO_TMP_DIR}/completion_response.json")"

  if [[ "${completion_status}" == 2* ]]; then
    final_status="$(jq -r '.status' "${DEMO_TMP_DIR}/completion_response.json")"
    if [[ "${final_status}" == "completed" ]]; then
      echo "Issuing access credential" >&2
      credential_json="$(json_post "/access-credentials" "${completion_body}")"
      jq . <<<"${credential_json}" >"${DEMO_TMP_DIR}/access_credential_response.json"
      access_credential="$(jq -r '.credential' <<<"${credential_json}")"
      local proxy_status
      proxy_status="$(locks_get "/priv-resources/content/example.txt" "${DEMO_TMP_DIR}/proxy_read_body.txt" -H "authorization: Bearer ${access_credential}")"
      if [[ "${proxy_status}" != 2* ]]; then
        echo "Unexpected HTTP ${proxy_status}: GET /priv-resources/content/example.txt" >&2
        cat "${DEMO_TMP_DIR}/proxy_read_body.txt" >&2 || true
        exit 1
      fi
      proxy_body="$(cat "${DEMO_TMP_DIR}/proxy_read_body.txt")"
      jq -n \
        --arg creator "${creator}" \
        --arg bundle_id "${bundle_id}" \
        --arg content_lock_path "${content_lock_path}" \
        --arg private_resource_path "${guarded_resource_path}" \
        --arg status "completed" \
        --arg proxy_read_body "${proxy_body}" \
        '{creator: $creator, bundle_id: $bundle_id, content_lock_path: $content_lock_path, private_resource_path: $private_resource_path, verification_status: $status, proxy_read_body: $proxy_read_body}'
      return
    fi
  else
    echo "Verification completion route unavailable or rejected request (HTTP ${completion_status}); leaving proof bundle in ${final_status} state." >&2
    jq . "${DEMO_TMP_DIR}/completion_response.json" >&2 || true
  fi

  jq -n \
    --arg creator "${creator}" \
    --arg bundle_id "${bundle_id}" \
    --arg content_lock_path "${content_lock_path}" \
    --arg private_resource_path "${guarded_resource_path}" \
    --arg status "${final_status}" \
    --arg limitation "access credential/proxy-read skipped because verification completion is not available in the running server configuration" \
    '{creator: $creator, bundle_id: $bundle_id, content_lock_path: $content_lock_path, private_resource_path: $private_resource_path, verification_status: $status, limitation: $limitation}'
}

case "${MODE}" in
  auth) run_auth ;;
  locked-content) run_locked_content ;;
esac

if [[ "${KEEP_DEMO_TMP}" == "1" ]]; then
  echo "tmp_dir: ${DEMO_TMP_DIR}" >&2
fi
