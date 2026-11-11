#!/bin/sh
set -eu
compose_file=compose.provider-v2-deletion-e2e.yaml
prefix="${PROVIDER_V2_DELETION_PROJECT_PREFIX:-locks-card3c-cross}"
compose_timeout="${PROVIDER_V2_DELETION_COMPOSE_TIMEOUT_SECONDS:-1800}"
project=
scenario=
scenario_status=0

assert_project_absent() {
  checked_project="$1"
  containers=$(docker ps --all --quiet --filter "label=com.docker.compose.project=$checked_project")
  volumes=$(docker volume ls --quiet --filter "label=com.docker.compose.project=$checked_project")
  networks=$(docker network ls --quiet --filter "label=com.docker.compose.project=$checked_project")
  if [ -n "$containers$volumes$networks" ]; then
    echo "refusing to reuse non-fresh Compose project: $checked_project" >&2
    return 1
  fi
}

cleanup_active_project() {
  [ -n "$project" ] || return 0
  cleanup_status=0
  PROVIDER_V2_DELETION_SCENARIO="$scenario" docker compose \
    --project-name "$project" \
    --file "$compose_file" \
    down --volumes --remove-orphans || cleanup_status=$?
  assert_project_absent "$project" || {
    [ "$cleanup_status" -ne 0 ] || cleanup_status=1
  }
  return "$cleanup_status"
}

cleanup_on_exit() {
  exit_status=$?
  trap - EXIT INT TERM
  cleanup_status=0
  cleanup_active_project || cleanup_status=$?
  if [ "$scenario_status" -ne 0 ]; then
    exit "$scenario_status"
  fi
  if [ "$exit_status" -ne 0 ]; then
    exit "$exit_status"
  fi
  exit "$cleanup_status"
}

trap cleanup_on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

round=1
while [ "$round" -le 2 ]; do
  for scenario in create-first drain-first; do
    candidate_project="${prefix}-r${round}-${scenario}"
    scenario_status=0
    assert_project_absent "$candidate_project"
    project="$candidate_project"
    PROVIDER_V2_DELETION_SCENARIO="$scenario" docker compose --project-name "$project" --file "$compose_file" config --quiet
    PROVIDER_V2_DELETION_SCENARIO="$scenario" timeout "$compose_timeout" docker compose --project-name "$project" --file "$compose_file" up --build --abort-on-container-exit --exit-code-from provider-v2-deletion-e2e || scenario_status=$?
    cleanup_status=0
    cleanup_active_project || cleanup_status=$?
    if [ "$scenario_status" -ne 0 ]; then
      exit "$scenario_status"
    fi
    if [ "$cleanup_status" -ne 0 ]; then
      exit "$cleanup_status"
    fi
    project=
    scenario=
  done
  round=$((round + 1))
done
