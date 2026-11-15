#!/bin/sh
set -eu
compose_file=compose.mixed-graph-payment-v2-e2e.yaml
prefix="${MIXED_GRAPH_PAYMENT_V2_PROJECT_PREFIX:-locks-mixed-v2-$$}"
compose_timeout="${MIXED_GRAPH_PAYMENT_V2_TIMEOUT_SECONDS:-2400}"
active_project=""

compose() {
  docker compose --project-name "$active_project" --file "$compose_file" "$@"
}

assert_project_absent() {
  project="$1"
  containers=$(docker ps --all --quiet --filter "label=com.docker.compose.project=$project") || return 1
  volumes=$(docker volume ls --quiet --filter "label=com.docker.compose.project=$project") || return 1
  networks=$(docker network ls --quiet --filter "label=com.docker.compose.project=$project") || return 1
  if [ -n "$containers$volumes$networks" ]; then
    echo "Compose project state remains: $project" >&2
    return 1
  fi
}

cleanup() {
  status=$?
  trap - EXIT INT TERM
  if [ -n "$active_project" ]; then
    set +e
    compose down --volumes --remove-orphans
    down_status=$?
    assert_project_absent "$active_project"
    absent_status=$?
    set -e
    if [ "$down_status" -ne 0 ] || [ "$absent_status" -ne 0 ]; then
      echo "Compose cleanup failed for $active_project" >&2
      exit 1
    fi
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

round=1
while [ "$round" -le 2 ]; do
  project="${prefix}-r${round}"
  assert_project_absent "$project"
  active_project="$project"
  compose config --quiet
  timeout "$compose_timeout" docker compose --project-name "$active_project" --file "$compose_file" up --build --abort-on-container-exit --exit-code-from mixed-e2e
  compose down --volumes --remove-orphans
  assert_project_absent "$active_project"
  echo "mixed graph+payment protocol-real round $round passed with Paykit and Locks restart receipts"
  active_project=""
  round=$((round + 1))
done
