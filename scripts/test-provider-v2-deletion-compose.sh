#!/bin/sh
set -eu
compose_file=compose.provider-v2-deletion-e2e.yaml
prefix="${PROVIDER_V2_DELETION_PROJECT_PREFIX:-locks-card3c-cross}"
compose_timeout="${PROVIDER_V2_DELETION_COMPOSE_TIMEOUT_SECONDS:-1800}"

assert_project_absent() {
  project="$1"
  containers=$(docker ps --all --quiet --filter "label=com.docker.compose.project=$project")
  volumes=$(docker volume ls --quiet --filter "label=com.docker.compose.project=$project")
  networks=$(docker network ls --quiet --filter "label=com.docker.compose.project=$project")
  if [ -n "$containers$volumes$networks" ]; then
    echo "refusing to reuse non-fresh Compose project: $project" >&2
    exit 1
  fi
}

round=1
while [ "$round" -le 2 ]; do
  for scenario in create-first drain-first; do
    project="${prefix}-r${round}-${scenario}"
    assert_project_absent "$project"
    PROVIDER_V2_DELETION_SCENARIO="$scenario" docker compose --project-name "$project" --file "$compose_file" config --quiet
    PROVIDER_V2_DELETION_SCENARIO="$scenario" timeout "$compose_timeout" docker compose --project-name "$project" --file "$compose_file" up --build --abort-on-container-exit --exit-code-from provider-v2-deletion-e2e
  done
  round=$((round + 1))
done
