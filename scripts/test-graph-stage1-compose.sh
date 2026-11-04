#!/bin/sh
set -eu

compose_file="compose.graph-stage1-e2e.yaml"
project="${GRAPH_E2E_COMPOSE_PROJECT:-locks-graph-stage1-e2e}"
cleanup() {
  docker compose --project-name "$project" --file "$compose_file" down --remove-orphans
}
trap cleanup EXIT INT TERM
cleanup
docker compose --project-name "$project" --file "$compose_file" config --quiet
docker compose --project-name "$project" --file "$compose_file" up \
  --abort-on-container-exit \
  --exit-code-from graph-e2e
