#!/bin/sh
set -eu

compose_file="compose.payment-only-v2-e2e.yaml"
project="${PAYMENT_V2_E2E_COMPOSE_PROJECT:-locks-payment-only-v2-e2e}"

cleanup() {
  docker compose --project-name "$project" --file "$compose_file" down --volumes --remove-orphans
}
trap cleanup EXIT INT TERM

cleanup
docker compose --project-name "$project" --file "$compose_file" config --quiet
docker compose --project-name "$project" --file "$compose_file" up \
  --abort-on-container-exit \
  --exit-code-from payment-v2-e2e
