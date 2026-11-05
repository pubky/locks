#!/bin/sh
set -eu

compose_file="compose.payment-only-v2-e2e.yaml"
project="${PAYMENT_V2_E2E_COMPOSE_PROJECT:-locks-payment-only-v2-e2e}"

compose() {
  docker compose --project-name "$project" --file "$compose_file" "$@"
}

cleanup() {
  compose down --volumes --remove-orphans
}
trap cleanup EXIT INT TERM

compose config --quiet
round=1
while [ "$round" -le 2 ]; do
  cleanup
  compose up --detach postgres fake-provider-v2 locks-server
  compose run --rm -e PAYMENT_V2_E2E_PHASE=seed payment-v2-e2e
  compose restart locks-server
  compose run --rm -e PAYMENT_V2_E2E_PHASE=verify payment-v2-e2e
  round=$((round + 1))
done
