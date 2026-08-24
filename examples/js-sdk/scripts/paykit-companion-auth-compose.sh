#!/bin/sh
set -eu

if [ "$#" -ne 0 ]; then
    echo "paykit-companion-auth-compose does not accept arguments" >&2
    exit 2
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/../../.." && pwd)

exec docker compose \
    --project-directory "$repo_root" \
    --file "$repo_root/compose.paykit-local-demo.yaml" \
    exec -T creator-demo \
    /usr/local/bin/paykit-companion-auth
