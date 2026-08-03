#!/usr/bin/env bash
set -euo pipefail

cat >&2 <<'EOF'
scripts/local-operator-demo.sh has been retired.

The unauthenticated local/dev creator publishing HTTP surface was removed.
Creator publishing now requires a Locks-local frontend session derived from
legacy-connect creator authority acquisition.

Use the current authenticated local flow instead:

  scripts/dev-legacy-connect-testnet.sh locked-content

For manual steps, see docs/LOCAL_OPERATOR_DEMO.md.
EOF

exit 2
