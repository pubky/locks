#!/usr/bin/env bash
set -euo pipefail

: "${BITCOIN_RPC_USER:?BITCOIN_RPC_USER is required}"
: "${BITCOIN_RPC_PASSWORD:?BITCOIN_RPC_PASSWORD is required}"

umask 077
rpc_config="$(mktemp)"
cleanup() {
  rm -f -- "$rpc_config"
}
trap cleanup EXIT INT TERM
printf 'rpcuser=%s\nrpcpassword=%s\n' "$BITCOIN_RPC_USER" "$BITCOIN_RPC_PASSWORD" > "$rpc_config"

bitcoin_cli() {
  bitcoin-cli -conf="$rpc_config" -rpcconnect=127.0.0.1 -rpcport=18443 -regtest "$@"
}

for _ in $(seq 1 120); do
  if bitcoin_cli getblockchaininfo >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
bitcoin_cli getblockchaininfo >/dev/null 2>&1 || {
  printf '%s\n' 'bitcoin bootstrap failed: RPC unavailable' >&2
  exit 1
}

if ! bitcoin_cli listwalletdir | grep -q '"name": "miner"'; then
  bitcoin_cli createwallet miner >/dev/null
elif ! bitcoin_cli listwallets | grep -q '"miner"'; then
  bitcoin_cli loadwallet miner >/dev/null
fi

height="$(bitcoin_cli getblockcount)"
if (( height < 101 )); then
  address="$(bitcoin_cli -rpcwallet=miner getnewaddress)"
  bitcoin_cli -rpcwallet=miner generatetoaddress "$((101 - height))" "$address" >/dev/null
fi

printf '%s\n' 'bitcoin bootstrap ready'
