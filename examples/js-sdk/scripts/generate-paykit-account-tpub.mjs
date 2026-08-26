#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import { repoRoot } from './lib/paths.mjs';

const WALLET_NAME = 'paykit-creator';
const COMPOSE_FILE = 'compose.paykit-local-demo.yaml';
const MAX_OUTPUT_BYTES = 1024 * 1024;
const COMMAND_TIMEOUT_MS = 30_000;
const TESTNET_ACCOUNT_XPUB = /^(tpub[1-9A-HJ-NP-Za-km-z]{107})$/;
const EXTERNAL_BIP84_TESTNET_DESCRIPTOR = /^wpkh\(\[[0-9a-fA-F]{8}\/84h\/1h\/(0|[1-9][0-9]*)h\](tpub[1-9A-HJ-NP-Za-km-z]{107})\/0\/\*\)#[0-9a-z]{8}$/;

export function extractBip84AccountXpub(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || !Array.isArray(value.descriptors)) {
    throw new Error('Bitcoin Core returned an invalid descriptor response');
  }

  const matches = value.descriptors.flatMap((entry) => {
    if (
      !entry
      || typeof entry !== 'object'
      || entry.active !== true
      || entry.internal !== false
      || typeof entry.desc !== 'string'
    ) return [];
    const match = EXTERNAL_BIP84_TESTNET_DESCRIPTOR.exec(entry.desc);
    if (!match || !TESTNET_ACCOUNT_XPUB.test(match[2])) return [];
    const accountIndex = Number(match[1]);
    if (!Number.isSafeInteger(accountIndex) || accountIndex > 0xffff_ffff) return [];
    return [{ accountXpub: match[2], accountIndex }];
  });

  if (matches.length !== 1) {
    throw new Error('Bitcoin wallet must expose exactly one active external BIP84 testnet account descriptor');
  }
  return matches[0];
}

export function generatePaykitAccountXpub({
  run = runBitcoinDescriptorCommand,
} = {}) {
  const result = run();
  if (!result || result.status !== 0 || result.signal) {
    throw new Error('could not create or inspect the local Paykit Bitcoin wallet');
  }
  if (Buffer.byteLength(result.stdout ?? '', 'utf8') > MAX_OUTPUT_BYTES) {
    throw new Error('Bitcoin Core descriptor response exceeded the output limit');
  }

  let response;
  try {
    response = JSON.parse(result.stdout);
  } catch {
    throw new Error('Bitcoin Core returned an invalid descriptor response');
  }
  return extractBip84AccountXpub(response);
}

function runBitcoinDescriptorCommand() {
  const script = `
set -euo pipefail
wallet=${WALLET_NAME}
cli=(bitcoin-cli -conf="$BITCOIN_DATA/bitcoin.conf" -regtest)
if ! "\${cli[@]}" -rpcwallet="$wallet" getwalletinfo >/dev/null 2>&1; then
  "\${cli[@]}" loadwallet "$wallet" >/dev/null 2>&1 || \
    "\${cli[@]}" -named createwallet wallet_name="$wallet" descriptors=true load_on_startup=true >/dev/null
fi
"\${cli[@]}" -rpcwallet="$wallet" listdescriptors false
`;
  return spawnSync(
    'docker',
    ['compose', '-f', COMPOSE_FILE, 'exec', '-T', 'bitcoin', '/bin/bash', '-euc', script],
    {
      cwd: repoRoot,
      encoding: 'utf8',
      shell: false,
      timeout: COMMAND_TIMEOUT_MS,
      maxBuffer: MAX_OUTPUT_BYTES,
      env: {
        PATH: process.env.PATH,
        DOCKER_HOST: process.env.DOCKER_HOST,
        DOCKER_CONTEXT: process.env.DOCKER_CONTEXT,
        DOCKER_CONFIG: process.env.DOCKER_CONFIG,
      },
    },
  );
}

async function main() {
  try {
    const { accountXpub, accountIndex } = generatePaykitAccountXpub();
    process.stdout.write(`Paykit account tpub: ${accountXpub}\nPaykit account index: ${accountIndex}\n`);
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : 'could not generate Paykit account tpub'}\n`);
    process.exitCode = 1;
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  await main();
}
