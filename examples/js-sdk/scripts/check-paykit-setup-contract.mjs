#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const PAYKIT_SERVER_REMOTE = 'https://github.com/pubky/paykit-server.git';
const PAYKIT_SERVER_REF = 'v0.1.0-rc2';
const RELEASE_REF = `refs/tags/${PAYKIT_SERVER_REF}`;
const MAX_GIT_OUTPUT_BYTES = 64 * 1024;
const MAX_SOURCE_BYTES = 256 * 1024;
const TIMEOUT_MS = 30_000;

export function parsePaykitReleaseRevision(output) {
  const match = /^([0-9a-f]{40})\trefs\/tags\/v0\.1\.0-rc2\n?$/u.exec(output);
  if (!match) throw new Error('Paykit Server returned an invalid release revision');
  return match[1];
}

export function validatePaykitSetupStatusSources({ setupStatusSource, serverSource }) {
  if (
    !setupStatusSource.includes('.route("/setup/status", post(status))')
    || !serverSource.includes('.merge(http::setup_status::setup_status_router(')
  ) {
    throw new Error('Paykit Server release does not provide the setup-status contract');
  }
}

export async function checkPaykitSetupContract({
  run = runGit,
  fetchSource = fetchBoundedText,
} = {}) {
  const revisionResult = run([
    'ls-remote',
    PAYKIT_SERVER_REMOTE,
    RELEASE_REF,
  ]);
  if (revisionResult.error || revisionResult.status !== 0 || revisionResult.signal) {
    throw new Error('Could not resolve Paykit Server release');
  }
  const revision = parsePaykitReleaseRevision(revisionResult.stdout ?? '');
  const sourceBase = `https://raw.githubusercontent.com/pubky/paykit-server/${revision}`;
  const [setupStatusSource, serverSource] = await Promise.all([
    fetchSource(`${sourceBase}/paykit-server/src/http/setup_status.rs`),
    fetchSource(`${sourceBase}/paykit-server/src/server.rs`),
  ]);
  validatePaykitSetupStatusSources({ setupStatusSource, serverSource });
  return revision;
}

function runGit(args) {
  return spawnSync('git', args, {
    shell: false,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'ignore'],
    timeout: TIMEOUT_MS,
    maxBuffer: MAX_GIT_OUTPUT_BYTES,
  });
}

async function fetchBoundedText(url) {
  const response = await fetch(url, { signal: AbortSignal.timeout(TIMEOUT_MS) });
  return readBoundedResponseText(response);
}

export async function readBoundedResponseText(response) {
  if (!response.ok) throw new Error('Could not read Paykit Server contract source');
  const declaredLength = Number(response.headers.get('content-length') ?? 0);
  if (declaredLength > MAX_SOURCE_BYTES) {
    throw new Error('Paykit Server contract source exceeded the size limit');
  }
  if (!response.body) return '';

  const bytes = new Uint8Array(MAX_SOURCE_BYTES);
  const reader = response.body.getReader();
  let length = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value.byteLength > MAX_SOURCE_BYTES - length) {
        try {
          await reader.cancel();
        } catch {
          // The size failure remains authoritative even if cancellation also fails.
        }
        throw new Error('Paykit Server contract source exceeded the size limit');
      }
      bytes.set(value, length);
      length += value.byteLength;
    }
  } finally {
    reader.releaseLock();
  }
  return new TextDecoder().decode(bytes.subarray(0, length));
}

async function main() {
  try {
    const revision = await checkPaykitSetupContract();
    process.stdout.write(`Paykit setup-status contract passed at ${revision.slice(0, 12)}\n`);
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : 'Paykit setup-status contract failed'}\n`);
    process.exitCode = 1;
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) await main();
