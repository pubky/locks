import { rm } from 'node:fs/promises';
import { PublicKey } from '@synonymdev/pubky';

import { writeJson } from './paths.mjs';

export const STAGING_CREATOR_ORIGIN = 'http://127.0.0.1:8080';
export const STAGING_READER_ORIGIN = 'http://127.0.0.1:8088';
export const STAGING_LOCKS_ORIGIN = 'https://locks.staging.pubky.app';
export const STAGING_PAYKIT_ORIGIN = 'https://paykit.staging.pubky.app';

const LOCKS_DISCOVERY_PATH = '/.well-known/locks-server';
const LOCKS_SERVICE = 'pubky-locks-server';
const LOCKS_API_VERSION = '0.1';
const DEFAULT_TIMEOUT_MS = 15_000;
const MAX_DISCOVERY_BYTES = 64 * 1024;

export function validateStagingLocksDiscovery(value) {
  if (
    value === null
    || typeof value !== 'object'
    || Array.isArray(value)
    || value.service !== LOCKS_SERVICE
    || value.api_version !== LOCKS_API_VERSION
    || typeof value.lock_server !== 'string'
    || !isCanonicalPubky(value.lock_server)
  ) {
    throw new Error('invalid Locks discovery');
  }
  return {
    service: value.service,
    api_version: value.api_version,
    lock_server: value.lock_server,
  };
}

function isCanonicalPubky(value) {
  let publicKey;
  try {
    publicKey = PublicKey.from(value);
    return publicKey.toString() === value;
  } catch {
    return false;
  } finally {
    publicKey?.free();
  }
}

export function buildStagingDemoConfig(value) {
  const discovery = validateStagingLocksDiscovery(value);
  return {
    mode: 'staging',
    demoServer: { url: STAGING_CREATOR_ORIGIN },
    readerServer: { url: STAGING_READER_ORIGIN },
    lockServer: {
      url: STAGING_LOCKS_ORIGIN,
      pubky: discovery.lock_server,
    },
    paykit: { url: STAGING_PAYKIT_ORIGIN },
  };
}

export async function refreshStagingDemoConfig({
  output,
  fetchImpl = fetch,
  timeoutMs = DEFAULT_TIMEOUT_MS,
} = {}) {
  if (typeof output !== 'string' || output.length === 0) {
    throw new Error('staging config output is required');
  }

  await rm(output, { force: true });

  let response;
  try {
    response = await fetchImpl(`${STAGING_LOCKS_ORIGIN}${LOCKS_DISCOVERY_PATH}`, {
      method: 'GET',
      cache: 'no-store',
      redirect: 'error',
      signal: AbortSignal.timeout(timeoutMs),
    });
  } catch {
    throw new Error('Locks discovery failed');
  }
  if (!response.ok) throw new Error('Locks discovery failed');

  let discovery;
  try {
    discovery = validateStagingLocksDiscovery(await readBoundedDiscoveryJson(response));
  } catch (error) {
    if (error instanceof Error && error.message === 'invalid Locks discovery') throw error;
    throw new Error('invalid Locks discovery');
  }

  const config = buildStagingDemoConfig(discovery);
  await writeJson(output, config);
  return config;
}

async function readBoundedDiscoveryJson(response) {
  const contentType = response.headers.get('content-type')?.split(';', 1)[0].trim();
  if (contentType !== 'application/json' || !response.body) {
    throw new Error('invalid Locks discovery');
  }
  const reader = response.body.getReader();
  const chunks = [];
  let size = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > MAX_DISCOVERY_BYTES) {
        await reader.cancel();
        throw new Error('invalid Locks discovery');
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  try {
    return JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw new Error('invalid Locks discovery');
  }
}
