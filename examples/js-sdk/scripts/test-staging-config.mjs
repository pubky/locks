#!/usr/bin/env node
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  STAGING_CREATOR_ORIGIN,
  STAGING_LOCKS_ORIGIN,
  STAGING_PAYKIT_ORIGIN,
  STAGING_READER_ORIGIN,
  buildStagingDemoConfig,
  refreshStagingDemoConfig,
  validateStagingLocksDiscovery,
} from './lib/staging-config.mjs';

const lockServer = 'pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo';
const discovery = {
  service: 'pubky-locks-server',
  api_version: '0.1',
  lock_server: lockServer,
};

assert.deepEqual(validateStagingLocksDiscovery(discovery), discovery);
assert.throws(
  () => validateStagingLocksDiscovery({ ...discovery, service: 'other' }),
  /invalid Locks discovery/,
);
assert.throws(
  () => validateStagingLocksDiscovery({ ...discovery, api_version: '9.9' }),
  /invalid Locks discovery/,
);
assert.throws(
  () => validateStagingLocksDiscovery({ ...discovery, lock_server: 'not-a-pubky' }),
  /invalid Locks discovery/,
);
assert.throws(
  () => validateStagingLocksDiscovery({ ...discovery, lock_server: `${lockServer.slice(0, -1)}t` }),
  /invalid Locks discovery/,
);

assert.deepEqual(buildStagingDemoConfig(discovery), {
  mode: 'staging',
  demoServer: { url: STAGING_CREATOR_ORIGIN },
  readerServer: { url: STAGING_READER_ORIGIN },
  lockServer: {
    url: STAGING_LOCKS_ORIGIN,
    pubky: lockServer,
  },
  paykit: { url: STAGING_PAYKIT_ORIGIN },
});

const root = await mkdtemp(join(tmpdir(), 'locks-staging-config-'));
try {
  const output = join(root, 'config', 'config.json');
  const requests = [];
  const result = await refreshStagingDemoConfig({
    output,
    fetchImpl: async (url, options) => {
      requests.push({ url, options });
      return new Response(JSON.stringify(discovery), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    },
  });
  assert.deepEqual(result, buildStagingDemoConfig(discovery));
  assert.equal(requests.length, 1);
  assert.equal(requests[0].url, `${STAGING_LOCKS_ORIGIN}/.well-known/locks-server`);
  assert.equal(requests[0].options.method, 'GET');
  assert.equal(requests[0].options.cache, 'no-store');
  assert.equal(requests[0].options.redirect, 'error');
  assert.equal(requests[0].options.signal instanceof AbortSignal, true);
  assert.deepEqual(JSON.parse(await readFile(output, 'utf8')), result);

  await writeFile(output, '{"stale":true}\n');
  await assert.rejects(
    refreshStagingDemoConfig({
      output,
      fetchImpl: async () => new Response('unavailable', { status: 503 }),
    }),
    /Locks discovery failed/,
  );
  await assert.rejects(readFile(output, 'utf8'), { code: 'ENOENT' });

  await writeFile(output, '{"stale":true}\n');
  await assert.rejects(
    refreshStagingDemoConfig({
      output,
      fetchImpl: async () => new Response(JSON.stringify({ ...discovery, api_version: '2' }), { status: 200 }),
    }),
    /invalid Locks discovery/,
  );
  await assert.rejects(readFile(output, 'utf8'), { code: 'ENOENT' });

  await assert.rejects(
    refreshStagingDemoConfig({
      output,
      fetchImpl: async () => new Response(JSON.stringify(discovery), {
        status: 200,
        headers: { 'content-type': 'text/plain' },
      }),
    }),
    /invalid Locks discovery/,
  );

  let oversizedCancelled = false;
  await assert.rejects(
    refreshStagingDemoConfig({
      output,
      fetchImpl: async () => new Response(new ReadableStream({
        start(controller) {
          controller.enqueue(new Uint8Array(64 * 1024));
          controller.enqueue(new Uint8Array(1));
        },
        cancel() {
          oversizedCancelled = true;
        },
      }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    }),
    /invalid Locks discovery/,
  );
  assert.equal(oversizedCancelled, true);

  await writeFile(output, '{"stale":true}\n');
  await assert.rejects(
    refreshStagingDemoConfig({
      output,
      timeoutMs: 5,
      fetchImpl: async (_url, { signal }) => {
        await new Promise((resolve) => setTimeout(resolve, 20));
        signal.throwIfAborted();
        return new Response(JSON.stringify(discovery), { status: 200 });
      },
    }),
    /Locks discovery failed/,
  );
  await assert.rejects(readFile(output, 'utf8'), { code: 'ENOENT' });
} finally {
  await rm(root, { recursive: true, force: true });
}

console.log('staging config tests passed');
