#!/usr/bin/env node
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  localStagingDemoStatePath,
  resetPaykitStagingDemoLocal,
} from './reset-paykit-staging-demo-local.mjs';

const root = await mkdtemp(join(tmpdir(), 'locks-staging-reset-'));
try {
  const target = localStagingDemoStatePath(root);
  const outside = join(root, '.local', 'paykit-local-demo-sentinel');
  await writeFile(join(root, 'outside-sentinel'), 'keep');
  await writeFile(outside, 'keep', { recursive: true }).catch(async () => {
    const { mkdir } = await import('node:fs/promises');
    await mkdir(join(root, '.local'), { recursive: true });
    await writeFile(outside, 'keep');
  });
  const { mkdir } = await import('node:fs/promises');
  await mkdir(join(target, 'creator-session'), { recursive: true });
  await writeFile(join(target, 'creator-session', 'session.json'), 'remove');

  const calls = [];
  const removed = await resetPaykitStagingDemoLocal({
    root,
    run: (args) => {
      calls.push(args);
      return { status: 0, signal: null };
    },
  });

  assert.equal(removed, target);
  assert.deepEqual(calls, [[
    'compose',
    '--file',
    'compose.paykit-staging-demo.yaml',
    'down',
    '--remove-orphans',
  ]]);
  await assert.rejects(readFile(join(target, 'creator-session', 'session.json')), { code: 'ENOENT' });
  assert.equal(await readFile(join(root, 'outside-sentinel'), 'utf8'), 'keep');
  assert.equal(await readFile(outside, 'utf8'), 'keep');

  await assert.rejects(
    resetPaykitStagingDemoLocal({
      root,
      run: () => ({ status: 1, signal: null }),
    }),
    /could not stop local staging-demo containers/,
  );
} finally {
  await rm(root, { recursive: true, force: true });
}

console.log('local staging-demo reset tests passed');
