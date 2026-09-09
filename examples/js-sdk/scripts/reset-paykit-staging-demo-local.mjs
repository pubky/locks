#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import { rm } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { repoRoot } from './lib/paths.mjs';

const COMPOSE_FILE = 'compose.paykit-staging-demo.yaml';

export function localStagingDemoStatePath(root = repoRoot) {
  return join(root, '.local', 'paykit-staging-demo');
}

export async function resetPaykitStagingDemoLocal({
  root = repoRoot,
  run = runCompose,
} = {}) {
  const stopped = run([
    'compose',
    '--file',
    COMPOSE_FILE,
    'down',
    '--remove-orphans',
  ], root);
  if (stopped.error || stopped.status !== 0 || stopped.signal) {
    throw new Error('could not stop local staging-demo containers');
  }

  const target = localStagingDemoStatePath(root);
  await rm(target, { recursive: true, force: true });
  return target;
}

function runCompose(args, cwd) {
  return spawnSync('docker', args, {
    cwd,
    shell: false,
    stdio: 'inherit',
  });
}

async function main() {
  try {
    await resetPaykitStagingDemoLocal();
    process.stdout.write('Reset local Paykit staging-demo client state only. Remote staging state was not changed.\n');
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : 'local staging-demo reset failed'}\n`);
    process.exitCode = 1;
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) await main();
