#!/usr/bin/env node
import { rm } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';

import { bitcoinBootstrapDir, paykitReaderDir, repoRoot } from './lib/paths.mjs';

const COMPOSE_FILE = 'compose.paykit-local-demo.yaml';
const dockerEnvironment = Object.fromEntries(Object.entries({
  HOME: process.env.HOME,
  PATH: process.env.PATH,
  DOCKER_HOST: process.env.DOCKER_HOST,
  DOCKER_CONTEXT: process.env.DOCKER_CONTEXT,
}).filter(([, value]) => typeof value === 'string'));
const disposableVolumes = [
  'pubky-locks-paykit-demo-locks-postgres',
  'pubky-locks-paykit-demo-paykit-postgres',
  'pubky-locks-paykit-demo-bitcoin',
  'pubky-locks-paykit-demo-fulcrum',
];

const result = spawnSync('docker', ['compose', '-f', COMPOSE_FILE, 'down', '--remove-orphans'], {
  cwd: repoRoot,
  shell: false,
  stdio: 'inherit',
  timeout: 120_000,
  env: dockerEnvironment,
});
if (result.error || result.status !== 0) {
  console.error('reset-paykit-demo failed: docker compose down failed');
  process.exitCode = 1;
} else {
  const removeVolumes = spawnSync('docker', ['volume', 'rm', '--force', ...disposableVolumes], {
    cwd: repoRoot,
    shell: false,
    stdio: 'ignore',
    timeout: 30_000,
    env: dockerEnvironment,
  });
  if (removeVolumes.error || removeVolumes.status !== 0) {
    console.error('reset-paykit-demo failed: disposable volume removal failed');
    process.exitCode = 1;
  } else {
    await Promise.all([
      rm(bitcoinBootstrapDir, { recursive: true, force: true }),
      rm(paykitReaderDir, { recursive: true, force: true }),
    ]);
    console.log('Paykit demo runtime reset; generated config and role identities preserved');
  }
}
