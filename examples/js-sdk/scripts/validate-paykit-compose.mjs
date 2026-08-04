#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import { repoRoot } from './lib/paths.mjs';

const MAX_MODEL_BYTES = 2 * 1024 * 1024;
const COMPOSE_FILE = 'compose.paykit-local-demo.yaml';
const REQUIRED_SERVICES = [
  'postgres',
  'paykit-postgres',
  'bitcoin',
  'bitcoin-bootstrap',
  'fulcrum',
  'electrum-readiness',
  'pubky-testnet',
  'locks-server',
  'paykit-config',
  'demo-config',
  'paykit-server',
  'creator-demo',
  'reader-demo',
];

export function validateSafeComposeModel(model) {
  if (!model || typeof model !== 'object' || Array.isArray(model) || !model.services) {
    throw new Error('Compose returned an invalid model');
  }
  for (const service of REQUIRED_SERVICES) {
    if (!model.services[service]) throw new Error(`Compose model is missing service ${service}`);
  }
  for (const [name, service] of Object.entries(model.services)) {
    if (typeof service.image === 'string' && !service.build && !service.image.includes('@sha256:')) {
      throw new Error(`external image for ${name} is not digest-pinned`);
    }
  }
  for (const name of [
    'bitcoin-bootstrap',
    'electrum-readiness',
    'paykit-config',
    'demo-config',
    'creator-demo',
    'reader-demo',
  ]) {
    const service = model.services[name];
    if (String(service.user ?? '') !== '1000:1000') {
      throw new Error(`${name} must run as an unprivileged user`);
    }
  }
  for (const name of ['creator-demo', 'reader-demo']) {
    const service = model.services[name];
    for (const mount of service.volumes ?? []) {
      if (
        mount.source === '.'
        || mount.source === 'lock-home'
        || mount.source === 'pubky-locks-demo-identity'
        || mount.source === 'locks_lock-home'
        || mount.target === '/workspace'
        || mount.target === '/workspace/.local'
        || mount.target === '/root'
        || mount.target === '/var/lib/pubky-lock'
      ) {
        throw new Error(`${name} crosses a private source or identity boundary`);
      }
    }
  }
  const readerTargets = new Set(
    (model.services['reader-demo'].volumes ?? []).map((mount) => mount.target),
  );
  for (const privateTarget of [
    '/workspace/.local/js-sdk-demo',
    '/workspace/.local/content-creator',
  ]) {
    if (readerTargets.has(privateTarget)) {
      throw new Error('reader-demo crosses a creator-private state boundary');
    }
  }
  const bootstrapHome = (model.services['bitcoin-bootstrap'].volumes ?? []).find(
    (mount) => mount.target === '/home/bitcoin/.bitcoin',
  );
  if (
    bootstrapHome?.type !== 'bind'
    || !bootstrapHome.source.endsWith('/.local/bitcoin-bootstrap')
  ) {
    throw new Error('bitcoin-bootstrap must use reset-managed scratch state');
  }
  for (const port of model.services['pubky-testnet'].ports ?? []) {
    if (port.host_ip !== '127.0.0.1') throw new Error('published demo ports must bind to loopback');
  }
  return model;
}

export function validatePaykitCompose({ run = runCompose } = {}) {
  const quiet = run(['compose', '-f', COMPOSE_FILE, 'config', '--quiet'], { capture: false });
  if (quiet.error || quiet.status !== 0 || quiet.signal) {
    throw new Error('Compose quiet validation failed');
  }
  const rendered = run(
    ['compose', '-f', COMPOSE_FILE, 'config', '--no-env-resolution', '--format', 'json'],
    { capture: true },
  );
  if (rendered.error || rendered.status !== 0 || rendered.signal) {
    throw new Error('Compose safe model rendering failed');
  }
  if (Buffer.byteLength(rendered.stdout ?? '', 'utf8') > MAX_MODEL_BYTES) {
    throw new Error('Compose model exceeded the output limit');
  }
  let model;
  try {
    model = JSON.parse(rendered.stdout);
  } catch {
    throw new Error('Compose returned invalid JSON');
  }
  validateSafeComposeModel(model);
}

function runCompose(args, { capture }) {
  const environment = Object.fromEntries(Object.entries({
    HOME: process.env.HOME,
    PATH: process.env.PATH,
    DOCKER_HOST: process.env.DOCKER_HOST,
    DOCKER_CONTEXT: process.env.DOCKER_CONTEXT,
    DOCKER_CONFIG: process.env.DOCKER_CONFIG,
  }).filter(([, value]) => typeof value === 'string'));
  return spawnSync('docker', args, {
    cwd: repoRoot,
    shell: false,
    encoding: capture ? 'utf8' : undefined,
    stdio: capture ? ['ignore', 'pipe', 'ignore'] : 'ignore',
    timeout: 30_000,
    maxBuffer: MAX_MODEL_BYTES,
    env: environment,
  });
}

function main() {
  try {
    validatePaykitCompose();
    process.stdout.write('Paykit Compose validation passed\n');
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : 'Compose validation failed'}\n`);
    process.exitCode = 1;
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) main();
