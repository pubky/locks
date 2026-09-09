#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import { repoRoot } from './lib/paths.mjs';

export const STAGING_COMPOSE_NAME = 'pubky-locks-paykit-staging-demo';
export const STAGING_DEMO_IMAGE = 'pubky-locks-paykit-staging-demo:local';
const COMPOSE_FILE = 'compose.paykit-staging-demo.yaml';
const STAGING_DOCKERFILE = 'docker/js-staging-demo.Dockerfile';
const REQUIRED_SERVICES = ['creator-demo', 'reader-demo', 'staging-config'];
const CREATOR_COMMAND = ['npm', '--prefix', 'examples/js-sdk', 'run', 'start-server', '--', '--external-wallet', '--staging'];
const READER_COMMAND = ['npm', '--prefix', 'examples/js-sdk', 'run', 'start-reader-server', '--', '--staging'];
const MAX_MODEL_BYTES = 1024 * 1024;

export function validateSafeStagingComposeModel(model) {
  if (!model || typeof model !== 'object' || Array.isArray(model) || !model.services) {
    throw new Error('Compose returned an invalid staging model');
  }
  if (model.name !== STAGING_COMPOSE_NAME) {
    throw new Error('staging Compose project name is not fixed');
  }
  const serviceNames = Object.keys(model.services).sort();
  if (JSON.stringify(serviceNames) !== JSON.stringify(REQUIRED_SERVICES)) {
    throw new Error('staging Compose must contain exactly staging-config, creator-demo, and reader-demo');
  }

  for (const [name, service] of Object.entries(model.services)) {
    if (
      service.build?.context !== repoRoot
      || service.build?.dockerfile !== STAGING_DOCKERFILE
      || service.build?.additional_contexts
    ) {
      throw new Error(`${name} must use the helper-free staging Dockerfile`);
    }
    if (service.image !== STAGING_DEMO_IMAGE) {
      throw new Error('all services must reuse one staging demo image');
    }
  }

  if (String(model.services['staging-config'].user ?? '') !== '0:0') {
    throw new Error('staging-config must own local bootstrap permissions');
  }
  for (const name of ['creator-demo', 'reader-demo']) {
    const service = model.services[name];
    if (String(service.user ?? '') !== '1000:1000') {
      throw new Error(`${name} must run unprivileged`);
    }
    if (service.depends_on?.['staging-config']?.condition !== 'service_completed_successfully') {
      throw new Error(`${name} must wait for successful staging config`);
    }
    if (service.environment?.LOCKS_DEMO_MODE !== 'staging') {
      throw new Error(`${name} must use staging demo mode`);
    }
    for (const port of service.ports ?? []) {
      if (port.host_ip !== '127.0.0.1') throw new Error('staging demo ports must bind to loopback');
    }
    if ((service.ports ?? []).length !== 1) {
      throw new Error(`${name} must have exactly one published port`);
    }
    const configMount = (service.volumes ?? []).find(
      (mount) => mount.target === '/workspace/.local/paykit-staging-demo/config',
    );
    if (!configMount?.read_only) throw new Error(`${name} must mount staging config read-only`);
  }

  if ((model.services['staging-config'].ports ?? []).length !== 0) {
    throw new Error('staging-config must not publish ports');
  }
  if (JSON.stringify(model.services['creator-demo'].command) !== JSON.stringify(CREATOR_COMMAND)) {
    throw new Error('invalid creator-demo command');
  }
  if (JSON.stringify(model.services['reader-demo'].command) !== JSON.stringify(READER_COMMAND)) {
    throw new Error('invalid reader-demo command');
  }

  const creatorPorts = model.services['creator-demo'].ports ?? [];
  const readerPorts = model.services['reader-demo'].ports ?? [];
  if (!creatorPorts.some((port) => String(port.published) === '8080' && Number(port.target) === 8080)) {
    throw new Error('creator-demo must publish loopback port 8080');
  }
  if (!readerPorts.some((port) => String(port.published) === '8088' && Number(port.target) === 8088)) {
    throw new Error('reader-demo must publish loopback port 8088');
  }

  const readerTargets = new Set(
    (model.services['reader-demo'].volumes ?? []).map((mount) => mount.target),
  );
  if (readerTargets.has('/workspace/.local/paykit-staging-demo/creator-session')) {
    throw new Error('reader-demo must not mount the Creator session');
  }
  if (
    readerTargets.size !== 1
    || !readerTargets.has('/workspace/.local/paykit-staging-demo/config')
  ) {
    throw new Error('reader-demo has an unexpected mount');
  }
  const creatorSession = (model.services['creator-demo'].volumes ?? []).find(
    (mount) => mount.target === '/workspace/.local/paykit-staging-demo/creator-session',
  );
  if (!creatorSession || creatorSession.read_only) {
    throw new Error('creator-demo must have isolated writable Creator session state');
  }
  const creatorTargets = new Set(
    (model.services['creator-demo'].volumes ?? []).map((mount) => mount.target),
  );
  if (
    creatorTargets.size !== 2
    || !creatorTargets.has('/workspace/.local/paykit-staging-demo/config')
    || !creatorTargets.has('/workspace/.local/paykit-staging-demo/creator-session')
  ) {
    throw new Error('creator-demo has an unexpected mount');
  }
  const stagingTargets = new Set(
    (model.services['staging-config'].volumes ?? []).map((mount) => mount.target),
  );
  if (
    stagingTargets.size !== 1
    || !stagingTargets.has('/workspace/.local/paykit-staging-demo')
  ) {
    throw new Error('staging-config has an unexpected mount');
  }

  const serialized = JSON.stringify(model);
  for (const forbidden of [
    'paykit-runtime',
    'paykit-companion-auth',
    'paykit-reader-demo',
    'PAYKIT_READER_WORKER_ENABLED',
    'PAYKIT_EXTERNAL_READER_PUBKY',
    'LOCKS_INTERNAL_',
  ]) {
    if (serialized.includes(forbidden)) throw new Error(`staging Compose contains forbidden ${forbidden} wiring`);
  }
  return model.name;
}

export function validateStagingCompose({ run = runCompose } = {}) {
  const quiet = run([
    'compose', '--env-file', '/dev/null', '--file', COMPOSE_FILE, 'config', '--quiet',
  ], { capture: false });
  if (quiet.error || quiet.status !== 0 || quiet.signal) {
    throw new Error('staging Compose quiet validation failed');
  }
  const rendered = run([
    'compose', '--env-file', '/dev/null', '--file', COMPOSE_FILE,
    'config', '--no-env-resolution', '--format', 'json',
  ], { capture: true });
  if (rendered.error || rendered.status !== 0 || rendered.signal) {
    throw new Error('staging Compose model rendering failed');
  }
  if (Buffer.byteLength(rendered.stdout ?? '', 'utf8') > MAX_MODEL_BYTES) {
    throw new Error('staging Compose model exceeded output limit');
  }
  let model;
  try {
    model = JSON.parse(rendered.stdout);
  } catch {
    throw new Error('staging Compose returned invalid JSON');
  }
  return validateSafeStagingComposeModel(model);
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
    validateStagingCompose();
    process.stdout.write('staging Compose validation passed\n');
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : 'staging Compose validation failed'}\n`);
    process.exitCode = 1;
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) main();
