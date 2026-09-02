#!/usr/bin/env node
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

import {
  STAGING_COMPOSE_NAME,
  STAGING_DEMO_IMAGE,
  validateSafeStagingComposeModel,
  validateStagingCompose,
} from './validate-staging-compose.mjs';
import { repoRoot } from './lib/paths.mjs';

const build = {
  context: repoRoot,
  dockerfile: 'docker/js-staging-demo.Dockerfile',
};
const configMount = {
  type: 'bind',
  source: '/repo/.local/paykit-staging-demo/config',
  target: '/workspace/.local/paykit-staging-demo/config',
  read_only: true,
};
const model = {
  name: STAGING_COMPOSE_NAME,
  services: {
    'staging-config': {
      build,
      image: STAGING_DEMO_IMAGE,
      user: '0:0',
      volumes: [{
        type: 'bind',
        source: '/repo/.local/paykit-staging-demo',
        target: '/workspace/.local/paykit-staging-demo',
      }],
    },
    'creator-demo': {
      build,
      image: STAGING_DEMO_IMAGE,
      command: ['npm', '--prefix', 'examples/js-sdk', 'run', 'start-server', '--', '--external-wallet', '--staging'],
      user: '1000:1000',
      depends_on: { 'staging-config': { condition: 'service_completed_successfully' } },
      ports: [{ host_ip: '127.0.0.1', published: '8080', target: 8080 }],
      volumes: [
        configMount,
        {
          type: 'bind',
          source: '/repo/.local/paykit-staging-demo/creator-session',
          target: '/workspace/.local/paykit-staging-demo/creator-session',
        },
      ],
      environment: { LOCKS_DEMO_MODE: 'staging' },
    },
    'reader-demo': {
      build,
      image: STAGING_DEMO_IMAGE,
      command: ['npm', '--prefix', 'examples/js-sdk', 'run', 'start-reader-server', '--', '--staging'],
      user: '1000:1000',
      depends_on: { 'staging-config': { condition: 'service_completed_successfully' } },
      ports: [{ host_ip: '127.0.0.1', published: '8088', target: 8088 }],
      volumes: [configMount],
      environment: { LOCKS_DEMO_MODE: 'staging' },
    },
  },
};

assert.equal(validateSafeStagingComposeModel(structuredClone(model)), model.name);

const extraService = structuredClone(model);
extraService.services.postgres = { image: 'postgres:17' };
assert.throws(() => validateSafeStagingComposeModel(extraService), /exactly staging-config, creator-demo, and reader-demo/);

const helperContext = structuredClone(model);
helperContext.services['creator-demo'].build.additional_contexts = { 'paykit-runtime': 'service:paykit-server' };
assert.throws(() => validateSafeStagingComposeModel(helperContext), /helper-free staging Dockerfile/);

const duplicateImage = structuredClone(model);
duplicateImage.services['reader-demo'].image = 'other-image:local';
assert.throws(() => validateSafeStagingComposeModel(duplicateImage), /reuse one staging demo image/);

const exposedPort = structuredClone(model);
exposedPort.services['reader-demo'].ports[0].host_ip = '0.0.0.0';
assert.throws(() => validateSafeStagingComposeModel(exposedPort), /loopback/);

const extraPort = structuredClone(model);
extraPort.services['reader-demo'].ports.push({ host_ip: '127.0.0.1', published: '9999', target: 9999 });
assert.throws(() => validateSafeStagingComposeModel(extraPort), /exactly one published port/);

const readerCreatorState = structuredClone(model);
readerCreatorState.services['reader-demo'].volumes.push({
  type: 'bind',
  source: '/repo/.local/paykit-staging-demo/creator-session',
  target: '/workspace/.local/paykit-staging-demo/creator-session',
});
assert.throws(() => validateSafeStagingComposeModel(readerCreatorState), /reader-demo.*Creator session/);

const writableConfig = structuredClone(model);
writableConfig.services['reader-demo'].volumes[0].read_only = false;
assert.throws(() => validateSafeStagingComposeModel(writableConfig), /config read-only/);

const extraMount = structuredClone(model);
extraMount.services['reader-demo'].volumes.push({ type: 'bind', source: '/tmp', target: '/tmp' });
assert.throws(() => validateSafeStagingComposeModel(extraMount), /unexpected mount/);

const wrongCommand = structuredClone(model);
wrongCommand.services['reader-demo'].command = ['node', 'other.js'];
assert.throws(() => validateSafeStagingComposeModel(wrongCommand), /reader-demo command/);

validateStagingCompose();

const [composeSource, dockerfileSource] = await Promise.all([
  readFile(`${repoRoot}/compose.paykit-staging-demo.yaml`, 'utf8'),
  readFile(`${repoRoot}/docker/js-staging-demo.Dockerfile`, 'utf8'),
]);
for (const source of [composeSource, dockerfileSource]) {
  assert.doesNotMatch(source, /paykit-runtime|paykit-companion-auth|paykit-reader-demo/);
}
assert.match(composeSource, /^name: pubky-locks-paykit-staging-demo$/m);
assert.match(dockerfileSource, /wasm-pack build --target web --out-dir pkg/);
console.log('staging Compose tests passed');
