#!/usr/bin/env node
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

import {
  demoAuthRelayForConfig,
  pkarrRelaysForDemoConfig,
} from '../demo-network.js';
import {
  readDemoConfigPath,
  resolveCreatorSessionOptions,
} from './lib/demo-runtime.mjs';
import { pubkyForConfig } from './lib/pubky.mjs';
import { validateDemoConfig } from './lib/config.mjs';
import { repoRoot } from './lib/paths.mjs';
import {
  captureCreatorOperation,
  creatorOperationMatches,
} from '../creator-identity.js';

const lockServer = 'pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo';
const stagingConfig = {
  mode: 'staging',
  demoServer: { url: 'http://127.0.0.1:8080' },
  readerServer: { url: 'http://127.0.0.1:8088' },
  lockServer: { url: 'https://locks.staging.pubky.app', pubky: lockServer },
  paykit: { url: 'https://paykit.staging.pubky.app' },
};
const localConfig = {
  demoServer: { url: 'http://127.0.0.1:8080' },
  lockServer: { url: 'http://127.0.0.1:3000', pubky: lockServer, configPath: '/tmp/config.toml' },
  paykit: { url: 'http://127.0.0.1:3001' },
  testnet: {
    homeserver: lockServer,
    httpRelay: 'http://127.0.0.1:15412',
    pkarrRelay: 'http://127.0.0.1:15411',
    dhtBootstrap: '127.0.0.1:6881',
  },
};

assert.deepEqual(validateDemoConfig(structuredClone(stagingConfig)), stagingConfig);
assert.deepEqual(validateDemoConfig(structuredClone(localConfig)), localConfig);
assert.deepEqual(pkarrRelaysForDemoConfig(stagingConfig), []);
assert.deepEqual(pkarrRelaysForDemoConfig(localConfig), ['http://127.0.0.1:15411']);
assert.equal(demoAuthRelayForConfig(stagingConfig), undefined);
assert.equal(demoAuthRelayForConfig(localConfig), 'http://127.0.0.1:15412/inbox/');

class FakePubky {
  constructor() {
    this.kind = 'public';
  }

  static testnet(host) {
    return { kind: 'testnet', host };
  }
}
assert.equal(pubkyForConfig(stagingConfig, FakePubky).kind, 'public');
assert.deepEqual(pubkyForConfig(localConfig, FakePubky), { kind: 'testnet', host: '127.0.0.1' });

assert.equal(
  readDemoConfigPath({ LOCKS_DEMO_CONFIG_PATH: '/workspace/.local/paykit-staging-demo/config/config.json' }),
  '/workspace/.local/paykit-staging-demo/config/config.json',
);
assert.deepEqual(
  resolveCreatorSessionOptions({
    mode: 'staging',
    env: { LOCKS_DEMO_CREATOR_SESSION_PATH: '/workspace/.local/paykit-staging-demo/creator-session/content-creator-session.json' },
  }),
  {
    sessionPath: '/workspace/.local/paykit-staging-demo/creator-session/content-creator-session.json',
    profilePath: null,
  },
);
assert.throws(
  () => resolveCreatorSessionOptions({ mode: 'staging', env: {} }),
  /Creator session path is required/,
);

const creatorState = {
  creatorIdentityGeneration: 7,
  creatorPubky: lockServer,
  feLockSessionToken: 'frontend-session',
};
const operation = captureCreatorOperation(creatorState);
assert.equal(creatorOperationMatches(creatorState, operation), true);
assert.equal(creatorOperationMatches({ ...creatorState, creatorIdentityGeneration: 8 }, operation), false);
assert.equal(creatorOperationMatches({ ...creatorState, creatorPubky: 'other' }, operation), false);
assert.equal(creatorOperationMatches({ ...creatorState, feLockSessionToken: 'replacement' }, operation), false);

const [creatorServerSource, creatorAppSource] = await Promise.all([
  readFile(`${repoRoot}/examples/js-sdk/scripts/start-demo-server.mjs`, 'utf8'),
  readFile(`${repoRoot}/examples/js-sdk/app-iframe.js`, 'utf8'),
]);
assert.match(creatorServerSource, /readDemoConfig\(readDemoConfigPath\(\)\)/);
assert.match(creatorServerSource, /demoAuthRelayForConfig\(serviceConfig\)/);
assert.match(creatorServerSource, /resolveCreatorSessionOptions\(/);
assert.doesNotMatch(creatorAppSource, /state\.config\.testnet\.pkarrRelay/);
assert.match(creatorAppSource, /pkarrRelaysForDemoConfig\(state\.config\)/);

console.log('staging Creator mode tests passed');
