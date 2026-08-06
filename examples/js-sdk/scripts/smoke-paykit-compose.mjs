#!/usr/bin/env node
import assert from 'node:assert/strict';
import { chmod, lstat, mkdir, mkdtemp, readFile, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  buildPaykitServerConfig,
  validatePaykitComposeEnvironment,
} from './lib/config.mjs';
import { repoRoot, writeSecret } from './lib/paths.mjs';
import { initializePaykitCompose } from './init-paykit-compose.mjs';
import { resolveReaderEnvironment } from './lib/paykit-reader-helper.mjs';
import { extractBip84AccountXpub } from './generate-paykit-account-tpub.mjs';
import { resolveCreatorStaticPath } from './lib/creator-static-path.mjs';
import { publishCreatorProfile } from './publish-creator-profile.mjs';

const lockServerPubky = 'pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo';
const creatorPubky = 'pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy';
const composeSource = await readFile(join(repoRoot, 'compose.paykit-local-demo.yaml'), 'utf8');
const defaultComposeSource = await readFile(join(repoRoot, 'docker-compose.yml'), 'utf8');
const creatorAppSource = await readFile(join(repoRoot, 'examples/js-sdk/app-iframe.js'), 'utf8');
const creatorServerSource = await readFile(join(repoRoot, 'examples/js-sdk/scripts/start-demo-server.mjs'), 'utf8');
const lockAuthoritySource = await readFile(join(repoRoot, 'locks-server/src/api/creator_authority.rs'), 'utf8');
assert.match(composeSource, /^# Local development and demonstration only;/);
assert.match(composeSource, /^name: pubky-locks-paykit-demo$/m);
assert.match(creatorAppSource, /hasExactKeys\(event\.data, \['type', 'state', 'code'\]\)/);
assert.match(creatorAppSource, /body: JSON\.stringify\(\{ level \}\)/);
assert.match(creatorAppSource, /\[result\.authorizationUrl, result\.command\]\.filter\(Boolean\)/);
assert.doesNotMatch(creatorServerSource, /JSON\.stringify\(entry\)|url\.search/);
assert.match(creatorServerSource, /if \(!externalWallet\) \{\s+result\.command =/);
assert.doesNotMatch(lockAuthoritySource, /dev: legacy-connect authorization URL/);
assert.match(defaultComposeSource, /^services:/);
assert.doesNotMatch(defaultComposeSource, /^  paykit-server:/m);
assert.match(composeSource, /PAYKIT_READER_RECEIVER_PATH: bitkit\/wallet/);
assert.doesNotMatch(composeSource, /PAYKIT_READER_RECEIVER_PATH: paykit\/reader/);
assert.equal(
  resolveCreatorStaticPath('/.local/compose-secrets.json', { repoRoot, examplesRoot: join(repoRoot, 'examples/js-sdk') }),
  null,
);
assert.equal(
  resolveCreatorStaticPath('/Cargo.toml', { repoRoot, examplesRoot: join(repoRoot, 'examples/js-sdk') }),
  null,
);
assert.equal(
  resolveCreatorStaticPath('/examples/js-sdk/index.html', { repoRoot, examplesRoot: join(repoRoot, 'examples/js-sdk') }),
  join(repoRoot, 'examples/js-sdk/index.html'),
);
assert.equal(
  resolveCreatorStaticPath('/examples/js-sdk/', { repoRoot, examplesRoot: join(repoRoot, 'examples/js-sdk') }),
  join(repoRoot, 'examples/js-sdk/index.html'),
);
const readerBaseEnvironment = {
  PAYKIT_READER_STATE_PATH: '/workspace/.local/paykit-reader/state.v1',
  PAYKIT_READER_PUBKY_TESTNET_HOST: 'pubky-testnet',
  PAYKIT_READER_RECEIVER_PATH: 'bitkit/wallet',
  PAYKIT_READER_SERVER_PATH: 'bitkit/server',
};
const testAccountXpub = `tpub${'A'.repeat(107)}`;
assert.deepEqual(
  extractBip84AccountXpub({
    walletName: 'paykit-creator',
    descriptors: [
      { active: true, internal: true, desc: `wpkh([01020304/84h/1h/0h]${testAccountXpub}/1/*)#internal` },
      { active: true, internal: false, desc: `wpkh([01020304/84h/1h/0h]${testAccountXpub}/0/*)#external` },
    ],
  }),
  { accountXpub: testAccountXpub, accountIndex: 0 },
);
assert.throws(
  () => extractBip84AccountXpub({
    walletName: 'paykit-creator',
    descriptors: [
      { active: true, internal: false, desc: `wpkh([01020304/84h/0h/0h]xpub${'A'.repeat(107)}/0/*)#mainnet` },
    ],
  }),
  /external BIP84 testnet account descriptor/,
);
assert.equal(
  (await resolveReaderEnvironment({
    env: readerBaseEnvironment,
    loadProfile: async (role) => ({ role, pubky: creatorPubky }),
  })).PAYKIT_READER_SERVER_PUBKY,
  creatorPubky,
);
await assert.rejects(
  resolveReaderEnvironment({
    env: readerBaseEnvironment,
    loadProfile: async () => ({ role: 'content-viewer', pubky: creatorPubky }),
  }),
  /content-creator profile/,
);
const config = buildPaykitServerConfig({ lockServerPubky });
assert.equal(config, `[http]\nlisten_addr = "0.0.0.0:3001"\n\n[locks]\ntrusted_public_key = "${lockServerPubky}"\n\n[setup]\nallowed_origins = ["http://127.0.0.1:8080", "http://localhost:8080"]\n\n[paykit]\nreceiver_path = "bitkit/server"\nreceiver_path_priority = ["bitkit"]\nnetwork = "testnet"\n\n[bitcoin]\nnetwork = "regtest"\n\n[electrum]\nendpoint = "tcp://fulcrum:50001"\npoll_interval = "1s"\nrequest_timeout = "10s"\nconnect_retries = 1\n\n[outbox]\npoll_interval = "500ms"\nbatch_size = 16\nlease_duration = "30s"\nretry_initial = "1s"\nretry_max = "5m"\n`);
assert.throws(() => buildPaykitServerConfig({ lockServerPubky: 'invalid' }), /Lock Server Pubky/);
assert.deepEqual(validatePaykitComposeEnvironment({
  PAYKIT_DATABASE_URL: 'postgres://paykit:secret@paykit-postgres:5432/paykit',
  PAYKIT_MASTER_KEY: 'AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE',
  BITCOIN_RPC_USER: 'bitcoin-user',
  BITCOIN_RPC_PASSWORD: 'bitcoin-password',
}), {
  databaseUrl: 'postgres://paykit:secret@paykit-postgres:5432/paykit',
  masterKey: 'AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE',
  bitcoinRpcUser: 'bitcoin-user',
  bitcoinRpcPassword: 'bitcoin-password',
});
for (const [name, value] of [
  ['PAYKIT_DATABASE_URL', ''],
  ['PAYKIT_MASTER_KEY', 'short'],
  ['BITCOIN_RPC_USER', 'bad user'],
  ['BITCOIN_RPC_PASSWORD', ''],
]) {
  const environment = {
    PAYKIT_DATABASE_URL: 'postgres://paykit:secret@paykit-postgres:5432/paykit',
    PAYKIT_MASTER_KEY: 'AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE',
    BITCOIN_RPC_USER: 'bitcoin-user',
    BITCOIN_RPC_PASSWORD: 'bitcoin-password',
    [name]: value,
  };
  assert.throws(() => validatePaykitComposeEnvironment(environment), new RegExp(name));
}

const generatedRoot = await mkdtemp(join(tmpdir(), 'locks-paykit-compose-'));
try {
  const staticRoot = join(generatedRoot, 'static-root');
  const outsideStaticRoot = join(generatedRoot, 'outside-static-root');
  await mkdir(staticRoot);
  await mkdir(outsideStaticRoot);
  await writeFile(join(outsideStaticRoot, 'secret.txt'), 'not public');
  await symlink(join(outsideStaticRoot, 'secret.txt'), join(staticRoot, 'escape.txt'));
  assert.equal(
    resolveCreatorStaticPath('/examples/js-sdk/escape.txt', { repoRoot, examplesRoot: staticRoot }),
    null,
    'creator static paths must not follow symlinks outside their allowlisted root',
  );
  const outsideSecretPath = join(generatedRoot, 'outside-secret.txt');
  const secretLinkPath = join(generatedRoot, 'secret-link.txt');
  await writeFile(outsideSecretPath, 'outside remains unchanged', { mode: 0o600 });
  await symlink(outsideSecretPath, secretLinkPath);
  await writeSecret(secretLinkPath, 'replacement');
  assert.equal(await readFile(outsideSecretPath, 'utf8'), 'outside remains unchanged');
  assert.equal((await lstat(secretLinkPath)).isSymbolicLink(), false, 'secret writes must replace symlinks');
  assert.equal((await stat(secretLinkPath)).mode & 0o777, 0o600);
  const lockConfigPath = join(generatedRoot, 'lock-server.toml');
  await writeFile(lockConfigPath, `lock_server_public_key = "${lockServerPubky}"\n`, { mode: 0o600 });
  const first = await initializePaykitCompose({ root: generatedRoot });
  assert.equal(first.configGenerated, false);
  const generatedFiles = [
    'compose-secrets.json',
    'locks-postgres/locks-postgres.env',
    'locks-server/compose.env',
    'paykit-postgres/postgres.env',
    'paykit-server/paykit.env',
    'bitcoin-rpc/bitcoin-rpc.env',
    'pubky-homeserver/config.toml',
    'homegate-bridge/homegate.env',
  ];
  for (const file of generatedFiles) {
    assert.equal((await stat(join(generatedRoot, file))).mode & 0o777, 0o600, `${file} must be mode 0600`);
  }
  for (const directory of [
    'js-sdk-demo',
    'demo-config',
    'creator-public',
    'bitcoin-bootstrap',
    'content-creator',
    'content-viewer',
    'paykit-reader',
    'locks-postgres',
    'paykit-postgres',
    'bitcoin-rpc',
    'pubky-homeserver',
    'paykit-server',
    'paykit-config',
    'homegate-bridge',
  ]) {
    assert.equal((await stat(join(generatedRoot, directory))).mode & 0o777, 0o700, `${directory} must be mode 0700`);
  }
  const paykitEnvironment = await readFile(join(generatedRoot, 'paykit-server/paykit.env'), 'utf8');
  assert.match(paykitEnvironment, /^PAYKIT_DATABASE_URL=postgres:\/\/paykit:[A-Za-z0-9_-]+@paykit-postgres:5432\/paykit$/m);
  assert.match(paykitEnvironment, /^PAYKIT_MASTER_KEY=[A-Za-z0-9_-]{43}$/m);
  const firstSecrets = await readFile(join(generatedRoot, 'compose-secrets.json'), 'utf8');
  await initializePaykitCompose({ root: generatedRoot });
  assert.equal(await readFile(join(generatedRoot, 'compose-secrets.json'), 'utf8'), firstSecrets);
  await chmod(join(generatedRoot, 'compose-secrets.json'), 0o644);
  await assert.rejects(
    initializePaykitCompose({ root: generatedRoot }),
    /persisted Compose secrets are invalid/,
  );
  await chmod(join(generatedRoot, 'compose-secrets.json'), 0o600);

  const configured = await initializePaykitCompose({
    root: generatedRoot,
    lockConfigPath,
    configOnly: true,
  });
  assert.equal(configured.configGenerated, true);
  assert.equal(
    await readFile(join(generatedRoot, 'paykit-config/config.toml'), 'utf8'),
    buildPaykitServerConfig({ lockServerPubky }),
  );
  assert.equal((await stat(join(generatedRoot, 'paykit-config/config.toml'))).mode & 0o777, 0o644);

  await writeFile(
    join(generatedRoot, 'compose-secrets.json'),
    `${JSON.stringify({ ...JSON.parse(firstSecrets), unexpected: true })}\n`,
  );
  await assert.rejects(
    initializePaykitCompose({ root: generatedRoot }),
    /persisted Compose secrets are invalid/,
  );
} finally {
  await rm(generatedRoot, { recursive: true, force: true });
}

const configOnlyRoot = await mkdtemp(join(tmpdir(), 'locks-paykit-public-config-'));
try {
  const lockConfigPath = join(configOnlyRoot, 'lock-server.toml');
  await writeFile(lockConfigPath, `lock_server_public_key = "${lockServerPubky}"\n`, { mode: 0o644 });
  await initializePaykitCompose({ root: configOnlyRoot, lockConfigPath, configOnly: true });
  assert.equal(
    await readFile(join(configOnlyRoot, 'paykit-config/config.toml'), 'utf8'),
    buildPaykitServerConfig({ lockServerPubky }),
  );
  await assert.rejects(readFile(join(configOnlyRoot, 'compose-secrets.json'), 'utf8'), { code: 'ENOENT' });

  const privateProfilePath = join(configOnlyRoot, 'private-profile.json');
  const publicProfilePath = join(configOnlyRoot, 'creator-public/profile.json');
  await writeFile(privateProfilePath, `${JSON.stringify({
    role: 'content-creator',
    pubky: creatorPubky,
    homeserver: lockServerPubky,
    created_at: '2026-01-01T00:00:00.000Z',
  })}\n`, { mode: 0o600 });
  await publishCreatorProfile({ source: privateProfilePath, destination: publicProfilePath });
  assert.deepEqual(JSON.parse(await readFile(publicProfilePath, 'utf8')), {
    role: 'content-creator',
    pubky: creatorPubky,
  });
  const readerFromPublicProfile = await resolveReaderEnvironment({
    env: {
      ...readerBaseEnvironment,
      PAYKIT_READER_CREATOR_PROFILE_PATH: publicProfilePath,
    },
    loadProfile: async () => { throw new Error('private creator profile must not be loaded'); },
  });
  assert.equal(readerFromPublicProfile.PAYKIT_READER_SERVER_PUBKY, creatorPubky);
  await publishCreatorProfile({
    profile: { role: 'content-creator', pubky: lockServerPubky },
    destination: publicProfilePath,
  });
  assert.deepEqual(JSON.parse(await readFile(publicProfilePath, 'utf8')), {
    role: 'content-creator',
    pubky: lockServerPubky,
  });
} finally {
  await rm(configOnlyRoot, { recursive: true, force: true });
}

const compose = await readFile(join(repoRoot, 'compose.paykit-local-demo.yaml'), 'utf8');
const jsDemoDockerfile = await readFile(join(repoRoot, 'docker/js-demo.Dockerfile'), 'utf8');
const locksEntrypoint = await readFile(join(repoRoot, 'docker/locks-server-compose-entrypoint.sh'), 'utf8');
const resetScript = await readFile(join(repoRoot, 'examples/js-sdk/scripts/reset-paykit-demo.mjs'), 'utf8');
const validateScript = await readFile(join(repoRoot, 'examples/js-sdk/scripts/validate-paykit-compose.mjs'), 'utf8');
const accountScript = await readFile(join(repoRoot, 'examples/js-sdk/scripts/generate-paykit-account-tpub.mjs'), 'utf8');
const packageJson = JSON.parse(await readFile(join(repoRoot, 'examples/js-sdk/package.json'), 'utf8'));
const bootstrapMode = (await stat(join(repoRoot, 'docker/bitcoin-bootstrap.sh'))).mode;
assert.notEqual(bootstrapMode & 0o111, 0, 'Bitcoin bootstrap script must be executable');
assert.match(locksEntrypoint, /level = "info,pubky::actors::session=warn"/);
assert.match(compose, /RUST_LOG: \$\{LOCKS_RUST_LOG:-info,pubky::actors::session=warn\}/);
for (const required of ['--no-install-recommends ca-certificates util-linux', 'rm -rf /var/lib/apt/lists/*']) {
  assert.ok(jsDemoDockerfile.includes(required), `JS demo image missing ${required}`);
}
for (const service of [
  'compose-bootstrap:',
  'paykit-postgres:',
  'bitcoin:',
  'bitcoin-bootstrap:',
  'fulcrum:',
  'electrum-readiness:',
  'homegate-bridge:',
  'paykit-config:',
  'demo-config:',
  'paykit-server:',
]) {
  assert.ok(compose.includes(`  ${service}`), `missing Compose service ${service}`);
}
for (const required of [
  'postgres:17-bookworm@sha256:4f736ae292687621d4dbe0d499ffd024a36bd2ee7d8ca6f2ccd4c800f047b394',
  'bitcoin/bitcoin:29.1@sha256:de62c536feb629bed65395f63afd02e3a7a777a3ec82fbed773d50336a739319',
  'cculianu/fulcrum:v1.11.1@sha256:70f06b93ab5863997992d4b4508312fe81ce576017e16ecc7e69c7d38165bdf2',
  'node:22-bookworm-slim@sha256:813a7480f28fdadac1f7f5c824bcdad435b5bc1322a5968bbbdef8d058f9dff4',
  'additional_contexts:',
  'PUBKY_CORE_REV: 75eb1324f86e8caa16c41f18a2cd6b8e1909ee7b',
  'https://github.com/pubky/paykit-server.git#5ed3e8e849a16045c26c37a75068625dda333785',
  'https://github.com/pubky/paykit-rs.git#6b241878a9bba5cecea919c0298c3f90624be6ff:paykit-lib',
  'https://github.com/pubky/paykit-rs.git#6b241878a9bba5cecea919c0298c3f90624be6ff:paykit-sdk',
  'https://github.com/pubky/locks.git#df5ea1b6d8dcdec3a9b5a915c3f57bca69d75c8a',
  '127.0.0.1:${LOCKS_PAYKIT_PORT:-3001}:3001',
  '127.0.0.1:${LOCKS_READER_DEMO_PORT:-8088}:8081',
  '127.0.0.1:${LOCKS_ELECTRUM_PORT:-60001}:50001',
  '127.0.0.1:${LOCKS_HOMEGATE_PORT:-6288}:8082',
  'bitcoin-cli -conf=\\"$${BITCOIN_DATA}/bitcoin.conf\\" -regtest getblockchaininfo',
  'user: "1000:1000"',
  '.local/bitcoin-bootstrap:/home/bitcoin/.bitcoin',
  'PAYKIT_CONFIG:',
  '/run/compose-local/paykit-server/paykit.env',
  '/run/compose-local/bitcoin-rpc.env',
  './.local/locks-postgres:/run/compose-local:ro',
  './.local/paykit-postgres:/run/compose-local/paykit-server:ro',
  './.local/bitcoin-rpc:/run/compose-local:ro',
  './.local/pubky-homeserver:/run/compose-local/pubky-homeserver:ro',
  './.local/locks-server:/run/compose-local/locks-server:ro',
  './.local/paykit-server:/run/compose-local/paykit-server:ro',
  './.local/homegate-bridge:/run/compose-local/homegate-bridge:ro',
  'node examples/js-sdk/scripts/init-paykit-compose.mjs',
  'exec /entrypoint.sh Fulcrum',
  'PAYKIT_READER_DEMO_BIN:',
  'PAYKIT_COMPANION_AUTH_BIN:',
  'condition: service_healthy',
  'condition: service_completed_successfully',
  'locks-postgres-data:/var/lib/postgresql/data',
  'paykit-postgres-data:/var/lib/postgresql/data',
  'bitcoin-data:/home/bitcoin/.bitcoin',
  'fulcrum-data:/data',
  '.local/paykit-config:/etc/paykit-server:ro',
  'check /health/live && check /health/ready',
]) {
  assert.ok(compose.includes(required), `Compose missing ${required}`);
}
assert.ok(!compose.includes('env_file:'), 'Compose must bootstrap before loading generated environments');
assert.ok(!compose.includes('chown -R 1000:1000 .local\n'), 'bootstrap must not rewrite ownership of the complete local state tree');
assert.equal(
  compose.split('./.local:/').length - 1,
  1,
  'only compose-bootstrap may mount the complete generated local state tree',
);
for (const siblingContext of ['../pubky-core', '../../Paykit/', '../paykit-rs', '../../Pubky/locks']) {
  assert.ok(!compose.includes(siblingContext), `Compose must not require sibling context ${siblingContext}`);
}
for (const privateVolume of ['name: locks_lock-home', 'name: pubky-locks-demo-public']) {
  assert.ok(!compose.includes(privateVolume), `Compose must not reuse private-era volume ${privateVolume}`);
}
assert.ok(!compose.includes('- ./:/workspace'), 'services must not mount the repository root');
assert.ok(!compose.includes('- lock-home:/root'), 'demo services must not mount Lock Server identity state');
const creatorService = compose.slice(compose.indexOf('  creator-demo:'), compose.indexOf('\n  reader-demo:'));
assert.ok(creatorService.includes('--external-wallet'), 'creator must use the authenticated external wallet identity');
assert.ok(creatorService.includes('rm -f /workspace/.local/creator-public/profile.json'), 'creator must clear stale public identity before external wallet auth');
assert.ok(!creatorService.includes('create-user -- --role content-creator'), 'external wallet mode must not create a second creator identity');
assert.ok(!creatorService.includes('.local/content-creator'), 'external wallet mode must not mount creator recovery state');
assert.ok(!creatorService.includes('--allow-unhealthy'), 'creator preflight must fail closed');
const readerService = compose.slice(compose.indexOf('  reader-demo:'), compose.indexOf('\nvolumes:'));
assert.ok(!readerService.includes('.local/js-sdk-demo'), 'reader must not mount creator session state');
assert.ok(!readerService.includes('.local/content-creator'), 'reader must not mount creator recovery state');
assert.ok(readerService.includes('.local/creator-public'), 'reader requires only the closed creator public profile');
assert.ok(readerService.includes('PAYKIT_READER_WORKER_ENABLED: "1"'), 'reader must enable its embedded Paykit worker');
assert.ok(readerService.includes('npm --prefix examples/js-sdk run create-user -- --role content-viewer'), 'reader must create or reuse its recovery identity');
assert.ok(readerService.includes('exec node examples/js-sdk/scripts/start-reader-demo-server.mjs'), 'reader server must replace its bootstrap shell as PID 1');
assert.ok(!readerService.includes('--allow-unhealthy'), 'reader preflight must fail closed');
assert.ok(readerService.includes('healthcheck:'), 'reader must expose worker-aware Compose health');
assert.ok(readerService.includes('http://127.0.0.1:8081/api/paykit-reader/status'), 'reader health must use the closed worker status endpoint');
assert.ok(readerService.includes('restart: unless-stopped'), 'reader worker must have an explicit restart policy');
assert.ok(!compose.includes('POSTGRES_PASSWORD: locks'), 'database credentials must not be committed inline');
assert.ok(!compose.includes('./locks-sdk/bindings/js/pkg:/workspace/locks-sdk/bindings/js/pkg'), 'demo images must provide their own WASM package');
for (const required of ['FROM rust:1.91.1-slim-bookworm@sha256:8514999d4786ef12efe89239e86b3d0a021b94b9d35108c8efe6c79ca7dc1a65 AS locks-sdk-wasm', 'cargo install wasm-pack --version 0.13.1 --locked', 'wasm-pack build --target web --out-dir pkg', 'COPY --from=locks-sdk-wasm']) {
  assert.ok(jsDemoDockerfile.includes(required), `JS demo image missing ${required}`);
}
assert.ok(locksEntrypoint.includes('LOCKS_PUBLIC_CONFIG'), 'Lock Server must publish an explicit public artifact');
for (const required of ['[paykit]', 'server_url = "http://127.0.0.1:3001"', 'minimum_confirmations = 0']) {
  assert.ok(locksEntrypoint.includes(required), `Locks generated config missing ${required}`);
}
assert.equal(packageJson.scripts['init-paykit-compose'], 'node scripts/init-paykit-compose.mjs');
assert.equal(packageJson.scripts['reset-paykit-demo'], 'node scripts/reset-paykit-demo.mjs');
for (const required of ['bitcoinBootstrapDir', 'rm(bitcoinBootstrapDir']) {
  assert.ok(resetScript.includes(required), `reset script missing ${required}`);
}
for (const volume of [
  'pubky-locks-paykit-demo-locks-postgres',
  'pubky-locks-paykit-demo-paykit-postgres',
  'pubky-locks-paykit-demo-bitcoin',
  'pubky-locks-paykit-demo-fulcrum',
]) {
  assert.ok(compose.includes(`name: ${volume}`), `Compose missing isolated volume ${volume}`);
  assert.ok(resetScript.includes(`'${volume}'`), `reset script missing isolated volume ${volume}`);
}
for (const [script, description] of [
  [resetScript, 'reset'],
  [validateScript, 'validation'],
  [accountScript, 'account generation'],
]) {
  assert.ok(script.includes("const COMPOSE_FILE = 'compose.paykit-local-demo.yaml';"), `${description} script must select the local demo Compose file`);
  assert.ok(script.includes("['compose', '-f', COMPOSE_FILE"), `${description} script must pass the local demo Compose file explicitly`);
}
assert.equal(packageJson.scripts['validate:paykit-compose'], 'node scripts/validate-paykit-compose.mjs');
assert.equal(
  packageJson.scripts['smoke:paykit-compose'],
  'npm run validate:paykit-compose && npm run test:paykit-reader-worker && node scripts/smoke-paykit-compose.mjs',
);

console.log('Paykit Compose smoke check passed');
