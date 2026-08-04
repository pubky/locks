#!/usr/bin/env node
import { randomBytes } from 'node:crypto';
import { chmod, mkdir } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  buildPaykitServerConfig,
  buildPubkyHomeserverComposeConfig,
  readLockServerPublicKey,
  validatePaykitComposeEnvironment,
} from './lib/config.mjs';
import { localPath, parseArgs, readPrivateText, writeAtomicFile } from './lib/paths.mjs';

const SECRET_KEYS = [
  'version',
  'locksPostgresPassword',
  'paykitPostgresPassword',
  'paykitMasterKey',
  'bitcoinRpcUser',
  'bitcoinRpcPassword',
  'locksCreatorAuthKey',
  'pubkyHomeserverAdminPassword',
];
const TOKEN = /^[A-Za-z0-9_-]{16,128}$/;
const MASTER_KEY = /^[A-Za-z0-9_-]{43}$/;

function randomToken(bytes = 24) {
  return randomBytes(bytes).toString('base64url');
}

export function createComposeSecrets() {
  return Object.freeze({
    version: 1,
    locksPostgresPassword: randomToken(),
    paykitPostgresPassword: randomToken(),
    paykitMasterKey: randomToken(32),
    bitcoinRpcUser: `bitcoin_${randomToken(12)}`,
    bitcoinRpcPassword: randomToken(32),
    locksCreatorAuthKey: randomToken(32),
    pubkyHomeserverAdminPassword: randomToken(24),
  });
}

export function validateComposeSecrets(value) {
  if (
    value === null
    || typeof value !== 'object'
    || Array.isArray(value)
    || Object.keys(value).length !== SECRET_KEYS.length
    || !SECRET_KEYS.every((key) => Object.hasOwn(value, key))
    || value.version !== 1
  ) {
    throw new Error('invalid persisted Compose secrets');
  }
  for (const key of [
    'locksPostgresPassword',
    'paykitPostgresPassword',
    'bitcoinRpcUser',
    'bitcoinRpcPassword',
    'pubkyHomeserverAdminPassword',
  ]) {
    if (typeof value[key] !== 'string' || !TOKEN.test(value[key])) {
      throw new Error('invalid persisted Compose secrets');
    }
  }
  for (const key of ['paykitMasterKey', 'locksCreatorAuthKey']) {
    if (typeof value[key] !== 'string' || !MASTER_KEY.test(value[key])) {
      throw new Error('invalid persisted Compose secrets');
    }
  }
  return Object.freeze({ ...value });
}

async function writeSecure(path, content, mode = 0o600) {
  await writeAtomicFile(path, content, mode);
}

function generatedPaths(root) {
  return {
    secrets: join(root, 'compose-secrets.json'),
    locksPostgres: join(root, 'locks-postgres', 'locks-postgres.env'),
    locksServer: join(root, 'locks-server', 'compose.env'),
    paykitPostgres: join(root, 'paykit-postgres', 'postgres.env'),
    paykitServer: join(root, 'paykit-server', 'paykit.env'),
    paykitConfig: join(root, 'paykit-config', 'config.toml'),
    bitcoinRpc: join(root, 'bitcoin-rpc', 'bitcoin-rpc.env'),
    pubkyHomeserver: join(root, 'pubky-homeserver', 'config.toml'),
  };
}

export async function initializePaykitCompose({
  root = localPath,
  lockConfigPath,
  configOnly = false,
} = {}) {
  const paths = generatedPaths(root);
  if (configOnly) {
    if (!lockConfigPath) throw new Error('--lock-config is required with --config-only');
    const lockServerPubky = await readLockServerPublicKey(lockConfigPath);
    await writeSecure(paths.paykitConfig, buildPaykitServerConfig({ lockServerPubky }), 0o644);
    return Object.freeze({ root: resolve(root), configGenerated: true });
  }

  await Promise.all([
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
  ].map(async (directory) => {
    const path = join(root, directory);
    await mkdir(path, { recursive: true, mode: 0o700 });
    await chmod(path, 0o700);
  }));

  let secrets;
  try {
    secrets = validateComposeSecrets(JSON.parse(await readPrivateText(paths.secrets)));
  } catch (error) {
    if (error?.code !== 'ENOENT') throw new Error('persisted Compose secrets are invalid');
    secrets = createComposeSecrets();
    await writeSecure(paths.secrets, `${JSON.stringify(secrets, null, 2)}\n`);
  }

  const paykitDatabaseUrl = `postgres://paykit:${secrets.paykitPostgresPassword}@paykit-postgres:5432/paykit`;
  validatePaykitComposeEnvironment({
    PAYKIT_DATABASE_URL: paykitDatabaseUrl,
    PAYKIT_MASTER_KEY: secrets.paykitMasterKey,
    BITCOIN_RPC_USER: secrets.bitcoinRpcUser,
    BITCOIN_RPC_PASSWORD: secrets.bitcoinRpcPassword,
  });

  await Promise.all([
    writeSecure(paths.locksPostgres, `POSTGRES_DB=locks_test\nPOSTGRES_USER=locks\nPOSTGRES_PASSWORD=${secrets.locksPostgresPassword}\n`),
    writeSecure(paths.locksServer, `PUBKY_LOCK_DATABASE_URL=postgres://locks:${secrets.locksPostgresPassword}@postgres:5432/locks_test\nPUBKY_LOCK_CREATOR_AUTH_ENCRYPTION_KEY=${secrets.locksCreatorAuthKey}\n`),
    writeSecure(paths.paykitPostgres, `POSTGRES_DB=paykit\nPOSTGRES_USER=paykit\nPOSTGRES_PASSWORD=${secrets.paykitPostgresPassword}\n`),
    writeSecure(paths.paykitServer, `PAYKIT_DATABASE_URL=${paykitDatabaseUrl}\nPAYKIT_MASTER_KEY=${secrets.paykitMasterKey}\n`),
    writeSecure(paths.bitcoinRpc, `BITCOIN_RPC_USER=${secrets.bitcoinRpcUser}\nBITCOIN_RPC_PASSWORD=${secrets.bitcoinRpcPassword}\nRPCUSER=${secrets.bitcoinRpcUser}\nRPCPASSWORD=${secrets.bitcoinRpcPassword}\n`),
    writeSecure(paths.pubkyHomeserver, buildPubkyHomeserverComposeConfig({
      databasePassword: secrets.locksPostgresPassword,
      adminPassword: secrets.pubkyHomeserverAdminPassword,
    })),
  ]);

  if (lockConfigPath) {
    const lockServerPubky = await readLockServerPublicKey(lockConfigPath);
    await writeSecure(paths.paykitConfig, buildPaykitServerConfig({ lockServerPubky }), 0o644);
  }

  return Object.freeze({ root: resolve(root), configGenerated: Boolean(lockConfigPath) });
}

export async function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  const result = await initializePaykitCompose({
    root: typeof args['local-dir'] === 'string' ? resolve(args['local-dir']) : localPath,
    lockConfigPath: typeof args['lock-config'] === 'string' ? args['lock-config'] : undefined,
    configOnly: args['config-only'] === true,
  });
  console.log(JSON.stringify({ ok: true, localDir: result.root, configGenerated: result.configGenerated }));
}

const isMain = process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  main().catch((error) => {
    console.error(`init-paykit-compose failed: ${error.message}`);
    process.exitCode = 1;
  });
}
