#!/usr/bin/env node
import { existsSync } from 'node:fs';

import {
  ensureDir,
  parseArgs,
  requiredRole,
  roleDir,
  rolePassphrasePath,
  roleProfilePath,
  roleRecoveryFilePath,
  writeJson,
  writeSecret,
} from './lib/paths.mjs';
import { readDemoConfig } from './lib/config.mjs';
import { Keypair, publicKeyString, randomPassphrase } from './lib/pubky.mjs';

// Creates ./.local/<role>/passphrase, ./.local/<role>/recovery_file, and ./.local/<role>/profile.json.
// Valid roles: lock-server, content-creator, content-viewer. Use --force to overwrite.

const args = parseArgs();
let role;
try {
  role = requiredRole(args);
} catch (error) {
  console.error(error.message);
  console.error('usage: npm --prefix examples/js-sdk run create-user -- --role content-creator [--force]');
  process.exit(2);
}

const force = Boolean(args.force);
const recoveryFile = roleRecoveryFilePath(role);
const passphraseFile = rolePassphrasePath(role);
const profileFile = roleProfilePath(role);

try {
  const existing = existsSync(recoveryFile) && existsSync(passphraseFile) && existsSync(profileFile);
  if (existing && !force) {
    console.log(JSON.stringify({ ok: true, reused: true, role, profile: profileFile }, null, 2));
    process.exit(0);
  }

  const config = await readDemoConfig().catch(() => undefined);
  await ensureDir(roleDir(role));

  const keypair = Keypair.random();
  const passphrase = randomPassphrase();
  const recoveryBytes = keypair.createRecoveryFile(passphrase);
  const pubky = publicKeyString(keypair);
  const profile = {
    role,
    pubky,
    homeserver: config?.testnet?.homeserver ?? null,
    created_at: new Date().toISOString(),
  };

  await writeSecret(passphraseFile, `${passphrase}\n`);
  await writeSecret(recoveryFile, Buffer.from(recoveryBytes));
  await writeJson(profileFile, profile);

  console.log(JSON.stringify({ ok: true, reused: false, forced: force, role, pubky, profile: profileFile }, null, 2));
} catch (error) {
  console.error(`create-user failed: ${error.message}`);
  process.exit(1);
}
