import { readFile } from 'node:fs/promises';
import { randomBytes } from 'node:crypto';

import { AuthFlowKind, Keypair, Pubky, PublicKey } from '@synonymdev/pubky';

import {
  readJson,
  rolePassphrasePath,
  roleRecoveryFilePath,
} from './paths.mjs';

export { AuthFlowKind, Keypair, Pubky, PublicKey };

export function pubkyForConfig(config) {
  const host = new URL(config.testnet.httpRelay).hostname;
  return Pubky.testnet(host);
}

export function randomPassphrase() {
  return randomBytes(32).toString('base64url');
}

export async function loadRoleKeypair(role, { readFile: read = readFile } = {}) {
  const passphrase = (await readRoleIdentityFile(rolePassphrasePath(role), role, read, 'utf8')).trim();
  const recoveryFile = await readRoleIdentityFile(roleRecoveryFilePath(role), role, read);
  return Keypair.fromRecoveryFile(new Uint8Array(recoveryFile), passphrase);
}

export function secretFromRecoveryFile(recoveryFile, passphrase) {
  const keypair = Keypair.fromRecoveryFile(new Uint8Array(recoveryFile), passphrase);
  try {
    const secret = keypair.secret();
    if (!(secret instanceof Uint8Array) || secret.length !== 32) {
      throw new Error('recovery file did not contain a 32-byte Pubky secret');
    }
    return secret;
  } finally {
    keypair.free();
  }
}

export async function loadRoleSecret(role, { readFile: read = readFile } = {}) {
  const passphrase = (await readRoleIdentityFile(rolePassphrasePath(role), role, read, 'utf8')).trim();
  const recoveryFile = await readRoleIdentityFile(roleRecoveryFilePath(role), role, read);
  return secretFromRecoveryFile(recoveryFile, passphrase);
}

async function readRoleIdentityFile(path, role, read, encoding) {
  try {
    return await read(path, encoding);
  } catch (error) {
    if (error?.code !== 'ENOENT') throw error;
    const missing = new Error(
      `missing local identity for ${role}; run \`npm --prefix examples/js-sdk run create-user -- --role ${role}\` before authentication`,
    );
    missing.code = 'ROLE_IDENTITY_MISSING';
    throw missing;
  }
}

export async function loadRoleProfile(role) {
  return readJson(new URL(`../../../../.local/${role}/profile.json`, import.meta.url).pathname);
}

export function publicKeyString(keypair) {
  return keypair.publicKey.toString();
}

export function homeserverPublicKey(config) {
  return PublicKey.from(config.testnet.homeserver);
}

export function isAlreadyRegisteredError(error) {
  const message = String(error?.message ?? error).toLowerCase();
  return message.includes('already') || message.includes('409') || message.includes('conflict');
}

export async function signupBestEffort(signer, homeserver) {
  try {
    return await signer.signup(homeserver, null);
  } catch (error) {
    if (isAlreadyRegisteredError(error)) return await signer.signinCookieBlocking();
    throw error;
  }
}
