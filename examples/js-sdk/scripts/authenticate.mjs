#!/usr/bin/env node
import { createInterface } from 'node:readline/promises';
import { stdin as input, stdout as output } from 'node:process';

import { parseArgs, requiredRole } from './lib/paths.mjs';
import { pubkyAuthRelayInboxUrl, readDemoConfig, withInternalServiceUrls } from './lib/config.mjs';
import { homeserverPublicKey, loadRoleKeypair, pubkyForConfig, signupBestEffort } from './lib/pubky.mjs';

// Contract: signer.signup is attempted before approveAuthRequest; already registered is OK.

async function readAuthFromPrompt() {
  if (!process.stdin.isTTY) {
    const chunks = [];
    for await (const chunk of process.stdin) chunks.push(chunk);
    const value = Buffer.concat(chunks).toString('utf8').trim();
    if (value) return value;
  }

  const rl = createInterface({ input, output });
  try {
    return (await rl.question('Paste auth string: ')).trim();
  } finally {
    rl.close();
  }
}

const args = parseArgs();
let role;
try {
  role = requiredRole(args);
} catch (error) {
  console.error(error.message);
  console.error('usage: npm --prefix examples/js-sdk run authenticate -- --role content-creator [--auth pubkyauth://...]');
  process.exit(2);
}

try {
  const rawAuth = typeof args.auth === 'string' ? args.auth.trim() : await readAuthFromPrompt();
  const config = withInternalServiceUrls(await readDemoConfig());
  const auth = withInternalRelay(rawAuth, config);
  if (!auth.startsWith('pubkyauth://')) {
    throw new Error('auth string must start with pubkyauth://');
  }

  const pubky = pubkyForConfig(config);
  const keypair = await loadRoleKeypair(role);
  const signer = pubky.signer(keypair);
  const homeserver = homeserverPublicKey(config);

  await signupBestEffort(signer, homeserver); // treats already registered as OK
  await signer.approveAuthRequest(auth);

  console.log(JSON.stringify({ ok: true, role, pubky: signer.publicKey.toString(), approved: true }, null, 2));
} catch (error) {
  console.error(`authenticate failed: ${error.message}`);
  process.exit(1);
}

function withInternalRelay(auth, config) {
  if (!process.env.LOCKS_INTERNAL_HTTP_RELAY) {
    return auth;
  }
  const url = new URL(auth);
  url.searchParams.set('relay', pubkyAuthRelayInboxUrl(config.testnet.httpRelay));
  return url.toString();
}
