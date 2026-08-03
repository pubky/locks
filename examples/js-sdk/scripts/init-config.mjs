#!/usr/bin/env node
import { demoConfigPath, parseArgs, writeJson } from './lib/paths.mjs';
import { buildDefaultDemoConfig } from './lib/config.mjs';

// Defaults: read lock_server_public_key from ~/.pubky-lock/config.toml and write ./.local/js-sdk-demo/config.json.
// Local testnet defaults: http://localhost:15411, http://localhost:15412, localhost:6881.

const args = parseArgs();
const output = typeof args.output === 'string' ? args.output : demoConfigPath;

try {
  const config = await buildDefaultDemoConfig();
  await writeJson(output, config);
  console.log(JSON.stringify({ ok: true, config: output, lockServer: config.lockServer }, null, 2));
} catch (error) {
  console.error(`init-config failed: ${error.message}`);
  process.exit(1);
}
