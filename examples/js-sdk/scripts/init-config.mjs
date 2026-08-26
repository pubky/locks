#!/usr/bin/env node
import { demoConfigPath, parseArgs, writeJson } from './lib/paths.mjs';
import { buildDefaultDemoConfig } from './lib/config.mjs';

// Defaults: read lock_server_public_key from ~/.pubky-lock/config.toml and write ./.local/demo-config/config.json.
// Local testnet defaults: http://127.0.0.1:15411, http://127.0.0.1:15412, 127.0.0.1:6881.

const args = parseArgs();
const output = typeof args.output === 'string' ? args.output : demoConfigPath;

try {
  const lockConfigPath = typeof args['lock-config'] === 'string' ? args['lock-config'] : undefined;
  const config = await buildDefaultDemoConfig(lockConfigPath);
  await writeJson(output, config);
  console.log(JSON.stringify({ ok: true, config: output, lockServer: config.lockServer }, null, 2));
} catch (error) {
  console.error(`init-config failed: ${error.message}`);
  process.exit(1);
}
