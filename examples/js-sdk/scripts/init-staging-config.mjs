#!/usr/bin/env node
import { stagingDemoConfigPath } from './lib/paths.mjs';
import { refreshStagingDemoConfig } from './lib/staging-config.mjs';

try {
  await refreshStagingDemoConfig({ output: stagingDemoConfigPath });
  console.log(JSON.stringify({ ok: true, config: stagingDemoConfigPath }));
} catch (error) {
  console.error(error instanceof Error ? error.message : 'staging config failed');
  process.exitCode = 1;
}