import { existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { resolve } from 'node:path';

import { demoConfigPath, writeJson, readJson } from './paths.mjs';

export const defaultLockServerConfigPath = '~/.pubky-lock/config.toml';

export const defaultDemoConfig = {
  demoServer: {
    url: 'http://localhost:8080',
  },
  lockServer: {
    url: 'http://127.0.0.1:3000',
    pubky: '',
    configPath: defaultLockServerConfigPath,
  },
  testnet: {
    homeserver: 'pubky8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo',
    httpRelay: 'http://localhost:15412',
    pkarrRelay: 'http://localhost:15411',
    dhtBootstrap: 'localhost:6881',
  },
};

export function expandHome(path) {
  if (path === '~') return homedir();
  if (path.startsWith('~/')) return resolve(homedir(), path.slice(2));
  return path;
}

export function parseLockServerTomlPublicKey(tomlText) {
  const match = tomlText.match(/^[ \t]*lock_server_public_key[ \t]*=[ \t]*"([^"]+)"[ \t]*(?:#[^\r\n]*)?$/m);
  if (!match) {
    throw new Error('missing lock_server_public_key in ~/.pubky-lock/config.toml');
  }
  const publicKey = match[1].trim();
  if (!publicKey || publicKey.includes('<') || publicKey.includes('derived-on-first-run')) {
    throw new Error('lock_server_public_key is still a placeholder; start locks-server once to generate real config first');
  }
  return publicKey.startsWith('pubky') ? publicKey : `pubky${publicKey}`;
}

export async function readLockServerPublicKey(configPath = defaultLockServerConfigPath) {
  const expanded = expandHome(configPath);
  if (!existsSync(expanded)) {
    throw new Error(`missing Lock Server config: ${expanded}; start locks-server first so it generates ~/.pubky-lock/config.toml`);
  }
  return parseLockServerTomlPublicKey(await readFile(expanded, 'utf8'));
}

export function pubkyAuthRelayInboxUrl(httpRelayUrl) {
  const url = new URL(httpRelayUrl);
  const normalizedPath = url.pathname.endsWith('/') ? url.pathname : `${url.pathname}/`;
  if (!normalizedPath.endsWith('/inbox/')) {
    url.pathname = `${normalizedPath}inbox/`.replace(/\/+/g, '/');
  }
  return url.toString();
}

export function validateDemoConfig(config) {
  for (const path of [
    ['demoServer', 'url'],
    ['lockServer', 'url'],
    ['lockServer', 'pubky'],
    ['lockServer', 'configPath'],
    ['testnet', 'homeserver'],
    ['testnet', 'httpRelay'],
    ['testnet', 'pkarrRelay'],
    ['testnet', 'dhtBootstrap'],
  ]) {
    const value = path.reduce((acc, key) => acc?.[key], config);
    if (typeof value !== 'string' || value.length === 0) {
      throw new Error(`invalid demo config: ${path.join('.')} is required`);
    }
  }

  new URL(config.demoServer.url);
  new URL(config.lockServer.url);
  new URL(config.testnet.httpRelay);
  new URL(config.testnet.pkarrRelay);

  if (!config.lockServer.pubky.startsWith('pubky')) {
    throw new Error('invalid demo config: lockServer.pubky must start with pubky');
  }
  if (!config.testnet.homeserver.startsWith('pubky')) {
    throw new Error('invalid demo config: testnet.homeserver must start with pubky');
  }
  if (!/^[^:]+:\d+$/.test(config.testnet.dhtBootstrap)) {
    throw new Error('invalid demo config: testnet.dhtBootstrap must look like host:port');
  }
  return config;
}

export async function writeDemoConfig(config, path = demoConfigPath) {
  validateDemoConfig(config);
  await writeJson(path, config);
  return config;
}

export async function readDemoConfig(path = demoConfigPath) {
  return validateDemoConfig(await readJson(path));
}

export function withInternalServiceUrls(config, env = process.env) {
  const internal = structuredClone(config);
  if (env.LOCKS_INTERNAL_DEMO_SERVER_URL) {
    internal.demoServer.url = env.LOCKS_INTERNAL_DEMO_SERVER_URL;
  }
  if (env.LOCKS_INTERNAL_LOCK_SERVER_URL) {
    internal.lockServer.url = env.LOCKS_INTERNAL_LOCK_SERVER_URL;
  }
  if (env.LOCKS_INTERNAL_HTTP_RELAY) {
    internal.testnet.httpRelay = env.LOCKS_INTERNAL_HTTP_RELAY;
  }
  if (env.LOCKS_INTERNAL_PKARR_RELAY) {
    internal.testnet.pkarrRelay = env.LOCKS_INTERNAL_PKARR_RELAY;
  }
  if (env.LOCKS_INTERNAL_DHT_BOOTSTRAP) {
    internal.testnet.dhtBootstrap = env.LOCKS_INTERNAL_DHT_BOOTSTRAP;
  }
  return validateDemoConfig(internal);
}

export async function buildDefaultDemoConfig() {
  const config = structuredClone(defaultDemoConfig);
  config.lockServer.pubky = await readLockServerPublicKey(config.lockServer.configPath);
  return validateDemoConfig(config);
}
