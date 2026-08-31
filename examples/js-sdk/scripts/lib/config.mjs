import { existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { resolve } from 'node:path';

import { demoConfigPath, writeJson, readJson } from './paths.mjs';

export const defaultLockServerConfigPath = '~/.pubky-lock/config.toml';

export const defaultDemoConfig = {
  demoServer: {
    url: 'http://127.0.0.1:8080',
  },
  lockServer: {
    url: 'http://127.0.0.1:3000',
    pubky: '',
    configPath: defaultLockServerConfigPath,
  },
  paykit: {
    url: 'http://127.0.0.1:3001',
  },
  testnet: {
    homeserver: 'pubky8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo',
    httpRelay: 'http://127.0.0.1:15412',
    pkarrRelay: 'http://127.0.0.1:15411',
    dhtBootstrap: '127.0.0.1:6881',
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

export function parseLockServerTomlPaykitServerUrl(tomlText) {
  const publicMatch = tomlText.match(/^[ \t]*paykit_server_url[ \t]*=[ \t]*"([^"]+)"[ \t]*(?:#[^\r\n]*)?$/m);
  const lines = tomlText.split(/\r?\n/);
  const paykitStart = lines.findIndex((line) => /^[ \t]*\[paykit\][ \t]*(?:#.*)?$/.test(line));
  const remainingLines = paykitStart < 0 ? [] : lines.slice(paykitStart + 1);
  const nextSection = remainingLines.findIndex((line) => /^[ \t]*\[/.test(line));
  const paykitSection = remainingLines
    .slice(0, nextSection < 0 ? remainingLines.length : nextSection)
    .join('\n');
  const sectionMatch = paykitSection.match(/^[ \t]*server_url[ \t]*=[ \t]*"([^"]+)"[ \t]*(?:#[^\r\n]*)?$/m);
  const match = publicMatch ?? sectionMatch;
  if (!match) throw new Error('missing paykit_server_url in Lock Server public config');
  const value = match[1].trim();
  const url = new URL(value);
  if (
    !['http:', 'https:'].includes(url.protocol)
    || url.username
    || url.password
    || value !== url.origin
  ) {
    throw new Error('invalid paykit_server_url in Lock Server public config');
  }
  return value;
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
    ['paykit', 'url'],
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
  const paykitUrl = new URL(config.paykit.url);
  new URL(config.testnet.httpRelay);
  new URL(config.testnet.pkarrRelay);

  if (
    !['http:', 'https:'].includes(paykitUrl.protocol)
    || paykitUrl.username
    || paykitUrl.password
    || config.paykit.url !== paykitUrl.origin
  ) {
    throw new Error('invalid demo config: paykit.url must be an exact HTTP(S) origin without credentials');
  }

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

export async function buildDefaultDemoConfig(lockServerConfigPath = defaultLockServerConfigPath) {
  const config = structuredClone(defaultDemoConfig);
  config.lockServer.configPath = lockServerConfigPath;
  const publicConfig = await readFile(expandHome(lockServerConfigPath), 'utf8');
  config.lockServer.pubky = parseLockServerTomlPublicKey(publicConfig);
  config.paykit.url = parseLockServerTomlPaykitServerUrl(publicConfig);
  return validateDemoConfig(config);
}

export function buildPaykitServerConfig({ lockServerPubky }) {
  if (!/^pubky[ybndrfg8ejkmcpqxot1uwisza345h769]{52}$/.test(lockServerPubky ?? '')) {
    throw new Error('invalid Lock Server Pubky');
  }
  return `[http]
listen_addr = "0.0.0.0:3001"

[locks]
trusted_public_key = "${lockServerPubky}"

[setup]
allowed_origins = ["http://127.0.0.1:8080", "http://localhost:8080"]
log_authorization_url = true

[paykit]
client_id = "app.paykit.server"
receiver_path = "bitkit/server"
receiver_path_priority = ["bitkit"]
network = "testnet"

[bitcoin]
network = "regtest"

[electrum]
endpoint = "tcp://fulcrum:50001"
poll_interval = "1s"
request_timeout = "10s"
connect_retries = 1

[outbox]
poll_interval = "500ms"
batch_size = 16
lease_duration = "30s"
retry_initial = "1s"
retry_max = "5m"
`;
}

export function validatePaykitComposeEnvironment(environment) {
  const databaseUrl = environment.PAYKIT_DATABASE_URL;
  const masterKey = environment.PAYKIT_MASTER_KEY;
  const bitcoinRpcUser = environment.BITCOIN_RPC_USER;
  const bitcoinRpcPassword = environment.BITCOIN_RPC_PASSWORD;
  if (typeof databaseUrl !== 'string' || !/^postgres:\/\/[^\s]+$/.test(databaseUrl)) {
    throw new Error('PAYKIT_DATABASE_URL is invalid');
  }
  if (typeof masterKey !== 'string' || !/^[A-Za-z0-9_-]{43}$/.test(masterKey)) {
    throw new Error('PAYKIT_MASTER_KEY is invalid');
  }
  if (typeof bitcoinRpcUser !== 'string' || !/^[A-Za-z0-9_-]{8,128}$/.test(bitcoinRpcUser)) {
    throw new Error('BITCOIN_RPC_USER is invalid');
  }
  if (typeof bitcoinRpcPassword !== 'string' || !/^[A-Za-z0-9_-]{16,128}$/.test(bitcoinRpcPassword)) {
    throw new Error('BITCOIN_RPC_PASSWORD is invalid');
  }
  return { databaseUrl, masterKey, bitcoinRpcUser, bitcoinRpcPassword };
}

export function buildPubkyHomeserverComposeConfig({ databasePassword, adminPassword }) {
  for (const [name, value] of Object.entries({ databasePassword, adminPassword })) {
    if (typeof value !== 'string' || !/^[A-Za-z0-9_-]{16,128}$/.test(value)) {
      throw new Error(`invalid Pubky homeserver ${name}`);
    }
  }
  return `[general]
database_url = "postgres://locks:${databasePassword}@postgres:5432/pubky_homeserver"
signup_mode = "open"

[drive]
pubky_listen_socket = "0.0.0.0:6287"
icann_listen_socket = "0.0.0.0:6286"

[storage]
type = "file_system"

[admin]
enabled = true
listen_socket = "0.0.0.0:6288"
admin_password = "${adminPassword}"

[metrics]
enabled = false
listen_socket = "0.0.0.0:6289"

[pkdns]
public_ip = "127.0.0.1"
public_pubky_tls_port = 6287
public_icann_http_port = 6286
icann_domain = "localhost"
user_keys_republisher_interval = 14400
dht_request_timeout_ms = 2000

[logging]
level = "info"
module_levels = ["pubky_homeserver=debug", "tower_http=debug"]
`;
}
