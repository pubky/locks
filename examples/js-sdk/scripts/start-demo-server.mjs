#!/usr/bin/env node
import { createReadStream, existsSync, statSync } from 'node:fs';
import { createServer } from 'node:http';
import { extname, join } from 'node:path';

import { AuthFlowKind, pubkyForConfig } from './lib/pubky.mjs';
import { contentCreatorSessionPath, examplesRoot, parseArgs, repoRoot } from './lib/paths.mjs';
import { readDemoConfig, validateDemoConfig, pubkyAuthRelayInboxUrl, withInternalServiceUrls } from './lib/config.mjs';
import {
  readCreatorDemoSessionForCurrentRole,
  writeCreatorDemoSessionForCurrentRole,
} from './lib/creator-session-state.mjs';
import { resolveCreatorStaticPath } from './lib/creator-static-path.mjs';
import { publishCreatorProfile } from './publish-creator-profile.mjs';

const args = parseArgs();
const allowUnhealthy = Boolean(args['allow-unhealthy']);
const externalWallet = Boolean(args['external-wallet']);
const config = await readDemoConfig();
const serviceConfig = withInternalServiceUrls(config);
const port = Number(new URL(config.demoServer.url).port || 8080);
const debugEnabled = args.debug !== false;
let activeDemoAuthFlow = null;
let activeDemoAuthUrl = null;
let activeDemoAuthStartedAt = null;
let demoAuthPromise = null;
let preflightStatus = await runPreflight(serviceConfig);

logStartupDiagnostics(config, preflightStatus);

if (!allowUnhealthy && preflightStatus.checks.some((check) => !check.ok)) {
  console.error('start-demo-server preflight failed:');
  for (const check of preflightStatus.checks) {
    console.error(`- ${check.ok ? 'ok' : 'FAIL'} ${check.name}: ${check.message}`);
  }
  console.error('Use --allow-unhealthy to start anyway.');
  process.exit(1);
}

if (allowUnhealthy && preflightStatus.checks.some((check) => !check.ok)) {
  console.warn('Starting despite unhealthy preflight because --allow-unhealthy was provided.');
}

if (externalWallet) await readCurrentCreatorSession();

const server = createServer(async (request, response) => {
  try {
    const url = new URL(request.url, config.demoServer.url);
    logRequest(request, url);
    if (request.method === 'GET' && url.pathname === '/config.json') {
      return sendJson(response, publicBrowserConfig(config));
    }
    if (request.method === 'GET' && url.pathname === '/api/preflight') {
      return sendJson(response, preflightStatus);
    }
    if (request.method === 'GET' && url.pathname === '/api/debug/config') {
      return sendJson(response, debugSnapshot(config, preflightStatus));
    }
    if (request.method === 'POST' && url.pathname === '/api/client-log') {
      let level;
      try {
        level = await readClientLogLevel(request);
      } catch {
        return sendJson(response, { error: 'invalid client log' }, 400);
      }
      console.log(`[creator-client:${level}] event`);
      return sendJson(response, { ok: true });
    }
    if (request.method === 'POST' && url.pathname === '/api/demo-auth/start') {
      const result = await startDemoAuth();
      return sendJson(response, result);
    }
    if (request.method === 'GET' && url.pathname === '/api/demo-auth/status') {
      return sendJson(response, await demoAuthStatus());
    }
    if (request.method === 'GET' && url.pathname === '/auth/lock-server/callback') {
      // Legacy callback URLs still resolve to the creator page. Both current creator pages use
      // direct postMessage delivery and never navigate to this route.
      return serveStatic(response, join(examplesRoot, 'index.html'));
    }
    if (request.method !== 'GET') {
      response.writeHead(405).end('method not allowed');
      return;
    }
    return servePath(url.pathname, response);
  } catch (error) {
    console.error('demo request failed');
    sendJson(response, { error: 'request failed' }, 500);
  }
});

server.listen(port, () => {
  console.log(`JS SDK demo server listening at ${config.demoServer.url}`);
  console.log(`Open ${config.demoServer.url}/examples/js-sdk/`);
});

async function startDemoAuth() {
  if (await hasPersistedDemoSession()) {
    console.log(`[demo] reusing persisted content-creator session at ./.local/js-sdk-demo/content-creator-session.json`);
    return { authenticated: true, role: 'content-creator' };
  }
  if (!activeDemoAuthFlow) {
    const pubky = pubkyForConfig(serviceConfig);
    const capabilities = '/pub/locks.app/:rw,/priv/locks.app/:rw';
    const authRelay = pubkyAuthRelayInboxUrl(serviceConfig.testnet.httpRelay);
    activeDemoAuthFlow = pubky.startCookieAuthFlow(capabilities, AuthFlowKind.signin(), authRelay);
    activeDemoAuthUrl = activeDemoAuthFlow.authorizationUrl;
    activeDemoAuthStartedAt = new Date().toISOString();
    demoAuthPromise = activeDemoAuthFlow
      .awaitApproval()
      .then(async (session) => {
        const creatorSession = {
          role: 'content-creator',
          pubky: session.info.publicKey.toString(),
          capabilities: session.info.capabilities,
          exported_session: session.export(),
          authenticated_at: new Date().toISOString(),
        };
        await writeCreatorDemoSessionForCurrentRole(creatorSession, sessionStateOptions());
        if (externalWallet) await publishCreatorProfile({ profile: creatorSession });
        return session;
      })
      .catch((error) => {
        console.error(`demo auth failed: ${error instanceof Error ? error.message : String(error)}`);
      })
      .finally(() => {
        activeDemoAuthFlow = null;
        activeDemoAuthUrl = null;
        activeDemoAuthStartedAt = null;
        demoAuthPromise = null;
      });
  }
  const result = {
    authenticated: false,
    role: 'content-creator',
    authorizationUrl: activeDemoAuthUrl,
    startedAt: activeDemoAuthStartedAt,
  };
  if (!externalWallet) {
    result.command = `npm --prefix examples/js-sdk run authenticate -- --role content-creator --auth "${activeDemoAuthUrl}"`;
  }
  return result;
}

async function demoAuthStatus() {
  const session = await readCurrentCreatorSession();
  if (session) {
    if (debugEnabled) {
      console.log(`[demo] demo-auth persisted session pubky=${session.pubky} path=./.local/js-sdk-demo/content-creator-session.json`);
    }
    return {
      authenticated: true,
      role: 'content-creator',
      pubky: session.pubky,
      homeserver: config.testnet.homeserver,
      sessionPath: './.local/js-sdk-demo/content-creator-session.json',
    };
  }
  return {
    authenticated: false,
    role: 'content-creator',
    pending: Boolean(activeDemoAuthFlow || demoAuthPromise),
    authorizationUrl: activeDemoAuthUrl,
    startedAt: activeDemoAuthStartedAt,
  };
}

async function hasPersistedDemoSession() {
  return Boolean(await readCurrentCreatorSession());
}

function sessionStateOptions() {
  return externalWallet ? { profilePath: null } : {};
}

async function readCurrentCreatorSession() {
  const session = await readCreatorDemoSessionForCurrentRole(sessionStateOptions());
  if (session && externalWallet) await publishCreatorProfile({ profile: session });
  return session;
}

function publicBrowserConfig(source) {
  validateDemoConfig(source);
  return {
    demoServer: source.demoServer,
    lockServer: source.lockServer,
    paykit: source.paykit,
    testnet: source.testnet,
    paths: {
      lockServerCallback: `${source.demoServer.url}/auth/lock-server/callback`,
    },
  };
}

function logStartupDiagnostics(source, preflight) {
  if (!debugEnabled) return;
  const authRelay = pubkyAuthRelayInboxUrl(source.testnet.httpRelay);
  console.log('[demo] startup diagnostics');
  console.log('[demo] config path: ./.local/demo-config/config.json');
  console.log(`[demo] demoServer.url=${source.demoServer.url}`);
  console.log(`[demo] lockServer.url=${source.lockServer.url}`);
  console.log(`[demo] lockServer.pubky=${source.lockServer.pubky}`);
  console.log(`[demo] lockServer.callback=${source.demoServer.url}/auth/lock-server/callback`);
  console.log(`[demo] testnet.pkarrRelay=${source.testnet.pkarrRelay}`);
  console.log(`[demo] testnet.httpRelay=${source.testnet.httpRelay}`);
  console.log(`[demo] testnet.authRelayInbox=${authRelay}`);
  console.log(`[demo] testnet.homeserver=${source.testnet.homeserver}`);
  for (const check of preflight.checks) {
    console.log(`[demo] preflight ${check.ok ? 'ok' : 'FAIL'} ${check.name}: ${check.message}`);
  }
}

function debugSnapshot(source, preflight) {
  return {
    checkedAt: new Date().toISOString(),
    config: publicBrowserConfig(source),
    derived: {
      lockServerCallback: `${source.demoServer.url}/auth/lock-server/callback`,
      authRelayInbox: pubkyAuthRelayInboxUrl(source.testnet.httpRelay),
      expectedConnectUrlHost: source.lockServer.pubky,
      contentCreatorSessionPath: './.local/js-sdk-demo/content-creator-session.json',
    },
    state: {
      hasPersistedDemoSession: existsSync(contentCreatorSessionPath),
      activeDemoAuth: Boolean(activeDemoAuthFlow),
      activeDemoAuthStartedAt,
    },
    preflight,
  };
}

function logRequest(request, url) {
  if (!debugEnabled) return;
  const interesting = url.pathname.startsWith('/api/') || url.pathname.startsWith('/auth/') || url.pathname === '/config.json';
  if (!interesting) return;
  console.log(`[demo] ${request.method} ${url.pathname}`);
}

async function runPreflight(source) {
  const checks = [];
  const push = (name, ok, message) => checks.push({ name, ok, message });

  try {
    validateDemoConfig(source);
    push('config', true, 'valid');
  } catch (error) {
    push('config', false, error.message);
  }

  const wasmPackage = join(repoRoot, 'locks-sdk/bindings/js/pkg/locks_sdk_wasm_bg.wasm');
  push('WASM package', existsSync(wasmPackage), existsSync(wasmPackage) ? 'present' : 'missing');

  await checkHttp(`${source.lockServer.url}/healthz`, 'lock-server /healthz', checks, (status) => status >= 200 && status < 300);
  await checkHttp(`${source.lockServer.url}/readyz`, 'lock-server /readyz', checks, (status) => status >= 200 && status < 300);
  await checkHttp(source.testnet.pkarrRelay, 'pkarr relay', checks, (status) => status < 500); // status < 500
  await checkHttp(source.testnet.httpRelay, 'http/auth relay', checks, (status) => status < 500); // status < 500

  if (/^[^:]+:\d+$/.test(source.testnet.dhtBootstrap)) {
    push('dht bootstrap', true, 'valid host:port syntax');
  } else {
    push('dht bootstrap', false, 'expected host:port');
  }

  if (source.testnet.homeserver.startsWith('pubky')) {
    push('homeserver', true, 'pubky-shaped');
  } else {
    push('homeserver', false, 'expected pubky... public key');
  }

  return { ok: checks.every((check) => check.ok), checks, checkedAt: new Date().toISOString() };
}

async function checkHttp(url, name, checks, acceptsStatus) {
  try {
    const response = await fetch(url, { method: 'GET' });
    checks.push({ name, ok: acceptsStatus(response.status), message: `HTTP ${response.status}` });
  } catch (error) {
    checks.push({ name, ok: false, message: error.message });
  }
}

async function readClientLogLevel(request) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > 128) throw new Error('client log too large');
    chunks.push(chunk);
  }
  const text = Buffer.concat(chunks).toString('utf8');
  const value = JSON.parse(text);
  if (
    value === null
    || typeof value !== 'object'
    || Array.isArray(value)
    || Object.keys(value).length !== 1
    || !Object.hasOwn(value, 'level')
    || !['info', 'warn', 'error'].includes(value.level)
  ) throw new Error('invalid client log');
  return value.level;
}

function servePath(pathname, response) {
  const filePath = resolveCreatorStaticPath(pathname, { repoRoot, examplesRoot });
  if (!filePath) {
    response.writeHead(404).end('not found');
    return;
  }
  if (!existsSync(filePath) || statSync(filePath).isDirectory()) {
    response.writeHead(404).end('not found');
    return;
  }
  return serveStatic(response, filePath);
}

function serveStatic(response, filePath) {
  response.writeHead(200, {
    'content-type': contentType(filePath),
    'cache-control': 'no-store',
  });
  const stream = createReadStream(filePath);
  stream.on('error', () => {
    if (!response.headersSent) response.writeHead(500);
    response.end();
  });
  stream.pipe(response);
}

function sendJson(response, value, status = 200) {
  response.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' });
  response.end(`${JSON.stringify(value, null, 2)}\n`);
}

function contentType(filePath) {
  return {
    '.html': 'text/html; charset=utf-8',
    '.js': 'text/javascript; charset=utf-8',
    '.mjs': 'text/javascript; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.wasm': 'application/wasm',
  }[extname(filePath)] ?? 'application/octet-stream';
}
