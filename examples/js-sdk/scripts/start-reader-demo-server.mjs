#!/usr/bin/env node
import { createReadStream, existsSync, statSync } from 'node:fs';
import { createServer } from 'node:http';
import { extname, join, normalize } from 'node:path';

import { readDemoConfig, validateDemoConfig, pubkyAuthRelayInboxUrl, withInternalServiceUrls } from './lib/config.mjs';
import { examplesRoot, parseArgs, readJson, repoRoot, roleProfilePath } from './lib/paths.mjs';
import { resolveExistingPathWithin } from './lib/creator-static-path.mjs';
import { readReaderCreatorProfile } from './lib/paykit-reader-helper.mjs';
import {
  runPaykitReaderWorker,
  supervisePaykitReaderWorker,
  waitForCreatorProfile,
} from './lib/paykit-reader-worker.mjs';
import {
  buildPaykitReaderBrowserStatus,
  readPaykitReaderWorkerStatus,
  writePaykitReaderWorkerStatus,
} from './lib/paykit-reader-status.mjs';

const args = parseArgs(process.argv.slice(2));
const allowUnhealthy = args['allow-unhealthy'] === true;
const config = await readDemoConfig();
const serviceConfig = withInternalServiceUrls(config);
const readerUrl = new URL(config.demoServer.url);
readerUrl.port = String(args.port ?? 8088);
const readerServerUrl = readerUrl.toString().replace(/\/$/, '');
const preflightStatus = await runPreflight(serviceConfig);
const externalReaderPubky = process.env.PAYKIT_EXTERNAL_READER_PUBKY?.trim() ?? '';
if (externalReaderPubky && !/^pubky[ybndrfg8ejkmcpqxot1uwisza345h769]{52}$/.test(externalReaderPubky)) {
  throw new Error('PAYKIT_EXTERNAL_READER_PUBKY must be a canonical Pubky');
}
const workerEnabled = !externalReaderPubky && process.env.PAYKIT_READER_WORKER_ENABLED === '1';
const workerController = new AbortController();
let workerOwnsState = false;
let workerWaitingForCreator = workerEnabled;

logStartupDiagnostics(config, preflightStatus);

if (!allowUnhealthy && preflightStatus.checks.some((check) => !check.ok)) {
  console.error('Reader demo preflight failed:');
  for (const check of preflightStatus.checks) {
    if (!check.ok) console.error(`- ${check.name}: ${check.message}`);
  }
  console.error('Pass --allow-unhealthy to start anyway.');
  process.exit(1);
}
if (allowUnhealthy && preflightStatus.checks.some((check) => !check.ok)) {
  console.warn('Starting reader demo despite unhealthy preflight because --allow-unhealthy was provided.');
}

const server = createServer(async (request, response) => {
  // Reader demo server is static/debug only: never proxy Lock Server viewer APIs.
  try {
    const url = new URL(request.url, readerServerUrl);
    logRequest(request, url);
    if (request.method === 'GET' && url.pathname === '/api/health') {
      return sendJson(response, { status: 'ok' });
    }
    if (request.method === 'GET' && url.pathname === '/config.json') {
      return sendJson(response, publicBrowserConfig(config));
    }
    if (request.method === 'GET' && url.pathname === '/api/preflight') {
      return sendJson(response, preflightStatus);
    }
    if (request.method === 'GET' && url.pathname === '/api/debug/config') {
      return sendJson(response, debugSnapshot(config, preflightStatus));
    }
    if (request.method === 'GET' && url.pathname === '/api/paykit-reader/status') {
      const status = await publicPaykitReaderStatus({
        currentOwner: workerEnabled && workerOwnsState,
        waitingForCreator: workerWaitingForCreator,
      });
      return sendJson(response, status, ['starting', 'failed'].includes(status.state) ? 503 : 200);
    }
    if (request.method === 'POST' && url.pathname === '/api/client-log') {
      let level;
      try {
        level = await readClientLogLevel(request);
      } catch {
        return sendJson(response, { error: 'invalid client log' }, 400);
      }
      console.log(`[reader-client:${level}] event`);
      return sendJson(response, { ok: true });
    }
    if (request.method !== 'GET') {
      response.writeHead(405).end('method not allowed');
      return;
    }
    if (url.pathname === '/' || url.pathname === '/reader/' || url.pathname === '/reader.html') {
      return serveStatic(response, join(examplesRoot, 'reader.html'));
    }
    return servePath(url.pathname, response);
  } catch {
    console.error('reader demo request failed');
    sendJson(response, { error: 'request failed' }, 500);
  }
});

server.listen(Number(readerUrl.port), () => {
  console.log(`JS SDK reader demo server listening at ${readerServerUrl}`);
  console.log(`Open ${readerServerUrl}/reader/`);
});

const workerTask = workerEnabled
  ? supervisePaykitReaderWorker(
    runWorkerAfterCreatorProfile(),
    { onTerminalFailure: handleTerminalWorkerFailure },
  )
  : Promise.resolve({ status: 'stopped' });
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => { void shutdown(signal); });
}

function publicBrowserConfig(source) {
  validateDemoConfig(source);
  return {
    demoServer: { url: readerServerUrl },
    lockServer: source.lockServer,
    testnet: source.testnet,
    paths: {},
  };
}

async function runWorkerAfterCreatorProfile() {
  await waitForCreatorProfile({
    signal: workerController.signal,
    readProfile: readReaderCreatorProfile,
  });
  if (workerController.signal.aborted) return { status: 'stopped' };
  workerWaitingForCreator = false;
  return runPaykitReaderWorker({
    signal: workerController.signal,
    writeWorkerStatus: writePaykitReaderWorkerStatus,
    onOwnershipChange: (owned) => { workerOwnsState = owned; },
  });
}

export async function publicPaykitReaderStatus({
  readWorker = readPaykitReaderWorkerStatus,
  readProfile = () => readJson(roleProfilePath('content-viewer')),
  currentOwner = false,
  waitingForCreator = false,
} = {}) {
  if (externalReaderPubky) {
    return { version: 1, state: 'waiting', reader_pubky: externalReaderPubky };
  }
  const [worker, profile] = await Promise.all([
    Promise.resolve().then(readWorker).catch(() => null),
    Promise.resolve().then(readProfile).catch(() => null),
  ]);
  return buildPaykitReaderBrowserStatus(worker, profile, { currentOwner, waitingForCreator });
}

async function handleTerminalWorkerFailure(error) {
  const terminalError = [
    'invalid_input',
    'invalid_config',
    'invalid_state',
    'output_failed',
    'prepare_timeout',
  ].includes(error) ? error : 'worker_failed';
  console.error(`[reader-demo] Paykit reader worker stopped (${terminalError})`);
  workerController.abort();
  process.exitCode = 1;
  server.close();
}

let shutdownStarted = false;
async function shutdown(signal) {
  if (shutdownStarted) return;
  shutdownStarted = true;
  console.log(`[reader-demo] shutting down after ${signal}`);
  workerController.abort();
  await Promise.allSettled([
    workerTask,
    new Promise((resolveClose) => server.close(resolveClose)),
  ]);
}

function debugSnapshot(source, preflight) {
  return {
    checkedAt: new Date().toISOString(),
    config: publicBrowserConfig(source),
    derived: {
      authRelayInbox: pubkyAuthRelayInboxUrl(source.testnet.httpRelay),
      readerUrl: `${readerServerUrl}/reader/`,
    },
    preflight,
  };
}

async function runPreflight(source) {
  const checks = [];
  try {
    validateDemoConfig(source);
    checks.push({ name: 'config', ok: true, message: 'valid' });
  } catch (error) {
    checks.push({ name: 'config', ok: false, message: error.message });
  }
  const wasmPackage = join(repoRoot, 'locks-sdk/bindings/js/pkg/locks_sdk_wasm_bg.wasm');
  checks.push({ name: 'WASM package', ok: existsSync(wasmPackage), message: existsSync(wasmPackage) ? 'present' : 'missing' });
  await checkHttp(`${source.lockServer.url}/healthz`, 'lock-server /healthz', checks, (status) => status === 200);
  await checkHttp(`${source.lockServer.url}/readyz`, 'lock-server /readyz', checks, (status) => status === 200);
  await checkHttp(source.testnet.pkarrRelay, 'pkarr relay', checks, (status) => status === 200 || status === 404);
  await checkHttp(source.testnet.httpRelay, 'http/auth relay', checks, (status) => status === 200 || status === 404);
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

function servePath(pathname, response) {
  const normalizedPath = normalize(pathname).replace(/^[/\\]+/, '');
  const packagePrefix = 'locks-sdk/bindings/js/pkg/';
  const resolved = normalizedPath.startsWith(packagePrefix)
    ? resolveExistingPathWithin(join(repoRoot, 'locks-sdk/bindings/js/pkg'), normalizedPath.slice(packagePrefix.length))
    : normalizedPath.startsWith('locks-sdk/')
      ? null
      : resolveExistingPathWithin(examplesRoot, normalizedPath);
  if (!resolved) {
    response.writeHead(404).end('not found');
    return;
  }
  if (!existsSync(resolved) || statSync(resolved).isDirectory()) {
    response.writeHead(404).end('not found');
    return;
  }
  serveStatic(response, resolved);
}

function serveStatic(response, filePath) {
  response.writeHead(200, { 'content-type': contentType(filePath) });
  const stream = createReadStream(filePath);
  stream.on('error', () => {
    if (!response.headersSent) response.writeHead(500);
    response.end();
  });
  stream.pipe(response);
}

function contentType(filePath) {
  switch (extname(filePath)) {
    case '.html': return 'text/html; charset=utf-8';
    case '.js': return 'text/javascript; charset=utf-8';
    case '.wasm': return 'application/wasm';
    case '.json': return 'application/json; charset=utf-8';
    case '.css': return 'text/css; charset=utf-8';
    default: return 'application/octet-stream';
  }
}

function sendJson(response, body, status = 200) {
  response.writeHead(status, {
    'cache-control': 'no-store',
    'content-type': 'application/json; charset=utf-8',
  });
  response.end(JSON.stringify(body));
}

async function readClientLogLevel(request) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > 128) throw new Error('client log too large');
    chunks.push(chunk);
  }
  const value = JSON.parse(Buffer.concat(chunks).toString('utf8'));
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

function logStartupDiagnostics(source, preflight) {
  console.log('[reader-demo] startup diagnostics');
  console.log(JSON.stringify(debugSnapshot(source, preflight), null, 2));
}

function logRequest(request, url) {
  if (url.pathname.startsWith('/api/') || url.pathname === '/config.json' || url.pathname.startsWith('/reader')) {
    console.log(`[reader-demo] ${request.method} ${url.pathname}`);
  }
}
