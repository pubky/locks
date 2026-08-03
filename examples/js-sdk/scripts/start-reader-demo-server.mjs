#!/usr/bin/env node
import { createReadStream, existsSync, statSync } from 'node:fs';
import { createServer } from 'node:http';
import { extname, join, normalize, resolve, sep } from 'node:path';

import { readDemoConfig, validateDemoConfig, pubkyAuthRelayInboxUrl, withInternalServiceUrls } from './lib/config.mjs';
import { examplesRoot, parseArgs, repoRoot } from './lib/paths.mjs';

const args = parseArgs(process.argv.slice(2));
const allowUnhealthy = args['allow-unhealthy'] === true;
const config = await readDemoConfig();
const serviceConfig = withInternalServiceUrls(config);
const readerUrl = new URL(config.demoServer.url);
readerUrl.port = String(args.port ?? 8081);
const readerServerUrl = readerUrl.toString().replace(/\/$/, '');
const preflightStatus = await runPreflight(serviceConfig);

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
      const entry = await readJsonBody(request);
      console.log(`[reader-client:${entry.level ?? 'info'}] ${entry.event ?? 'event'} ${JSON.stringify(entry)}`);
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
  } catch (error) {
    console.error(error);
    sendJson(response, { error: error.message }, 500);
  }
});

server.listen(Number(readerUrl.port), () => {
  console.log(`JS SDK reader demo server listening at ${readerServerUrl}`);
  console.log(`Open ${readerServerUrl}/reader/`);
});

function publicBrowserConfig(source) {
  validateDemoConfig(source);
  return {
    demoServer: { url: readerServerUrl },
    lockServer: source.lockServer,
    testnet: source.testnet,
    paths: {},
  };
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
  const resolved = normalizedPath.startsWith('locks-sdk/')
    ? resolve(repoRoot, normalizedPath)
    : resolve(examplesRoot, normalizedPath);
  const allowedRoots = [examplesRoot, resolve(repoRoot, 'locks-sdk/bindings/js/pkg')];
  if (!allowedRoots.some((root) => resolved === root || resolved.startsWith(`${root}${sep}`))) {
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
  response.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  response.end(JSON.stringify(body));
}

async function readJsonBody(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  if (chunks.length === 0) return {};
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
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
