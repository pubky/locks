#!/usr/bin/env node
import { readFile } from 'node:fs/promises';
import { createServer } from 'node:http';

const listenPort = Number(process.env.HOMEGATE_BRIDGE_PORT ?? 8082);
const configPath = process.env.HOMEGATE_BRIDGE_CONFIG;
const homeserverAdminUrl = process.env.HOMEGATE_BRIDGE_HOMESERVER_ADMIN_URL;
const homeserverAdminPassword = process.env.PUBKY_HOMESERVER_ADMIN_PASSWORD;

if (!configPath || !homeserverAdminUrl || !homeserverAdminPassword) {
  throw new Error('Homegate bridge configuration is incomplete');
}

const server = createServer(async (request, response) => {
  if (request.method === 'GET' && request.url === '/health') {
    return sendJson(response, { ok: true });
  }
  if (request.method !== 'POST' || request.url !== '/ip_verification') {
    response.writeHead(404).end('not found');
    return;
  }

  try {
    const [{ testnet }, signupResponse] = await Promise.all([
      readDemoConfig(configPath),
      fetch(`${homeserverAdminUrl}/generate_signup_token`, {
        headers: { 'x-admin-password': homeserverAdminPassword },
      }),
    ]);
    if (!signupResponse.ok) throw new Error(`homeserver returned HTTP ${signupResponse.status}`);
    const signupCode = (await signupResponse.text()).trim();
    if (!signupCode || typeof testnet?.homeserver !== 'string') {
      throw new Error('homeserver signup response is incomplete');
    }
    sendJson(response, { signupCode, homeserverPubky: testnet.homeserver });
  } catch {
    sendJson(response, { error: 'signup unavailable' }, 503);
  }
});

server.listen(listenPort, '0.0.0.0', () => {
  console.log(`Homegate bridge listening on port ${listenPort}`);
});

async function readDemoConfig(path) {
  return JSON.parse(await readFile(path, 'utf8'));
}

function sendJson(response, value, status = 200) {
  response.writeHead(status, {
    'cache-control': 'no-store',
    'content-type': 'application/json; charset=utf-8',
  });
  response.end(`${JSON.stringify(value)}\n`);
}
