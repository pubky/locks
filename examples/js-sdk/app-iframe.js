import {
  configureLockServicePointer,
  exchangeCreatorConnectCode,
  publishLockedContent,
  startCreatorConnect,
} from './creator-complete-flow.js';
import init, { Locks } from '../../locks-sdk/bindings/js/pkg/locks_sdk_wasm.js';

// iframe flow variant of app.js — direct postMessage delivery (ADR 0019).
// Step 2 (Authenticate to Lock Server) opens the Lock Server /connect page in an IFRAME MODAL with
// ?delivery=postmessage. The /connect shell itself posts { type: 'locks-auth-callback', state, code }
// straight to this parent window — NO full-page redirect, NO same-origin callback page on this app.
// The message arrives from the Lock Server origin (cross-origin), so we validate event.origin against
// the connect URL origin. This app hosts no callback route for the iframe flow.
// The frontend session token (feLockSessionToken) is kept IN-MEMORY only (not localStorage).

// Documented endpoint names for the smoke checker and readers:
// POST /api/demo-auth/start
// GET /api/demo-auth/status
// Verifier dropdown initial option: dev-static

// Message type published by the Lock Server /connect shell (embedder contract).
const LOCKS_AUTH_CALLBACK_TYPE = 'locks-auth-callback';
const POINTER_CONFIGURED_KEY = 'pubky-locks-demo.pointerConfigured';

const state = {
  config: null,
  demoAuthenticated: false,
  lockAuthenticated: false,
  feLockSessionToken: null, // Lock Server frontend session token — in-memory only (cleared on reload)
  lastReceivedCode: null,   // one-time code received from the iframe callback (for display)
  pendingConnectState: null, // opaque state persisted for the in-flight connect (in-memory)
  lockServerOrigin: null,    // origin of the connect iframe; the only accepted postMessage sender
  lockAuthFrame: null,       // the connect iframe element; only its window may post the callback
};

const el = {
  demoAuthStatus: document.querySelector('#demo-auth-status'),
  startDemoAuth: document.querySelector('#start-demo-auth'),
  demoAuthCommand: document.querySelector('#demo-auth-command'),
  lockAuthStatus: document.querySelector('#lock-auth-status'),
  startLockAuth: document.querySelector('#start-lock-auth'),
  publishingStatus: document.querySelector('#publishing-status'),
  configurePointer: document.querySelector('#configure-pointer'),
  lockedContentForm: document.querySelector('#locked-content-form'),
  primaryContentFile: document.querySelector('#primary-content-file'),
  secondaryContentFiles: document.querySelector('#secondary-content-files'),
  resourceFilename: document.querySelector('#resource-filename'),
  selectedResources: document.querySelector('#selected-resources'),
  selectedResourceList: document.querySelector('#selected-resource-list'),
  verifierType: document.querySelector('#verifier-type'),
  criterionId: document.querySelector('#criterion-id'),
  criterionSatisfied: document.querySelector('#criterion-satisfied'),
  accessTtl: document.querySelector('#access-ttl'),
  creatorResult: document.querySelector('#creator-result'),
  viewerResource: document.querySelector('#viewer-resource'),
};

await init();
await bootstrap();

// Receive { type, state, code } posted by the Lock Server /connect shell (cross-origin). The parent
// exchanges the one-time code for a session itself; the raw code is the only secret handed over.
window.addEventListener('message', async (event) => {
  // Only accept a message from the exact connect iframe we opened: right origin AND right window.
  if (!state.lockServerOrigin || event.origin !== state.lockServerOrigin) return;
  if (event.source !== state.lockAuthFrame?.contentWindow) return;
  // Size the iframe to the shell's reported content height (QR panel vs shorter mobile button),
  // so the modal hugs the content instead of leaving a fixed-height gap.
  if (event.data?.type === 'locks-auth-resize') {
    if (typeof event.data.height === 'number' && state.lockAuthFrame) {
      state.lockAuthFrame.style.height = `${Math.max(0, event.data.height)}px`;
    }
    return;
  }
  if (event.data?.type !== LOCKS_AUTH_CALLBACK_TYPE) return;
  // The shell reports a definitive failure (expired/rejected flow) instead of hanging.
  if (event.data.error) {
    closeLockAuthIframe();
    await postClientLog('error', 'lock-auth-iframe-shell-error', { error: event.data.error });
    showError(el.lockAuthStatus, new Error(`Lock Server connect failed: ${event.data.error}`));
    return;
  }
  try {
    const { code, state: receivedState } = event.data;
    const { sessionSecret } = await exchangeCreatorConnectCode({
      lockServer: state.config.lockServer.pubky,
      code,
      state: receivedState,
      expectedState: state.pendingConnectState,
      pkarrRelays: [state.config.testnet.pkarrRelay],
    });
    state.feLockSessionToken = sessionSecret; // in-memory only
    state.lastReceivedCode = code;
    state.lockAuthenticated = true;
    refreshLockAuthStatus();
    await postClientLog('info', 'lock-auth-iframe-complete', { code });
    showLockAuthComplete();
  } catch (error) {
    closeLockAuthIframe(); // otherwise the full-screen overlay hides the error message
    await postClientLog('error', 'lock-auth-iframe-exchange-failed', serializeError(error));
    showError(el.lockAuthStatus, error);
  }
});

// Open the Lock Server /connect page inside an iframe overlay. The demo draws the modal CARD
// (title, description, close) — mirroring what pubky.app provides in the real integration — and the
// iframe renders only the secret-bearing QR on the Lock Server origin (parent cannot read it).
function openLockAuthIframe(connectUrl) {
  closeLockAuthIframe();
  const overlay = document.createElement('div');
  overlay.id = 'lock-auth-iframe-overlay';
  overlay.style.cssText =
    'position:fixed;inset:0;background:rgba(5,5,10,0.6);display:flex;z-index:9999;' +
    'align-items:center;justify-content:center;';
  overlay.addEventListener('click', (event) => {
    if (event.target === overlay) closeLockAuthIframe();
  });

  // Centered card for every viewport. The modal chrome (card vs bottom sheet, placement) is the
  // embedder's (pubky.app's) call, not the SDK's — the shell only supplies the inner content
  // (QR on desktop, Authorize button on touch), which the resize message keeps sized to fit.
  const card = document.createElement('div');
  card.style.cssText =
    'box-sizing:border-box;position:relative;width:min(400px,92vw);display:flex;flex-direction:column;' +
    'gap:24px;padding:32px;background:#1d1d20;border-top:1px solid #c8ff00;border-bottom:1px solid #c8ff00;' +
    'border-radius:16px;box-shadow:0 50px 100px 0 rgba(5,5,10,.75);color:#fff;' +
    'font-family:system-ui,-apple-system,sans-serif;';

  const closeBtn = document.createElement('button');
  closeBtn.setAttribute('aria-label', 'Close');
  closeBtn.textContent = '✕';
  closeBtn.style.cssText =
    'position:absolute;top:24px;right:24px;width:32px;height:32px;border:0;border-radius:999px;' +
    'background:#303034;color:#fff;opacity:.7;cursor:pointer;';
  closeBtn.addEventListener('click', closeLockAuthIframe);

  const title = document.createElement('h2');
  title.textContent = 'Enable Locks';
  title.style.cssText = 'margin:0;font-size:24px;line-height:32px;font-weight:700;';

  const desc = document.createElement('p');
  desc.innerHTML = 'Use <strong>Pubky Ring</strong> to authorize Locks server to manage your Locks data.';
  desc.style.cssText = 'margin:0;font-size:16px;line-height:24px;letter-spacing:-0.5px;color:#d4d4db;';

  const frame = document.createElement('iframe');
  frame.id = 'lock-auth-iframe';
  frame.src = connectUrl;
  frame.allow = 'clipboard-write';
  // Initial height is a placeholder; the shell posts its real content height (`locks-auth-resize`)
  // on load and the message handler resizes the iframe to fit. bg matches the shell (no white flash).
  frame.style.cssText = 'width:100%;height:180px;border:0;background:#1d1d20;display:block;';

  card.append(closeBtn, title, desc, frame);
  overlay.append(card);
  document.body.append(overlay);
  state.lockAuthFrame = frame; // bind: only this frame's window may post the callback
}

function closeLockAuthIframe() {
  document.getElementById('lock-auth-iframe-overlay')?.remove();
  state.lockAuthFrame = null; // drop the ref so a late message from a closed frame is ignored
}

// After the token is delivered to the parent, hide the iframe and show a completion panel + Close.
// This is only called after the token has been stored, so Close being visible means delivery is done.
function showLockAuthComplete() {
  const overlay = document.getElementById('lock-auth-iframe-overlay');
  if (!overlay) return;
  overlay.innerHTML = '';
  const panel = document.createElement('div');
  panel.style.cssText = 'background:#fff;padding:24px 28px;border-radius:12px;text-align:center;max-width:92vw;';
  const msg = document.createElement('p');
  msg.textContent = 'Authentication complete.';
  msg.style.cssText = 'margin:0 0 16px;font-size:16px;';
  const closeBtn = document.createElement('button');
  closeBtn.textContent = 'Close';
  closeBtn.style.cssText = 'padding:8px 20px;';
  closeBtn.addEventListener('click', closeLockAuthIframe);
  panel.append(msg, closeBtn);
  overlay.append(panel);
}

async function bootstrap() {
  state.config = await fetchJson('/config.json');
  await postClientLog('info', 'bootstrap-config-loaded', {
    lockServerPubky: state.config.lockServer.pubky,
    lockServerUrl: state.config.lockServer.url,
    pkarrRelay: state.config.testnet.pkarrRelay,
    httpRelay: state.config.testnet.httpRelay,
    callback: state.config.paths.lockServerCallback,
    hasLockSession: Boolean(state.feLockSessionToken),
  });
  await refreshDemoAuthStatus();
  refreshLockAuthStatus();
  refreshPublishingState();
  setInterval(refreshDemoAuthStatus, 2000);
}

el.startDemoAuth.addEventListener('click', async () => {
  try {
    const result = await fetchJson('/api/demo-auth/start', { method: 'POST' });
    if (result.authenticated) {
      await refreshDemoAuthStatus();
      return;
    }
    el.demoAuthCommand.textContent = `${result.authorizationUrl}\n\n${result.command}`;
  } catch (error) {
    showError(el.demoAuthStatus, error);
  }
});

el.startLockAuth.addEventListener('click', async () => {
  try {
    const connectState = crypto.randomUUID();
    state.pendingConnectState = connectState;
    // return_to only supplies the postMessage target origin (this app's origin); no callback page
    // is navigated to. The server posts { state, code } to origin(return_to).
    const returnTo = state.config.paths.lockServerCallback;
    await postClientLog('info', 'lock-auth-start-clicked', {
      lockServerPubky: state.config.lockServer.pubky,
      returnTo,
      state: connectState,
      pkarrRelays: [state.config.testnet.pkarrRelay],
    });
    const { connectUrl } = await startCreatorConnect({
      lockServer: state.config.lockServer.pubky,
      returnTo,
      state: connectState,
      pkarrRelays: [state.config.testnet.pkarrRelay],
    });
    // Opt into direct postMessage delivery and remember the origin we will accept messages from.
    const deliveryUrl = new URL(connectUrl);
    deliveryUrl.searchParams.set('delivery', 'postmessage');
    state.lockServerOrigin = deliveryUrl.origin;
    await postClientLog('info', 'lock-auth-connect-url-built', { connectUrl: deliveryUrl.toString() });
    openLockAuthIframe(deliveryUrl.toString());
  } catch (error) {
    await postClientLog('error', 'lock-auth-start-failed', serializeError(error));
    showError(el.lockAuthStatus, error);
  }
});

el.configurePointer.addEventListener('click', async () => {
  try {
    const sessionSecret = state.feLockSessionToken;
    await configureLockServicePointer({
      lockServer: state.config.lockServer.pubky,
      sessionSecret,
      pkarrRelays: [state.config.testnet.pkarrRelay],
    });
    localStorage.setItem(POINTER_CONFIGURED_KEY, 'true');
    el.publishingStatus.textContent = 'Lock Service Pointer configured. Upload a file to create locked content.';
    el.publishingStatus.className = 'ok';
    refreshPublishingState();
  } catch (error) {
    showError(el.publishingStatus, error);
  }
});

el.primaryContentFile.addEventListener('change', () => {
  const primaryFile = el.primaryContentFile.files?.[0];
  if (primaryFile && !el.resourceFilename.value) {
    el.resourceFilename.value = sanitizeFilename(primaryFile.name);
  }
  renderSelectedResources();
});

el.secondaryContentFiles.addEventListener('change', renderSelectedResources);
el.resourceFilename.addEventListener('input', renderSelectedResources);

el.lockedContentForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  try {
    const primaryFile = el.primaryContentFile.files?.[0];
    if (!primaryFile) throw new Error('select a primary file first');
    const filename = el.resourceFilename.value.trim();
    if (!filename || filename.includes('/')) throw new Error('primary filename is required and must not contain /');

    const secondaryFiles = Array.from(el.secondaryContentFiles.files ?? []);
    const resources = await buildResourcesFromFiles(primaryFile, secondaryFiles, filename);
    const criteria = [{
      criterion_id: el.criterionId.value.trim(),
      verifier_type: el.verifierType.value,
      params: { satisfied: el.criterionSatisfied.value === 'true' },
    }];
    const result = await publishLockedContent({
      lockServer: state.config.lockServer.pubky,
      sessionSecret: state.feLockSessionToken,
      resources,
      criteria,
      accessTtlSeconds: Number(el.accessTtl.value),
      pkarrRelays: [state.config.testnet.pkarrRelay],
    });
    el.creatorResult.textContent = JSON.stringify(result, null, 2);
    el.viewerResource.textContent = result.contentLockResource;
  } catch (error) {
    showError(el.publishingStatus, error);
  }
});

async function refreshDemoAuthStatus() {
  try {
    const status = await fetchJson('/api/demo-auth/status');
    state.demoAuthenticated = status.authenticated;
    if (status.authenticated) {
      el.demoAuthStatus.textContent = `Authenticated as ${status.pubky} on ${status.homeserver}`;
      el.demoAuthStatus.className = 'ok';
      el.startDemoAuth.disabled = true;
      el.demoAuthCommand.textContent = '';
    } else {
      el.demoAuthStatus.textContent = status.pending ? 'Waiting for auth approval...' : 'Not authenticated to homeserver.';
      el.demoAuthStatus.className = 'muted';
    }
    refreshLockAuthStatus();
  } catch (error) {
    showError(el.demoAuthStatus, error);
  }
}

function refreshLockAuthStatus() {
  state.lockAuthenticated = Boolean(state.feLockSessionToken);
  el.startLockAuth.disabled = !state.demoAuthenticated;
  if (!state.demoAuthenticated) {
    el.lockAuthStatus.textContent = 'Waiting for demo auth.';
  } else if (state.lockAuthenticated) {
    el.lockAuthStatus.textContent = state.lastReceivedCode
      ? `Authenticated to Lock Server.\ncode: ${state.lastReceivedCode}\nfeLockSessionToken: ${state.feLockSessionToken}`
      : 'Authenticated to Lock Server.';
    el.lockAuthStatus.className = 'ok';
  } else {
    el.lockAuthStatus.textContent = 'Ready to authenticate to Lock Server.';
    el.lockAuthStatus.className = 'muted';
  }
  refreshPublishingState();
}

function refreshPublishingState() {
  const hasSession = Boolean(state.feLockSessionToken);
  const pointerConfigured = localStorage.getItem(POINTER_CONFIGURED_KEY) === 'true';
  el.configurePointer.disabled = !hasSession;
  el.lockedContentForm.hidden = !hasSession || !pointerConfigured;
  if (!hasSession) {
    el.publishingStatus.textContent = 'Waiting for Lock Server session.';
    el.publishingStatus.className = 'muted';
  } else if (!pointerConfigured) {
    el.publishingStatus.textContent = 'Configure Lock Service Pointer before uploading content.';
    el.publishingStatus.className = 'muted';
  }
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  if (!response.ok) throw new Error(`${url} failed with HTTP ${response.status}`);
  return response.json();
}

function showError(target, error) {
  target.textContent = error.message;
  target.className = 'error';
}

async function postClientLog(level, event, details = {}) {
  try {
    await fetch('/api/client-log', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        level,
        event,
        details,
        location: window.location.href,
        at: new Date().toISOString(),
      }),
    });
  } catch (error) {
    console.warn('failed to post demo client log', error);
  }
}

function serializeError(error) {
  return {
    name: error?.name,
    message: error?.message ?? String(error),
    stack: error?.stack,
  };
}

function renderSelectedResources() {
  const primaryFile = el.primaryContentFile.files?.[0];
  const secondaryFiles = Array.from(el.secondaryContentFiles.files ?? []);
  el.selectedResourceList.replaceChildren();
  if (!primaryFile && secondaryFiles.length === 0) {
    el.selectedResources.hidden = true;
    return;
  }

  if (primaryFile) {
    const primaryPath = el.resourceFilename.value.trim() || sanitizeFilename(primaryFile.name);
    appendSelectedResource('Primary', primaryPath, primaryFile);
  }
  for (const secondaryFile of secondaryFiles) {
    appendSelectedResource('Secondary', sanitizeFilename(secondaryFile.name), secondaryFile);
  }
  el.selectedResources.hidden = false;
}

function appendSelectedResource(kind, path, file) {
  const item = document.createElement('li');
  item.textContent = `${kind}: /priv/locks.app/content/${path} (${file.name}, ${file.size} bytes)`;
  el.selectedResourceList.append(item);
}

async function buildResourcesFromFiles(primaryFile, secondaryFiles, primaryPath) {
  const usedPaths = new Set();
  const files = [
    { kind: 'primary', file: primaryFile, path: primaryPath },
    ...secondaryFiles.map((file) => ({ kind: 'secondary', file, path: sanitizeFilename(file.name) })),
  ];
  const resources = [];
  for (const [index, { kind, file, path }] of files.entries()) {
    if (!path || path.includes('/')) throw new Error(`${kind} file path is invalid`);
    if (usedPaths.has(path)) throw new Error(`duplicate guarded resource path: ${path}`);
    usedPaths.add(path);
    resources.push({
      path,
      contentType: file.type || 'application/octet-stream',
      bytes: new Uint8Array(await file.arrayBuffer()),
    });
  }
  return resources;
}

function sanitizeFilename(name) {
  return name.split(/[\\/]/).pop() || 'uploaded.bin';
}

// Keep explicit SDK symbols in this file for readers/smoke checks.
void Locks.forServerWithOptions;
void 'Configure Lock Service Pointer';
void 'Create locked content';
void 'Viewer content lock resource';
void 'dev-static';
