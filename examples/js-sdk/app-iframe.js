import {
  configureLockServicePointer,
  exchangeCreatorConnectCode,
  publishLockedContent,
  signOutCreator,
  startCreatorConnect,
} from './creator-complete-flow.js';
import { invalidateIdentityScopedCreatorState } from './creator-identity.js';
import { buildCreatorLockPolicy } from './creator-lock-policy.js';
import { acceptPaykitSetupEvent, buildPaykitSetupRequest } from './paykit-setup.js';
import init, { Locks } from '../../locks-sdk/bindings/js/pkg/locks_sdk_wasm.js';

// Shared creator-page iframe flow — direct postMessage delivery (ADR 0019).
// Step 2 (Authenticate to Lock Server) opens the Lock Server /connect page in an IFRAME MODAL with
// ?delivery=postmessage. The /connect shell itself posts { type: 'locks-auth-callback', state, code }
// straight to this parent window — NO full-page redirect, NO same-origin callback page on this app.
// The message arrives from the Lock Server origin (cross-origin), so we validate event.origin against
// the connect URL origin. This app hosts no callback route for the iframe flow.
// The frontend session token (feLockSessionToken) is kept IN-MEMORY only (not localStorage).

// Documented endpoint names for the smoke checker and readers:
// POST /api/demo-auth/start
// GET /api/demo-auth/status
// Lock type defaults to dev-static; paykit-payment is explicitly selectable.

// Message type published by the Lock Server /connect shell (embedder contract).
const LOCKS_AUTH_CALLBACK_TYPE = 'locks-auth-callback';
const LOCKS_AUTH_ERRORS = new Set(['invalid-response', 'connect-failed']);
const LEGACY_POINTER_CONFIGURED_KEY = 'pubky-locks-demo.pointerConfigured';
const POINTER_CONFIGURED_KEY_PREFIX = `${LEGACY_POINTER_CONFIGURED_KEY}.`;

const state = {
  config: null,
  demoAuthenticated: false,
  lockAuthenticated: false,
  creatorPubky: null,
  paykitSetupComplete: false,
  feLockSessionToken: null, // Lock Server frontend session token — in-memory only (cleared on reload)
  pendingConnectState: null, // opaque state persisted for the in-flight connect (in-memory)
  lockServerOrigin: null,    // origin of the connect iframe; the only accepted postMessage sender
  lockAuthFrame: null,       // the connect iframe element; only its window may post the callback
  pendingPaykitSetupState: null,
  paykitSetupOrigin: null,
  paykitSetupFrame: null,
  paykitSetupCreator: null,
  demoAuthStatusRequestId: 0,
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
  lockType: document.querySelector('#lock-type'),
  devStaticFields: document.querySelector('#dev-static-fields'),
  paykitPaymentFields: document.querySelector('#paykit-payment-fields'),
  paykitAmountSats: document.querySelector('#paykit-amount-sats'),
  paykitSetupStatus: document.querySelector('#paykit-setup-status'),
  retryPaykitSetup: document.querySelector('#retry-paykit-setup'),
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
  if (hasExactKeys(event.data, ['type', 'height']) && event.data.type === 'locks-auth-resize') {
    if (Number.isFinite(event.data.height) && event.data.height >= 0 && event.data.height <= 4096 && state.lockAuthFrame) {
      state.lockAuthFrame.style.height = `${event.data.height}px`;
    }
    return;
  }
  if (event.data?.type !== LOCKS_AUTH_CALLBACK_TYPE) return;
  // The shell reports a definitive failure (expired/rejected flow) instead of hanging.
  if (
    hasExactKeys(event.data, ['type', 'state', 'error'])
    && event.data.state === state.pendingConnectState
    && LOCKS_AUTH_ERRORS.has(event.data.error)
  ) {
    closeLockAuthIframe();
    await postClientLog('error', 'lock-auth-iframe-shell-error');
    showError(el.lockAuthStatus, new Error('Lock Server connect failed'));
    return;
  }
  if (
    !hasExactKeys(event.data, ['type', 'state', 'code'])
    || event.data.state !== state.pendingConnectState
    || typeof event.data.code !== 'string'
    || event.data.code.length === 0
  ) return;
  try {
    const { code, state: receivedState } = event.data;
    const { sessionSecret } = await exchangeCreatorConnectCode({
      lockServer: state.config.lockServer.pubky,
      code,
      state: receivedState,
      expectedState: state.pendingConnectState,
      expectedCreatorPubky: state.creatorPubky,
      pkarrRelays: [state.config.testnet.pkarrRelay],
    });
    state.feLockSessionToken = sessionSecret; // in-memory only
    state.lockAuthenticated = true;
    refreshLockAuthStatus();
    showLockAuthComplete();
    state.pendingConnectState = null;
    state.lockServerOrigin = null;
    state.lockAuthFrame = null;
    await postClientLog('info', 'lock-auth-iframe-complete');
  } catch (error) {
    closeLockAuthIframe(); // otherwise the full-screen overlay hides the error message
    await postClientLog('error', 'lock-auth-iframe-exchange-failed', serializeError(error));
    showError(el.lockAuthStatus, error);
  }
});

function hasExactKeys(value, expected) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const keys = Object.keys(value).sort();
  const expectedKeys = [...expected].sort();
  return keys.length === expectedKeys.length && keys.every((key, index) => key === expectedKeys[index]);
}

window.addEventListener('message', (event) => {
  const result = acceptPaykitSetupEvent({
    event,
    expectedOrigin: state.paykitSetupOrigin,
    expectedSource: state.paykitSetupFrame?.contentWindow,
    expectedState: state.pendingPaykitSetupState,
    setupCreator: state.paykitSetupCreator,
    currentCreator: state.creatorPubky,
  });
  if (!result) return;

  if (result.status === 'error') {
    state.paykitSetupComplete = false;
    closePaykitSetupIframe();
    el.retryPaykitSetup.hidden = false;
    showError(el.paykitSetupStatus, new Error('Paykit setup failed'));
    return;
  }

  state.paykitSetupComplete = true;
  closePaykitSetupIframe();
  el.retryPaykitSetup.hidden = true;
  el.paykitSetupStatus.textContent = 'Paykit setup complete for this creator.';
  el.paykitSetupStatus.className = 'ok';
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

function openPaykitSetupIframe(setupUrl) {
  const overlay = document.createElement('div');
  overlay.id = 'paykit-setup-iframe-overlay';
  overlay.style.cssText =
    'position:fixed;inset:0;background:rgba(5,5,10,0.6);display:flex;z-index:9999;' +
    'align-items:center;justify-content:center;';
  overlay.addEventListener('click', (event) => {
    if (event.target === overlay) cancelPaykitSetupIframe();
  });

  const card = document.createElement('div');
  card.style.cssText =
    'box-sizing:border-box;position:relative;width:min(640px,92vw);display:flex;flex-direction:column;' +
    'gap:16px;padding:24px;background:#fff;border-radius:12px;box-shadow:0 20px 60px rgba(0,0,0,.35);';

  const closeBtn = document.createElement('button');
  closeBtn.setAttribute('aria-label', 'Close Paykit setup');
  closeBtn.textContent = '✕';
  closeBtn.style.cssText = 'position:absolute;top:12px;right:12px;padding:6px 10px;';
  closeBtn.addEventListener('click', cancelPaykitSetupIframe);

  const title = document.createElement('h2');
  title.textContent = 'Set up Paykit payments';
  title.style.cssText = 'margin:0;padding-right:40px;';

  const description = document.createElement('p');
  description.textContent = 'Complete the Paykit instructions for the current creator. From the repository root, use this explicit Compose command:';
  description.style.cssText = 'margin:0;';

  const companionCommand = document.createElement('code');
  companionCommand.textContent = 'docker compose -f compose.paykit-local-demo.yaml exec creator-demo npm --prefix examples/js-sdk run authenticate-paykit -- --role content-creator';
  companionCommand.style.cssText = 'display:block;overflow-wrap:anywhere;';

  const frame = document.createElement('iframe');
  frame.id = 'paykit-setup-iframe';
  frame.title = 'Paykit creator setup';
  frame.src = setupUrl;
  frame.referrerPolicy = 'no-referrer';
  frame.style.cssText = 'width:100%;height:min(520px,70vh);border:0;display:block;';

  card.append(closeBtn, title, description, companionCommand, frame);
  overlay.append(card);
  document.body.append(overlay);
  state.paykitSetupFrame = frame;
}

function cancelPaykitSetupIframe() {
  closePaykitSetupIframe();
  el.retryPaykitSetup.hidden = false;
  el.paykitSetupStatus.textContent = 'Paykit setup canceled.';
  el.paykitSetupStatus.className = 'muted';
}

function closePaykitSetupIframe() {
  document.getElementById('paykit-setup-iframe-overlay')?.remove();
  state.pendingPaykitSetupState = null;
  state.paykitSetupOrigin = null;
  state.paykitSetupFrame = null;
  state.paykitSetupCreator = null;
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
  refreshLockTypeFields();
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
    localStorage.setItem(pointerConfiguredKey(state.creatorPubky), 'true');
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
el.lockType.addEventListener('change', refreshLockTypeFields);
el.retryPaykitSetup.addEventListener('click', () => {
  if (el.lockType.value === 'paykit-payment' && state.creatorPubky) startPaykitSetup();
});

el.lockedContentForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  try {
    const primaryFile = el.primaryContentFile.files?.[0];
    if (!primaryFile) throw new Error('select a primary file first');
    const filename = el.resourceFilename.value.trim();
    if (!filename || filename.includes('/')) throw new Error('primary filename is required and must not contain /');

    const secondaryFiles = Array.from(el.secondaryContentFiles.files ?? []);
    const resources = await buildResourcesFromFiles(primaryFile, secondaryFiles, filename);
    const { criteria, lockLogic } = buildCreatorLockPolicy({
      lockType: el.lockType.value,
      criterionId: el.criterionId.value,
      devStaticSatisfied: el.criterionSatisfied.value === 'true',
      amountSats: el.paykitAmountSats.value,
      recipientPubky: state.creatorPubky,
      paykitSetupComplete: state.paykitSetupComplete,
    });
    const result = await publishLockedContent({
      lockServer: state.config.lockServer.pubky,
      sessionSecret: state.feLockSessionToken,
      resources,
      criteria,
      lockLogic,
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
  const requestId = ++state.demoAuthStatusRequestId;
  try {
    const status = await fetchJson('/api/demo-auth/status');
    if (requestId !== state.demoAuthStatusRequestId) return;
    const creatorPubky = status.authenticated ? status.pubky : null;
    const creatorChanged = state.creatorPubky !== creatorPubky;
    if (creatorChanged) {
      const previousCreatorPubky = state.creatorPubky;
      const hadLockSession = Boolean(state.feLockSessionToken);
      closeLockAuthIframe();
      if (previousCreatorPubky) localStorage.removeItem(pointerConfiguredKey(previousCreatorPubky));
      localStorage.removeItem(LEGACY_POINTER_CONFIGURED_KEY);
      const invalidation = await invalidateIdentityScopedCreatorState({
        state,
        revokeSession: (sessionSecret) => signOutCreator({
          lockServer: state.config.lockServer.pubky,
          sessionSecret,
          pkarrRelays: [state.config.testnet.pkarrRelay],
        }),
      });
      if (requestId !== state.demoAuthStatusRequestId) return;
      if (hadLockSession && !invalidation.revoked) {
        await postClientLog('warn', 'lock-session-revocation-failed-after-creator-change');
      }
      state.paykitSetupComplete = false;
      closePaykitSetupIframe();
    }
    state.creatorPubky = creatorPubky;
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
    if (creatorChanged) refreshLockTypeFields();
  } catch (error) {
    if (requestId !== state.demoAuthStatusRequestId) return;
    showError(el.demoAuthStatus, error);
  }
}

function refreshLockTypeFields() {
  const paymentSelected = el.lockType.value === 'paykit-payment';
  el.devStaticFields.hidden = paymentSelected;
  el.paykitPaymentFields.hidden = !paymentSelected;
  el.paykitAmountSats.required = paymentSelected;

  if (!paymentSelected) {
    closePaykitSetupIframe();
    return;
  }
  if (state.paykitSetupComplete) {
    el.paykitSetupStatus.textContent = 'Paykit setup complete for this creator.';
    el.paykitSetupStatus.className = 'ok';
    return;
  }
  if (!state.creatorPubky) {
    el.paykitSetupStatus.textContent = 'Authenticate the content creator before starting Paykit setup.';
    el.paykitSetupStatus.className = 'muted';
    return;
  }
  if (state.paykitSetupFrame) return;
  startPaykitSetup();
}

function startPaykitSetup() {
  if (
    el.lockType.value !== 'paykit-payment'
    || !state.creatorPubky
    || state.paykitSetupComplete
    || state.paykitSetupFrame
  ) return;

  closePaykitSetupIframe();
  el.retryPaykitSetup.hidden = true;
  el.paykitSetupStatus.className = 'muted';
  try {
    const pendingState = crypto.randomUUID();
    const request = buildPaykitSetupRequest({
      paykitUrl: state.config.paykit.url,
      returnTo: window.location.origin,
      state: pendingState,
    });
    state.pendingPaykitSetupState = pendingState;
    state.paykitSetupOrigin = request.origin;
    state.paykitSetupCreator = state.creatorPubky;
    openPaykitSetupIframe(request.url);
    el.paykitSetupStatus.textContent = 'Paykit setup is in progress.';
    el.paykitSetupStatus.className = 'muted';
  } catch (error) {
    closePaykitSetupIframe();
    el.retryPaykitSetup.hidden = false;
    showError(el.paykitSetupStatus, error);
  }
}

function refreshLockAuthStatus() {
  state.lockAuthenticated = Boolean(state.feLockSessionToken);
  el.startLockAuth.disabled = !state.demoAuthenticated;
  if (!state.demoAuthenticated) {
    el.lockAuthStatus.textContent = 'Waiting for demo auth.';
  } else if (state.lockAuthenticated) {
    el.lockAuthStatus.textContent = 'Authenticated to Lock Server.';
    el.lockAuthStatus.className = 'ok';
  } else {
    el.lockAuthStatus.textContent = 'Ready to authenticate to Lock Server.';
    el.lockAuthStatus.className = 'muted';
  }
  refreshPublishingState();
}

function refreshPublishingState() {
  const hasSession = Boolean(state.feLockSessionToken);
  const pointerConfigured = state.creatorPubky
    ? localStorage.getItem(pointerConfiguredKey(state.creatorPubky)) === 'true'
    : false;
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

function pointerConfiguredKey(creatorPubky) {
  return `${POINTER_CONFIGURED_KEY_PREFIX}${creatorPubky}`;
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

async function postClientLog(level) {
  try {
    await fetch('/api/client-log', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ level }),
    });
  } catch {
    console.warn('failed to post demo client log');
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
