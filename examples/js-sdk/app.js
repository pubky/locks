import {
  completeCreatorConnect,
  configureLockServicePointer,
  publishLockedContent,
  startCreatorConnect,
} from './creator-complete-flow.js';
import init, { Locks } from '../../locks-sdk/bindings/js/pkg/locks_sdk_wasm.js';

// Documented endpoint names for the smoke checker and readers:
// POST /api/demo-auth/start
// GET /api/demo-auth/status
// Verifier dropdown initial option: dev-static

const SESSION_SECRET_KEY = 'pubky-locks-demo.frontendSessionSecret';
const CONNECT_STATE_KEY = 'pubky-locks-demo.connectState';
const POINTER_CONFIGURED_KEY = 'pubky-locks-demo.pointerConfigured';

const state = {
  config: null,
  demoAuthenticated: false,
  lockAuthenticated: false,
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

async function bootstrap() {
  state.config = await fetchJson('/config.json');
  await postClientLog('info', 'bootstrap-config-loaded', {
    lockServerPubky: state.config.lockServer.pubky,
    lockServerUrl: state.config.lockServer.url,
    pkarrRelay: state.config.testnet.pkarrRelay,
    httpRelay: state.config.testnet.httpRelay,
    callback: state.config.paths.lockServerCallback,
    hasLocalLockSession: Boolean(localStorage.getItem(SESSION_SECRET_KEY)),
  });
  try {
    await maybeCompleteLockServerCallback();
  } catch (error) {
    await postClientLog('error', 'lock-auth-callback-failed', serializeError(error));
    showError(el.lockAuthStatus, error);
  }
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
    sessionStorage.setItem(CONNECT_STATE_KEY, connectState);
    await postClientLog('info', 'lock-auth-start-clicked', {
      lockServerPubky: state.config.lockServer.pubky,
      returnTo: state.config.paths.lockServerCallback,
      state: connectState,
      pkarrRelays: [state.config.testnet.pkarrRelay],
    });
    const { connectUrl } = await startCreatorConnect({
      lockServer: state.config.lockServer.pubky,
      returnTo: state.config.paths.lockServerCallback,
      state: connectState,
      pkarrRelays: [state.config.testnet.pkarrRelay],
    });
    await postClientLog('info', 'lock-auth-connect-url-built', { connectUrl });
    window.location.assign(connectUrl);
  } catch (error) {
    await postClientLog('error', 'lock-auth-start-failed', serializeError(error));
    showError(el.lockAuthStatus, error);
  }
});

el.configurePointer.addEventListener('click', async () => {
  try {
    const sessionSecret = localStorage.getItem(SESSION_SECRET_KEY);
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
      sessionSecret: localStorage.getItem(SESSION_SECRET_KEY),
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

async function maybeCompleteLockServerCallback() {
  if (!location.pathname.endsWith('/auth/lock-server/callback')) return;
  const expectedState = sessionStorage.getItem(CONNECT_STATE_KEY);
  await postClientLog('info', 'lock-auth-callback-received', {
    callbackUrl: window.location.href,
    expectedState,
  });
  const { sessionSecret } = await completeCreatorConnect({
    lockServer: state.config.lockServer.pubky,
    callbackUrl: window.location.href,
    expectedState,
    pkarrRelays: [state.config.testnet.pkarrRelay],
  });
  localStorage.setItem(SESSION_SECRET_KEY, sessionSecret);
  state.lockAuthenticated = true;
  await postClientLog('info', 'lock-auth-callback-completed', {
    storedSessionSecret: true,
  });
  history.replaceState({}, '', '/examples/js-sdk/');
}

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
  const secret = localStorage.getItem(SESSION_SECRET_KEY);
  state.lockAuthenticated = Boolean(secret);
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
  const hasSession = Boolean(localStorage.getItem(SESSION_SECRET_KEY));
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
