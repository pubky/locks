import {
  completeDevVerification,
  issueAccessCredential,
  loadContentLock,
  lookupVerificationTask,
  readGuardedContent,
  submitDevStaticProof,
  creatorFromResource,
} from './reader-flow.js';

const STATE_KEY = 'pubky-locks-reader-demo.state';

const state = {
  config: null,
  resource: '',
  guardedResourcePath: '',
  lockResources: [],
  proofSatisfied: true,
  loaded: null,
  creator: null,
  bundleId: null,
  submittedProofBundle: null,
  lifecycle: null,
  completion: null,
  accessCredential: null,
  accessCredentialResponse: null,
  readResult: null,
};

const el = {
  configStatus: document.querySelector('#config-status'),
  reset: document.querySelector('#reset-reader-state'),
  resource: document.querySelector('#content-lock-resource'),
  lockResources: document.querySelector('#lock-resources'),
  primaryResourceList: document.querySelector('#primary-resource-list'),
  secondaryResourceList: document.querySelector('#secondary-resource-list'),
  load: document.querySelector('#load-content-lock'),
  loadStatus: document.querySelector('#load-status'),
  loadedOutput: document.querySelector('#loaded-lock-output'),
  verifierType: document.querySelector('#verifier-type'),
  proofSatisfied: document.querySelector('#proof-satisfied'),
  submitProof: document.querySelector('#submit-proof'),
  proofStatus: document.querySelector('#proof-status'),
  proofOutput: document.querySelector('#proof-output'),
  completeVerification: document.querySelector('#complete-verification'),
  completionStatus: document.querySelector('#completion-status'),
  completionOutput: document.querySelector('#completion-output'),
  issueCredential: document.querySelector('#issue-credential'),
  credentialStatus: document.querySelector('#credential-status'),
  credentialOutput: document.querySelector('#credential-output'),
  readContent: document.querySelector('#read-content'),
  readStatus: document.querySelector('#read-status'),
  readOutput: document.querySelector('#read-output'),
};

async function bootstrap() {
  state.config = await fetchJson('/config.json');
  el.configStatus.textContent = `Reader demo using Lock Server ${state.config.lockServer.pubky}`;
  el.configStatus.className = 'ok';
  restoreState();
  bindEvents();
  render();
  await postClientLog('info', 'reader-bootstrap-config-loaded', {
    lockServerPubky: state.config.lockServer.pubky,
    lockServerUrl: state.config.lockServer.url,
    pkarrRelay: state.config.testnet.pkarrRelay,
    location: window.location.href,
    hasState: Boolean(localStorage.getItem(STATE_KEY)),
  });
}

function bindEvents() {
  el.reset.addEventListener('click', async () => {
    localStorage.removeItem(STATE_KEY);
    Object.assign(state, {
      resource: '',
      guardedResourcePath: '',
      lockResources: [],
      proofSatisfied: true,
      loaded: null,
      creator: null,
      bundleId: null,
      submittedProofBundle: null,
      lifecycle: null,
      completion: null,
      accessCredential: null,
      accessCredentialResponse: null,
      readResult: null,
    });
    await postClientLog('info', 'reader-state-reset');
    render();
  });

  el.resource.addEventListener('input', () => {
    state.resource = el.resource.value.trim();
    persistState();
    render();
  });

  el.proofSatisfied.addEventListener('change', () => {
    state.proofSatisfied = el.proofSatisfied.value === 'true';
    persistState();
    render();
  });

  el.load.addEventListener('click', loadLock);
  el.submitProof.addEventListener('click', submitProof);
  el.completeVerification.addEventListener('click', completeVerification);
  el.issueCredential.addEventListener('click', issueCredential);
  el.readContent.addEventListener('click', () => readContent(state.guardedResourcePath));
  el.lockResources.addEventListener('click', (event) => {
    const button = event.target.closest('[data-read-resource-path]');
    if (!button) return;
    readContent(button.dataset.readResourcePath);
  });
}

async function loadLock() {
  try {
    const resource = el.resource.value.trim();
    await postClientLog('info', 'reader-load-lock-started', { resource });
    el.loadStatus.textContent = 'Loading content lock...';
    el.loadStatus.className = 'muted';
    const loaded = await loadContentLock({
      resource,
      pkarrRelays: pkarrRelays(),
    });
    state.resource = resource;
    state.creator = creatorFromResource(resource);
    state.lockResources = loaded.resources;
    state.guardedResourcePath = loaded.resources[0]?.readPath ?? '';
    state.loaded = {
      resource,
      creator: state.creator,
      primaryFile: loaded.resources.find((item) => item.kind === 'primary') ?? null,
      secondaryFiles: loaded.resources.filter((item) => item.kind === 'secondary'),
      contentLock: toPlainJson(loaded.contentLock),
    };
    persistState();
    render();
    await postClientLog('info', 'reader-load-lock-succeeded', state.loaded);
  } catch (error) {
    await postClientLog('error', 'reader-load-lock-failed', serializeError(error));
    showError(el.loadStatus, error);
  }
}

async function submitProof() {
  try {
    await postClientLog('info', 'reader-submit-proof-started', {
      resource: state.resource,
      satisfied: state.proofSatisfied,
    });
    el.proofStatus.textContent = 'Submitting proof bundle...';
    el.proofStatus.className = 'muted';
    const result = await submitDevStaticProof({
      resource: state.resource,
      satisfied: state.proofSatisfied,
      pkarrRelays: pkarrRelays(),
    });
    state.creator = result.creator;
    state.bundleId = result.bundleId;
    state.submittedProofBundle = result.submittedProofBundle;
    state.lifecycle = result.lifecycle;
    state.completion = null;
    state.accessCredential = null;
    state.accessCredentialResponse = null;
    state.readResult = null;
    persistState();
    render();
    await postClientLog('info', 'reader-submit-proof-succeeded', {
      creator: state.creator,
      bundleId: state.bundleId,
      lifecycle: state.lifecycle,
    });
  } catch (error) {
    await postClientLog('error', 'reader-submit-proof-failed', serializeError(error));
    showError(el.proofStatus, error);
  }
}

async function completeVerification() {
  try {
    await postClientLog('info', 'reader-complete-verification-started', handleDetails());
    el.completionStatus.textContent = 'Completing dev verification...';
    el.completionStatus.className = 'muted';
    const completion = await completeDevVerification({
      resource: state.resource,
      creator: state.creator,
      bundleId: state.bundleId,
      pkarrRelays: pkarrRelays(),
    });
    state.completion = completion;
    state.lifecycle = completion;
    persistState();
    render();
    await postClientLog('info', 'reader-complete-verification-succeeded', completion);
  } catch (error) {
    if (String(error?.message ?? error).includes('HTTP 409')) {
      await postClientLog('info', 'reader-complete-verification-conflict-looking-up', {
        ...handleDetails(),
        error: serializeError(error),
      });
      try {
        const lookup = await lookupVerificationTask({
          resource: state.resource,
          creator: state.creator,
          bundleId: state.bundleId,
          pkarrRelays: pkarrRelays(),
        });
        state.completion = lookup;
        state.lifecycle = lookup;
        persistState();
        render();
        await postClientLog('info', 'reader-complete-verification-conflict-lookup-succeeded', lookup);
        return;
      } catch (lookupError) {
        await postClientLog('error', 'reader-complete-verification-conflict-lookup-failed', serializeError(lookupError));
      }
    }
    await postClientLog('error', 'reader-complete-verification-failed', serializeError(error));
    showError(el.completionStatus, error);
  }
}

async function issueCredential() {
  try {
    await postClientLog('info', 'reader-issue-credential-started', handleDetails());
    el.credentialStatus.textContent = 'Issuing access credential...';
    el.credentialStatus.className = 'muted';
    const response = await issueAccessCredential({
      resource: state.resource,
      creator: state.creator,
      bundleId: state.bundleId,
      pkarrRelays: pkarrRelays(),
    });
    state.accessCredentialResponse = response;
    state.accessCredential = response.credential;
    persistState();
    render();
    await postClientLog('info', 'reader-issue-credential-succeeded', {
      expiresAt: response.expires_at,
      credentialLength: response.credential?.length,
    });
  } catch (error) {
    await postClientLog('error', 'reader-issue-credential-failed', serializeError(error));
    showError(el.credentialStatus, error);
  }
}

async function readContent(path) {
  try {
    path = path?.trim();
    if (!path) throw new Error('guarded resource path is required');
    await postClientLog('info', 'reader-proxy-read-started', {
      resource: state.resource,
      path,
      hasCredential: Boolean(state.accessCredential),
    });
    el.readStatus.textContent = 'Reading guarded content...';
    el.readStatus.className = 'muted';
    const result = await readGuardedContent({
      resource: state.resource,
      accessCredential: state.accessCredential,
      path,
      pkarrRelays: pkarrRelays(),
    });
    state.guardedResourcePath = path;
    state.readResult = {
      path,
      size: result.size,
      text: result.text,
    };
    persistState();
    render();
    await postClientLog('info', 'reader-proxy-read-succeeded', { size: result.size });
  } catch (error) {
    await postClientLog('error', 'reader-proxy-read-failed', serializeError(error));
    showError(el.readStatus, error);
  }
}

function render() {
  el.resource.value = state.resource ?? '';
  el.proofSatisfied.value = String(Boolean(state.proofSatisfied));

  if (state.loaded) {
    el.loadStatus.textContent = 'Content lock loaded.';
    el.loadStatus.className = 'ok';
  } else if (state.resource) {
    el.loadStatus.textContent = 'Ready to load content lock.';
    el.loadStatus.className = 'muted';
  } else {
    el.loadStatus.textContent = 'Paste a content lock resource.';
    el.loadStatus.className = 'muted';
  }
  el.loadedOutput.textContent = format(state.loaded);
  renderLockResources();

  el.submitProof.disabled = !state.loaded;
  if (state.lifecycle) {
    el.proofStatus.textContent = `Proof submitted. Status: ${state.lifecycle.status}`;
    el.proofStatus.className = state.lifecycle.status === 'failed' ? 'error' : 'ok';
  } else {
    el.proofStatus.textContent = state.loaded ? 'Ready to submit proof bundle.' : 'Waiting for loaded lock.';
    el.proofStatus.className = 'muted';
  }
  el.proofOutput.textContent = format({
    submittedProofBundle: state.submittedProofBundle,
    lifecycle: state.lifecycle,
  });

  el.completeVerification.disabled = !state.bundleId;
  if (state.completion) {
    el.completionStatus.textContent = `Dev verification completed. Status: ${state.completion.status}`;
    el.completionStatus.className = state.completion.status === 'failed' ? 'error' : 'ok';
  } else {
    el.completionStatus.textContent = state.bundleId ? 'Ready to complete dev verification.' : 'Waiting for proof bundle.';
    el.completionStatus.className = 'muted';
  }
  el.completionOutput.textContent = format(state.completion);

  el.issueCredential.disabled = !state.bundleId || state.lifecycle?.status !== 'completed';
  if (state.accessCredentialResponse) {
    el.credentialStatus.textContent = 'Access credential issued. Treat it as bearer-secret local-dev data.';
    el.credentialStatus.className = 'ok';
  } else {
    el.credentialStatus.textContent = state.lifecycle?.status === 'completed' ? 'Ready to issue access credential.' : 'Waiting for completed verification.';
    el.credentialStatus.className = 'muted';
  }
  el.credentialOutput.textContent = format(state.accessCredentialResponse);

  el.readContent.disabled = !state.accessCredential || !state.guardedResourcePath;
  if (state.readResult) {
    el.readStatus.textContent = `Read ${state.readResult.size} bytes from ${state.readResult.path}.`;
    el.readStatus.className = 'ok';
  } else {
    el.readStatus.textContent = state.accessCredential ? 'Ready to read guarded content.' : 'Waiting for access credential.';
    el.readStatus.className = 'muted';
  }
  el.readOutput.textContent = state.readResult ? state.readResult.text : '';
}

function renderLockResources() {
  el.primaryResourceList.replaceChildren();
  el.secondaryResourceList.replaceChildren();
  const resources = state.lockResources ?? [];
  el.lockResources.hidden = resources.length === 0;
  for (const resource of resources) {
    const item = document.createElement('li');
    const button = document.createElement('button');
    button.type = 'button';
    button.dataset.readResourcePath = resource.readPath;
    button.disabled = !state.accessCredential;
    button.textContent = state.accessCredential ? 'Read' : 'Read after credential';
    const label = document.createElement('span');
    label.textContent = ` ${resource.path} (${resource.contentType}, ${resource.size} bytes)`;
    item.append(button, label);
    if (resource.kind === 'primary') {
      el.primaryResourceList.append(item);
    } else {
      el.secondaryResourceList.append(item);
    }
  }
  if (el.primaryResourceList.childElementCount === 0) {
    const item = document.createElement('li');
    item.className = 'muted';
    item.textContent = 'No primary file in this content lock.';
    el.primaryResourceList.append(item);
  }
  if (el.secondaryResourceList.childElementCount === 0) {
    const item = document.createElement('li');
    item.className = 'muted';
    item.textContent = 'No secondary files in this content lock.';
    el.secondaryResourceList.append(item);
  }
}

function restoreState() {
  const raw = localStorage.getItem(STATE_KEY);
  if (!raw) return;
  try {
    Object.assign(state, JSON.parse(raw));
  } catch {
    localStorage.removeItem(STATE_KEY);
  }
}

function persistState() {
  const { config: _config, ...persisted } = state;
  localStorage.setItem(STATE_KEY, JSON.stringify(persisted));
}

function pkarrRelays() {
  return [state.config.testnet.pkarrRelay];
}

function handleDetails() {
  return { creator: state.creator, bundleId: state.bundleId };
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  if (!response.ok) throw new Error(`${url} failed with HTTP ${response.status}`);
  return response.json();
}

function showError(target, error) {
  target.textContent = error.message ?? String(error);
  target.className = 'error';
}

function format(value) {
  if (!value) return '';
  return JSON.stringify(toPlainJson(value), null, 2);
}

function toPlainJson(value) {
  if (value instanceof Map) {
    return Object.fromEntries(Array.from(value.entries(), ([key, entry]) => [key, toPlainJson(entry)]));
  }
  if (Array.isArray(value)) return value.map(toPlainJson);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([key, entry]) => [key, toPlainJson(entry)]));
  }
  return value;
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
    console.warn('failed to post reader client log', error);
  }
}

function serializeError(error) {
  return {
    name: error?.name,
    message: error?.message ?? String(error),
    stack: error?.stack,
  };
}

bootstrap().catch((error) => {
  showError(el.configStatus, error);
  postClientLog('error', 'reader-bootstrap-failed', serializeError(error));
});
