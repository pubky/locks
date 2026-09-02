import {
  classifyPaymentLifecycle,
  completeDevVerification,
  hasPaykitData,
  issueAccessCredential,
  loadContentLock,
  lookupVerificationTask,
  readGuardedContent,
  submitDevStaticProof,
  submitPaykitPaymentProof,
  creatorFromResource,
  createLatestRequestGate,
  parsePaykitReaderBrowserStatus,
  selectCurrentPaykitPaymentRequest,
  workflowHandleMatches,
} from './reader-flow.js';
import { pkarrRelaysForDemoConfig } from './demo-network.js';
import {
  checkExternalReaderPaykitData,
  createPaykitDataCheckController,
} from './reader-staging-paykit.js';
import { buildPersistedReaderState, restorePersistedReaderState } from './reader-persistence.js';

const STATE_KEY = 'pubky-locks-reader-demo.state';

const state = {
  config: null,
  resource: '',
  guardedResourcePath: '',
  lockResources: [],
  proofSatisfied: true,
  verifierType: 'dev-static',
  readerPublicKey: '',
  paykitReaderPrepared: false,
  paykitReaderState: 'starting',
  paykitDataMessage: '',
  paykitPaymentRequest: null,
  baselinePaymentRequestId: null,
  loadingLock: false,
  submittingProof: false,
  paymentPolling: false,
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

let workflowIncarnation = 0;
const paykitReaderStatusRequests = createLatestRequestGate();
const paykitDataChecks = createPaykitDataCheckController();
let activeLoadToken = null;
let activeSubmissionToken = null;
let activePollToken = null;

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
  paykitReaderCommands: document.querySelector('#paykit-reader-commands'),
  paykitReaderGuidance: document.querySelector('#paykit-reader-guidance'),
  readerPublicKey: document.querySelector('#reader-public-key'),
  refreshPaykitReader: document.querySelector('#refresh-paykit-reader'),
  paykitReaderStatus: document.querySelector('#paykit-reader-status'),
  paykitReaderPayment: document.querySelector('#paykit-reader-payment'),
  pollPayment: document.querySelector('#poll-payment'),
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
  const resource = new URL(window.location.href).searchParams.get('resource')?.trim();
  if (resource) state.resource = resource;
  bindEvents();
  if (state.config.mode === 'staging') {
    state.paykitReaderState = 'unchecked';
  } else {
    await refreshPaykitReaderStatus();
  }
  render();
  if (state.config.mode !== 'staging') {
    setInterval(() => { void refreshPaykitReaderStatus(); }, 1_000);
  }
  await postClientLog('info', 'reader-bootstrap-config-loaded', {
    mode: state.config.mode ?? 'local-testnet',
    lockServerPubky: state.config.lockServer.pubky,
    lockServerUrl: state.config.lockServer.url,
    customPkarrRelays: pkarrRelaysForDemoConfig(state.config),
    hasState: Boolean(localStorage.getItem(STATE_KEY)),
  });
}

function bindEvents() {
  el.reset.addEventListener('click', async () => {
    invalidateWorkflow();
    clearReadResult();
    localStorage.removeItem(STATE_KEY);
    Object.assign(state, {
      resource: '',
      guardedResourcePath: '',
      lockResources: [],
      proofSatisfied: true,
      verifierType: 'dev-static',
      readerPublicKey: '',
      paykitReaderPrepared: false,
      paykitReaderState: 'starting',
      paykitDataMessage: '',
      paykitPaymentRequest: null,
      baselinePaymentRequestId: null,
      loadingLock: false,
      submittingProof: false,
      paymentPolling: false,
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
    const resource = el.resource.value.trim();
    if (resource !== state.resource) {
      invalidateWorkflow();
      clearVerificationState({ clearLoaded: true });
      state.resource = resource;
    }
    persistState();
    render();
  });

  el.proofSatisfied.addEventListener('change', () => {
    state.proofSatisfied = el.proofSatisfied.value === 'true';
    persistState();
    render();
  });

  el.readerPublicKey.addEventListener('input', () => {
    if (state.config.mode !== 'staging') return;
    paykitDataChecks.invalidate();
    state.readerPublicKey = el.readerPublicKey.value.trim();
    state.paykitReaderPrepared = false;
    state.paykitReaderState = 'unchecked';
    state.paykitDataMessage = '';
    render();
  });

  el.refreshPaykitReader.addEventListener('click', refreshPaykitReaderStatus);
  el.load.addEventListener('click', loadLock);
  el.submitProof.addEventListener('click', submitProof);
  el.pollPayment.addEventListener('click', pollPaymentLifecycle);
  el.completeVerification.addEventListener('click', completeVerification);
  el.issueCredential.addEventListener('click', issueCredential);
  el.readContent.addEventListener('click', () => readContent(state.guardedResourcePath));
  el.lockResources.addEventListener('click', (event) => {
    const button = event.target.closest('[data-read-resource-path]');
    if (!button) return;
    readContent(button.dataset.readResourcePath);
  });
}

async function refreshPaykitReaderStatus() {
  if (state.config.mode === 'staging') {
    state.paykitReaderPrepared = false;
    state.paykitReaderState = 'checking';
    state.paykitDataMessage = 'Checking public Paykit v0 data...';
    render();
    try {
      if (!state.creator) throw new Error('Load the content lock before checking the reader.');
      const result = await paykitDataChecks.check({
        incarnation: workflowIncarnation,
        resource: state.resource,
        readerPubky: state.readerPublicKey,
        creatorPubky: state.creator,
        lookup: (readerPublicKey) => hasPaykitData({ readerPublicKey }),
        isCurrent: paykitDataSnapshotMatches,
      });
      if (!result) return;
      state.readerPublicKey = result.readerPubky;
      state.paykitReaderPrepared = result.canSubmit;
      state.paykitReaderState = result.state;
      state.paykitDataMessage = result.message;
    } catch (error) {
      state.paykitReaderPrepared = false;
      state.paykitReaderState = 'invalid';
      state.paykitDataMessage = error.message ?? String(error);
    }
    render();
    return;
  }
  const request = paykitReaderStatusRequests.begin(workflowIncarnation);
  try {
    const response = await fetch('/api/paykit-reader/status', { method: 'GET', cache: 'no-store' });
    const status = parsePaykitReaderBrowserStatus(await response.json());
    if (!paykitReaderStatusRequests.isCurrent(request, workflowIncarnation)) return;
    const available = !['starting', 'failed'].includes(status.state);
    if (available !== response.ok) {
      throw new Error('contradictory Paykit reader worker status');
    }
    const paymentRequest = selectCurrentPaykitPaymentRequest({
      status,
      baselinePaymentRequestId: state.baselinePaymentRequestId,
      currentPaymentRequest: state.paykitPaymentRequest,
    });
    state.paykitReaderState = status.state === 'request_received' && !paymentRequest
      ? 'waiting'
      : status.state;
    state.paykitReaderPrepared = available;
    state.readerPublicKey = status.reader_pubky ?? '';
    state.paykitPaymentRequest = paymentRequest;
  } catch {
    if (!paykitReaderStatusRequests.isCurrent(request, workflowIncarnation)) return;
    state.paykitReaderState = 'failed';
    state.paykitReaderPrepared = false;
    state.readerPublicKey = '';
    state.paykitPaymentRequest = null;
  }
  paykitReaderStatusRequests.finish(request);
  persistState();
  render();
}

async function loadLock() {
  const resource = el.resource.value.trim();
  invalidateWorkflow();
  clearVerificationState({ clearLoaded: true });
  state.resource = resource;
  state.loadingLock = true;
  const incarnation = workflowIncarnation;
  const loadToken = Symbol('load-lock');
  activeLoadToken = loadToken;
  persistState();
  render();
  try {
    await postClientLog('info', 'reader-load-lock-started', { resource });
    if (activeLoadToken !== loadToken || incarnation !== workflowIncarnation) return;
    el.loadStatus.textContent = 'Loading content lock...';
    el.loadStatus.className = 'muted';
    const loaded = await loadContentLock({
      resource,
      pkarrRelays: pkarrRelays(),
    });
    if (activeLoadToken !== loadToken || incarnation !== workflowIncarnation || state.resource !== resource) return;
    state.creator = creatorFromResource(resource);
    state.lockResources = loaded.resources;
    state.verifierType = contentLockVerifierType(loaded.contentLock);
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
    if (activeLoadToken !== loadToken || incarnation !== workflowIncarnation) return;
    await postClientLog('error', 'reader-load-lock-failed', serializeError(error));
    showError(el.loadStatus, error);
  } finally {
    if (activeLoadToken === loadToken) {
      activeLoadToken = null;
      state.loadingLock = false;
      render();
    }
  }
}

async function submitProof() {
  if (state.submittingProof) return;
  if (
    state.verifierType === 'paykit-payment'
    && (!state.paykitReaderPrepared || !state.readerPublicKey)
  ) {
    showError(el.proofStatus, new Error('prepare and confirm the Paykit reader before submitting payment proof'));
    return;
  }
  invalidateWorkflow();
  clearVerificationState();
  state.submittingProof = true;
  const incarnation = workflowIncarnation;
  const submissionToken = Symbol('submit-proof');
  activeSubmissionToken = submissionToken;
  const snapshot = Object.freeze({
    incarnation,
    resource: state.resource,
    verifierType: state.verifierType,
    readerPublicKey: state.readerPublicKey,
    paykitCreator: state.loaded?.creator,
    paykitReaderPrepared: state.paykitReaderPrepared,
    proofSatisfied: state.proofSatisfied,
    primaryPath: state.lockResources.find((resource) => resource.kind === 'primary')?.readPath ?? '',
    pkarrRelays: Object.freeze([...pkarrRelays()]),
  });
  persistState();
  render();
  try {
    await postClientLog('info', 'reader-submit-proof-started', {
      resource: snapshot.resource,
      verifierType: snapshot.verifierType,
    });
    if (activeSubmissionToken !== submissionToken || !workflowMatches(snapshot)) return;
    el.proofStatus.textContent = 'Submitting proof bundle...';
    el.proofStatus.className = 'muted';
    const common = {
      resource: snapshot.resource,
      pkarrRelays: snapshot.pkarrRelays,
    };
    if (state.config.mode === 'staging' && snapshot.verifierType === 'paykit-payment') {
      const paykitData = await checkExternalReaderPaykitData({
        readerPubky: snapshot.readerPublicKey,
        creatorPubky: snapshot.paykitCreator,
        lookup: (readerPublicKey) => hasPaykitData({ readerPublicKey }),
      });
      if (
        activeSubmissionToken !== submissionToken
        || !workflowMatches(snapshot)
        || state.loaded?.creator !== snapshot.paykitCreator
        || state.readerPublicKey !== snapshot.readerPublicKey
      ) return;
      state.paykitReaderPrepared = paykitData.canSubmit;
      state.paykitReaderState = paykitData.state;
      state.paykitDataMessage = paykitData.message;
      if (!paykitData.canSubmit) throw new Error(paykitData.message);
    }
    const result = snapshot.verifierType === 'paykit-payment'
      ? await submitPaykitPaymentProof({
        ...common,
        readerPublicKey: snapshot.paykitReaderPrepared ? snapshot.readerPublicKey : '',
      })
      : await submitDevStaticProof({ ...common, satisfied: snapshot.proofSatisfied });
    if (
      activeSubmissionToken !== submissionToken
      || !workflowMatches(snapshot)
    ) return;
    state.creator = result.creator;
    state.bundleId = result.bundleId;
    state.submittedProofBundle = result.submittedProofBundle;
    state.lifecycle = result.lifecycle;
    state.completion = null;
    state.accessCredential = null;
    state.accessCredentialResponse = null;
    clearReadResult();
    activeSubmissionToken = null;
    state.submittingProof = false;
    persistState();
    render();
    await postClientLog('info', 'reader-submit-proof-succeeded', {
      creator: result.creator,
      bundleId: result.bundleId,
      lifecycle: result.lifecycle,
    });
    if (snapshot.verifierType === 'paykit-payment') {
      void pollPaymentLifecycle(createPaymentHandle(snapshot, result));
    }
  } catch (error) {
    if (activeSubmissionToken !== submissionToken || !workflowMatches(snapshot)) return;
    await postClientLog('error', 'reader-submit-proof-failed', serializeError(error));
    showError(el.proofStatus, error);
  } finally {
    if (activeSubmissionToken === submissionToken) {
      activeSubmissionToken = null;
      state.submittingProof = false;
      render();
    }
  }
}

async function completeVerification() {
  try {
    if (state.verifierType === 'paykit-payment') {
      throw new Error('paykit-payment verification is completed by the Lock Server payment verifier');
    }
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

async function pollPaymentLifecycle(handle = currentPaymentHandle()) {
  if (activePollToken) return;
  const pollToken = Symbol('payment-poll');
  activePollToken = pollToken;
  state.paymentPolling = true;
  render();
  try {
    if (!handle || !workflowMatches(handle)) {
      throw new Error('payment proof bundle is required before polling');
    }
    await postClientLog('info', 'reader-payment-poll-started', handleDetails(handle));
    for (let attempt = 0; attempt < 600; attempt += 1) {
      if (!workflowMatches(handle) || activePollToken !== pollToken) return;
      const lifecycle = await lookupVerificationTask({
        resource: handle.resource,
        creator: handle.creator,
        bundleId: handle.bundleId,
        pkarrRelays: handle.pkarrRelays,
      });
      if (!workflowMatches(handle) || activePollToken !== pollToken) return;
      state.lifecycle = lifecycle;
      persistState();
      render();

      const status = lifecycle.status;
      const classification = classifyPaymentLifecycle(lifecycle);
      if (status === 'pending' || status === 'in_progress') {
        await delay(1_000);
        continue;
      }
      if (status === 'failed' || status === 'expired' || classification === 'failed') {
        throw new Error(`payment verification ended with status ${status}`);
      }
      if (classification === 'completed') {
        await postClientLog('info', 'reader-payment-poll-completed', handleDetails(handle));
        if (!workflowMatches(handle) || activePollToken !== pollToken) return;
        const credential = await issuePaymentCredential(handle);
        if (credential && handle.primaryPath) {
          await readPaymentContent(handle, handle.primaryPath, credential);
        }
        return;
      }
    }
    throw new Error('payment verification polling timed out');
  } catch (error) {
    if (!handle || !workflowMatches(handle) || activePollToken !== pollToken) return;
    await postClientLog('error', 'reader-payment-poll-failed', serializeError(error));
    showError(el.proofStatus, error);
  } finally {
    if (activePollToken === pollToken) {
      activePollToken = null;
      state.paymentPolling = false;
      render();
    }
  }
}

async function issuePaymentCredential(handle) {
  const response = await issueAccessCredential({
    resource: handle.resource,
    creator: handle.creator,
    bundleId: handle.bundleId,
    pkarrRelays: handle.pkarrRelays,
  });
  if (!workflowMatches(handle)) return null;
  state.accessCredentialResponse = response;
  state.accessCredential = response.credential;
  persistState();
  render();
  return response.credential;
}

async function readPaymentContent(handle, path, accessCredential) {
  const result = await readGuardedContent({
    resource: handle.resource,
    accessCredential,
    path,
    pkarrRelays: handle.pkarrRelays,
  });
  if (!workflowMatches(handle)) return;
  state.guardedResourcePath = path;
  setReadResult(path, result);
  persistState();
  render();
}

async function issueCredential() {
  const handle = currentVerificationHandle();
  try {
    if (!handle) throw new Error('completed verification handle is required');
    await postClientLog('info', 'reader-issue-credential-started', handleDetails(handle));
    if (!workflowMatches(handle)) return;
    el.credentialStatus.textContent = 'Issuing access credential...';
    el.credentialStatus.className = 'muted';
    const response = await issueAccessCredential({
      resource: handle.resource,
      creator: handle.creator,
      bundleId: handle.bundleId,
      pkarrRelays: handle.pkarrRelays,
    });
    if (!workflowMatches(handle)) return;
    state.accessCredentialResponse = response;
    state.accessCredential = response.credential;
    persistState();
    render();
    await postClientLog('info', 'reader-issue-credential-succeeded', {
      expiresAt: response.expires_at,
      credentialLength: response.credential?.length,
    });
  } catch (error) {
    if (handle && !workflowMatches(handle)) return;
    await postClientLog('error', 'reader-issue-credential-failed', serializeError(error));
    showError(el.credentialStatus, error);
  }
}

async function readContent(path) {
  const handle = currentVerificationHandle();
  const accessCredential = state.accessCredential;
  try {
    if (!handle) throw new Error('verification handle is required');
    path = path?.trim();
    if (!path) throw new Error('guarded resource path is required');
    await postClientLog('info', 'reader-proxy-read-started', {
      resource: handle.resource,
      path,
      hasCredential: Boolean(accessCredential),
    });
    if (!workflowMatches(handle)) return;
    el.readStatus.textContent = 'Reading guarded content...';
    el.readStatus.className = 'muted';
    const result = await readGuardedContent({
      resource: handle.resource,
      accessCredential,
      path,
      pkarrRelays: handle.pkarrRelays,
    });
    if (!workflowMatches(handle)) return;
    state.guardedResourcePath = path;
    setReadResult(path, result);
    persistState();
    render();
    await postClientLog('info', 'reader-proxy-read-succeeded', {
      size: result.size,
      contentType: result.contentType,
    });
  } catch (error) {
    if (handle && !workflowMatches(handle)) return;
    await postClientLog('error', 'reader-proxy-read-failed', serializeError(error));
    showError(el.readStatus, error);
  }
}

function render() {
  el.resource.value = state.resource ?? '';
  el.proofSatisfied.value = String(Boolean(state.proofSatisfied));
  el.verifierType.value = state.verifierType;
  el.verifierType.disabled = true;
  el.readerPublicKey.value = state.readerPublicKey ?? '';
  const stagingMode = state.config.mode === 'staging';
  el.paykitReaderGuidance.textContent = stagingMode
    ? 'Use a second Bitkit identity: paste its public Pubky and check public Paykit v0 data before submitting.'
    : 'The Paykit reader identity is prepared automatically by the local demo.';
  el.readerPublicKey.readOnly = !stagingMode;
  el.refreshPaykitReader.textContent = stagingMode ? 'Check Paykit data' : 'Refresh Paykit reader';
  const paymentMode = state.verifierType === 'paykit-payment';
  el.proofSatisfied.closest('label').hidden = paymentMode;
  el.paykitReaderCommands.hidden = !paymentMode;
  el.load.disabled = state.loadingLock || state.submittingProof;
  if (stagingMode) {
    el.paykitReaderStatus.textContent = state.paykitDataMessage
      || 'Paste the distinct reader Bitkit Pubky, then check public Paykit data.';
    el.paykitReaderStatus.className = state.paykitReaderState === 'present'
      ? 'ok'
      : ['absent', 'unavailable'].includes(state.paykitReaderState)
        ? 'warning'
        : state.paykitReaderState === 'invalid'
          ? 'error'
          : 'muted';
  } else if (state.paykitReaderState === 'request_received') {
    el.paykitReaderStatus.textContent = 'Paykit reader received and validated the Payment Request.';
    el.paykitReaderStatus.className = 'ok';
  } else if (state.paykitReaderState === 'waiting') {
    el.paykitReaderStatus.textContent = 'Paykit reader is prepared and waiting for a Payment Request.';
    el.paykitReaderStatus.className = 'ok';
  } else if (state.paykitReaderState === 'retrying') {
    el.paykitReaderStatus.textContent = 'Paykit reader is retrying private protocol processing.';
    el.paykitReaderStatus.className = 'warning';
  } else if (state.paykitReaderState === 'waiting_for_creator') {
    el.paykitReaderStatus.textContent = 'Paykit reader is waiting for the content creator to authenticate.';
    el.paykitReaderStatus.className = 'muted';
  } else if (state.paykitReaderState === 'failed') {
    el.paykitReaderStatus.textContent = 'Paykit reader worker failed. Inspect coarse container logs.';
    el.paykitReaderStatus.className = 'error';
  } else {
    el.paykitReaderStatus.textContent = 'Paykit reader worker is starting.';
    el.paykitReaderStatus.className = 'muted';
  }
  el.paykitReaderPayment.textContent = stagingMode
    ? (state.bundleId ? 'Complete the Payment Request in the external reader Bitkit, then resume payment verification polling.' : '')
    : state.paykitPaymentRequest
    ? format({
      payment_request_id: state.paykitPaymentRequest.payment_request_id,
      asset: state.paykitPaymentRequest.asset,
      amount_sats: state.paykitPaymentRequest.amount_sats,
      address: state.paykitPaymentRequest.address,
      payment_command: state.paykitPaymentRequest.payment_command,
      optional_mining_command: state.paykitPaymentRequest.optional_mining_command,
    })
    : '';

  if (state.loadingLock) {
    el.loadStatus.textContent = 'Loading content lock...';
    el.loadStatus.className = 'muted';
  } else if (state.loaded) {
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

  el.submitProof.disabled = state.loadingLock
    || state.submittingProof
    || !state.loaded
    || (paymentMode && (!state.paykitReaderPrepared || !state.readerPublicKey));
  if (state.submittingProof) {
    el.proofStatus.textContent = 'Submitting proof bundle...';
    el.proofStatus.className = 'muted';
  } else if (state.lifecycle) {
    el.proofStatus.textContent = `Proof submitted. Status: ${state.lifecycle.status}`;
    el.proofStatus.className = ['failed', 'expired'].includes(state.lifecycle.status) ? 'error' : 'ok';
  } else {
    el.proofStatus.textContent = state.loaded ? 'Ready to submit proof bundle.' : 'Waiting for loaded lock.';
    el.proofStatus.className = 'muted';
  }
  el.proofOutput.textContent = format({
    submittedProofBundle: state.submittedProofBundle,
    lifecycle: state.lifecycle,
  });
  el.pollPayment.disabled = !paymentMode || !state.bundleId || state.paymentPolling;

  el.completeVerification.disabled = paymentMode || !state.bundleId;
  if (state.completion) {
    el.completionStatus.textContent = `Dev verification completed. Status: ${state.completion.status}`;
    el.completionStatus.className = state.completion.status === 'failed' ? 'error' : 'ok';
  } else if (paymentMode) {
    el.completionStatus.textContent = 'Payment verification is completed by the Lock Server; no dev completion call is used.';
    el.completionStatus.className = 'muted';
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
    el.readStatus.textContent = `Read ${state.readResult.size} bytes (${state.readResult.contentType}) from ${state.readResult.path}.`;
    el.readStatus.className = 'ok';
  } else {
    el.readStatus.textContent = state.accessCredential ? 'Ready to read guarded content.' : 'Waiting for access credential.';
    el.readStatus.className = 'muted';
  }
  renderReadOutput();
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

function contentLockVerifierType(contentLock) {
  const criteria = toPlainJson(contentLock)?.criteria;
  if (!Array.isArray(criteria) || criteria.length === 0) {
    throw new Error('content lock has no verifier criteria');
  }
  const types = new Set(criteria.map((criterion) => criterion?.verifier_type));
  if (types.size !== 1) throw new Error('reader demo requires one verifier type per content lock');
  const [verifierType] = types;
  if (!['dev-static', 'paykit-payment'].includes(verifierType)) {
    throw new Error(`reader demo does not support verifier type ${String(verifierType)}`);
  }
  return verifierType;
}

function restoreState() {
  const raw = localStorage.getItem(STATE_KEY);
  if (!raw) return;
  try {
    Object.assign(state, restorePersistedReaderState(JSON.parse(raw)), {
      loadingLock: false,
      submittingProof: false,
      paymentPolling: false,
      paykitReaderPrepared: false,
      readerPublicKey: '',
      paykitReaderState: 'starting',
      paykitPaymentRequest: null,
      baselinePaymentRequestId: null,
      readResult: null,
    });
  } catch {
    localStorage.removeItem(STATE_KEY);
  }
}

function persistState() {
  localStorage.setItem(STATE_KEY, JSON.stringify(buildPersistedReaderState(state)));
}

function pkarrRelays() {
  return pkarrRelaysForDemoConfig(state.config);
}

function paykitDataSnapshotMatches(snapshot) {
  return snapshot.incarnation === workflowIncarnation
    && snapshot.resource === state.resource
    && snapshot.creatorPubky === state.creator
    && snapshot.readerPubky === state.readerPublicKey;
}

function invalidateWorkflow() {
  workflowIncarnation += 1;
  paykitReaderStatusRequests.invalidate();
  paykitDataChecks.invalidate();
  if (state.paykitPaymentRequest?.payment_request_id) {
    state.baselinePaymentRequestId = state.paykitPaymentRequest.payment_request_id;
  }
  state.paykitPaymentRequest = null;
  activeLoadToken = null;
  activeSubmissionToken = null;
  activePollToken = null;
  state.loadingLock = false;
  state.submittingProof = false;
  state.paymentPolling = false;
}

function clearVerificationState({ clearLoaded = false } = {}) {
  if (clearLoaded) {
    state.loaded = null;
    state.lockResources = [];
    state.guardedResourcePath = '';
    state.verifierType = 'dev-static';
    if (state.config?.mode === 'staging') {
      state.paykitReaderPrepared = false;
      state.paykitReaderState = 'unchecked';
      state.paykitDataMessage = '';
    }
  }
  state.creator = null;
  state.bundleId = null;
  state.submittedProofBundle = null;
  state.lifecycle = null;
  state.completion = null;
  state.accessCredential = null;
  state.accessCredentialResponse = null;
  clearReadResult();
}

function setReadResult(path, result) {
  clearReadResult();
  const objectUrl = result.kind === 'text'
    ? null
    : URL.createObjectURL(new Blob([result.bytes], { type: result.contentType }));
  state.readResult = {
    path,
    size: result.size,
    contentType: result.contentType,
    kind: result.kind,
    text: result.text,
    objectUrl,
  };
}

function clearReadResult() {
  if (state.readResult?.objectUrl) URL.revokeObjectURL(state.readResult.objectUrl);
  state.readResult = null;
}

function renderReadOutput() {
  el.readOutput.replaceChildren();
  const result = state.readResult;
  if (!result) return;

  if (result.kind === 'text') {
    const output = document.createElement('pre');
    output.textContent = result.text;
    el.readOutput.append(output);
    return;
  }

  if (result.kind === 'image') {
    const image = document.createElement('img');
    image.src = result.objectUrl;
    image.alt = `Guarded content from ${result.path}`;
    el.readOutput.append(image);
    return;
  }

  const metadata = document.createElement('p');
  metadata.textContent = `${result.contentType}, ${result.size} bytes`;
  const download = document.createElement('a');
  download.href = result.objectUrl;
  download.download = result.path.split('/').pop() || 'guarded-content';
  download.textContent = 'Download guarded content';
  el.readOutput.append(metadata, download);
}

window.addEventListener('pagehide', clearReadResult);

function workflowMatches(handle) {
  return workflowHandleMatches(handle, {
    incarnation: workflowIncarnation,
    resource: state.resource,
    creator: state.creator,
    bundleId: state.bundleId,
  });
}

function createPaymentHandle(snapshot, result) {
  return Object.freeze({
    incarnation: snapshot.incarnation,
    resource: snapshot.resource,
    creator: result.creator,
    bundleId: result.bundleId,
    primaryPath: snapshot.primaryPath,
    pkarrRelays: snapshot.pkarrRelays,
  });
}

function currentVerificationHandle() {
  if (!state.creator || !state.bundleId) return null;
  return Object.freeze({
    incarnation: workflowIncarnation,
    resource: state.resource,
    creator: state.creator,
    bundleId: state.bundleId,
    pkarrRelays: Object.freeze([...pkarrRelays()]),
  });
}

function currentPaymentHandle() {
  if (state.verifierType !== 'paykit-payment') return null;
  const handle = currentVerificationHandle();
  if (!handle) return null;
  return Object.freeze({
    ...handle,
    primaryPath: state.lockResources.find((resource) => resource.kind === 'primary')?.readPath ?? '',
  });
}

function handleDetails(handle = currentPaymentHandle()) {
  return { creator: handle?.creator ?? state.creator, bundleId: handle?.bundleId ?? state.bundleId };
}

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
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

async function postClientLog(level) {
  try {
    await fetch('/api/client-log', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ level }),
    });
  } catch {
    console.warn('failed to post reader client log');
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
