import init, {
  Locks,
  LocksOptions,
  VerificationTaskHandleOptions,
} from '../../locks-sdk/bindings/js/pkg/locks_sdk_wasm.js';

const CONNECTION_STATES = new Set([
  'none',
  'handshake',
  'connected',
  'recovery_required',
  'blocked',
]);

const TERMINAL_REASONS = new Set([
  'payment_request_rejected',
  'payment_request_canceled',
  'proposal_expired',
  'payment_deadline_expired',
]);

/**
 * Complete a Paykit-backed viewer flow using only public JS/WASM SDK exports.
 * Caller must generate and durably store bundleId before invoking this helper.
 */
export async function unlockPaykitContent({
  resource,
  readerPublicKey,
  guardedPath,
  bundleId,
  pkarrRelays = [],
  pollIntervalMs = 1_000,
  maxPollAttempts = 120,
  onConnectionState = () => {},
  onConnectionError = () => {},
}) {
  await init();

  const options = new LocksOptions();
  for (const relay of pkarrRelays) options.addPkarrRelay(relay);

  const [locks, contentLock] = await Promise.all([
    Locks.forContentLockWithOptions(resource, options),
    Locks.readContentLockWithOptions(resource, options),
  ]);
  const viewer = locks.viewer;
  const creator = creatorFromResource(resource);
  const criterionId = paykitCriterionId(contentLock);
  const submittedProofBundle = {
    version: 1,
    bundle_id: bundleId,
    pubky_lock_resource: resource,
    reader_public_key: readerPublicKey,
    proofs: [{
      criterion_id: criterionId,
      verifier_type: 'paykit-payment',
      payload: {},
    }],
  };
  const handle = new VerificationTaskHandleOptions(creator, bundleId);

  const result = await runPaykitViewerFlow({
    viewer,
    handle,
    submittedProofBundle,
    guardedPath,
    pollIntervalMs,
    maxPollAttempts,
    onConnectionState,
    onConnectionError,
  });
  return { bundleId, ...result };
}

/**
 * Execute the task lifecycle once caller has a Viewer and validated handle.
 * Exported separately so applications can resume from a stored handle.
 */
export async function runPaykitViewerFlow({
  viewer,
  handle,
  submittedProofBundle,
  guardedPath,
  pollIntervalMs = 1_000,
  maxPollAttempts = 120,
  onConnectionState = () => {},
  onConnectionError = () => {},
  wait = delay,
}) {
  let lifecycle = submittedProofBundle
    ? await viewer.submitProofBundle(submittedProofBundle)
    : await viewer.lookupVerificationTask(handle);
  let pollAttempts = 0;
  let observeConnection = true;
  let connectionLookupInFlight = false;

  while (true) {
    const status = validateLifecycle(lifecycle);
    if (status === 'pending' || status === 'in_progress') {
      if (observeConnection && !connectionLookupInFlight) {
        connectionLookupInFlight = true;
        void viewer.lookupPaykitConnectionState(handle)
          .then((response) => {
            const state = parseConnectionState(response);
            if (state === 'blocked') observeConnection = false;
            onConnectionState(state);
          })
          .catch(onConnectionError)
          .finally(() => {
            connectionLookupInFlight = false;
          });
      }
      pollAttempts += 1;
      if (pollAttempts > maxPollAttempts) {
        throw new Error('Paykit verification polling exhausted');
      }
      await wait(pollIntervalMs);
      lifecycle = await viewer.lookupVerificationTask(handle);
      continue;
    }
    if (status === 'failed') {
      throw new Error(
        `Paykit verification failed: ${lifecycle.failure_message}; no access credential was issued; start a new attempt with a fresh Bundle ID and payment request`,
      );
    }
    if (status === 'expired') {
      throw new Error(
        `Paykit verification expired: ${lifecycle.terminal_reason}; no access credential was issued; start a new attempt with a fresh Bundle ID and payment request`,
      );
    }

    const issued = await viewer.issueAccessCredential(handle);
    const response = await viewer.proxyReadGuardedResourceResponse(
      issued.credential,
      guardedPath,
    );
    return {
      lifecycle,
      credentialExpiresAt: issued.expires_at,
      response,
    };
  }
}

function validateLifecycle(lifecycle) {
  const status = lifecycle?.status;
  const started = lifecycle?.started_at != null;
  const completed = lifecycle?.completed_at != null;
  const failure = lifecycle?.failure_message;
  const terminalReason = lifecycle?.terminal_reason;
  const valid = (
    (status === 'pending'
      && !started && !completed && failure === null && terminalReason === null)
    || (status === 'in_progress'
      && started && !completed && failure === null && terminalReason === null)
    || (status === 'completed'
      && started && completed && failure === null && terminalReason === null)
    || (status === 'failed'
      && started && completed && typeof failure === 'string' && failure.trim() !== ''
      && terminalReason === null)
    || (status === 'expired'
      && started && completed && failure === null && TERMINAL_REASONS.has(terminalReason))
  );
  if (!valid) {
    throw new Error(`invalid verification lifecycle response: ${String(status)}`);
  }
  return status;
}

function parseConnectionState(response) {
  const state = response?.state;
  if (!CONNECTION_STATES.has(state)) {
    throw new Error(`unknown Paykit connection state: ${String(state)}`);
  }
  return state;
}

function paykitCriterionId(contentLock) {
  const criteria = contentLock?.criteria ?? [];
  const matches = criteria.filter((criterion) => criterion?.verifier_type === 'paykit-payment');
  if (matches.length !== 1 || !matches[0].criterion_id) {
    throw new Error('content lock must contain exactly one paykit-payment criterion');
  }
  return matches[0].criterion_id;
}

function creatorFromResource(resource) {
  const slash = resource.indexOf('/');
  if (slash <= 0) throw new Error('content lock resource must start with pubky<creator>/...');
  return resource.slice(0, slash);
}

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
