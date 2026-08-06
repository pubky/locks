import init, {
  BundleId,
  Locks,
  LocksOptions,
  VerificationTaskHandleOptions,
} from '../../locks-sdk/bindings/js/pkg/locks_sdk_wasm.js';

export function buildLocksOptions({ pkarrRelays = [] } = {}) {
  const options = new LocksOptions();
  for (const relay of pkarrRelays) {
    options.addPkarrRelay(relay);
  }
  return options;
}

export async function loadContentLock({ resource, pkarrRelays = [] } = {}) {
  await init();
  const options = buildLocksOptions({ pkarrRelays });
  const [locks, contentLock] = await Promise.all([
    Locks.forContentLockWithOptions(resource, options),
    Locks.readContentLockWithOptions(resource, options),
  ]);
  return {
    locks,
    viewer: locks.viewer,
    contentLock,
    resources: describeContentLockResources(contentLock),
  };
}

export function describeContentLockResources(contentLock) {
  const resources = [];
  const primaryResource = getField(contentLock, 'primary_resource');
  if (primaryResource) {
    resources.push(describeResource('primary', getField(primaryResource, 'path'), primaryResource));
  }
  const secondaryResources = getField(contentLock, 'secondary_resources') ?? {};
  for (const [path, resource] of objectEntries(secondaryResources)) {
    resources.push(describeResource('secondary', path, resource));
  }
  return resources;
}

function describeResource(kind, path, resource) {
  return {
    kind,
    path,
    readPath: privateContentPathToReadPath(path),
    contentType: getField(resource, 'content_type'),
    size: getField(resource, 'size'),
    hash: getField(resource, 'hash'),
  };
}

function getField(value, key) {
  if (value instanceof Map) return value.get(key);
  return value?.[key];
}

function objectEntries(value) {
  if (value instanceof Map) return value.entries();
  return Object.entries(value ?? {});
}

function privateContentPathToReadPath(path) {
  const prefix = '/priv/locks.app/content/';
  if (!path?.startsWith(prefix)) throw new Error(`content lock resource path is outside ${prefix}: ${path}`);
  return path.slice(prefix.length);
}

export async function submitDevStaticProof({ resource, satisfied, pkarrRelays = [] }) {
  const { locks, viewer } = await loadContentLock({ resource, pkarrRelays });
  const bundleId = BundleId.generate().toString();
  const creator = creatorFromResource(resource);
  const submittedProofBundle = {
    version: 1,
    bundle_id: bundleId,
    pubky_lock_resource: resource,
    proofs: [
      {
        criterion_id: 'criterion-1',
        verifier_type: 'dev-static',
        payload: { satisfied: Boolean(satisfied) },
      },
    ],
  };
  const lifecycle = await viewer.submitProofBundle(submittedProofBundle);
  return { locks, viewer, creator, bundleId, submittedProofBundle, lifecycle };
}

export function buildPaykitPaymentProofBundle({ resource, readerPublicKey, criterionId, bundleId }) {
  if (typeof resource !== 'string' || !resource) throw new Error('content lock resource is required');
  if (typeof readerPublicKey !== 'string' || !readerPublicKey) throw new Error('reader Pubky is required');
  if (typeof criterionId !== 'string' || !criterionId) throw new Error('payment criterion id is required');
  if (typeof bundleId !== 'string' || !bundleId) throw new Error('bundle id is required');
  return {
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
}

export function paykitCriterionId(contentLock) {
  const criteria = getField(contentLock, 'criteria');
  const matches = Array.from(criteria ?? []).filter(
    (criterion) => getField(criterion, 'verifier_type') === 'paykit-payment',
  );
  if (matches.length !== 1) throw new Error('content lock must contain exactly one paykit-payment criterion');
  const criterionId = getField(matches[0], 'criterion_id');
  if (typeof criterionId !== 'string' || !criterionId) throw new Error('paykit-payment criterion id is missing');
  return criterionId;
}

export async function submitPaykitPaymentProof({
  resource,
  readerPublicKey,
  pkarrRelays = [],
}) {
  const { locks, viewer, contentLock } = await loadContentLock({ resource, pkarrRelays });
  const bundleId = BundleId.generate().toString();
  const creator = creatorFromResource(resource);
  const submittedProofBundle = buildPaykitPaymentProofBundle({
    resource,
    readerPublicKey,
    criterionId: paykitCriterionId(contentLock),
    bundleId,
  });
  const lifecycle = await viewer.submitProofBundle(submittedProofBundle);
  return { locks, viewer, creator, bundleId, submittedProofBundle, lifecycle };
}

export function classifyPaymentLifecycle(lifecycle) {
  const status = getField(lifecycle, 'status');
  if (status === 'pending' || status === 'in_progress') return 'retry';
  if (status === 'completed') return 'completed';
  if (status === 'failed' || status === 'expired') return 'failed';
  throw new Error(`unknown lifecycle status: ${String(status)}`);
}

export async function completeDevVerification({ resource, creator, bundleId, pkarrRelays = [] }) {
  const { viewer } = await loadContentLock({ resource, pkarrRelays });
  return viewer.completeVerificationTask(new VerificationTaskHandleOptions(creator, bundleId));
}

export async function lookupVerificationTask({ resource, creator, bundleId, pkarrRelays = [] }) {
  const { viewer } = await loadContentLock({ resource, pkarrRelays });
  return viewer.lookupVerificationTask(new VerificationTaskHandleOptions(creator, bundleId));
}

export async function issueAccessCredential({ resource, creator, bundleId, pkarrRelays = [] }) {
  const { viewer } = await loadContentLock({ resource, pkarrRelays });
  return viewer.issueAccessCredential(new VerificationTaskHandleOptions(creator, bundleId));
}

export function workflowHandleMatches(handle, current) {
  return handle?.incarnation === current.incarnation
    && handle.resource === current.resource
    && (handle.creator === undefined || handle.creator === current.creator)
    && (handle.bundleId === undefined || handle.bundleId === current.bundleId);
}

export function createLatestRequestGate() {
  let activeToken = null;
  return Object.freeze({
    begin(incarnation) {
      const request = Object.freeze({ token: Symbol('request'), incarnation });
      activeToken = request.token;
      return request;
    },
    isCurrent(request, incarnation) {
      return request?.token === activeToken && request.incarnation === incarnation;
    },
    finish(request) {
      if (request?.token === activeToken) activeToken = null;
    },
    invalidate() {
      activeToken = null;
    },
  });
}

export function selectCurrentPaykitPaymentRequest({
  status,
  baselinePaymentRequestId,
  currentPaymentRequest,
}) {
  if (status?.state !== 'request_received') return null;
  if (status.payment_request_id !== baselinePaymentRequestId) return status;
  return currentPaymentRequest?.payment_request_id !== baselinePaymentRequestId
    ? currentPaymentRequest
    : null;
}

export function parsePreparedReaderBrowserStatus(value) {
  if (value?.version !== 1 || typeof value.prepared !== 'boolean') {
    throw new Error('invalid prepared Paykit reader status');
  }
  const expectedKeys = value.prepared
    ? ['version', 'prepared', 'reader_pubky']
    : ['version', 'prepared'];
  if (
    Object.keys(value).length !== expectedKeys.length
    || !expectedKeys.every((key) => Object.hasOwn(value, key))
    || (value.prepared && !/^pubky[ybndrfg8ejkmcpqxot1uwisza345h769]{52}$/.test(value.reader_pubky))
  ) {
    throw new Error('invalid prepared Paykit reader status');
  }
  return Object.freeze({ ...value });
}

export function parsePaykitReaderBrowserStatus(value) {
  const pubky = /^pubky[ybndrfg8ejkmcpqxot1uwisza345h769]{52}$/;
  if (value?.version !== 1 || typeof value.state !== 'string') {
    throw new Error('invalid Paykit reader status');
  }
  if (value.state === 'starting' && hasExactKeys(value, ['version', 'state'])) {
    return Object.freeze({ ...value });
  }
  if (value.state === 'waiting_for_creator' && hasExactKeys(value, ['version', 'state'])) {
    return Object.freeze({ ...value });
  }
  if (
    value.state === 'waiting'
    && hasExactKeys(value, ['version', 'state', 'reader_pubky'])
    && pubky.test(value.reader_pubky)
  ) {
    return Object.freeze({ ...value });
  }
  if (
    value.state === 'retrying'
    && hasExactKeys(value, ['version', 'state', 'reader_pubky', 'error'])
    && pubky.test(value.reader_pubky)
    && ['receive_timeout', 'protocol_failed'].includes(value.error)
  ) {
    return Object.freeze({ ...value });
  }
  if (
    value.state === 'failed'
    && hasExactKeys(value, ['version', 'state', 'error'])
    && [
      'invalid_input',
      'invalid_config',
      'invalid_state',
      'output_failed',
      'prepare_timeout',
      'worker_failed',
      'identity_mismatch',
    ].includes(value.error)
  ) {
    return Object.freeze({ ...value });
  }
  if (
    value.state === 'request_received'
    && hasExactKeys(value, [
      'version',
      'state',
      'reader_pubky',
      'payment_request_id',
      'address',
      'asset',
      'amount_sats',
      'payment_command',
      'optional_mining_command',
    ])
    && pubky.test(value.reader_pubky)
    && /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value.payment_request_id)
    && /^bcrt1[02-9ac-hj-np-z]{8,86}$/.test(value.address)
    && value.asset === 'BTC'
    && /^[1-9][0-9]*$/.test(value.amount_sats)
    && canonicalPaymentCommandMatches(value)
  ) {
    return Object.freeze({ ...value });
  }
  throw new Error('invalid Paykit reader status');
}

function hasExactKeys(value, expected) {
  return Object.keys(value).length === expected.length
    && expected.every((key) => Object.hasOwn(value, key));
}

function canonicalPaymentCommandMatches(value) {
  const payment = /^docker compose --file \.\/compose\.paykit-local-demo\.yaml exec -T bitcoin sh -ec 'bitcoin-cli -conf="\$BITCOIN_DATA\/bitcoin\.conf" -regtest -rpcwallet=miner sendtoaddress "([^"]+)" "((?:0|[1-9][0-9]*)(?:\.[0-9]{1,8})?)"'$/.exec(value.payment_command);
  const mining = "docker compose --file ./compose.paykit-local-demo.yaml exec -T bitcoin sh -ec 'bitcoin-cli -conf=\"$BITCOIN_DATA/bitcoin.conf\" -regtest -rpcwallet=miner generatetoaddress 6 \"$(bitcoin-cli -conf=\"$BITCOIN_DATA/bitcoin.conf\" -regtest -rpcwallet=miner getnewaddress)\"'";
  return Boolean(payment)
    && payment[1] === value.address
    && browserBtcToSats(payment[2]) === BigInt(value.amount_sats)
    && value.optional_mining_command === mining;
}

function browserBtcToSats(value) {
  const [whole, fraction = ''] = value.split('.');
  return (BigInt(whole) * 100_000_000n) + BigInt(fraction.padEnd(8, '0'));
}

export async function readGuardedContent({ resource, accessCredential, path, pkarrRelays = [] }) {
  const { viewer } = await loadContentLock({ resource, pkarrRelays });
  if (!path) throw new Error('guarded resource path is required');
  const response = await viewer.proxyReadGuardedResourceResponse(accessCredential, path);
  return decodeGuardedContentResponse(response);
}

export async function decodeGuardedContentResponse(response) {
  if (!(response instanceof Response)) throw new Error('guarded resource read returned a non-Response value');
  const contentType = response.headers.get('content-type') || 'application/octet-stream';
  const array = new Uint8Array(await response.arrayBuffer());
  const mediaType = contentType.split(';', 1)[0].trim().toLowerCase();
  const kind = mediaType.startsWith('text/') || mediaType === 'application/json' || mediaType.endsWith('+json')
    ? 'text'
    : mediaType.startsWith('image/')
      ? 'image'
      : 'binary';
  return {
    response,
    bytes: array,
    kind,
    text: kind === 'text' ? new TextDecoder().decode(array) : null,
    size: array.byteLength,
    contentType,
  };
}

export function creatorFromResource(resource) {
  const slash = resource.indexOf('/');
  if (slash <= 0) throw new Error('content lock resource must start with pubky<creator>/...');
  return resource.slice(0, slash);
}
