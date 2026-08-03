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

export async function readGuardedContent({ resource, accessCredential, path, pkarrRelays = [] }) {
  const { viewer } = await loadContentLock({ resource, pkarrRelays });
  if (!path) throw new Error('guarded resource path is required');
  const bytes = await viewer.proxyReadGuardedResource(accessCredential, path);
  const array = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  return {
    bytes: array,
    text: new TextDecoder().decode(array),
    size: array.byteLength,
  };
}

export function creatorFromResource(resource) {
  const slash = resource.indexOf('/');
  if (slash <= 0) throw new Error('content lock resource must start with pubky<creator>/...');
  return resource.slice(0, slash);
}
