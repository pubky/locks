import init, {
  ConnectUrlOptions,
  ExchangeFrontendSessionCodeOptions,
  Locks,
  CreateContentLockRequestBuilder,
  LocksOptions,
  RegisterGuardedResourceOptions,
  SetLockServicePointerOptions,
} from '../../locks-sdk/bindings/js/pkg/locks_sdk_wasm.js';
import { enforceCreatorIdentityMatch } from './creator-identity.js';

export function buildLocksOptions({ pkarrRelays = [] } = {}) {
  const options = new LocksOptions();
  for (const relay of pkarrRelays) {
    options.addPkarrRelay(relay);
  }
  return options;
}

/**
 * Starts the creator connect flow.
 *
 * The caller must persist `state` until the callback returns. The Lock Server
 * hosts legacy-connect; the app receives only `code` and `state` on callback,
 * not the raw legacy Pubky authorization URL.
 */
export async function startCreatorConnect({ lockServer, returnTo, state, pkarrRelays = [] }) {
  await init();

  const locks = Locks.forServerWithOptions(lockServer, buildLocksOptions({ pkarrRelays }));
  const connectUrl = await locks.createConnectUrl(
    new ConnectUrlOptions(returnTo, state),
  );

  return { connectUrl, state };
}

/**
 * Completes the creator connect flow on the callback page.
 *
 * `expectedState` must be the caller-managed value persisted before redirect.
 * The returned session secret is bearer-equivalent; store it according to the
 * host application's security model.
 */
export async function completeCreatorConnect({
  lockServer,
  callbackUrl,
  expectedState,
  expectedCreatorPubky,
  pkarrRelays = [],
}) {
  await init();

  const callback = Locks.parseConnectCallback(callbackUrl);
  return exchangeCreatorConnectCode({
    lockServer,
    code: callback.code,
    state: callback.state,
    expectedState,
    expectedCreatorPubky,
    pkarrRelays,
  });
}

/**
 * Exchanges a raw `{ code, state }` pair (delivered by the Lock Server `/connect` shell via
 * postMessage, or parsed from a redirect callback) for a frontend session.
 *
 * `expectedState` is required and must equal the value the caller persisted before starting the
 * flow — the CSRF binding is always enforced (fail closed). The returned session secret is
 * bearer-equivalent.
 */
export async function exchangeCreatorConnectCode({
  lockServer,
  code,
  state,
  expectedState,
  expectedCreatorPubky,
  pkarrRelays = [],
}) {
  await init();

  if (state !== expectedState) {
    throw new Error('invalid Locks connect state');
  }

  const locks = Locks.forServerWithOptions(lockServer, buildLocksOptions({ pkarrRelays }));
  const session = await locks.exchangeFrontendSessionCode(
    new ExchangeFrontendSessionCodeOptions(code, state),
  );
  await enforceCreatorIdentityMatch({ session, expectedCreatorPubky });
  return {
    session,
    sessionSecret: session.exportSecret(),
    lockServer: session.lockServer(),
  };
}

/**
 * Restores an existing creator session secret and idempotently configures the
 * creator default Lock Service Pointer. The demo calls this before file upload.
 */
export async function configureLockServicePointer({ lockServer, sessionSecret, pkarrRelays = [] }) {
  await init();

  const locks = Locks.forServerWithOptions(lockServer, buildLocksOptions({ pkarrRelays }));
  const session = locks.restoreSession(sessionSecret);

  await session.creator.setLockServicePointer(
    new SetLockServicePointerOptions(lockServer),
  );
}

/**
 * Queries Paykit setup readiness through the current authenticated Locks session.
 */
export async function queryPaykitSetupStatus({ lockServer, sessionSecret, pkarrRelays = [] }) {
  await init();

  const locks = Locks.forServerWithOptions(lockServer, buildLocksOptions({ pkarrRelays }));
  const session = locks.restoreSession(sessionSecret);
  return session.creator.paykitSetupStatus();
}

/**
 * Restores an existing creator session secret and publishes one or more guarded
 * resources plus a content lock for the full resource set.
 *
 * Pass `resources` for multi-file publishing. The first resource becomes the
 * primary resource and the remaining resources become secondary resources keyed
 * by their full private paths. The legacy single-file `path`/`contentType`/`bytes`
 * arguments are still accepted for small examples and tests.
 */
export async function publishLockedContent({
  lockServer,
  sessionSecret,
  resources,
  path,
  contentType,
  bytes,
  criteria,
  lockLogic,
  accessTtlSeconds = 3600,
  pkarrRelays = [],
}) {
  await init();

  const locks = Locks.forServerWithOptions(lockServer, buildLocksOptions({ pkarrRelays }));
  const session = locks.restoreSession(sessionSecret);
  const resourcesToPublish = normalizeResources({ resources, path, contentType, bytes });

  const registeredResources = [];
  for (const resource of resourcesToPublish) {
    const registered = await session.creator.registerGuardedResource(
      new RegisterGuardedResourceOptions(resource.path, resource.contentType, resource.bytes),
    );
    registeredResources.push(registered.guarded_resource);
  }

  const [primaryResource, ...secondaryResources] = registeredResources;
  let builder = new CreateContentLockRequestBuilder()
    .primaryResource(primaryResource)
    .criteria(criteria)
    .lockLogic(lockLogic)
    .accessPolicy({ requested_credential_ttl_seconds: accessTtlSeconds })
    .lockServer({ override: lockServer });

  for (const secondaryResource of secondaryResources) {
    builder = builder.secondaryResource(secondaryResource);
  }

  const contentLockRequest = builder.build();
  const contentLock = await session.creator.createContentLock(contentLockRequest);

  return {
    registered: registeredResources[0],
    registeredResources,
    contentLock,
    contentLockResource: `${contentLock.content_lock.creator}${contentLock.content_lock_path}`,
  };
}

function normalizeResources({ resources, path, contentType, bytes }) {
  if (resources?.length) {
    return resources.map((resource, index) => normalizeResource(resource, index));
  }
  return [normalizeResource({ path, contentType, bytes }, 0)];
}

function normalizeResource(resource, index) {
  if (!resource?.path) throw new Error(`resource ${index + 1} path is required`);
  if (resource.path.includes('/')) throw new Error(`resource ${index + 1} path must not contain /`);
  if (!resource.bytes?.byteLength) throw new Error(`resource ${index + 1} bytes are required`);
  return {
    path: resource.path,
    contentType: resource.contentType || 'application/octet-stream',
    bytes: resource.bytes,
  };
}

/**
 * Optional cleanup: revokes the current frontend session.
 */
export async function signOutCreator({ lockServer, sessionSecret, pkarrRelays = [] }) {
  await init();

  const locks = Locks.forServerWithOptions(lockServer, buildLocksOptions({ pkarrRelays }));
  const session = locks.restoreSession(sessionSecret);
  await session.signout();
}
