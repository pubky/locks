import init, {
  BundleId,
  ConnectUrlOptions,
  ExchangeFrontendSessionCodeOptions,
  Locks,
  RegisterGuardedResourceOptions,
  SetLockServicePointerOptions,
} from '../pkg/locks_sdk_wasm.js';

const stateKey = 'pubky-locks-demo-state';
const secretKey = 'pubky-locks-demo-session-secret';

const elements = {
  lockServer: document.querySelector('#lock-server'),
  creator: document.querySelector('#creator'),
  discoverCreator: document.querySelector('#discover-creator'),
  creatorPointer: document.querySelector('#creator-pointer'),
  useCreatorPointer: document.querySelector('#use-creator-pointer'),
  returnTo: document.querySelector('#return-to'),
  connect: document.querySelector('#connect'),
  handleCallback: document.querySelector('#handle-callback'),
  restoreSession: document.querySelector('#restore-session'),
  signout: document.querySelector('#signout'),
  contentLockResource: document.querySelector('#content-lock-resource'),
  readContentLock: document.querySelector('#read-content-lock'),
  selectContentLockServer: document.querySelector('#select-content-lock-server'),
  bundleId: document.querySelector('#bundle-id'),
  generateBundleId: document.querySelector('#generate-bundle-id'),
  resourcePath: document.querySelector('#resource-path'),
  resourceText: document.querySelector('#resource-text'),
  registerResource: document.querySelector('#register-resource'),
  setPointer: document.querySelector('#set-pointer'),
  log: document.querySelector('#log'),
};

let locks = null;
let session = null;

function log(message, value) {
  const rendered = value === undefined ? message : `${message}\n${JSON.stringify(value, null, 2)}`;
  elements.log.textContent = `${new Date().toISOString()} ${rendered}\n\n${elements.log.textContent}`;
}

function requireLockServer() {
  const lockServer = elements.lockServer.value.trim();
  if (!lockServer) throw new Error('Lock Server Pubky is required');
  return lockServer;
}

function getLocks() {
  const lockServer = requireLockServer();
  if (!locks || locks.lockServer() !== lockServer) {
    locks = Locks.forServer(lockServer);
    session = null;
  }
  return locks;
}

function requireSession() {
  if (!session) throw new Error('No active session. Connect or restore a session first.');
  return session;
}

async function run(label, fn) {
  try {
    const result = await fn();
    log(`${label}: ok`, result);
  } catch (error) {
    console.error(error);
    log(`${label}: error`, { message: error?.message ?? String(error) });
  }
}

function randomState() {
  return crypto.randomUUID();
}

function textToBytes(text) {
  return new TextEncoder().encode(text);
}

async function main() {
  await init();

  elements.returnTo.value = `${window.location.origin}${window.location.pathname}`;

  elements.discoverCreator.addEventListener('click', () => run('discover from creator', async () => {
    const creator = elements.creator.value.trim();
    if (!creator) throw new Error('Creator Pubky is required');
    locks = await Locks.forCreator(creator);
    session = null;
    elements.lockServer.value = locks.lockServer();
    return { lockServer: locks.lockServer() };
  }));

  elements.useCreatorPointer.addEventListener('click', () => run('use creator pointer JSON', async () => {
    const pointer = JSON.parse(elements.creatorPointer.value);
    locks = Locks.fromCreatorLockServicePointer(pointer);
    session = null;
    elements.lockServer.value = locks.lockServer();
    return { lockServer: locks.lockServer() };
  }));

  elements.connect.addEventListener('click', () => run('create connect URL', async () => {
    const currentLocks = getLocks();
    const state = randomState();
    sessionStorage.setItem(stateKey, state);
    const connectUrl = await currentLocks.createConnectUrl(
      new ConnectUrlOptions(elements.returnTo.value, state),
    );
    window.location.href = connectUrl;
    return { connectUrl, state };
  }));

  elements.handleCallback.addEventListener('click', () => run('handle callback', async () => {
    const currentLocks = getLocks();
    const callback = Locks.parseConnectCallback(window.location.href);
    const expectedState = sessionStorage.getItem(stateKey);
    if (callback.state !== expectedState) {
      throw new Error('invalid Locks connect state');
    }
    session = await currentLocks.exchangeFrontendSessionCode(
      new ExchangeFrontendSessionCodeOptions(callback.code, callback.state),
    );
    sessionStorage.setItem(secretKey, session.exportSecret());
    return { lockServer: session.lockServer() };
  }));

  elements.restoreSession.addEventListener('click', () => run('restore session', async () => {
    const secret = sessionStorage.getItem(secretKey);
    if (!secret) throw new Error('No saved session secret in sessionStorage');
    const currentLocks = getLocks();
    session = currentLocks.restoreSession(secret);
    return { lockServer: session.lockServer() };
  }));

  elements.signout.addEventListener('click', () => run('signout', async () => {
    const currentSession = requireSession();
    await currentSession.signout();
    sessionStorage.removeItem(secretKey);
    session = null;
  }));

  elements.readContentLock.addEventListener('click', () => run('read content lock', async () => {
    const resource = elements.contentLockResource.value.trim();
    if (!resource) throw new Error('Content lock resource is required');
    return await Locks.readContentLock(resource);
  }));

  elements.selectContentLockServer.addEventListener('click', () => run('select content lock server', async () => {
    const resource = elements.contentLockResource.value.trim();
    if (!resource) throw new Error('Content lock resource is required');
    locks = await Locks.forContentLock(resource);
    session = null;
    elements.lockServer.value = locks.lockServer();
    return { lockServer: locks.lockServer() };
  }));

  elements.generateBundleId.addEventListener('click', () => run('generate bundle id', async () => {
    const bundleId = BundleId.generate().toString();
    elements.bundleId.value = bundleId;
    return { bundleId };
  }));

  elements.registerResource.addEventListener('click', () => run('register guarded resource', async () => {
    const currentSession = requireSession();
    return await currentSession.creator.registerGuardedResource(
      new RegisterGuardedResourceOptions(
        elements.resourcePath.value,
        'text/plain',
        textToBytes(elements.resourceText.value),
      ),
    );
  }));

  elements.setPointer.addEventListener('click', () => run('set lock service pointer', async () => {
    const currentSession = requireSession();
    await currentSession.creator.setLockServicePointer(
      new SetLockServicePointerOptions(requireLockServer()),
    );
  }));

  log('demo initialized');
}

main();
