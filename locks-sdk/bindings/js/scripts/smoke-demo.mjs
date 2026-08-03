import { readFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';

const root = new URL('..', import.meta.url).pathname;
const demoDir = join(root, 'demo');
const htmlPath = join(demoDir, 'index.html');
const appPath = join(demoDir, 'app.js');

for (const path of [htmlPath, appPath]) {
  if (!existsSync(path)) {
    throw new Error(`missing browser demo file: ${path}`);
  }
}

const html = readFileSync(htmlPath, 'utf8');
const app = readFileSync(appPath, 'utf8');

const htmlSnippets = [
  '<script type="module" src="./app.js"></script>',
  'id="lock-server"',
  'id="creator"',
  'id="discover-creator"',
  'id="creator-pointer"',
  'id="use-creator-pointer"',
  'id="connect"',
  'id="handle-callback"',
  'id="content-lock-resource"',
  'id="read-content-lock"',
  'id="select-content-lock-server"',
  'id="bundle-id"',
  'id="generate-bundle-id"',
  'id="register-resource"',
  'id="set-pointer"',
  'Viewer/access SDK helpers are available',
];

const appSnippets = [
  "from '../pkg/locks_sdk_wasm.js'",
  'await init()',
  'Locks.forServer(lockServer)',
  'await Locks.forCreator(creator)',
  'Locks.fromCreatorLockServicePointer(pointer)',
  'locks.lockServer()',
  'await currentLocks.createConnectUrl(',
  'sessionStorage.setItem(stateKey, state)',
  'Locks.parseConnectCallback(window.location.href)',
  'callback.state !== expectedState',
  'await currentLocks.exchangeFrontendSessionCode(',
  'session.exportSecret()',
  'currentLocks.restoreSession(secret)',
  'await currentSession.signout()',
  'await Locks.readContentLock(resource)',
  'await Locks.forContentLock(resource)',
  'BundleId.generate().toString()',
  'currentSession.creator.registerGuardedResource(',
  'currentSession.creator.setLockServicePointer(',
  'new RegisterGuardedResourceOptions(',
  'new SetLockServicePointerOptions(',
];

for (const snippet of htmlSnippets) {
  if (!html.includes(snippet)) {
    throw new Error(`demo HTML missing expected snippet: ${snippet}`);
  }
}

for (const snippet of appSnippets) {
  if (!app.includes(snippet)) {
    throw new Error(`demo app missing expected snippet: ${snippet}`);
  }
}

console.log('browser demo smoke check passed');
