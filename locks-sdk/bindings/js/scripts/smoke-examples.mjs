import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const repoRoot = resolve(new URL('../../../..', import.meta.url).pathname);
const examplesDir = join(repoRoot, 'examples', 'js-sdk');
const files = {
  readme: join(examplesDir, 'README.md'),
  packageJson: join(examplesDir, 'package.json'),
  app: join(examplesDir, 'app.js'),
  index: join(examplesDir, 'index.html'),
  creator: join(examplesDir, 'creator-complete-flow.js'),
  readerHtml: join(examplesDir, 'reader.html'),
  readerApp: join(examplesDir, 'reader-app.js'),
  readerFlow: join(examplesDir, 'reader-flow.js'),
  initConfig: join(examplesDir, 'scripts', 'init-config.mjs'),
  createUser: join(examplesDir, 'scripts', 'create-user.mjs'),
  authenticate: join(examplesDir, 'scripts', 'authenticate.mjs'),
  startServer: join(examplesDir, 'scripts', 'start-demo-server.mjs'),
  startReaderServer: join(examplesDir, 'scripts', 'start-reader-demo-server.mjs'),
  pathsLib: join(examplesDir, 'scripts', 'lib', 'paths.mjs'),
  configLib: join(examplesDir, 'scripts', 'lib', 'config.mjs'),
  pubkyLib: join(examplesDir, 'scripts', 'lib', 'pubky.mjs'),
};

for (const [name, path] of Object.entries(files)) {
  if (!existsSync(path)) {
    throw new Error(`missing JS SDK example ${name}: ${path}`);
  }
}

const texts = Object.fromEntries(
  Object.entries(files).map(([name, path]) => [name, readFileSync(path, 'utf8')]),
);

const required = {
  readme: [
    'npm --prefix examples/js-sdk install',
    'npm --prefix examples/js-sdk run init-config',
    'npm --prefix examples/js-sdk run create-user -- --role content-creator',
    'npm --prefix examples/js-sdk run authenticate -- --role content-creator',
    'npm --prefix examples/js-sdk run start-server',
    'npm --prefix examples/js-sdk run start-reader-server',
    './.local/js-sdk-demo/config.json',
    './.local/content-creator/recovery_file',
    'http://localhost:15411',
    'http://localhost:8081/reader/',
    '/priv/locks.app/content/',
    'Viewer content lock resource',
    'Reset reader state',
  ],
  packageJson: [
    '"@synonymdev/pubky"',
    '"init-config": "node scripts/init-config.mjs"',
    '"create-user": "node scripts/create-user.mjs"',
    '"authenticate": "node scripts/authenticate.mjs"',
    '"start-server": "node scripts/start-demo-server.mjs"',
    '"start-reader-server": "node scripts/start-reader-demo-server.mjs"',
  ],
  app: [
    "from './creator-complete-flow.js'",
    'POST /api/demo-auth/start',
    'GET /api/demo-auth/status',
    '/auth/lock-server/callback',
    'localStorage.setItem',
    'Configure Lock Service Pointer',
    'Create locked content',
    'dev-static',
    'Viewer content lock resource',
    'Locks.forServerWithOptions',
    'buildResourcesFromFiles',
    'renderSelectedResources',
    'selected-resource-list',
    'primaryContentFile',
    'secondaryContentFiles',
    'appendSelectedResource',
    'duplicate guarded resource path',
  ],
  index: ['id="demo-auth"', 'id="creator-publishing"', '/examples/js-sdk/app.js', 'Select primary file', 'id="primary-content-file"', 'Select secondary files', 'id="secondary-content-files"', 'multiple', 'id="selected-resources"', 'id="selected-resource-list"'],
  readerHtml: ['id="content-lock-resource"', 'id="lock-resources"', 'id="primary-resource-list"', 'id="secondary-resource-list"', 'id="reset-reader-state"', 'id="read-content"', '/reader-app.js'],
  initConfig: ['~/.pubky-lock/config.toml', './.local/js-sdk-demo/config.json', 'lock_server_public_key', 'http://localhost:15411', 'http://localhost:15412', 'localhost:6881'],
  createUser: ['requiredRole', 'Keypair.random()', 'createRecoveryFile', 'profile.json', '--force', 'content-creator', 'content-viewer', 'lock-server'],
  authenticate: ['requiredRole', 'readAuthFromPrompt', 'signer.signup', 'approveAuthRequest', '--auth', 'already'],
  startServer: ['createServer', '--allow-unhealthy', 'pubkyAuthRelayInboxUrl', '/api/demo-auth/start', '/api/demo-auth/status', '/config.json', 'awaitApproval', 'content-creator-session.json', '/healthz', '/readyz'],
  startReaderServer: ['createServer', '--allow-unhealthy', '/reader/', '/config.json', '/api/preflight', '/api/debug/config', '/api/client-log', '8081', 'never proxy'],
  pathsLib: ['localPath', 'roleDir', 'demoConfigPath', 'contentCreatorSessionPath'],
  configLib: ['readDemoConfig', 'writeDemoConfig', 'parseLockServerTomlPublicKey', 'pubkyAuthRelayInboxUrl', 'validateDemoConfig'],
  pubkyLib: ["from '@synonymdev/pubky'", 'Pubky.testnet', 'Keypair.fromRecoveryFile', 'AuthFlowKind', 'PublicKey.from'],
  creator: [
    "from '../../locks-sdk/bindings/js/pkg/locks_sdk_wasm.js'",
    'startCreatorConnect',
    'completeCreatorConnect',
    'publishLockedContent',
    'LocksOptions',
    'options.addPkarrRelay(relay)',
    'Locks.forServerWithOptions(lockServer, buildLocksOptions({ pkarrRelays }))',
    'new ConnectUrlOptions(returnTo, state)',
    'Locks.parseConnectCallback(callbackUrl)',
    'new ExchangeFrontendSessionCodeOptions(code, state)',
    'locks.restoreSession(sessionSecret)',
    'new RegisterGuardedResourceOptions(resource.path, resource.contentType, resource.bytes)',
    'registeredResources',
    'builder.secondaryResource(secondaryResource)',
    'new CreateContentLockRequestBuilder()',
    'session.creator.createContentLock(contentLockRequest)',
    'normalizeResources',
    'new SetLockServicePointerOptions(lockServer)',
    'session.signout()',
  ],
  readerFlow: [
    "from '../../locks-sdk/bindings/js/pkg/locks_sdk_wasm.js'",
    'loadContentLock',
    'submitDevStaticProof',
    'completeDevVerification',
    'lookupVerificationTask',
    'issueAccessCredential',
    'readGuardedContent',
    'LocksOptions',
    'options.addPkarrRelay(relay)',
    'Locks.forContentLockWithOptions(',
    'Locks.readContentLockWithOptions(resource, options)',
    'describeContentLockResources',
    'getField',
    'objectEntries',
    'BundleId.generate().toString()',
    'viewer.submitProofBundle(submittedProofBundle)',
    'new VerificationTaskHandleOptions(creator, bundleId)',
    'viewer.completeVerificationTask',
    'viewer.issueAccessCredential',
    'viewer.proxyReadGuardedResource(accessCredential, path)',
  ],
  readerApp: [
    "from './reader-flow.js'",
    'pubky-locks-reader-demo.state',
    'reader-load-lock-started',
    'reader-submit-proof-started',
    'reader-complete-verification-started',
    'reader-complete-verification-conflict-looking-up',
    'reader-issue-credential-started',
    'reader-proxy-read-started',
    'lockResources',
    'renderLockResources',
    'data-read-resource-path',
    'toPlainJson',
    'localStorage.setItem',
  ],
};

for (const [label, snippets] of Object.entries(required)) {
  for (const snippet of snippets) {
    if (!texts[label].includes(snippet)) {
      throw new Error(`${label} missing expected snippet: ${snippet}`);
    }
  }
}

const { parseLockServerTomlPublicKey } = await import(pathToFileURL(files.configLib).href);
const lockServerPubky = 'pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo';
assert.equal(
  parseLockServerTomlPublicKey(`
[credentials]
lock_server_public_key = "${lockServerPubky}" # Public Pubky derived from lock_server_secret_key.
`),
  lockServerPubky,
);

console.log('JS SDK examples smoke check passed');
