import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { chmodSync, existsSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { PassThrough } from 'node:stream';
import { pathToFileURL } from 'node:url';

const repoRoot = resolve(new URL('../../../..', import.meta.url).pathname);
const examplesDir = join(repoRoot, 'examples', 'js-sdk');
const files = {
  readme: join(examplesDir, 'README.md'),
  packageJson: join(examplesDir, 'package.json'),
  app: join(examplesDir, 'app.js'),
  appIframe: join(examplesDir, 'app-iframe.js'),
  index: join(examplesDir, 'index.html'),
  iframe: join(examplesDir, 'iframe.html'),
  flows: join(examplesDir, 'flows.html'),
  creator: join(examplesDir, 'creator-complete-flow.js'),
  creatorIdentity: join(examplesDir, 'creator-identity.js'),
  creatorPolicy: join(examplesDir, 'creator-lock-policy.js'),
  paykitSetup: join(examplesDir, 'paykit-setup.js'),
  readerHtml: join(examplesDir, 'reader.html'),
  readerApp: join(examplesDir, 'reader-app.js'),
  readerFlow: join(examplesDir, 'reader-flow.js'),
  initConfig: join(examplesDir, 'scripts', 'init-config.mjs'),
  createUser: join(examplesDir, 'scripts', 'create-user.mjs'),
  authenticate: join(examplesDir, 'scripts', 'authenticate.mjs'),
  authenticatePaykit: join(examplesDir, 'scripts', 'authenticate-paykit.mjs'),
  composeCompanionHelper: join(examplesDir, 'scripts', 'paykit-companion-auth-compose.sh'),
  preparePaykitReader: join(examplesDir, 'scripts', 'prepare-paykit-reader.mjs'),
  receivePaykitRequest: join(examplesDir, 'scripts', 'receive-paykit-request.mjs'),
  registerPaykitReader: join(examplesDir, 'scripts', 'register-paykit-reader.mjs'),
  paykitReaderLib: join(examplesDir, 'scripts', 'lib', 'paykit-reader-helper.mjs'),
  paykitReaderStatus: join(examplesDir, 'scripts', 'lib', 'paykit-reader-status.mjs'),
  paykitReaderWorker: join(examplesDir, 'scripts', 'lib', 'paykit-reader-worker.mjs'),
  creatorSessionState: join(examplesDir, 'scripts', 'lib', 'creator-session-state.mjs'),
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
    'docker compose --file compose.paykit-local-demo.yaml up -d --build',
    'http://127.0.0.1:8080/examples/js-sdk/',
    'npm --prefix examples/js-sdk run start-server',
    'npm --prefix examples/js-sdk run start-reader-server',
    './.local/demo-config/config.json',
    './.local/content-creator/recovery_file',
    'http://127.0.0.1:15411',
    'http://127.0.0.1:8088/reader/',
    '/priv/locks.app/content/',
    'Viewer content lock resource',
    'Reset reader state',
    'Both creator pages open the Lock Server `/connect` shell in an iframe modal.',
    'kept in memory only',
    'lock type defaults to `dev-static`',
    '`paykit-payment`',
    'asset is fixed to `BTC`',
    'recipient is the authenticated content creator',
    'must both be approved by that same content-creator identity',
    'Paykit browser  = http://127.0.0.1:3001',
    'opens `GET http://127.0.0.1:3001/setup` in a Paykit-origin iframe',
    'exact iframe window and origin with the pending state',
    'npm --prefix examples/js-sdk run authenticate-paykit -- --role content-creator',
    'Non-TTY stdin is exactly those three ordered lines',
    'in-process Paykit reader worker starts with `reader-demo`',
    'sole mutable owner of `./.local/paykit-reader/state.v1`',
    './.local/paykit-reader/state.v1',
    './.local/paykit-reader/prepared.v1.json',
    './.local/paykit-reader/worker.v1.json',
    'Reader Pubky matches the current `content-viewer` identity',
    'top-level `reader_public_key`',
    'minimum_confirmations = 0',
  ],
  packageJson: [
    '"@synonymdev/pubky"',
    '"init-config": "node scripts/init-config.mjs"',
    '"create-user": "node scripts/create-user.mjs"',
    '"authenticate": "node scripts/authenticate.mjs"',
    '"authenticate-paykit": "node scripts/authenticate-paykit.mjs"',
    '"prepare-paykit-reader": "node scripts/prepare-paykit-reader.mjs"',
    '"receive-paykit-request": "node scripts/receive-paykit-request.mjs"',
    '"test:paykit-reader-worker": "node scripts/test-paykit-reader-worker.mjs"',
    '"start-server": "node scripts/start-demo-server.mjs"',
    '"start-reader-server": "node scripts/start-reader-demo-server.mjs"',
    '"smoke": "npm --prefix ../../locks-sdk/bindings/js run smoke:examples"',
    'node --check creator-lock-policy.js',
    'node --check paykit-setup.js',
    'node --check scripts/authenticate-paykit.mjs',
    'node --check scripts/prepare-paykit-reader.mjs',
    'node --check scripts/receive-paykit-request.mjs',
    'node --check scripts/lib/creator-session-state.mjs',
    'node --check creator-identity.js',
  ],
  app: ["import './app-iframe.js'"],
  appIframe: [
    "from './creator-complete-flow.js'",
    'POST /api/demo-auth/start',
    'GET /api/demo-auth/status',
    'const returnTo = `${window.location.origin}/auth/lock-server/callback`',
    "deliveryUrl.searchParams.set('delivery', 'postmessage')",
    'openLockAuthIframe(deliveryUrl.toString())',
    'frame.src = connectUrl',
    'event.origin !== state.lockServerOrigin',
    'event.source !== state.lockAuthFrame?.contentWindow',
    'expectedState: state.pendingConnectState',
    'expectedCreatorPubky: state.creatorPubky',
    'state.pendingConnectState = null',
    'state.lockServerOrigin = null',
    'state.lockAuthFrame = null',
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
    'buildCreatorLockPolicy',
    'refreshLockTypeFields',
    'paykitSetupComplete: false',
    'const creatorPubky = status.authenticated ? status.pubky : null',
    'const creatorChanged = state.creatorPubky !== creatorPubky',
    'if (creatorChanged) {',
    'state.paykitSetupComplete = false',
    'closePaykitSetupIframe()',
    'state.creatorPubky = creatorPubky',
    'invalidateIdentityScopedCreatorState',
    'signOutCreator',
    'localStorage.removeItem(pointerConfiguredKey(previousCreatorPubky))',
    'recipientPubky: state.creatorPubky',
    'paykitSetupComplete: state.paykitSetupComplete',
    "const paymentSelected = el.lockType.value === 'paykit-payment'",
    'el.devStaticFields.hidden = paymentSelected',
    'el.paykitPaymentFields.hidden = !paymentSelected',
    'el.paykitAmountSats.required = paymentSelected',
    'openPaykitSetupIframe',
    'npm --prefix examples/js-sdk run authenticate-paykit -- --role content-creator',
    'acceptPaykitSetupEvent',
    'state.paykitSetupComplete = true',
    "el.paykitSetupStatus.className = 'ok'",
    'paykitUrl: state.config.paykit.url',
    'returnTo: window.location.origin',
    'expectedOrigin: state.paykitSetupOrigin',
    'expectedSource: state.paykitSetupFrame?.contentWindow',
    'expectedState: state.pendingPaykitSetupState',
    'setupCreator: state.paykitSetupCreator',
    'currentCreator: state.creatorPubky',
    "if (el.lockType.value === 'paykit-payment' && state.creatorPubky) startPaykitSetup()",
    'el.retryPaykitSetup.hidden = false',
  ],
  index: ['iframe modal', 'id="demo-auth"', 'id="creator-publishing"', '/examples/js-sdk/app.js', 'Select primary file', 'id="primary-content-file"', 'Select secondary files', 'id="secondary-content-files"', 'multiple', 'id="selected-resources"', 'id="selected-resource-list"', 'id="lock-type"', '<option value="dev-static">dev-static</option>', '<option value="paykit-payment">paykit-payment</option>', 'id="dev-static-fields"', 'id="paykit-payment-fields" hidden', 'id="paykit-amount-sats"', 'id="paykit-setup-status"', 'id="retry-paykit-setup"'],
  iframe: ['iframe modal', 'id="demo-auth"', 'id="creator-publishing"', '/examples/js-sdk/app-iframe.js', 'id="lock-type"', '<option value="dev-static">dev-static</option>', '<option value="paykit-payment">paykit-payment</option>', 'id="dev-static-fields"', 'id="paykit-payment-fields" hidden', 'id="paykit-amount-sats"', 'id="paykit-setup-status"', 'id="retry-paykit-setup"'],
  flows: ['Both creator pages use iframe auth', '/examples/js-sdk/', '/examples/js-sdk/iframe.html'],
  readerHtml: ['id="content-lock-resource"', 'id="lock-resources"', 'id="primary-resource-list"', 'id="secondary-resource-list"', 'id="reset-reader-state"', 'id="read-content"', 'id="reader-public-key" readonly', 'id="refresh-paykit-reader"', 'id="paykit-reader-status"', 'id="paykit-reader-payment"', 'id="paykit-reader-commands"', 'id="poll-payment"', 'paykit-payment', 'Paykit reader identity is prepared automatically', '/reader-app.js'],
  initConfig: ['~/.pubky-lock/config.toml', './.local/demo-config/config.json', 'lock_server_public_key', 'http://127.0.0.1:15411', 'http://127.0.0.1:15412', '127.0.0.1:6881'],
  createUser: ['requiredRole', 'Keypair.random()', 'createRecoveryFile', 'profile.json', '--force', 'content-creator', 'content-viewer', 'lock-server', 'clearPreparedReaderStatus', 'clearCreatorDemoSession'],
  authenticate: ['requiredRole', 'readAuthFromPrompt', 'signer.signup', 'approveAuthRequest', '--auth', 'already'],
  authenticatePaykit: [
    'PAYKIT_COMPANION_AUTH_BIN',
    '/usr/local/bin/paykit-companion-auth',
    'paykit-companion-auth-compose.sh',
    'loadRoleSecret',
    'content-creator',
    'version: 1',
    'auth_url',
    'creator_secret',
    'creatorSecret.buffer',
    'account_xpub',
    'account_index',
    'spawnProcess(helperPath, helperArgs, spawnOptions)',
    'catch {\n      payload.fill(0);',
    "child.stdin.end",
    "child.kill('SIGTERM')",
    "child.kill('SIGKILL')",
  ],
  composeCompanionHelper: [
    'compose.paykit-local-demo.yaml',
    'PAYKIT_EXTERNAL_READER_PUBKY',
    'exec -T creator-demo',
    '/usr/local/bin/paykit-companion-auth',
  ],
  preparePaykitReader: ['runReaderOperation', "operation: 'prepare'", 'content-viewer', 'writePreparedReaderStatus', 'assertStandaloneReaderOperationAllowed', 'acquirePaykitReaderOwnership', 'ownership.release()'],
  receivePaykitRequest: ['runReaderOperation', "operation: 'receive'", 'content-viewer', 'assertStandaloneReaderOperationAllowed', 'acquirePaykitReaderOwnership', 'ownership.release()'],
  paykitReaderLib: [
    'PAYKIT_READER_DEMO_BIN',
    '/usr/local/bin/paykit-reader-demo',
    'version: 1',
    'reader_secret',
    'PAYKIT_READER_STATE_PATH',
    'loadRoleSecret',
    'runBoundedHelper',
    'payment_command',
    'optional_mining_command',
    'session?.free();',
    'signer?.free();',
    'keypair.free();',
  ],
  paykitReaderStatus: ['validatePreparedReaderStatus', 'clearPreparedReaderStatus', 'writePreparedReaderStatus', 'readPreparedReaderStatus', 'buildPreparedReaderBrowserStatus', 'writePaykitReaderWorkerStatus', 'readPaykitReaderWorkerStatus', 'buildPaykitReaderBrowserStatus', '0o600'],
  paykitReaderWorker: ['runPaykitReaderWorker', 'assertStandaloneReaderOperationAllowed', 'acquirePaykitReaderOwnership', 'supervisePaykitReaderWorker', '/usr/bin/flock', "'--no-fork'", 'shell: false', "operation: 'prepare'", "operation: 'receive'", "state: 'request_received'"],
  registerPaykitReader: ['signupReaderBestEffort', "request.operation !== 'register'", 'registration_failed'],
  creatorSessionState: ['clearCreatorDemoSession', 'readCreatorDemoSessionForCurrentRole', 'writeCreatorDemoSessionForCurrentRole', 'contentCreatorSessionPath', 'rm'],
  startServer: ['createServer', '--allow-unhealthy', 'pubkyAuthRelayInboxUrl', '/api/demo-auth/start', '/api/demo-auth/status', '/config.json', 'awaitApproval', 'content-creator-session.json', 'readCreatorDemoSessionForCurrentRole', 'writeCreatorDemoSessionForCurrentRole', '/healthz', '/readyz', 'paykit: source.paykit'],
  startReaderServer: ['createServer', '--allow-unhealthy', 'runPaykitReaderWorker', 'supervisePaykitReaderWorker', 'workerOwnsState', 'handleTerminalWorkerFailure', 'writePaykitReaderWorkerStatus', 'readPaykitReaderWorkerStatus', 'AbortController', 'SIGTERM', '/reader/', '/config.json', '/api/health', '/api/preflight', '/api/debug/config', '/api/paykit-reader/status', '/api/client-log', "'cache-control': 'no-store'", '8088', 'never proxy'],
  pathsLib: ['localPath', 'roleDir', 'demoConfigPath', 'contentCreatorSessionPath', 'paykitReaderPreparedPath', 'paykitReaderOwnershipPath', 'prepared.v1.json', 'owner.lock'],
  configLib: ['readDemoConfig', 'writeDemoConfig', 'parseLockServerTomlPublicKey', 'pubkyAuthRelayInboxUrl', 'validateDemoConfig', "url: 'http://127.0.0.1:3001'", "['paykit', 'url']"],
  pubkyLib: ["from '@synonymdev/pubky'", 'Pubky.testnet', 'Keypair.fromRecoveryFile', 'keypair.secret()', 'loadRoleSecret', 'AuthFlowKind', 'PublicKey.from'],
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
    '.lockLogic(lockLogic)',
  ],
  creatorIdentity: ['enforceCreatorIdentityMatch', 'invalidateIdentityScopedCreatorState', 'session.signout()', 'does not match the demo creator'],
  creatorPolicy: ['buildCreatorLockPolicy', 'paykit-payment', 'recipient_pubky', "asset: 'BTC'"],
  paykitSetup: ['buildPaykitSetupRequest', 'acceptPaykitSetupEvent', 'paykit-setup-callback'],
  readerFlow: [
    "from '../../locks-sdk/bindings/js/pkg/locks_sdk_wasm.js'",
    'loadContentLock',
    'submitDevStaticProof',
    'submitPaykitPaymentProof',
    'buildPaykitPaymentProofBundle',
    'reader_public_key',
    "verifier_type: 'paykit-payment'",
    'payload: {}',
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
    'viewer.proxyReadGuardedResourceResponse(accessCredential, path)',
    'response.headers.get',
    'response.arrayBuffer()',
  ],
  readerApp: [
    "from './reader-flow.js'",
    'pubky-locks-reader-demo.state',
    'reader-load-lock-started',
    'reader-submit-proof-started',
    'pollPaymentLifecycle',
    "status === 'in_progress'",
    "status === 'expired'",
    'reader-complete-verification-started',
    'reader-complete-verification-conflict-looking-up',
    'reader-issue-credential-started',
    'reader-proxy-read-started',
    'lockResources',
    'renderLockResources',
    'data-read-resource-path',
    'let workflowIncarnation = 0;',
    'if (state.submittingProof) return;',
    'activeSubmissionToken !== submissionToken',
    '!workflowMatches(handle) || activePollToken !== pollToken',
    'issuePaymentCredential(handle)',
    'readPaymentContent(handle, handle.primaryPath, credential)',
    "fetch('/api/paykit-reader/status'",
    "cache: 'no-store'",
    'parsePaykitReaderBrowserStatus',
    'selectCurrentPaykitPaymentRequest',
    'state.paykitReaderState',
    'state.baselinePaymentRequestId',
    'state.paykitPaymentRequest',
    'el.paykitReaderPayment.textContent',
    'createLatestRequestGate()',
    'paykitReaderStatusRequests.begin(workflowIncarnation)',
    'paykitReaderStatusRequests.isCurrent(request, workflowIncarnation)',
    'paykitReaderStatusRequests.invalidate()',
    'state.paykitReaderPrepared',
    "state.verifierType === 'paykit-payment'",
    '(!state.paykitReaderPrepared || !state.readerPublicKey)',
    'baselinePaymentRequestId: _baselinePaymentRequestId',
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

if (texts.startReaderServer.includes('8081')) {
  throw new Error('reader demo server must use the canonical port 8088');
}

const readerElementMapStart = texts.readerApp.indexOf('const el = {');
const readerElementMapEnd = texts.readerApp.indexOf('\n};', readerElementMapStart);
const readerElementMap = texts.readerApp.slice(readerElementMapStart, readerElementMapEnd);
const declaredReaderElements = new Set(
  [...readerElementMap.matchAll(/^\s*([A-Za-z][A-Za-z0-9]*):/gm)].map((match) => match[1]),
);
const usedReaderElements = new Set(
  [...texts.readerApp.matchAll(/\bel\.([A-Za-z][A-Za-z0-9]*)/g)].map((match) => match[1]),
);
const undeclaredReaderElements = [...usedReaderElements].filter((name) => !declaredReaderElements.has(name));
if (undeclaredReaderElements.length > 0) {
  throw new Error(`reader app uses undeclared DOM bindings: ${undeclaredReaderElements.join(', ')}`);
}

const sessionFreeIndex = texts.paykitReaderLib.indexOf('session?.free();');
const signerFreeIndex = texts.paykitReaderLib.indexOf('signer?.free();', sessionFreeIndex);
const keypairFreeIndex = texts.paykitReaderLib.indexOf('keypair.free();', signerFreeIndex);
if (sessionFreeIndex < 0 || signerFreeIndex < sessionFreeIndex || keypairFreeIndex < signerFreeIndex) {
  throw new Error('Paykit reader registration must free session, signer, and keypair in reverse ownership order');
}

if (texts.authenticatePaykit.includes('Buffer.from(creatorSecret)')) {
  throw new Error('authenticate-paykit must not create an untracked raw-secret Buffer copy');
}
if (texts.readerApp.includes("readerPublicKey.addEventListener('input'")) {
  throw new Error('reader payment identity must come from confirmed prepare status, not manual input');
}
const clearPreparedOnViewerRotation = texts.createUser.indexOf(
  "if (role === 'content-viewer') await clearPreparedReaderStatus();",
);
const generateReplacementViewer = texts.createUser.indexOf('Keypair.random()');
if (clearPreparedOnViewerRotation < 0 || generateReplacementViewer < clearPreparedOnViewerRotation) {
  throw new Error('content-viewer replacement must clear prepared Paykit reader evidence before key generation');
}

const demoAuthFinally = texts.startServer.indexOf('.finally(() => {');
const clearSettledDemoAuthPromise = texts.startServer.indexOf('demoAuthPromise = null;', demoAuthFinally);
if (demoAuthFinally < 0 || clearSettledDemoAuthPromise < demoAuthFinally) {
  throw new Error('settled demo auth flows must clear their pending promise');
}

const clearDemoSessionOnCreatorRotation = texts.createUser.indexOf(
  "if (role === 'content-creator') await clearCreatorDemoSession();",
);
const generateReplacementCreator = texts.createUser.indexOf('Keypair.random()');
const writeReplacementCreatorProfile = texts.createUser.indexOf('await writeJson(profileFile, profile);');
const clearDemoSessionAfterCreatorRotation = texts.createUser.lastIndexOf(
  "if (role === 'content-creator') await clearCreatorDemoSession();",
);
if (
  clearDemoSessionOnCreatorRotation < 0
  || generateReplacementCreator < clearDemoSessionOnCreatorRotation
  || clearDemoSessionAfterCreatorRotation <= writeReplacementCreatorProfile
) {
  throw new Error('content-creator replacement must clear stale demo auth before and after key generation');
}

const {
  clearCreatorDemoSession,
  readCreatorDemoSessionForCurrentRole,
  writeCreatorDemoSessionForCurrentRole,
} = await import(pathToFileURL(files.creatorSessionState).href);
const creatorSessionTestDir = mkdtempSync(join(tmpdir(), 'locks-creator-session-'));
const creatorSessionTestPath = join(creatorSessionTestDir, 'content-creator-session.json');
const creatorProfileTestPath = join(creatorSessionTestDir, 'profile.json');
const firstCreatorPubky = 'pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy';
const secondCreatorPubky = 'pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo';
try {
  writeFileSync(creatorSessionTestPath, '{"exported_session":"sensitive"}');
  await clearCreatorDemoSession(creatorSessionTestPath);
  assert.equal(existsSync(creatorSessionTestPath), false);
  await clearCreatorDemoSession(creatorSessionTestPath);

  writeFileSync(creatorProfileTestPath, JSON.stringify({ role: 'content-creator', pubky: secondCreatorPubky }));
  writeFileSync(creatorSessionTestPath, JSON.stringify({ role: 'content-creator', pubky: firstCreatorPubky, exported_session: 'old-secret' }));
  assert.equal(await readCreatorDemoSessionForCurrentRole({
    sessionPath: creatorSessionTestPath,
    profilePath: creatorProfileTestPath,
  }), null);
  assert.equal(existsSync(creatorSessionTestPath), false);

  await assert.rejects(
    writeCreatorDemoSessionForCurrentRole(
      { role: 'content-creator', pubky: firstCreatorPubky, exported_session: 'late-old-secret' },
      { sessionPath: creatorSessionTestPath, profilePath: creatorProfileTestPath },
    ),
    /creator identity changed during demo authentication/,
  );
  assert.equal(existsSync(creatorSessionTestPath), false);

  const currentSession = { role: 'content-creator', pubky: secondCreatorPubky, exported_session: 'current-secret' };
  await writeCreatorDemoSessionForCurrentRole(currentSession, {
    sessionPath: creatorSessionTestPath,
    profilePath: creatorProfileTestPath,
  });
  assert.deepEqual(
    await readCreatorDemoSessionForCurrentRole({
      sessionPath: creatorSessionTestPath,
      profilePath: creatorProfileTestPath,
    }),
    currentSession,
  );
  assert.equal(statSync(creatorSessionTestPath).mode & 0o777, 0o600);
  const externalSession = { role: 'content-creator', pubky: firstCreatorPubky, exported_session: 'external-secret' };
  await writeCreatorDemoSessionForCurrentRole(externalSession, {
    sessionPath: creatorSessionTestPath,
    profilePath: null,
  });
  assert.deepEqual(
    await readCreatorDemoSessionForCurrentRole({
      sessionPath: creatorSessionTestPath,
      profilePath: null,
    }),
    externalSession,
  );
} finally {
  rmSync(creatorSessionTestDir, { recursive: true, force: true });
}

const { buildCreatorLockPolicy } = await import(pathToFileURL(files.creatorPolicy).href);
assert.deepEqual(
  buildCreatorLockPolicy({ criterionId: 'criterion-1', devStaticSatisfied: true }),
  {
    criteria: [{
      criterion_id: 'criterion-1',
      verifier_type: 'dev-static',
      params: { satisfied: true },
    }],
    lockLogic: { type: 'all', criteria: ['criterion-1'] },
  },
);
assert.deepEqual(
  buildCreatorLockPolicy({ criterionId: 'criterion-2', devStaticSatisfied: false }),
  {
    criteria: [{
      criterion_id: 'criterion-2',
      verifier_type: 'dev-static',
      params: { satisfied: false },
    }],
    lockLogic: { type: 'all', criteria: ['criterion-2'] },
  },
);

const creatorPubky = 'pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy';
const { enforceCreatorIdentityMatch } = await import(pathToFileURL(files.creatorIdentity).href);
let mismatchSignouts = 0;
await assert.rejects(
  enforceCreatorIdentityMatch({
    session: {
      creatorPubky: () => 'pubkyyyr1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo',
      signout: async () => { mismatchSignouts += 1; },
    },
    expectedCreatorPubky: creatorPubky,
  }),
  /does not match the demo creator/,
);
assert.equal(mismatchSignouts, 1);

let matchingSignouts = 0;
await enforceCreatorIdentityMatch({
  session: {
    creatorPubky: () => creatorPubky,
    signout: async () => { matchingSignouts += 1; },
  },
  expectedCreatorPubky: creatorPubky,
});
assert.equal(matchingSignouts, 0);

const { invalidateIdentityScopedCreatorState } = await import(pathToFileURL(files.creatorIdentity).href);
const identityScopedState = {
  feLockSessionToken: 'old-creator-session',
  lockAuthenticated: true,
  pendingConnectState: 'old-connect-state',
  lockServerOrigin: 'https://locks.example',
  lockAuthFrame: {},
};
let revokedSession;
const invalidation = await invalidateIdentityScopedCreatorState({
  state: identityScopedState,
  revokeSession: async (secret) => { revokedSession = secret; },
});
assert.equal(revokedSession, 'old-creator-session');
assert.equal(invalidation.revoked, true);
assert.deepEqual(identityScopedState, {
  feLockSessionToken: null,
  lockAuthenticated: false,
  pendingConnectState: null,
  lockServerOrigin: null,
  lockAuthFrame: null,
});

const failedRevocationState = {
  feLockSessionToken: 'unreachable-session',
  lockAuthenticated: true,
  pendingConnectState: null,
  lockServerOrigin: null,
  lockAuthFrame: null,
};
const failedInvalidation = await invalidateIdentityScopedCreatorState({
  state: failedRevocationState,
  revokeSession: async () => { throw new Error('network unavailable'); },
});
assert.equal(failedInvalidation.revoked, false);
assert.equal(failedRevocationState.feLockSessionToken, null);
assert.equal(failedRevocationState.lockAuthenticated, false);

const completeCreatorConnectStart = texts.creator.indexOf('export async function completeCreatorConnect');
const exchangeCreatorConnectStart = texts.creator.indexOf('export async function exchangeCreatorConnectCode');
const completeCreatorConnectBody = texts.creator.slice(completeCreatorConnectStart, exchangeCreatorConnectStart);
if (!completeCreatorConnectBody.includes('expectedCreatorPubky') || !completeCreatorConnectBody.includes('expectedCreatorPubky,')) {
  throw new Error('completeCreatorConnect must require and forward expectedCreatorPubky');
}

assert.deepEqual(
  buildCreatorLockPolicy({
    lockType: 'paykit-payment',
    criterionId: 'payment-1',
    amountSats: '00018446744073709551616',
    recipientPubky: creatorPubky,
    paykitSetupComplete: true,
  }),
  {
    criteria: [{
      criterion_id: 'payment-1',
      verifier_type: 'paykit-payment',
      params: {
        recipient_pubky: creatorPubky,
        amount: '00018446744073709551616',
        asset: 'BTC',
      },
    }],
    lockLogic: { type: 'all', criteria: ['payment-1'] },
  },
);

for (const amountSats of ['', '0', '000', '-1', '1.5', '1e3', ' 1', '1 ']) {
  assert.throws(
    () => buildCreatorLockPolicy({
      lockType: 'paykit-payment',
      criterionId: 'payment-1',
      amountSats,
      recipientPubky: creatorPubky,
      paykitSetupComplete: true,
    }),
    /positive decimal integer string/,
  );
}
for (const amountSats of [1, null, undefined]) {
  assert.throws(
    () => buildCreatorLockPolicy({
      lockType: 'paykit-payment',
      criterionId: 'payment-1',
      amountSats,
      recipientPubky: creatorPubky,
      paykitSetupComplete: true,
    }),
    /positive decimal integer string/,
  );
}
assert.throws(
  () => buildCreatorLockPolicy({
    lockType: 'paykit-payment',
    criterionId: 'payment-1',
    amountSats: '1',
    recipientPubky: creatorPubky,
    paykitSetupComplete: false,
  }),
  /complete Paykit setup/,
);
assert.throws(
  () => buildCreatorLockPolicy({
    lockType: 'paykit-payment',
    criterionId: 'payment-1',
    amountSats: '1',
    recipientPubky: '',
    paykitSetupComplete: true,
  }),
  /authenticated creator/,
);

const { buildPaykitSetupRequest, acceptPaykitSetupEvent } = await import(
  pathToFileURL(files.paykitSetup).href
);
const setupRequest = buildPaykitSetupRequest({
  paykitUrl: 'http://localhost:3001',
  returnTo: 'http://localhost:8080',
  state: 'opaque-setup-state',
  xpub: 'must-not-enter-the-iframe-url',
});
const setupUrl = new URL(setupRequest.url);
assert.equal(setupRequest.origin, 'http://localhost:3001');
assert.equal(setupUrl.origin, 'http://localhost:3001');
assert.equal(setupUrl.pathname, '/setup');
assert.deepEqual([...setupUrl.searchParams.entries()], [
  ['return_to', 'http://localhost:8080'],
  ['state', 'opaque-setup-state'],
]);
assert.equal(setupRequest.url.includes('xpub'), false);
assert.equal(setupRequest.url.includes('must-not-enter'), false);
assert.throws(() => buildPaykitSetupRequest({
  paykitUrl: 'http://localhost:3001/setup?xpub=forbidden',
  returnTo: 'http://localhost:8080',
  state: 'opaque-setup-state',
}));

const paykitFrameWindow = {};
const acceptedEvent = {
  origin: 'http://localhost:3001',
  source: paykitFrameWindow,
  data: { type: 'paykit-setup-callback', state: 'opaque-setup-state' },
};
const acceptanceContext = {
  expectedOrigin: 'http://localhost:3001',
  expectedSource: paykitFrameWindow,
  expectedState: 'opaque-setup-state',
  setupCreator: creatorPubky,
  currentCreator: creatorPubky,
};
assert.deepEqual(
  acceptPaykitSetupEvent({ event: acceptedEvent, ...acceptanceContext }),
  { status: 'complete' },
);
assert.deepEqual(
  acceptPaykitSetupEvent({
    event: {
      ...acceptedEvent,
      data: { type: 'paykit-setup-callback', state: 'opaque-setup-state', error: 'setup-failed' },
    },
    ...acceptanceContext,
  }),
  { status: 'error', error: 'setup-failed' },
);
for (const event of [
  { ...acceptedEvent, origin: '*' },
  { ...acceptedEvent, origin: 'http://localhost:3002' },
  { ...acceptedEvent, source: {} },
  { ...acceptedEvent, data: { ...acceptedEvent.data, state: 'wrong-state' } },
  { ...acceptedEvent, data: { ...acceptedEvent.data, type: 'unrelated-message' } },
  { ...acceptedEvent, data: { ...acceptedEvent.data, xpub: 'forbidden' } },
  { ...acceptedEvent, data: null },
]) {
  assert.equal(acceptPaykitSetupEvent({ event, ...acceptanceContext }), null);
}
assert.equal(
  acceptPaykitSetupEvent({ event: acceptedEvent, ...acceptanceContext, expectedOrigin: '*' }),
  null,
);
assert.equal(
  acceptPaykitSetupEvent({ event: acceptedEvent, ...acceptanceContext, currentCreator: 'different' }),
  null,
);
assert.equal(
  acceptPaykitSetupEvent({ event: acceptedEvent, ...acceptanceContext, expectedState: null }),
  null,
);
assert.equal(
  acceptPaykitSetupEvent({
    event: { ...acceptedEvent, source: null },
    ...acceptanceContext,
    expectedSource: null,
  }),
  null,
);

const { defaultDemoConfig, validateDemoConfig } = await import(pathToFileURL(files.configLib).href);
const validDemoConfig = structuredClone(defaultDemoConfig);
validDemoConfig.lockServer.pubky = creatorPubky;
assert.equal(validateDemoConfig(validDemoConfig), validDemoConfig);
for (const paykitUrl of [
  '',
  'ftp://localhost:3001',
  'http://user:pass@localhost:3001',
  'http://localhost:3001/setup',
  'http://localhost:3001?query=forbidden',
]) {
  const invalidConfig = structuredClone(validDemoConfig);
  invalidConfig.paykit.url = paykitUrl;
  assert.throws(() => validateDemoConfig(invalidConfig));
}
const missingPaykitConfig = structuredClone(validDemoConfig);
delete missingPaykitConfig.paykit;
assert.throws(() => validateDemoConfig(missingPaykitConfig));

for (const label of ['index', 'iframe']) {
  const selectIndex = texts[label].indexOf('id="lock-type"');
  const devStaticIndex = texts[label].indexOf('<option value="dev-static">dev-static</option>', selectIndex);
  const paymentIndex = texts[label].indexOf('<option value="paykit-payment">paykit-payment</option>', selectIndex);
  if (selectIndex < 0 || devStaticIndex < selectIndex || paymentIndex < devStaticIndex) {
    throw new Error(`${label} must default the lock-type selector to dev-static`);
  }
  if (/<(?:input|select|textarea)[^>]*(?:recipient|asset)/i.test(texts[label])) {
    throw new Error(`${label} must not expose editable payment recipient or asset controls`);
  }
}

const publishCallIndex = texts.appIframe.indexOf('await publishLockedContent({');
const publishCallEndIndex = texts.appIframe.indexOf('});', publishCallIndex);
const publishCall = texts.appIframe.slice(publishCallIndex, publishCallEndIndex);
for (const binding of ['criteria,', 'lockLogic,']) {
  if (!publishCall.includes(binding)) {
    throw new Error(`creator publishing must pass tested policy binding: ${binding}`);
  }
}

const authStatusRefreshIndex = texts.appIframe.indexOf('async function refreshDemoAuthStatus()');
const identityResetIndex = texts.appIframe.indexOf(
  'state.paykitSetupComplete = false',
  authStatusRefreshIndex,
);
const identityAssignmentIndex = texts.appIframe.indexOf(
  'state.creatorPubky = creatorPubky',
  authStatusRefreshIndex,
);
if (
  authStatusRefreshIndex < 0
  || identityResetIndex < 0
  || identityAssignmentIndex < 0
  || identityResetIndex > identityAssignmentIndex
) {
  throw new Error('creator identity changes must reset Paykit setup before replacing the identity');
}

const lockTypeRefreshIndex = texts.appIframe.indexOf('function refreshLockTypeFields()');
const lockTypeRefreshEnd = texts.appIframe.indexOf('function startPaykitSetup()', lockTypeRefreshIndex);
const lockTypeRefresh = texts.appIframe.slice(lockTypeRefreshIndex, lockTypeRefreshEnd);
const setupStartIndex = lockTypeRefresh.indexOf('startPaykitSetup()');
for (const guard of [
  'if (!paymentSelected)',
  'if (state.paykitSetupComplete)',
  'if (!state.creatorPubky)',
  'if (state.paykitSetupFrame) return',
]) {
  const guardIndex = lockTypeRefresh.indexOf(guard);
  if (guardIndex < 0 || setupStartIndex < 0 || guardIndex > setupStartIndex) {
    throw new Error(`Paykit setup must enforce ${guard} before starting`);
  }
}

const setupStartFunctionIndex = texts.appIframe.indexOf('function startPaykitSetup()');
const setupStartFunctionEnd = texts.appIframe.indexOf('function refreshLockAuthStatus()', setupStartFunctionIndex);
const setupStartFunction = texts.appIframe.slice(setupStartFunctionIndex, setupStartFunctionEnd);
const setupOpenIndex = setupStartFunction.indexOf('openPaykitSetupIframe(request.url)');
for (const guard of [
  "el.lockType.value !== 'paykit-payment'",
  '|| !state.creatorPubky',
  '|| state.paykitSetupComplete',
  '|| state.paykitSetupFrame',
]) {
  const guardIndex = setupStartFunction.indexOf(guard);
  if (guardIndex < 0 || guardIndex > setupOpenIndex) {
    throw new Error(`Paykit setup action must enforce ${guard} before iframe navigation`);
  }
}
for (const binding of [
  'state.pendingPaykitSetupState = pendingState',
  'state.paykitSetupOrigin = request.origin',
  'state.paykitSetupCreator = state.creatorPubky',
]) {
  const bindingIndex = setupStartFunction.indexOf(binding);
  if (bindingIndex < 0 || setupOpenIndex < 0 || bindingIndex > setupOpenIndex) {
    throw new Error(`Paykit setup must bind ${binding} before iframe navigation`);
  }
}

const setupAcceptanceIndex = texts.appIframe.indexOf('const result = acceptPaykitSetupEvent({');
const setupAcceptedGuardIndex = texts.appIframe.indexOf('if (!result) return;', setupAcceptanceIndex);
const setupCompleteIndex = texts.appIframe.indexOf('state.paykitSetupComplete = true', setupAcceptanceIndex);
if (
  setupAcceptanceIndex < 0
  || setupAcceptedGuardIndex < setupAcceptanceIndex
  || setupCompleteIndex < setupAcceptedGuardIndex
) {
  throw new Error('Paykit setup must mark completion only after exact callback acceptance');
}

const setupCloseIndex = texts.appIframe.indexOf('function closePaykitSetupIframe()');
const setupCloseEnd = texts.appIframe.indexOf('function showLockAuthComplete()', setupCloseIndex);
const setupCloseFunction = texts.appIframe.slice(setupCloseIndex, setupCloseEnd);
for (const clearedBinding of [
  'state.pendingPaykitSetupState = null',
  'state.paykitSetupOrigin = null',
  'state.paykitSetupFrame = null',
  'state.paykitSetupCreator = null',
]) {
  if (!setupCloseFunction.includes(clearedBinding)) {
    throw new Error(`closing Paykit setup must clear ${clearedBinding}`);
  }
}

for (const label of ['appIframe', 'index', 'iframe', 'paykitSetup']) {
  if (/xpub/i.test(texts[label])) {
    throw new Error(`${label} must not receive, render, store, or log xpub material`);
  }
}
if (/localStorage\.(?:getItem|setItem)\([^)]*paykit/i.test(texts.appIframe)) {
  throw new Error('Paykit setup state must remain in memory only');
}
const setupCallbackEnd = texts.appIframe.indexOf('// Open the Lock Server /connect page', setupAcceptanceIndex);
const setupCallback = texts.appIframe.slice(setupAcceptanceIndex, setupCallbackEnd);
if (setupCallback.includes('postClientLog(') || setupStartFunction.includes('postClientLog(')) {
  throw new Error('Paykit setup URL and callback data must not enter client logs');
}

for (const [label, snippets] of Object.entries({
  app: ['window.location.assign', 'completeCreatorConnect'],
  appIframe: ['state.lastReceivedCode', 'feLockSessionToken: ${state.feLockSessionToken}', "'lock-auth-iframe-complete', { code }", 'xpub', 'account_xpub'],
  readme: ['redirect to the Lock-Server-hosted `/connect` shell', 'stores the Locks frontend session in `localStorage`', 'verifier dropdown has one option'],
  index: ['full-page redirect', 'Switch to iframe flow', 'id="paykit-recipient', 'id="paykit-asset', 'xpub', 'account_xpub'],
  iframe: ['full-page redirect', 'Switch to redirect flow', 'id="paykit-recipient', 'id="paykit-asset', 'xpub', 'account_xpub'],
  flows: ['Redirect flow', 'full-page redirect', 'localStorage'],
})) {
  for (const snippet of snippets) {
    if (texts[label].includes(snippet)) {
      throw new Error(`${label} contains forbidden auth snippet: ${snippet}`);
    }
  }
}

const stateGuardIndex = texts.creator.indexOf('if (state !== expectedState)');
const codeExchangeIndex = texts.creator.indexOf('locks.exchangeFrontendSessionCode(');
if (stateGuardIndex < 0 || codeExchangeIndex < 0 || stateGuardIndex > codeExchangeIndex) {
  throw new Error('creator must reject mismatched connect state before exchanging the one-time code');
}

const callbackExchangeIndex = texts.appIframe.indexOf('await exchangeCreatorConnectCode({');
const callbackExchangeEndIndex = texts.appIframe.indexOf('});', callbackExchangeIndex);
const callbackExchangeCall = texts.appIframe.slice(callbackExchangeIndex, callbackExchangeEndIndex);
for (const binding of ['state: receivedState', 'expectedState: state.pendingConnectState']) {
  if (!callbackExchangeCall.includes(binding)) {
    throw new Error(`iframe callback exchange must include ${binding}`);
  }
}
for (const guard of [
  'event.origin !== state.lockServerOrigin',
  'event.source !== state.lockAuthFrame?.contentWindow',
]) {
  const guardIndex = texts.appIframe.indexOf(guard);
  if (guardIndex < 0 || callbackExchangeIndex < 0 || guardIndex > callbackExchangeIndex) {
    throw new Error(`iframe callback must enforce ${guard} before exchanging the one-time code`);
  }
}

const {
  buildPaykitPaymentProofBundle,
  classifyPaymentLifecycle,
  createLatestRequestGate,
  decodeGuardedContentResponse,
  parsePreparedReaderBrowserStatus,
  selectCurrentPaykitPaymentRequest,
  workflowHandleMatches,
} = await import(pathToFileURL(files.readerFlow).href);
const paymentResource = 'pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/locks.app/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json';
const paymentBundleId = '000G40R40M30E209185GR38E1W';
const readerPublicKey = 'pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo';
assert.deepEqual(buildPaykitPaymentProofBundle({
  resource: paymentResource,
  readerPublicKey,
  criterionId: 'payment-criterion',
  bundleId: paymentBundleId,
}), {
  version: 1,
  bundle_id: paymentBundleId,
  pubky_lock_resource: paymentResource,
  reader_public_key: readerPublicKey,
  proofs: [{
    criterion_id: 'payment-criterion',
    verifier_type: 'paykit-payment',
    payload: {},
  }],
});
assert.equal(classifyPaymentLifecycle({ status: 'pending' }), 'retry');
assert.equal(classifyPaymentLifecycle({ status: 'in_progress' }), 'retry');
assert.equal(classifyPaymentLifecycle({ status: 'completed' }), 'completed');
assert.equal(classifyPaymentLifecycle({ status: 'failed' }), 'failed');
assert.equal(classifyPaymentLifecycle({ status: 'expired' }), 'failed');
assert.throws(() => classifyPaymentLifecycle({ status: 'unknown' }), /unknown lifecycle status/);
const workflowHandle = { incarnation: 7, resource: paymentResource, creator: creatorPubky, bundleId: paymentBundleId };
const currentWorkflow = { ...workflowHandle };
assert.equal(workflowHandleMatches(workflowHandle, currentWorkflow), true);
for (const staleWorkflow of [
  { ...currentWorkflow, incarnation: 8 },
  { ...currentWorkflow, resource: `${paymentResource}.changed` },
  { ...currentWorkflow, creator: readerPublicKey },
  { ...currentWorkflow, bundleId: `${paymentBundleId}0` },
]) {
  assert.equal(workflowHandleMatches(workflowHandle, staleWorkflow), false);
}
assert.equal(
  workflowHandleMatches(
    { incarnation: 7, resource: paymentResource },
    currentWorkflow,
  ),
  true,
);
const latestRequestGate = createLatestRequestGate();
const olderStatusRequest = latestRequestGate.begin(7);
const newerStatusRequest = latestRequestGate.begin(7);
assert.equal(latestRequestGate.isCurrent(olderStatusRequest, 7), false);
assert.equal(latestRequestGate.isCurrent(newerStatusRequest, 7), true);
assert.equal(latestRequestGate.isCurrent(newerStatusRequest, 8), false);
latestRequestGate.finish(newerStatusRequest);
assert.equal(latestRequestGate.isCurrent(newerStatusRequest, 7), false);
const invalidatedStatusRequest = latestRequestGate.begin(8);
latestRequestGate.invalidate();
assert.equal(latestRequestGate.isCurrent(invalidatedStatusRequest, 8), false);
const priorPaymentRequest = {
  state: 'request_received',
  payment_request_id: '12345678-1234-4123-8123-123456789abc',
};
const newerPaymentRequest = {
  state: 'request_received',
  payment_request_id: 'abcdef12-3456-4789-8123-123456789abc',
};
assert.equal(selectCurrentPaykitPaymentRequest({
  status: priorPaymentRequest,
  baselinePaymentRequestId: priorPaymentRequest.payment_request_id,
  currentPaymentRequest: null,
}), null);
assert.equal(selectCurrentPaykitPaymentRequest({
  status: priorPaymentRequest,
  baselinePaymentRequestId: priorPaymentRequest.payment_request_id,
  currentPaymentRequest: newerPaymentRequest,
}), newerPaymentRequest);
assert.equal(selectCurrentPaykitPaymentRequest({
  status: newerPaymentRequest,
  baselinePaymentRequestId: priorPaymentRequest.payment_request_id,
  currentPaymentRequest: null,
}), newerPaymentRequest);
assert.equal(selectCurrentPaykitPaymentRequest({
  status: newerPaymentRequest,
  baselinePaymentRequestId: priorPaymentRequest.payment_request_id,
  currentPaymentRequest: newerPaymentRequest,
}), newerPaymentRequest);
assert.deepEqual(parsePreparedReaderBrowserStatus({
  version: 1,
  prepared: true,
  reader_pubky: readerPublicKey,
}), {
  version: 1,
  prepared: true,
  reader_pubky: readerPublicKey,
});
assert.deepEqual(parsePreparedReaderBrowserStatus({ version: 1, prepared: false }), {
  version: 1,
  prepared: false,
});
for (const invalidStatus of [
  { version: 1, prepared: true },
  { version: 1, prepared: false, reader_pubky: readerPublicKey },
  { version: 2, prepared: false },
  { version: 1, prepared: true, reader_pubky: `${readerPublicKey}x` },
]) {
  assert.throws(() => parsePreparedReaderBrowserStatus(invalidStatus), /invalid prepared Paykit reader status/);
}
const guardedResponse = new Response(new TextEncoder().encode('payment unlocked'), {
  headers: { 'content-type': 'text/plain; charset=utf-8' },
});
const decodedGuarded = await decodeGuardedContentResponse(guardedResponse);
assert.equal(decodedGuarded.contentType, 'text/plain; charset=utf-8');
assert.equal(decodedGuarded.kind, 'text');
assert.equal(decodedGuarded.text, 'payment unlocked');
assert.equal(decodedGuarded.size, 16);
const decodedImage = await decodeGuardedContentResponse(new Response(Uint8Array.of(1, 2, 3), {
  headers: { 'content-type': 'image/png' },
}));
assert.equal(decodedImage.kind, 'image');
assert.equal(decodedImage.text, null);
assert.deepEqual([...decodedImage.bytes], [1, 2, 3]);

const {
  buildReaderHelperInput,
  parseReaderHelperSuccess,
  requireReaderEnvironment,
  runReaderOperation,
  signupReaderBestEffort,
} = await import(pathToFileURL(files.paykitReaderLib).href);
const {
  buildPreparedReaderBrowserStatus,
  readPreparedReaderStatus,
  validatePreparedReaderStatus,
  writePreparedReaderStatus,
} = await import(pathToFileURL(files.paykitReaderStatus).href);
const { main: preparePaykitReaderMain } = await import(pathToFileURL(files.preparePaykitReader).href);
const readerSecret = Uint8Array.from({ length: 32 }, (_, index) => index + 1);
const prepareInput = buildReaderHelperInput({ operation: 'prepare', readerSecret });
assert.deepEqual(Object.keys(prepareInput), ['version', 'operation', 'reader_secret']);
assert.equal(prepareInput.version, 1);
assert.equal(prepareInput.operation, 'prepare');
assert.equal(Buffer.from(prepareInput.reader_secret, 'base64url').length, 32);
assert.deepEqual(parseReaderHelperSuccess({
  operation: 'prepare',
  stdout: '{"version":1,"status":"prepared","reader_pubky":"pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo","receiver_path":"bitkit/wallet"}\n',
}), {
  version: 1,
  status: 'prepared',
  reader_pubky: readerPublicKey,
  receiver_path: 'bitkit/wallet',
});
const receivedOutput = {
  version: 1,
  status: 'received',
  payment_request_id: 'b7f9c2a1-6d43-4b0e-a8d4-0fe2c712ab33',
  address: 'bcrt1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqdku202',
  asset: 'btc',
  amount_sats: '50000',
  payment_command: "docker compose exec -T bitcoin sh -ec 'bitcoin-cli -conf=\"$BITCOIN_DATA/bitcoin.conf\" -regtest -rpcwallet=miner sendtoaddress \"bcrt1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqdku202\" \"0.00050000\"'",
  optional_mining_command: "docker compose exec -T bitcoin sh -ec 'bitcoin-cli -conf=\"$BITCOIN_DATA/bitcoin.conf\" -regtest -rpcwallet=miner generatetoaddress 6 \"$(bitcoin-cli -conf=\"$BITCOIN_DATA/bitcoin.conf\" -regtest -rpcwallet=miner getnewaddress)\"'",
};
const operatorReceivedOutput = {
  ...receivedOutput,
  asset: 'BTC',
  payment_command: `docker compose --file compose.paykit-local-demo.yaml exec -T bitcoin sh -ec 'bitcoin-cli -conf=/home/bitcoin/.bitcoin/bitcoin.conf -regtest -rpcwallet=miner sendtoaddress ${receivedOutput.address} 0.00050000'`,
  optional_mining_command: "docker compose --file compose.paykit-local-demo.yaml exec -T bitcoin sh -ec 'bitcoin-cli -conf=/home/bitcoin/.bitcoin/bitcoin.conf -regtest -rpcwallet=miner generatetoaddress 6 $(bitcoin-cli -conf=/home/bitcoin/.bitcoin/bitcoin.conf -regtest -rpcwallet=miner getnewaddress)'",
};
assert.deepEqual(parseReaderHelperSuccess({
  operation: 'receive',
  stdout: `${JSON.stringify(receivedOutput)}\n`,
}), operatorReceivedOutput);
for (const invalid of [
  `${JSON.stringify({ ...receivedOutput, extra: true })}\n`,
  `${JSON.stringify({ ...receivedOutput, asset: 'BTC' })}\n`,
  `${JSON.stringify({ ...receivedOutput, payment_command: 'echo unsafe' })}\n`,
  `${JSON.stringify({
    ...receivedOutput,
    payment_command: receivedOutput.payment_command.replace(receivedOutput.address, `${receivedOutput.address.slice(0, -1)}3`),
  })}\n`,
  `${JSON.stringify({
    ...receivedOutput,
    payment_command: receivedOutput.payment_command.replace('0.00050000', '0.00050001'),
  })}\n`,
  `${JSON.stringify({
    ...receivedOutput,
    payment_command: receivedOutput.payment_command.replace(receivedOutput.address, `${receivedOutput.address}\";echo unsafe`),
  })}\n`,
  `${JSON.stringify({ ...receivedOutput, amount_sats: '0' })}\n`,
]) {
  assert.throws(() => parseReaderHelperSuccess({ operation: 'receive', stdout: invalid }), /invalid reader helper output/);
}

const {
  buildCompanionHelperInput,
  companionResultCategory,
  collectPaykitInputs,
  parsePaykitInputLines,
  requirePaykitCreatorRole,
  resolveCompanionHelperPath,
  runCompanionHelper,
} = await import(pathToFileURL(files.authenticatePaykit).href);
const {
  Keypair,
  loadRoleKeypair,
  loadRoleSecret,
  secretFromRecoveryFile,
} = await import(pathToFileURL(files.pubkyLib).href);

const missingRoleFile = async () => {
  const error = new Error('missing role file');
  error.code = 'ENOENT';
  throw error;
};
for (const loadIdentity of [loadRoleKeypair, loadRoleSecret]) {
  await assert.rejects(
    loadIdentity('content-creator', { readFile: missingRoleFile }),
    /missing local identity for content-creator; run `npm --prefix examples\/js-sdk run create-user -- --role content-creator` before authentication/,
  );
}

const authUrl = 'pubkyauth://signin?secret=test-auth-secret';
const accountXpub = 'tpub-test-account-xpub';
assert.equal(resolveCompanionHelperPath({
  env: {},
  nativeHelperPath: '/native/helper',
  composeHelperPath: '/compose/helper',
  nativeHelperAvailable: () => false,
}), '/compose/helper');
assert.equal(resolveCompanionHelperPath({
  env: {},
  nativeHelperPath: '/native/helper',
  composeHelperPath: '/compose/helper',
  nativeHelperAvailable: () => true,
}), '/native/helper');
assert.equal(resolveCompanionHelperPath({
  env: { PAYKIT_COMPANION_AUTH_BIN: '/override/helper' },
  nativeHelperAvailable: () => false,
}), '/override/helper');
const parsedLines = parsePaykitInputLines(`${authUrl}\n${accountXpub}\n7\n`);
assert.deepEqual(parsedLines, { authUrl, accountXpub, accountIndex: 7 });
for (const invalid of [
  `${authUrl}\n${accountXpub}`,
  `${authUrl}\n${accountXpub}\n7\nextra`,
  `${authUrl}\n\n7`,
]) {
  assert.throws(() => parsePaykitInputLines(invalid), /three ordered lines/);
}

const prompts = [];
const answers = [authUrl, accountXpub, '7'];
assert.deepEqual(
  await collectPaykitInputs({
    isTTY: true,
    question: async (prompt) => {
      prompts.push(prompt);
      return answers.shift();
    },
  }),
  parsedLines,
);
const promptText = prompts.join('\n');
for (const sensitive of [authUrl, accountXpub, 'test-auth-secret']) {
  assert.equal(promptText.includes(sensitive), false);
}

const expectedSecret = Uint8Array.from({ length: 32 }, (_, index) => index + 1);
const recoveryPassphrase = 'test-only-recovery-passphrase';
const recoveryKeypair = Keypair.fromSecret(expectedSecret);
const recoveryFile = recoveryKeypair.createRecoveryFile(recoveryPassphrase);
recoveryKeypair.free();
assert.deepEqual(secretFromRecoveryFile(recoveryFile, recoveryPassphrase), expectedSecret);

const helperInput = buildCompanionHelperInput({ ...parsedLines, creatorSecret: expectedSecret });
assert.deepEqual(Object.keys(helperInput), [
  'version', 'auth_url', 'creator_secret', 'account_xpub', 'account_index',
]);
assert.equal(helperInput.version, 1);
assert.equal(Buffer.from(helperInput.creator_secret, 'base64url').length, 32);
expectedSecret[0] = 255;
assert.equal(Buffer.from(helperInput.creator_secret, 'base64url')[0], 1);
expectedSecret[0] = 1;
assert.equal(requirePaykitCreatorRole('content-creator'), 'content-creator');
for (const role of ['content-viewer', 'lock-server', undefined]) {
  assert.throws(() => requirePaykitCreatorRole(role), /requires --role content-creator/);
}
assert.deepEqual(companionResultCategory({ status: 'approved' }), {
  exitCode: 0,
  stream: 'stdout',
  message: 'Paykit companion authentication approved.',
});
assert.deepEqual(companionResultCategory({ status: 'failed' }), {
  exitCode: 1,
  stream: 'stderr',
  message: 'Paykit companion authentication failed.',
});
assert.deepEqual(companionResultCategory({ status: 'timeout' }), {
  exitCode: 1,
  stream: 'stderr',
  message: 'Paykit companion authentication timed out.',
});
assert.throws(
  () => parseReaderHelperSuccess({
    operation: 'prepare',
    stdout: `${JSON.stringify({
      version: 1,
      status: 'prepared',
      reader_pubky: readerPublicKey,
      receiver_path: 'bitkit/wallet',
      extra: true,
    })}\n`,
  }),
  /invalid reader helper output/,
);

const registrationCleanup = [];
await signupReaderBestEffort({
  readConfig: async () => ({}),
  normalizeConfig: (config) => config,
  loadKeypair: async (role) => {
    assert.equal(role, 'content-viewer');
    return { free: () => registrationCleanup.push('keypair') };
  },
  pubkyFactory: () => ({ signer: () => ({ free: () => registrationCleanup.push('signer') }) }),
  signup: async () => ({ free: () => registrationCleanup.push('session') }),
  getHomeserverPublicKey: () => 'homeserver',
});
assert.deepEqual(registrationCleanup, ['session', 'signer', 'keypair']);

const failedRegistrationCleanup = [];
await assert.rejects(
  signupReaderBestEffort({
    readConfig: async () => ({}),
    normalizeConfig: (config) => config,
    loadKeypair: async () => ({ free: () => failedRegistrationCleanup.push('keypair') }),
    pubkyFactory: () => ({ signer: () => ({ free: () => failedRegistrationCleanup.push('signer') }) }),
    signup: async () => { throw new Error('registration failed'); },
    getHomeserverPublicKey: () => 'homeserver',
  }),
  /registration failed/,
);
assert.deepEqual(failedRegistrationCleanup, ['signer', 'keypair']);

const helperDir = mkdtempSync(join(tmpdir(), 'locks-paykit-helper-'));
try {
  const preparedStatus = {
    version: 1,
    status: 'prepared',
    reader_pubky: readerPublicKey,
    receiver_path: 'bitkit/wallet',
  };
  assert.deepEqual(validatePreparedReaderStatus(preparedStatus), preparedStatus);
  const preparedStatusPath = join(helperDir, 'prepared.v1.json');
  await writePreparedReaderStatus(preparedStatus, preparedStatusPath);
  assert.deepEqual(await readPreparedReaderStatus(preparedStatusPath), preparedStatus);
  assert.equal(statSync(preparedStatusPath).mode & 0o777, 0o600);
  assert.deepEqual(
    buildPreparedReaderBrowserStatus(preparedStatus, { role: 'content-viewer', pubky: readerPublicKey }),
    { version: 1, prepared: true, reader_pubky: readerPublicKey },
  );
  assert.deepEqual(
    buildPreparedReaderBrowserStatus(preparedStatus, { role: 'content-viewer', pubky: creatorPubky }),
    { version: 1, prepared: false },
  );
  chmodSync(preparedStatusPath, 0o644);
  assert.equal(await readPreparedReaderStatus(preparedStatusPath), null);

  const prepareOrder = [];
  assert.equal(await preparePaykitReaderMain({
    clearStatus: async () => prepareOrder.push('clear'),
    runOperation: async () => ({ status: 'success', value: preparedStatus }),
    writeStatus: async (value) => {
      assert.deepEqual(value, preparedStatus);
      prepareOrder.push('write');
    },
    printSuccess: () => prepareOrder.push('print'),
  }), 0);
  assert.deepEqual(prepareOrder, ['clear', 'write', 'print']);
  let wroteFailedPrepare = false;
  let clearedFailedPrepare = false;
  assert.equal(await preparePaykitReaderMain({
    clearStatus: async () => { clearedFailedPrepare = true; },
    runOperation: async () => ({ status: 'failed', error: 'protocol_failed' }),
    writeStatus: async () => { wroteFailedPrepare = true; },
    printError: () => {},
  }), 1);
  assert.equal(clearedFailedPrepare, true);
  assert.equal(wroteFailedPrepare, false);

  const readerEnv = {
    PATH: process.env.PATH,
    PAYKIT_READER_STATE_PATH: join(helperDir, '.local', 'paykit-reader', 'state.v1'),
    PAYKIT_READER_PUBKY_TESTNET_HOST: 'pubky-testnet',
    PAYKIT_READER_RECEIVER_PATH: 'bitkit/wallet',
    PAYKIT_READER_SERVER_PUBKY: readerPublicKey,
    PAYKIT_READER_SERVER_PATH: 'bitkit/server',
  };
  assert.equal(requireReaderEnvironment(readerEnv), undefined);
  assert.throws(
    () => requireReaderEnvironment({ ...readerEnv, PAYKIT_READER_STATE_PATH: '/tmp/state.v1' }),
    /state path/,
  );
  const preparedHelper = join(helperDir, 'prepared-reader-helper');
  writeFileSync(preparedHelper, `#!/usr/bin/env node
let body = '';
for await (const chunk of process.stdin) body += chunk;
const value = JSON.parse(body);
const keys = ['version','operation','reader_secret'];
if (process.argv.length !== 2 || JSON.stringify(Object.keys(value)) !== JSON.stringify(keys)) process.exit(21);
if (value.version !== 1 || value.operation !== 'prepare' || Buffer.from(value.reader_secret, 'base64url').length !== 32) process.exit(22);
process.stdout.write('{"version":1,"status":"prepared","reader_pubky":"${readerPublicKey}","receiver_path":"bitkit/wallet"}\\n');
`);
  chmodSync(preparedHelper, 0o700);
  const readerSecretForRun = Uint8Array.from({ length: 32 }, (_, index) => index + 1);
  let readerRegistered = false;
  assert.deepEqual(await runReaderOperation({
    operation: 'prepare',
    helperPath: preparedHelper,
    env: readerEnv,
    readerSecret: readerSecretForRun,
    ensureRegistered: async () => { readerRegistered = true; },
  }), {
    status: 'success',
    value: {
      version: 1,
      status: 'prepared',
      reader_pubky: readerPublicKey,
      receiver_path: 'bitkit/wallet',
    },
  });
  assert.equal(readerRegistered, true);
  assert.deepEqual(readerSecretForRun, new Uint8Array(32));

  const readerFailureHelper = join(helperDir, 'failed-reader-helper');
  writeFileSync(readerFailureHelper, `#!/usr/bin/env node
for await (const _chunk of process.stdin) {}
process.stderr.write('{"version":1,"error":"invalid_state"}\\n');
process.exit(1);
`);
  chmodSync(readerFailureHelper, 0o700);
  assert.deepEqual(await runReaderOperation({
    operation: 'receive',
    helperPath: readerFailureHelper,
    env: readerEnv,
    readerSecret: Uint8Array.from({ length: 32 }, (_, index) => index + 1),
  }), { status: 'failed', error: 'invalid_state' });

  assert.deepEqual(
    await runCompanionHelper({ helperPath: 'invalid\0helper', input: helperInput }),
    { status: 'failed' },
  );
  assert.deepEqual(
    await runCompanionHelper({ helperPath: join(helperDir, 'missing-helper'), input: helperInput }),
    { status: 'failed' },
  );

  const observedSignals = [];
  class ErrorDuringTerminationChild extends EventEmitter {
    constructor() {
      super();
      this.stdin = new PassThrough();
      this.stdout = new PassThrough();
      this.stderr = new PassThrough();
      queueMicrotask(() => this.emit('spawn'));
    }

    kill(signal) {
      observedSignals.push(signal);
      if (signal === 'SIGTERM') queueMicrotask(() => this.emit('error', new Error('test kill error')));
      if (signal === 'SIGKILL') queueMicrotask(() => this.emit('close', null, 'SIGKILL'));
      return false;
    }

    unref() {}
  }
  assert.deepEqual(
    await runCompanionHelper({
      helperPath: 'injected-helper',
      input: helperInput,
      timeoutMs: 10,
      killGraceMs: 10,
      spawnProcess: () => new ErrorDuringTerminationChild(),
    }),
    { status: 'timeout' },
  );
  assert.deepEqual(observedSignals, ['SIGTERM', 'SIGKILL']);

  const approvedHelper = join(helperDir, 'approved-helper');
  writeFileSync(approvedHelper, `#!/usr/bin/env node
let body = '';
for await (const chunk of process.stdin) body += chunk;
const value = JSON.parse(body);
const keys = ['version','auth_url','creator_secret','account_xpub','account_index'];
if (process.argv.length !== 2 || JSON.stringify(Object.keys(value)) !== JSON.stringify(keys)) process.exit(21);
if (value.version !== 1 || value.auth_url !== ${JSON.stringify(authUrl)} || value.account_xpub !== ${JSON.stringify(accountXpub)} || value.account_index !== 7) process.exit(22);
if (Buffer.from(value.creator_secret, 'base64url').length !== 32) process.exit(23);
process.stdout.write('{"version":1,"status":"approved"}\\n');
`);
  chmodSync(approvedHelper, 0o700);
  assert.deepEqual(
    await runCompanionHelper({ helperPath: approvedHelper, input: helperInput }),
    { status: 'approved' },
  );

  const failedHelper = join(helperDir, 'failed-helper');
  writeFileSync(failedHelper, `#!/usr/bin/env node
let body = '';
for await (const chunk of process.stdin) body += chunk;
const value = JSON.parse(body);
process.stderr.write(value.auth_url + value.account_xpub + value.creator_secret);
process.exit(1);
`);
  chmodSync(failedHelper, 0o700);
  const failed = await runCompanionHelper({ helperPath: failedHelper, input: helperInput });
  assert.deepEqual(failed, { status: 'failed' });
  for (const sensitive of [authUrl, accountXpub, helperInput.creator_secret]) {
    assert.equal(JSON.stringify(failed).includes(sensitive), false);
  }

  const hangingHelper = join(helperDir, 'hanging-helper');
  writeFileSync(hangingHelper, `#!/usr/bin/env node
process.on('SIGTERM', () => {});
setInterval(() => {}, 1000);
`);
  chmodSync(hangingHelper, 0o700);
  assert.deepEqual(
    await runCompanionHelper({
      helperPath: hangingHelper,
      input: helperInput,
      timeoutMs: 25,
      killGraceMs: 25,
    }),
    { status: 'timeout' },
  );

  const floodingHelper = join(helperDir, 'flooding-helper');
  writeFileSync(floodingHelper, `#!/usr/bin/env node
process.on('SIGTERM', () => {});
process.stdout.write('x'.repeat(8192));
setInterval(() => {}, 1000);
`);
  chmodSync(floodingHelper, 0o700);
  assert.deepEqual(
    await runCompanionHelper({
      helperPath: floodingHelper,
      input: helperInput,
      timeoutMs: 1000,
      killGraceMs: 25,
    }),
    { status: 'failed' },
  );
} finally {
  rmSync(helperDir, { recursive: true, force: true });
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
