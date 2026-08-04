#!/usr/bin/env node
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { mkdtemp, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PassThrough } from 'node:stream';

import { runBoundedHelper } from './authenticate-paykit.mjs';
import { parsePaykitReaderBrowserStatus } from '../reader-flow.js';
import { runRegistrationStep } from './lib/paykit-reader-helper.mjs';
import {
  acquirePaykitReaderOwnership,
  runPaykitReaderWorker,
  supervisePaykitReaderWorker,
} from './lib/paykit-reader-worker.mjs';
import {
  buildPaykitReaderBrowserStatus,
  readPaykitReaderWorkerStatus,
  validatePaykitReaderWorkerStatus,
  writePaykitReaderWorkerStatus,
} from './lib/paykit-reader-status.mjs';

const readerPubky = `pubky${'y'.repeat(52)}`;
const received = {
  version: 1,
  status: 'received',
  payment_request_id: '12345678-1234-4123-8123-123456789abc',
  address: 'bcrt1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqdku202',
  asset: 'BTC',
  amount_sats: '50000',
  payment_command: "docker compose --file ./compose.paykit-local-demo.yaml exec -T bitcoin sh -ec 'bitcoin-cli -conf=\"$BITCOIN_DATA/bitcoin.conf\" -regtest -rpcwallet=miner sendtoaddress \"bcrt1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqdku202\" \"0.00050000\"'",
  optional_mining_command: "docker compose --file ./compose.paykit-local-demo.yaml exec -T bitcoin sh -ec 'bitcoin-cli -conf=\"$BITCOIN_DATA/bitcoin.conf\" -regtest -rpcwallet=miner generatetoaddress 6 \"$(bitcoin-cli -conf=\"$BITCOIN_DATA/bitcoin.conf\" -regtest -rpcwallet=miner getnewaddress)\"'",
};

const operations = [];
const statuses = [];
let concurrent = 0;
let maxConcurrent = 0;
let ownershipAcquired = 0;
let ownershipReleased = 0;
const ownershipStates = [];
const lifecycleEvents = [];
let workerSignal;
const controller = new AbortController();

await runPaykitReaderWorker({
  signal: controller.signal,
  runOperation: async ({ operation, signal }) => {
    if (!workerSignal) workerSignal = signal;
    assert.equal(signal, workerSignal);
    assert.equal(signal.aborted, false);
    concurrent += 1;
    maxConcurrent = Math.max(maxConcurrent, concurrent);
    operations.push(operation);
    concurrent -= 1;
    if (operation === 'prepare') {
      return { status: 'success', value: { version: 1, status: 'prepared', reader_pubky: readerPubky, receiver_path: 'bitkit/wallet' } };
    }
    if (operations.filter((value) => value === 'receive').length === 1) {
      return { status: 'failed', error: 'receive_timeout' };
    }
    return { status: 'success', value: received };
  },
  writePreparedStatus: async () => {},
  writeWorkerStatus: async (value) => {
    statuses.push(value);
    lifecycleEvents.push(`write:${value.state}`);
    if (value.state === 'request_received') controller.abort();
  },
  wait: async () => {},
  acquireOwnership: async () => {
    ownershipAcquired += 1;
    return { release: async () => { ownershipReleased += 1; } };
  },
  onOwnershipChange: (owned) => {
    ownershipStates.push(owned);
    lifecycleEvents.push(`owned:${owned}`);
  },
});

assert.deepEqual(operations, ['prepare', 'receive', 'receive']);
assert.equal(maxConcurrent, 1);
assert.equal(ownershipAcquired, 1);
assert.equal(ownershipReleased, 1);
assert.deepEqual(ownershipStates, [true, false]);
assert.equal(workerSignal.aborted, true);
assert.ok(lifecycleEvents.indexOf('write:starting') < lifecycleEvents.indexOf('owned:true'));
assert.deepEqual(statuses.map(({ state }) => state), [
  'starting',
  'waiting',
  'retrying',
  'request_received',
]);
assert.deepEqual(statuses.at(-1), {
  version: 1,
  state: 'request_received',
  reader_pubky: readerPubky,
  payment_request_id: received.payment_request_id,
  address: received.address,
  asset: 'BTC',
  amount_sats: '50000',
  payment_command: received.payment_command,
  optional_mining_command: received.optional_mining_command,
});
assert.deepEqual(parsePaykitReaderBrowserStatus(statuses.at(-1)), statuses.at(-1));
assert.deepEqual(parsePaykitReaderBrowserStatus({
  version: 1,
  state: 'retrying',
  reader_pubky: readerPubky,
  error: 'receive_timeout',
}), {
  version: 1,
  state: 'retrying',
  reader_pubky: readerPubky,
  error: 'receive_timeout',
});

const newerReceived = {
  ...received,
  payment_request_id: 'abcdef12-3456-4789-8123-123456789abc',
};
const continuousOperations = [];
const continuousStatuses = [];
let continuousWaits = 0;
const continuousController = new AbortController();
const continuousTimeout = setTimeout(() => continuousController.abort(), 100);
await runPaykitReaderWorker({
  signal: continuousController.signal,
  runOperation: async ({ operation }) => {
    continuousOperations.push(operation);
    if (operation === 'prepare') {
      return { status: 'success', value: { version: 1, status: 'prepared', reader_pubky: readerPubky, receiver_path: 'bitkit/wallet' } };
    }
    const receiveCount = continuousOperations.filter((value) => value === 'receive').length;
    if (receiveCount === 1) return { status: 'success', value: received };
    if (receiveCount === 2) return { status: 'success', value: received };
    return { status: 'success', value: newerReceived };
  },
  writePreparedStatus: async () => {},
  writeWorkerStatus: async (value) => {
    continuousStatuses.push(value);
    if (value.payment_request_id === newerReceived.payment_request_id) {
      continuousController.abort();
    }
  },
  wait: async () => { continuousWaits += 1; },
  acquireOwnership: async () => ({ release: async () => {} }),
  onOwnershipChange: () => {},
});
clearTimeout(continuousTimeout);
assert.deepEqual(continuousOperations, ['prepare', 'receive', 'receive', 'receive']);
assert.deepEqual(continuousStatuses.map(({ state }) => state), [
  'starting',
  'waiting',
  'request_received',
  'request_received',
]);
assert.equal(continuousWaits, 1);
assert.equal(continuousStatuses.at(-1).payment_request_id, newerReceived.payment_request_id);
assert.throws(
  () => parsePaykitReaderBrowserStatus({ ...statuses.at(-1), raw_request: 'private' }),
  /invalid Paykit reader status/,
);

const helperSignals = [];
class AbortableHelper extends EventEmitter {
  constructor() {
    super();
    this.stdin = new PassThrough();
    this.stdout = new PassThrough();
    this.stderr = new PassThrough();
    queueMicrotask(() => this.emit('spawn'));
  }

  kill(signal) {
    helperSignals.push(signal);
    queueMicrotask(() => this.emit('close', null, signal));
    return true;
  }
}
const helperController = new AbortController();
const helperResult = runBoundedHelper({
  helperPath: '/test/helper',
  input: { version: 1 },
  classifyClose: () => ({ status: 'success' }),
  timeoutMs: 1_000,
  signal: helperController.signal,
  spawnProcess: () => new AbortableHelper(),
});
helperController.abort();
assert.deepEqual(await helperResult, { status: 'failed' });
assert.deepEqual(helperSignals, ['SIGTERM']);

const preAborted = new AbortController();
preAborted.abort();
let preAbortedSpawned = false;
assert.deepEqual(await runBoundedHelper({
  helperPath: '/test/helper',
  input: { version: 1 },
  classifyClose: () => ({ status: 'success' }),
  timeoutMs: 1_000,
  signal: preAborted.signal,
  spawnProcess: () => {
    preAbortedSpawned = true;
    return new AbortableHelper();
  },
}), { status: 'failed' });
assert.equal(preAbortedSpawned, false);

const statusRoot = await mkdtemp(join(tmpdir(), 'paykit-reader-worker-'));
try {
  const ownershipPath = join(statusRoot, 'owner.lock');
  const firstOwner = await acquirePaykitReaderOwnership(ownershipPath);
  assert.equal((await stat(ownershipPath)).mode & 0o777, 0o600);
  await assert.rejects(
    acquirePaykitReaderOwnership(ownershipPath),
    /already owned/,
  );
  await firstOwner.release();
  const replacementOwner = await acquirePaykitReaderOwnership(ownershipPath);
  await replacementOwner.release();
  await writeFile(ownershipPath, 'stale', { mode: 0o600 });
  const recoveredOwner = await acquirePaykitReaderOwnership(ownershipPath);
  await recoveredOwner.release();

  const statusPath = join(statusRoot, 'worker.v1.json');
  await writePaykitReaderWorkerStatus(statuses.at(-1), statusPath);
  assert.equal((await stat(statusPath)).mode & 0o777, 0o600);
  assert.deepEqual(await readPaykitReaderWorkerStatus(statusPath), statuses.at(-1));
  assert.throws(
    () => validatePaykitReaderWorkerStatus({ ...statuses.at(-1), raw_request: 'private' }),
    /invalid Paykit reader worker status/,
  );
  assert.deepEqual(
    buildPaykitReaderBrowserStatus(
      statuses.at(-1),
      { role: 'content-viewer', pubky: readerPubky },
      { currentOwner: true },
    ),
    statuses.at(-1),
  );
  assert.deepEqual(
    buildPaykitReaderBrowserStatus(
      statuses.at(-1),
      { role: 'content-viewer', pubky: `pubky${'b'.repeat(52)}` },
      { currentOwner: true },
    ),
    { version: 1, state: 'failed', error: 'identity_mismatch' },
  );
  assert.deepEqual(
    buildPaykitReaderBrowserStatus(
      statuses.at(-1),
      { role: 'content-viewer', pubky: readerPubky },
      { currentOwner: false },
    ),
    { version: 1, state: 'starting' },
  );
} finally {
  await rm(statusRoot, { recursive: true, force: true });
}

const acquisitionAbortController = new AbortController();
let finishAcquisition;
let acquisitionAbortReleased = 0;
let operationStartedAfterAcquisitionAbort = false;
const acquisitionAbortWorker = runPaykitReaderWorker({
  signal: acquisitionAbortController.signal,
  acquireOwnership: () => new Promise((resolveOwnership) => {
    finishAcquisition = () => resolveOwnership({
      release: async () => { acquisitionAbortReleased += 1; },
    });
  }),
  runOperation: async () => {
    operationStartedAfterAcquisitionAbort = true;
    return { status: 'failed', error: 'worker_failed' };
  },
  writeWorkerStatus: async () => {},
});
acquisitionAbortController.abort();
finishAcquisition();
assert.deepEqual(await acquisitionAbortWorker, { status: 'stopped' });
assert.equal(operationStartedAfterAcquisitionAbort, false);
assert.equal(acquisitionAbortReleased, 1);

const startingAbortController = new AbortController();
let operationStartedAfterStartingAbort = false;
assert.deepEqual(await runPaykitReaderWorker({
  signal: startingAbortController.signal,
  acquireOwnership: async () => ({ release: async () => {} }),
  runOperation: async () => {
    operationStartedAfterStartingAbort = true;
    return { status: 'failed', error: 'worker_failed' };
  },
  writeWorkerStatus: async ({ state }) => {
    if (state === 'starting') startingAbortController.abort();
  },
}), { status: 'stopped' });
assert.equal(operationStartedAfterStartingAbort, false);

const prepareAbortController = new AbortController();
const prepareAbortWrites = [];
let preparedCheckpointAfterAbort = false;
assert.deepEqual(await runPaykitReaderWorker({
  signal: prepareAbortController.signal,
  acquireOwnership: async () => ({ release: async () => {} }),
  runOperation: async () => {
    prepareAbortController.abort();
    return {
      status: 'success',
      value: {
        version: 1,
        status: 'prepared',
        reader_pubky: readerPubky,
        receiver_path: 'bitkit/wallet',
      },
    };
  },
  writePreparedStatus: async () => { preparedCheckpointAfterAbort = true; },
  writeWorkerStatus: async ({ state }) => { prepareAbortWrites.push(state); },
}), { status: 'stopped' });
assert.equal(preparedCheckpointAfterAbort, false);
assert.deepEqual(prepareAbortWrites, ['starting']);

let resolveOwnershipLoss;
let ownershipLossReleased = 0;
const ownershipLossWrites = [];
const ownershipLossStates = [];
await assert.rejects(runPaykitReaderWorker({
  signal: new AbortController().signal,
  acquireOwnership: async () => ({
    lost: new Promise((resolveLoss) => { resolveOwnershipLoss = resolveLoss; }),
    release: async () => { ownershipLossReleased += 1; },
  }),
  runOperation: async () => ({
    status: 'success',
    value: {
      version: 1,
      status: 'prepared',
      reader_pubky: readerPubky,
      receiver_path: 'bitkit/wallet',
    },
  }),
  writePreparedStatus: async () => {
    resolveOwnershipLoss();
    await new Promise((resolve) => setImmediate(resolve));
  },
  writeWorkerStatus: async ({ state }) => { ownershipLossWrites.push(state); },
  onOwnershipChange: (owned) => ownershipLossStates.push(owned),
}), /ownership was lost/);
assert.deepEqual(ownershipLossWrites, ['starting']);
assert.deepEqual(ownershipLossStates, [true, false]);
assert.equal(ownershipLossReleased, 1);

let rejectedWorkerReleased = 0;
const rejectedWorker = runPaykitReaderWorker({
  signal: new AbortController().signal,
  writeWorkerStatus: async () => { throw new Error('status write failed'); },
  acquireOwnership: async () => ({ release: async () => { rejectedWorkerReleased += 1; } }),
});
let supervisedError;
assert.deepEqual(await supervisePaykitReaderWorker(rejectedWorker, {
  onTerminalFailure: async (error) => { supervisedError = error; },
}), { status: 'failed', error: 'worker_failed' });
assert.equal(supervisedError, 'worker_failed');
assert.equal(rejectedWorkerReleased, 1);

const registrationController = new AbortController();
let registrationCancelled = false;
let registrationCleanupComplete = false;
const registration = runRegistrationStep({
  signal: registrationController.signal,
  timeoutMs: 60_000,
  ensureRegistered: ({ signal }) => new Promise((resolveRegistration) => {
    const finishCleanup = () => {
      registrationCancelled = true;
      setTimeout(() => {
        registrationCleanupComplete = true;
        resolveRegistration({ status: 'failed' });
      }, 5);
    };
    if (signal.aborted) finishCleanup();
    else signal.addEventListener('abort', finishCleanup, { once: true });
  }),
});
registrationController.abort();
assert.deepEqual(await registration, { status: 'failed' });
assert.equal(registrationCancelled, true);
assert.equal(registrationCleanupComplete, true);

let registrationTimedOutSignal = false;
let timedRegistrationCleanupComplete = false;
const timedRegistration = runRegistrationStep({
  signal: new AbortController().signal,
  timeoutMs: 5,
  ensureRegistered: ({ signal }) => new Promise((resolveRegistration) => {
    signal.addEventListener('abort', () => {
      registrationTimedOutSignal = true;
      setTimeout(() => {
        timedRegistrationCleanupComplete = true;
        resolveRegistration({ status: 'failed' });
      }, 5);
    }, { once: true });
  }),
});
assert.deepEqual(await timedRegistration, { status: 'timeout' });
assert.equal(registrationTimedOutSignal, true);
assert.equal(timedRegistrationCleanupComplete, true);

console.log('Paykit reader worker check passed');