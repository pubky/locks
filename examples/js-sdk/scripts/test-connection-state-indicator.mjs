#!/usr/bin/env node
import assert from 'node:assert/strict';

import {
  connectionStateFromLookupResponse,
  connectionStateIndicator,
  createPaykitConnectionPoller,
  createPaykitConnectionObserverSlot,
  lookupPaykitConnectionState,
} from '../reader-flow.js';

assert.deepEqual(connectionStateIndicator(null), {
  label: 'Not reported yet',
  className: 'muted',
});
assert.deepEqual(connectionStateIndicator('none'), {
  label: 'None — handshake has not started',
  className: 'muted',
});
assert.deepEqual(connectionStateIndicator('handshake'), {
  label: 'Handshake in progress',
  className: 'warning',
});
assert.deepEqual(connectionStateIndicator('connected'), {
  label: 'Connected — Paykit Server link is usable',
  className: 'ok',
});
assert.deepEqual(connectionStateIndicator('recovery_required'), {
  label: 'Recovery required — runtime must relink; do not resubmit proof',
  className: 'warning',
});
assert.deepEqual(connectionStateIndicator('blocked'), {
  label: 'Blocked — operator action required',
  className: 'error',
});
assert.throws(
  () => connectionStateIndicator('unknown'),
  /unknown Noise connection state/,
);

assert.equal(connectionStateFromLookupResponse({ state: 'handshake' }), 'handshake');
assert.equal(connectionStateFromLookupResponse(new Map([['state', 'connected']])), 'connected');
assert.throws(
  () => connectionStateFromLookupResponse({}),
  /missing Noise connection state/,
);
assert.throws(
  () => connectionStateFromLookupResponse({ state: 'unknown' }),
  /unknown Noise connection state/,
);

const calls = [];
const response = { state: 'connected' };
const viewer = {
  async lookupPaykitConnectionState(handle) {
    calls.push([handle.creator, handle.bundleId]);
    return response;
  },
};
const result = await lookupPaykitConnectionState({
  viewer,
  handle: { creator: 'pubkycreator', bundleId: 'bundle-1' },
});
assert.equal(result, response);
assert.deepEqual(calls, [['pubkycreator', 'bundle-1']]);

let resolveSlowLookup;
const observedStates = [];
const slowPoller = createPaykitConnectionPoller({
  lookup: () => new Promise((resolve) => { resolveSlowLookup = resolve; }),
  onState: (state) => observedStates.push(state),
  maxAttempts: 2,
});
assert.equal(slowPoller.poll(), true);
assert.equal(slowPoller.poll(), false, 'must not overlap connection lookups');
assert.deepEqual(observedStates, [], 'poll() must not await or block lifecycle work');
await Promise.resolve();
resolveSlowLookup({ state: 'handshake' });
await new Promise((resolve) => setTimeout(resolve, 0));
assert.deepEqual(observedStates, ['handshake']);
assert.equal(slowPoller.poll(), true);

let blockedCalls = 0;
const blockedPoller = createPaykitConnectionPoller({
  lookup: async () => {
    blockedCalls += 1;
    return { state: 'blocked' };
  },
});
assert.equal(blockedPoller.poll(), true);
await new Promise((resolve) => setTimeout(resolve, 0));
assert.equal(blockedPoller.poll(), false);
assert.equal(blockedCalls, 1, 'blocked must stop connection retries');

let lifecycleContinues = 0;
let connectionErrors = 0;
const failingPoller = createPaykitConnectionPoller({
  lookup: async () => { throw new Error('connection timeout'); },
  onError: () => { connectionErrors += 1; },
  maxAttempts: 1,
  onExhausted: () => { lifecycleContinues += 1; },
});
assert.equal(failingPoller.poll(), true);
lifecycleContinues += 1; // Simulate authoritative lifecycle work proceeding immediately.
await new Promise((resolve) => setTimeout(resolve, 0));
assert.equal(connectionErrors, 1);
assert.equal(lifecycleContinues, 2, 'connection failure and exhaustion stay non-fatal');
assert.equal(failingPoller.poll(), false, 'bounded poller stops after max attempts');

let autonomousCalls = 0;
let autonomousExhausted = 0;
const autonomousPoller = createPaykitConnectionPoller({
  lookup: async () => {
    autonomousCalls += 1;
    return { state: 'recovery_required' };
  },
  maxAttempts: 3,
  onExhausted: () => { autonomousExhausted += 1; },
});
assert.equal(autonomousPoller.start(1), true);
await new Promise((resolve) => setTimeout(resolve, 20));
assert.equal(autonomousCalls, 3, 'connection observation runs without lifecycle loop progress');
assert.equal(autonomousExhausted, 1);
assert.equal(autonomousPoller.start(1), false, 'exhausted observer cannot restart');

let releaseOldObserver;
let oldStops = 0;
let oldStarts = 0;
let newStops = 0;
let newStarts = 0;
const oldObserver = {
  start() { oldStarts += 1; },
  stop() { oldStops += 1; },
  whenIdle() { return new Promise((resolve) => { releaseOldObserver = resolve; }); },
};
const newObserver = {
  start() { newStarts += 1; },
  stop() { newStops += 1; },
  whenIdle() { return Promise.resolve(); },
};
const observerSlot = createPaykitConnectionObserverSlot();
observerSlot.start(oldObserver, 1);
await Promise.resolve();
assert.equal(oldStarts, 1);
observerSlot.stop();
observerSlot.start(newObserver, 1);
await Promise.resolve();
assert.ok(oldStops >= 1, 'workflow invalidation stops old observer synchronously');
assert.equal(newStarts, 0, 'replacement waits for old in-flight lookup to settle');
releaseOldObserver();
await new Promise((resolve) => setTimeout(resolve, 0));
assert.equal(newStarts, 1, 'replacement starts after stale lookup settles');
observerSlot.release(oldObserver);
observerSlot.stop();
assert.equal(newStops, 1, 'stale release cannot detach current observer');

console.log('connection state indicator tests passed');
