#!/usr/bin/env node
import assert from 'node:assert/strict';

import {
  connectionStateFromSubmitResponse,
  connectionStateIndicator,
  resubmitProofBundle,
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
  label: 'Connected — handshake completed',
  className: 'ok',
});
assert.throws(
  () => connectionStateIndicator('unknown'),
  /unknown Noise connection state/,
);

assert.equal(connectionStateFromSubmitResponse({ connection_state: 'handshake' }), 'handshake');
assert.equal(connectionStateFromSubmitResponse(new Map([['connection_state', 'connected']])), 'connected');
assert.throws(
  () => connectionStateFromSubmitResponse({}),
  /missing Noise connection state/,
);
assert.throws(
  () => connectionStateFromSubmitResponse({ connection_state: 'unknown' }),
  /unknown Noise connection state/,
);

const submittedProofBundle = { bundle_id: 'bundle-1' };
const calls = [];
const response = { connection_state: 'connected' };
const viewer = {
  async submitProofBundle(bundle) {
    calls.push(bundle);
    return response;
  },
};
assert.equal(await resubmitProofBundle({ viewer, submittedProofBundle }), response);
assert.deepEqual(calls, [submittedProofBundle]);

console.log('connection state indicator tests passed');
