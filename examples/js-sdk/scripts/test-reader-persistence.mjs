#!/usr/bin/env node
import assert from 'node:assert/strict';

import {
  buildPersistedReaderState,
  restorePersistedReaderState,
} from '../reader-persistence.js';

const publicState = {
  resource: 'pubkycreator/pub/locks.app/LOCK.json',
  guardedResourcePath: 'file.txt',
  lockResources: [{ path: '/priv/locks.app/content/file.txt' }],
  proofSatisfied: true,
  verifierType: 'paykit-payment',
  loaded: { creator: 'pubkycreator', contentLock: { version: 1 } },
};
const sensitiveState = {
  ...publicState,
  readerPublicKey: 'pubkyreader',
  submittedProofBundle: { reader_public_key: 'pubkyreader', bundle_id: 'bundle-secret' },
  bundleId: 'bundle-secret',
  lifecycle: { bundle_id: 'bundle-secret', status: 'pending' },
  accessCredential: 'credential-secret',
  accessCredentialResponse: { credential: 'credential-secret' },
  completion: { bundle_id: 'bundle-secret' },
  paykitPaymentRequest: { payment_request_id: 'private-request' },
};

const persisted = buildPersistedReaderState(sensitiveState);
assert.deepEqual(persisted, publicState);
const serialized = JSON.stringify(persisted);
for (const forbidden of ['pubkyreader', 'bundle-secret', 'credential-secret', 'private-request']) {
  assert.equal(serialized.includes(forbidden), false, `persisted reader state leaked ${forbidden}`);
}

assert.deepEqual(
  restorePersistedReaderState({
    ...publicState,
    readerPublicKey: 'legacy-reader',
    submittedProofBundle: { reader_public_key: 'legacy-reader' },
    accessCredential: 'legacy-credential',
  }),
  publicState,
);
assert.deepEqual(restorePersistedReaderState(null), {});
assert.deepEqual(restorePersistedReaderState([]), {});

console.log('reader persistence tests passed');
