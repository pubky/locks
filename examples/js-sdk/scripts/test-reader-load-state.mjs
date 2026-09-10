#!/usr/bin/env node
import assert from 'node:assert/strict';

import {
  describeReaderLoadState,
  validateContentLockResource,
} from '../reader-load-state.js';

assert.throws(
  () => validateContentLockResource(''),
  /Paste a content lock resource/,
);
assert.throws(
  () => validateContentLockResource('pubkycreator/pub/locks.app/content-lock.json'),
  (error) => /retired/i.test(error.message)
    && /republish/i.test(error.message)
    && error.message.includes('/pub/app.locks/'),
);
assert.doesNotThrow(
  () => validateContentLockResource('pubkycreator/pub/app.locks/content-lock.json'),
);

assert.deepEqual(
  describeReaderLoadState({
    loadingLock: false,
    loaded: null,
    resource: 'pubkycreator/pub/locks.app/content-lock.json',
    loadError: 'Old namespace rejected.',
  }),
  { message: 'Old namespace rejected.', className: 'error' },
);

console.log('reader load state tests passed');
