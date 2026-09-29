#!/usr/bin/env node
import assert from 'node:assert/strict';

import { signupBestEffort } from './lib/pubky.mjs';

const homeserver = { id: 'homeserver' };

{
  const expectedSession = { id: 'new-session' };
  const calls = [];
  const signer = {
    signup: async (...args) => {
      calls.push(['signup', ...args]);
      return expectedSession;
    },
    pkdns: {
      publishHomeserverIfStale: async (...args) => calls.push(['publish', ...args]),
      free: () => calls.push(['free']),
    },
    signinCookieBlocking: async () => {
      calls.push(['signin']);
      return { id: 'unexpected' };
    },
  };

  assert.equal(await signupBestEffort(signer, homeserver), expectedSession);
  assert.deepEqual(calls, [['signup', homeserver, null]]);
}

{
  const expectedSession = { id: 'existing-session' };
  const calls = [];
  const signer = {
    signup: async (...args) => {
      calls.push(['signup', ...args]);
      throw new Error('HTTP 409 Conflict');
    },
    pkdns: {
      publishHomeserverIfStale: async (...args) => calls.push(['publish', ...args]),
      free: () => calls.push(['free']),
    },
    signinCookieBlocking: async () => {
      calls.push(['signin']);
      return expectedSession;
    },
  };

  assert.equal(await signupBestEffort(signer, homeserver), expectedSession);
  assert.deepEqual(calls, [
    ['signup', homeserver, null],
    ['publish', homeserver],
    ['free'],
    ['signin'],
  ]);
}

{
  const expectedError = new Error('network unavailable');
  const signer = {
    signup: async () => { throw expectedError; },
  };

  await assert.rejects(() => signupBestEffort(signer, homeserver), (error) => error === expectedError);
}

console.log('Pubky helper tests passed');
