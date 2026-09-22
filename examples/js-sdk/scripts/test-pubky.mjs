#!/usr/bin/env node
import assert from 'node:assert/strict';

import { signupBestEffort } from './lib/pubky.mjs';

const homeserver = { id: 'homeserver' };
const calls = [];
const session = { id: 'session' };
const signer = {
  pkdns: {
    free() {},
    async publishHomeserverIfStale(value) {
      calls.push(['publish', value]);
    },
  },
  async signup(value, token) {
    calls.push(['signup', value, token]);
    throw new Error('HTTP 409 conflict: user already registered');
  },
  async signinCookieBlocking() {
    calls.push(['signin']);
    return session;
  },
};

assert.equal(await signupBestEffort(signer, homeserver), session);
assert.deepEqual(calls, [
  ['signup', homeserver, null],
  ['publish', homeserver],
  ['signin'],
]);

console.log('pubky helper tests passed');
