#!/usr/bin/env node
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

import {
  checkExternalReaderPaykitData,
  createPaykitDataCheckController,
  validateExternalReaderPubky,
} from '../reader-staging-paykit.js';
import { repoRoot } from './lib/paths.mjs';

const creator = 'pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy';
const reader = 'pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo';

assert.equal(validateExternalReaderPubky(`  ${reader}  `, creator), reader);
assert.throws(() => validateExternalReaderPubky('not-a-pubky', creator), /canonical Bitkit reader Pubky/);
assert.throws(
  () => validateExternalReaderPubky(`${reader.slice(0, -1)}t`, creator),
  /canonical Bitkit reader Pubky/,
);
assert.throws(() => validateExternalReaderPubky(creator, creator), /distinct Bitkit identities/);

assert.deepEqual(
  await checkExternalReaderPaykitData({
    readerPubky: reader,
    creatorPubky: creator,
    lookup: async (value) => value === reader,
  }),
  {
    state: 'present',
    readerPubky: reader,
    canSubmit: true,
    message: 'Paykit v0 data is present. Invoice creation will validate the usable Bitkit receiver.',
  },
);
assert.deepEqual(
  await checkExternalReaderPaykitData({
    readerPubky: reader,
    creatorPubky: creator,
    lookup: async () => false,
  }),
  {
    state: 'absent',
    readerPubky: reader,
    canSubmit: false,
    message: 'No Paykit v0 data found. Enable Paykit in Bitkit, then retry.',
  },
);
assert.deepEqual(
  await checkExternalReaderPaykitData({
    readerPubky: reader,
    creatorPubky: creator,
    lookup: async () => { throw new Error('network detail must not be projected'); },
  }),
  {
    state: 'unavailable',
    readerPubky: reader,
    canSubmit: false,
    message: 'Paykit data lookup is unavailable. Retry.',
  },
);

const controller = createPaykitDataCheckController();
const oldDeferred = deferred();
const newDeferred = deferred();
let currentSnapshot = { incarnation: 1, resource: 'lock-a', creatorPubky: creator, readerPubky: reader };
const oldCheck = controller.check({
  ...currentSnapshot,
  lookup: () => oldDeferred.promise,
  isCurrent: (snapshot) => sameSnapshot(snapshot, currentSnapshot),
});
currentSnapshot = { incarnation: 2, resource: 'lock-b', creatorPubky: creator, readerPubky: reader };
const newCheck = controller.check({
  ...currentSnapshot,
  lookup: () => newDeferred.promise,
  isCurrent: (snapshot) => sameSnapshot(snapshot, currentSnapshot),
});
newDeferred.reject(new Error('newer lookup failed'));
assert.equal((await newCheck).state, 'unavailable');
oldDeferred.resolve(true);
assert.equal(await oldCheck, null);

const invalidatedDeferred = deferred();
const invalidated = controller.check({
  ...currentSnapshot,
  lookup: () => invalidatedDeferred.promise,
  isCurrent: (snapshot) => sameSnapshot(snapshot, currentSnapshot),
});
controller.invalidate();
invalidatedDeferred.resolve(true);
assert.equal(await invalidated, null);

const [readerAppSource, readerHtmlSource, readerFlowSource] = await Promise.all([
  readFile(`${repoRoot}/examples/js-sdk/reader-app.js`, 'utf8'),
  readFile(`${repoRoot}/examples/js-sdk/reader.html`, 'utf8'),
  readFile(`${repoRoot}/examples/js-sdk/reader-flow.js`, 'utf8'),
]);
assert.match(readerAppSource, /checkExternalReaderPaykitData/);
assert.match(readerAppSource, /createPaykitDataCheckController/);
assert.match(readerAppSource, /paykitDataChecks\.invalidate\(\)/);
assert.match(readerAppSource, /paykitDataSnapshotMatches/);
assert.match(readerAppSource, /creatorPubky: snapshot\.paykitCreator/);
assert.match(readerAppSource, /state\.loaded\?\.creator !== snapshot\.paykitCreator/);
assert.match(readerAppSource, /state\.config\.mode === 'staging'/);
assert.doesNotMatch(readerAppSource, /state\.config\.testnet\.pkarrRelay/);
assert.match(readerAppSource, /buildPersistedReaderState\(state\)/);
assert.doesNotMatch(readerHtmlSource, /id="reader-public-key" readonly/);
assert.match(readerFlowSource, /export async function hasPaykitData/);
assert.match(readerFlowSource, /Locks\.hasPaykitData\(readerPublicKey\)/);

console.log('staging reader mode tests passed');

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((settle, fail) => {
    resolve = settle;
    reject = fail;
  });
  return { promise, resolve, reject };
}

function sameSnapshot(left, right) {
  return left.incarnation === right.incarnation
    && left.resource === right.resource
    && left.creatorPubky === right.creatorPubky
    && left.readerPubky === right.readerPubky;
}
