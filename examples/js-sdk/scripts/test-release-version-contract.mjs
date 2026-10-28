#!/usr/bin/env node
import assert from 'node:assert/strict';

import { assertCargoLockPackageVersions } from './lib/release-version-contract.mjs';

const expectedVersions = new Map([
  ['locks-core', '0.1.0'],
  ['locks-server', '0.1.1-rc1'],
]);
const currentCargoLock = `[[package]]
name = "locks-core"
version = "0.1.0"

[[package]]
name = "locks-server"
version = "0.1.1-rc1"
`;

assert.doesNotThrow(() => assertCargoLockPackageVersions(currentCargoLock, expectedVersions));

const staleCargoLock = currentCargoLock.replace('version = "0.1.1-rc1"', 'version = "0.1.0"');
assert.throws(
  () => assertCargoLockPackageVersions(staleCargoLock, expectedVersions),
  /locks-server Cargo.lock version must be 0\.1\.1-rc1/,
);

console.log('Release version contract tests passed');
