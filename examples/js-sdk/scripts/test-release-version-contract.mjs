#!/usr/bin/env node
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

import {
  assertCargoLockPackageVersions,
  assertReleaseVersionContract,
} from './lib/release-version-contract.mjs';

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

const repoRoot = resolve(new URL('../../..', import.meta.url).pathname);
const workspaceManifest = readFileSync(resolve(repoRoot, 'Cargo.toml'), 'utf8');
const workspaceVersion = workspaceManifest.match(
  /\[workspace\.package\][\s\S]*?^version = "([^"]+)"$/m,
)?.[1];
assert.ok(workspaceVersion, 'workspace release version must exist');
const previousVersion = `${workspaceVersion}-mismatch`;
const escapedWorkspaceVersion = workspaceVersion.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
const cargoLock = readFileSync(resolve(repoRoot, 'Cargo.lock'), 'utf8');
const npmManifest = readFileSync(resolve(repoRoot, 'locks-sdk/bindings/js/package.json'), 'utf8');
const workspacePackages = [
  'locks-core',
  'locks-e2e',
  'locks-sdk',
  'locks-sdk-wasm',
  'locks-server',
  'locks-service',
];
const workspacePackageManifests = new Map([
  ['locks-core', 'locks-core/Cargo.toml'],
  ['locks-e2e', 'locks-e2e/Cargo.toml'],
  ['locks-sdk', 'locks-sdk/Cargo.toml'],
  ['locks-sdk-wasm', 'locks-sdk/bindings/js/Cargo.toml'],
  ['locks-server', 'locks-server/Cargo.toml'],
  ['locks-service', 'locks-service/Cargo.toml'],
]);
const releaseContract = { workspaceManifest, cargoLock, npmManifest, workspacePackages };
assert.doesNotThrow(() => assertReleaseVersionContract(releaseContract));
assert.throws(
  () => assertReleaseVersionContract({
    ...releaseContract,
    cargoLock: cargoLock.replace(
      `name = "locks-core"\nversion = "${workspaceVersion}"`,
      `name = "locks-core"\nversion = "${previousVersion}"`,
    ),
  }),
  new RegExp(`locks-core Cargo\\.lock version must be ${escapedWorkspaceVersion}`),
);
assert.throws(
  () => assertReleaseVersionContract({
    ...releaseContract,
    npmManifest: npmManifest.replace(
      `"version": "${workspaceVersion}"`,
      `"version": "${previousVersion}"`,
    ),
  }),
  new RegExp(`npm package version must be ${escapedWorkspaceVersion}`),
);
const expectedDistTag = workspaceVersion.includes('-') ? 'rc' : 'latest';
const wrongDistTag = expectedDistTag === 'rc' ? 'latest' : 'rc';
const npmPackage = JSON.parse(npmManifest);
assert.throws(
  () => assertReleaseVersionContract({
    ...releaseContract,
    npmManifest: JSON.stringify({
      ...npmPackage,
      publishConfig: { ...npmPackage.publishConfig, tag: wrongDistTag },
    }),
  }),
  new RegExp(`npm publishConfig\\.tag must be ${expectedDistTag}`),
);
for (const [packageName, manifestPath] of workspacePackageManifests) {
  const packageManifest = readFileSync(resolve(repoRoot, manifestPath), 'utf8');
  assert.match(
    packageManifest,
    /^version\.workspace = true$/m,
    `${packageName} must inherit the workspace release version`,
  );
}

const checkWorkflow = readFileSync(resolve(repoRoot, '.github/workflows/check.yml'), 'utf8');
assert.match(
  checkWorkflow,
  /^\s+run: npm --prefix examples\/js-sdk run test:release-version-contract$/m,
  'CI must run the release version contract against repository files',
);

const packageReadme = readFileSync(resolve(repoRoot, 'locks-sdk/bindings/js/README.md'), 'utf8');
assert.match(
  packageReadme,
  /Release candidates use the `rc` dist-tag:[\s\S]*npm install @synonymdev\/locks-sdk@rc/,
  'package README must install release candidates from the rc dist-tag',
);
assert.match(
  packageReadme,
  /Stable releases use the default `latest` dist-tag:[\s\S]*npm install @synonymdev\/locks-sdk\n/,
  'package README must keep stable installation explicit',
);

const releaseGuide = readFileSync(resolve(repoRoot, 'docs/SDK_RELEASE.md'), 'utf8');
assert.match(
  releaseGuide,
  /npm publish \.\/locks-sdk\/bindings\/js --access public\n/,
  'SDK release guide must let publishConfig choose the npm dist-tag',
);
assert.doesNotMatch(
  releaseGuide,
  /npm publish[^\n]*--tag/,
  'SDK release guide must not override publishConfig.tag',
);
assert.match(
  releaseGuide,
  /npm view "@synonymdev\/locks-sdk@\$\{PACKAGE_VERSION\}" version/,
  'SDK release guide must verify the published package version',
);
assert.match(
  releaseGuide,
  /npm view @synonymdev\/locks-sdk "dist-tags\.\$\{DIST_TAG\}"/,
  'SDK release guide must verify the manifest-selected dist-tag',
);

console.log('Release version contract tests passed');
