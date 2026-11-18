export function assertCargoLockPackageVersions(cargoLockSource, expectedVersions) {
  for (const [packageName, expectedVersion] of expectedVersions) {
    const packageEntry = `name = "${packageName}"\nversion = "${expectedVersion}"`;
    if (!cargoLockSource.includes(packageEntry)) {
      throw new Error(`${packageName} Cargo.lock version must be ${expectedVersion}`);
    }
  }
}

export function assertReleaseVersionContract({
  workspaceManifest,
  cargoLock,
  npmManifest,
  workspacePackages,
}) {
  const workspacePackageSection = workspaceManifest.match(
    /\[workspace\.package\]([\s\S]*?)(?=\n\[|$)/,
  )?.[1];
  const workspaceVersion = workspacePackageSection?.match(
    /^version = "([^"]+)"$/m,
  )?.[1];
  if (!workspaceVersion) {
    throw new Error('workspace package version is missing');
  }

  assertCargoLockPackageVersions(
    cargoLock,
    new Map(workspacePackages.map((packageName) => [packageName, workspaceVersion])),
  );

  const npmPackage = JSON.parse(npmManifest);
  if (npmPackage.version !== workspaceVersion) {
    throw new Error(`npm package version must be ${workspaceVersion}`);
  }

  const expectedDistTag = workspaceVersion.includes('-') ? 'rc' : 'latest';
  if (npmPackage.publishConfig?.tag !== expectedDistTag) {
    throw new Error(`npm publishConfig.tag must be ${expectedDistTag}`);
  }
}
