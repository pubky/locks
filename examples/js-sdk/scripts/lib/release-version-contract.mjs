export function assertCargoLockPackageVersions(cargoLockSource, expectedVersions) {
  for (const [packageName, expectedVersion] of expectedVersions) {
    const packageEntry = `name = "${packageName}"\nversion = "${expectedVersion}"`;
    if (!cargoLockSource.includes(packageEntry)) {
      throw new Error(`${packageName} Cargo.lock version must be ${expectedVersion}`);
    }
  }
}
