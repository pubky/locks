import { existsSync, readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';

const root = resolve(new URL('../../../..', import.meta.url).pathname);
const bindingRoot = resolve(new URL('..', import.meta.url).pathname);
const manifestPath = join(bindingRoot, 'package.json');
const cargoTomlPath = join(bindingRoot, 'Cargo.toml');
const licensePath = join(root, 'LICENSE');
const pkgDir = join(bindingRoot, 'pkg');
const generatedPackagePath = join(pkgDir, 'package.json');

const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
const cargoToml = readFileSync(cargoTomlPath, 'utf8');

const checks = [];

function add(status, name, detail) {
  checks.push({ status, name, detail });
}

add(manifest.name === '@pubky/locks-sdk' ? 'ready' : 'blocked', 'scaffold npm package name', `package.json name=${manifest.name}`);
add(manifest.private === true ? 'blocked' : 'ready', 'npm publishing enabled', manifest.private === true ? 'package.json still has private=true' : 'package is publishable');
add(existsSync(licensePath) ? 'ready' : 'blocked', 'repository LICENSE file', existsSync(licensePath) ? 'LICENSE exists' : 'workspace declares MIT but no LICENSE file exists');
add(cargoToml.includes('license.workspace = true') ? 'ready' : 'blocked', 'Cargo license metadata', 'bindings crate inherits workspace license');

if (existsSync(generatedPackagePath)) {
  const generated = JSON.parse(readFileSync(generatedPackagePath, 'utf8'));
  add(generated.name === manifest.name ? 'ready' : 'blocked', 'generated wasm-pack package name', `generated name=${generated.name}; scaffold name=${manifest.name}`);
} else {
  add('blocked', 'generated package artifacts', 'run npm run build before publish audit can compare pkg/package.json');
}

add(existsSync(join(bindingRoot, 'pkg')) ? 'ready' : 'blocked', 'generated pkg/ artifacts', existsSync(join(bindingRoot, 'pkg')) ? 'pkg/ exists locally' : 'run npm run build');

const blocked = checks.filter((check) => check.status === 'blocked');
console.log('SDK release readiness audit');
for (const check of checks) {
  const marker = check.status === 'ready' ? 'READY  ' : 'BLOCKED';
  console.log(`${marker} ${check.name}: ${check.detail}`);
}

if (blocked.length > 0) {
  console.log(`\nRelease is not publish-ready: ${blocked.length} blocker(s) remain.`);
  console.log('This command is informational and exits 0 so CI/local verification can document blockers without pretending policy is resolved.');
} else {
  console.log('\nRelease appears publish-ready from local metadata checks. Run npm publish dry-run before publishing.');
}
