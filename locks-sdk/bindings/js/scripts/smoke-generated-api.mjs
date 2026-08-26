import { existsSync, readFileSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const root = new URL('..', import.meta.url).pathname;
const pkgDir = join(root, 'pkg');
const pkgJsonPath = join(pkgDir, 'package.json');
const dtsPath = join(pkgDir, 'locks_sdk_wasm.d.ts');
const jsPath = join(pkgDir, 'locks_sdk_wasm.js');
const wasmPath = join(pkgDir, 'locks_sdk_wasm_bg.wasm');

for (const path of [pkgJsonPath, dtsPath, jsPath, wasmPath]) {
  if (!existsSync(path)) {
    throw new Error(`missing generated package artifact: ${path}`);
  }
}

const pkg = JSON.parse(readFileSync(pkgJsonPath, 'utf8'));
if (pkg.name !== 'locks-sdk-wasm') {
  throw new Error(`unexpected wasm-pack package name: ${pkg.name}`);
}
if (pkg.type !== 'module') {
  throw new Error(`generated package must be ESM; got type=${pkg.type}`);
}
if (pkg.main !== 'locks_sdk_wasm.js') {
  throw new Error(`unexpected generated package main: ${pkg.main}`);
}
if (pkg.types !== 'locks_sdk_wasm.d.ts') {
  throw new Error(`unexpected generated package types: ${pkg.types}`);
}

const dts = readFileSync(dtsPath, 'utf8');
const requiredSnippets = [
  'export class Locks',
  'static forServer(lock_server: string): Locks;',
  'static forServerWithOptions(lock_server: string, options: LocksOptions): Locks;',
  'static forCreator(creator: string): Promise<Locks>;',
  'static forCreatorWithOptions(creator: string, options: LocksOptions): Promise<Locks>;',
  'static forContentLock(resource: string): Promise<Locks>;',
  'static forContentLockWithOptions(resource: string, options: LocksOptions): Promise<Locks>;',
  'static readContentLock(resource: string): Promise<any>;',
  'static readContentLockWithOptions(resource: string, options: LocksOptions): Promise<any>;',
  'export class LocksOptions',
  'constructor();',
  'addPkarrRelay(relay_url: string): LocksOptions;',
  'readonly pkarrRelays: string[];',
  'static fromCreatorLockServicePointer(pointer: any): Locks;',
  'createConnectUrl(options: ConnectUrlOptions): Promise<string>;',
  'static parseConnectCallback(callback_url: string): ConnectCallback;',
  'exchangeFrontendSessionCode(options: ExchangeFrontendSessionCodeOptions): Promise<Session>;',
  'restoreSession(secret: string): Session;',
  'readonly viewer: Viewer;',
  'export class Viewer',
  'export class BundleId',
  'constructor(value: string);',
  'static generate(): BundleId;',
  'toString(): string;',
  'submitProofBundle(submitted_proof_bundle: any): Promise<any>;',
  'lookupVerificationTask(options: VerificationTaskHandleOptions): Promise<any>;',
  'issueAccessCredential(options: VerificationTaskHandleOptions): Promise<any>;',
  'proxyReadGuardedResource(access_credential: string, path: string): Promise<Uint8Array>;',
  'proxyReadGuardedResourceResponse(access_credential: string, path: string): Promise<Response>;',
  'export class VerificationTaskHandleOptions',
  'constructor(creator: string, bundle_id: string);',
  'export class Session',
  'exportSecret(): string;',
  'signout(): Promise<void>;',
  'readonly creator: Creator;',
  'export class Creator',
  'registerGuardedResource(options: RegisterGuardedResourceOptions): Promise<any>;',
  'createContentLock(body: any): Promise<any>;',
  'deleteGuardedResource(options: DeleteGuardedResourceOptions): Promise<void>;',
  'setLockServicePointer(options: SetLockServicePointerOptions): Promise<void>;',
  'paykitSetupStatus(): Promise<any>;',
  'export class CreateContentLockRequestBuilder',
  'primaryResource(resource: any): CreateContentLockRequestBuilder;',
  'secondaryResource(resource: any): CreateContentLockRequestBuilder;',
  'secondaryResources(resources: any): CreateContentLockRequestBuilder;',
  'accessPolicy(access_policy: any): CreateContentLockRequestBuilder;',
  'build(): any;',
  'export class DeleteGuardedResourceOptions',
  'constructor(path: string);',
  'export class ConnectUrlOptions',
  'constructor(return_to: string, state: string);',
  'export class ExchangeFrontendSessionCodeOptions',
  'constructor(code: string, state: string);',
];

for (const snippet of requiredSnippets) {
  if (!dts.includes(snippet)) {
    throw new Error(`generated TypeScript declarations missing: ${snippet}`);
  }
}

const sdk = await import(pathToFileURL(jsPath));
await sdk.default(await readFile(wasmPath));

if (typeof sdk.Creator.prototype.paykitSetupStatus !== 'function') {
  throw new Error('generated Creator missing paykitSetupStatus');
}
if (sdk.Creator.prototype.paykitSetupStatus.length !== 0) {
  throw new Error('paykitSetupStatus must not accept a caller-supplied Creator');
}

const primaryResource = {
  path: '/priv/locks.app/content/primary.txt',
  hash: '0W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3G',
  content_type: 'text/plain',
  size: 13,
};
const secondaryResource = {
  path: '/priv/locks.app/content/secondary.txt',
  hash: '0W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3G',
  content_type: 'text/plain',
  size: 7,
};
const request = new sdk.CreateContentLockRequestBuilder()
  .primaryResource(primaryResource)
  .secondaryResource(secondaryResource)
  .criteria([])
  .lockLogic({ type: 'all', criteria: [] })
  .accessPolicy({ requested_credential_ttl_seconds: 900 })
  .lockServer({ override: null })
  .build();

if (request instanceof Map) {
  throw new Error('content lock request builder returned a Map instead of a plain object');
}
if (request.primary_resource?.path !== primaryResource.path) {
  throw new Error('content lock request primary_resource is not accessible as a plain object property');
}
if (request.secondary_resources instanceof Map) {
  throw new Error('content lock request secondary_resources returned a nested Map');
}
if (request.secondary_resources?.[secondaryResource.path]?.size !== secondaryResource.size) {
  throw new Error('content lock request secondary_resources is not accessible as nested plain object properties');
}
if (JSON.stringify(request).includes('{}') && Object.keys(request.secondary_resources ?? {}).length === 0) {
  throw new Error('content lock request lost secondary resource fields during JSON serialization');
}

console.log('generated package API smoke check passed');
