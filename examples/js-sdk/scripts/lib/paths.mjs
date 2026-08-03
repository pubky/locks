import { existsSync } from 'node:fs';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export const repoRoot = resolve(fileURLToPath(new URL('../../../../', import.meta.url)));
export const examplesRoot = join(repoRoot, 'examples', 'js-sdk');
export const localPath = join(repoRoot, '.local');
export const demoStateDir = join(localPath, 'js-sdk-demo');
export const demoConfigPath = join(demoStateDir, 'config.json');
export const contentCreatorSessionPath = join(demoStateDir, 'content-creator-session.json');

export const validRoles = ['lock-server', 'content-creator', 'content-viewer'];

export function roleDir(role) {
  assertRole(role);
  return join(localPath, role);
}

export function roleProfilePath(role) {
  return join(roleDir(role), 'profile.json');
}

export function rolePassphrasePath(role) {
  return join(roleDir(role), 'passphrase');
}

export function roleRecoveryFilePath(role) {
  return join(roleDir(role), 'recovery_file');
}

export function assertRole(role) {
  if (!validRoles.includes(role)) {
    throw new Error(`invalid --role ${role ?? '<missing>'}; expected one of: ${validRoles.join(', ')}`);
  }
}

export function parseArgs(argv = process.argv.slice(2)) {
  const args = { _: [] };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '-f' || arg === '--force') {
      args.force = true;
    } else if (arg.startsWith('--') && arg.includes('=')) {
      const [key, ...rest] = arg.slice(2).split('=');
      args[key] = rest.join('=');
    } else if (arg.startsWith('--')) {
      const key = arg.slice(2);
      const next = argv[i + 1];
      if (next && !next.startsWith('-')) {
        args[key] = next;
        i += 1;
      } else {
        args[key] = true;
      }
    } else {
      args._.push(arg);
    }
  }
  return args;
}

export function requiredRole(args) {
  const role = args.role;
  assertRole(role);
  return role;
}

export async function ensureDir(path, mode = 0o700) {
  await mkdir(path, { recursive: true, mode });
}

export async function readJson(path) {
  return JSON.parse(await readFile(path, 'utf8'));
}

export async function writeJson(path, value, { mode = 0o644 } = {}) {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`, { mode });
}

export async function writeSecret(path, bytesOrText) {
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  await writeFile(path, bytesOrText, { mode: 0o600 });
}

export async function readMaybeText(path) {
  return existsSync(path) ? readFile(path, 'utf8') : undefined;
}
