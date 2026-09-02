import { randomBytes } from 'node:crypto';
import { existsSync } from 'node:fs';
import { chmod, lstat, mkdir, open, readFile, rename, rm } from 'node:fs/promises';
import { basename, dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export const repoRoot = resolve(fileURLToPath(new URL('../../../../', import.meta.url)));
export const examplesRoot = join(repoRoot, 'examples', 'js-sdk');
export const localPath = join(repoRoot, '.local');
export const demoStateDir = join(localPath, 'js-sdk-demo');
export const demoPublicConfigDir = join(localPath, 'demo-config');
export const demoConfigPath = join(demoPublicConfigDir, 'config.json');
export const stagingDemoRoot = join(localPath, 'paykit-staging-demo');
export const stagingDemoConfigDir = join(stagingDemoRoot, 'config');
export const stagingDemoConfigPath = join(stagingDemoConfigDir, 'config.json');
export const stagingCreatorSessionDir = join(stagingDemoRoot, 'creator-session');
export const stagingCreatorSessionPath = join(stagingCreatorSessionDir, 'content-creator-session.json');
export const contentCreatorSessionPath = join(demoStateDir, 'content-creator-session.json');
export const creatorPublicDir = join(localPath, 'creator-public');
export const creatorPublicProfilePath = join(creatorPublicDir, 'profile.json');
export const paykitReaderDir = join(localPath, 'paykit-reader');
export const bitcoinBootstrapDir = join(localPath, 'bitcoin-bootstrap');
export const paykitReaderPreparedPath = join(paykitReaderDir, 'prepared.v1.json');
export const paykitReaderWorkerStatusPath = join(paykitReaderDir, 'worker.v1.json');
export const paykitReaderOwnershipPath = join(paykitReaderDir, 'owner.lock');
export const composeSecretsPath = join(localPath, 'compose-secrets.json');
export const locksPostgresEnvPath = join(localPath, 'locks-postgres', 'locks-postgres.env');
export const locksServerComposeEnvPath = join(localPath, 'locks-server', 'compose.env');
export const paykitServerDir = join(localPath, 'paykit-server');
export const paykitPostgresEnvPath = join(localPath, 'paykit-postgres', 'postgres.env');
export const paykitServerEnvPath = join(paykitServerDir, 'paykit.env');
export const paykitPublicConfigDir = join(localPath, 'paykit-config');
export const paykitServerConfigPath = join(paykitPublicConfigDir, 'config.toml');
export const bitcoinRpcEnvPath = join(localPath, 'bitcoin-rpc', 'bitcoin-rpc.env');
export const pubkyHomeserverConfigPath = join(localPath, 'pubky-homeserver', 'config.toml');

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
  await writeAtomicFile(path, `${JSON.stringify(value, null, 2)}\n`, mode);
}

export async function writeSecret(path, bytesOrText) {
  await writeAtomicFile(path, bytesOrText, 0o600);
}

export async function writeAtomicFile(path, content, mode) {
  const directory = dirname(path);
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const directoryStat = await lstat(directory);
  if (!directoryStat.isDirectory() || directoryStat.isSymbolicLink()) {
    throw new Error('generated state parent must be a directory');
  }
  await chmod(directory, 0o700);
  const temporary = join(directory, `.${basename(path)}.${process.pid}.${randomBytes(8).toString('hex')}.tmp`);
  let handle;
  try {
    handle = await open(temporary, 'wx', mode);
    await handle.writeFile(content);
    await handle.chmod(mode);
    await handle.sync();
    await handle.close();
    handle = undefined;
    await rename(temporary, path);
  } finally {
    await handle?.close().catch(() => {});
    await rm(temporary, { force: true }).catch(() => {});
  }
}

export async function readPrivateText(path) {
  const info = await lstat(path);
  if (!info.isFile() || info.isSymbolicLink() || (info.mode & 0o077) !== 0) {
    throw new Error('persisted secret must be a regular owner-only file');
  }
  return readFile(path, 'utf8');
}

export async function readMaybeText(path) {
  return existsSync(path) ? readFile(path, 'utf8') : undefined;
}
