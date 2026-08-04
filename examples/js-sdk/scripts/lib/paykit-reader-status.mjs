import { chmod, lstat, mkdir, open, readFile, rename, rm } from 'node:fs/promises';
import { dirname } from 'node:path';
import { randomUUID } from 'node:crypto';

import { validateReaderOperatorResult } from './paykit-reader-helper.mjs';
import { paykitReaderPreparedPath, paykitReaderWorkerStatusPath } from './paths.mjs';

const READER_PUBKY = /^pubky[ybndrfg8ejkmcpqxot1uwisza345h769]{52}$/;
const RECEIVER_PATH = /^[A-Za-z0-9][A-Za-z0-9._/-]{0,254}$/;
const RETRYABLE_ERRORS = new Set(['receive_timeout', 'protocol_failed']);
const FAILURE_ERRORS = new Set([
  'invalid_input',
  'invalid_config',
  'invalid_state',
  'output_failed',
  'prepare_timeout',
  'worker_failed',
]);

function exactKeys(value, expected) {
  return value !== null
    && typeof value === 'object'
    && !Array.isArray(value)
    && Object.keys(value).length === expected.length
    && expected.every((key) => Object.hasOwn(value, key));
}

export function validatePreparedReaderStatus(value) {
  if (
    !exactKeys(value, ['version', 'status', 'reader_pubky', 'receiver_path'])
    || value.version !== 1
    || value.status !== 'prepared'
    || typeof value.reader_pubky !== 'string'
    || !READER_PUBKY.test(value.reader_pubky)
    || typeof value.receiver_path !== 'string'
    || !RECEIVER_PATH.test(value.receiver_path)
  ) {
    throw new Error('invalid prepared Paykit reader status');
  }
  return Object.freeze({ ...value });
}

export async function writePreparedReaderStatus(value, path = paykitReaderPreparedPath) {
  const status = validatePreparedReaderStatus(value);
  await writeAtomicStatus(status, path);
}

async function writeAtomicStatus(status, path) {
  const directory = dirname(path);
  const temporary = `${path}.${process.pid}.${randomUUID()}.tmp`;
  await mkdir(directory, { recursive: true, mode: 0o700 });
  let file;
  try {
    file = await open(temporary, 'wx', 0o600);
    await file.writeFile(`${JSON.stringify(status)}\n`, 'utf8');
    await file.sync();
    await file.close();
    file = undefined;
    await rename(temporary, path);
    await chmod(path, 0o600);
  } finally {
    await file?.close().catch(() => {});
    await rm(temporary, { force: true }).catch(() => {});
  }
}

export async function readPreparedReaderStatus(path = paykitReaderPreparedPath) {
  try {
    const metadata = await lstat(path);
    if (!metadata.isFile() || metadata.isSymbolicLink() || (metadata.mode & 0o077) !== 0) return null;
    return validatePreparedReaderStatus(JSON.parse(await readFile(path, 'utf8')));
  } catch {
    return null;
  }
}

export async function clearPreparedReaderStatus(path = paykitReaderPreparedPath) {
  await rm(path, { force: true });
}

export function buildPreparedReaderBrowserStatus(prepared, profile) {
  if (
    !prepared
    || profile?.role !== 'content-viewer'
    || profile.pubky !== prepared.reader_pubky
  ) {
    return { version: 1, prepared: false };
  }
  return { version: 1, prepared: true, reader_pubky: prepared.reader_pubky };
}

export function validatePaykitReaderWorkerStatus(value) {
  if (!value || value.version !== 1 || typeof value.state !== 'string') {
    throw new Error('invalid Paykit reader worker status');
  }
  if (value.state === 'starting' && exactKeys(value, ['version', 'state'])) {
    return Object.freeze({ ...value });
  }
  if (
    value.state === 'waiting'
    && exactKeys(value, ['version', 'state', 'reader_pubky'])
    && READER_PUBKY.test(value.reader_pubky)
  ) {
    return Object.freeze({ ...value });
  }
  if (
    value.state === 'retrying'
    && exactKeys(value, ['version', 'state', 'reader_pubky', 'error'])
    && READER_PUBKY.test(value.reader_pubky)
    && RETRYABLE_ERRORS.has(value.error)
  ) {
    return Object.freeze({ ...value });
  }
  if (
    value.state === 'failed'
    && exactKeys(value, ['version', 'state', 'error'])
    && FAILURE_ERRORS.has(value.error)
  ) {
    return Object.freeze({ ...value });
  }
  if (
    value.state === 'request_received'
    && exactKeys(value, [
      'version',
      'state',
      'reader_pubky',
      'payment_request_id',
      'address',
      'asset',
      'amount_sats',
      'payment_command',
      'optional_mining_command',
    ])
    && READER_PUBKY.test(value.reader_pubky)
  ) {
    validateReaderOperatorResult({
      version: value.version,
      status: 'received',
      payment_request_id: value.payment_request_id,
      address: value.address,
      asset: value.asset,
      amount_sats: value.amount_sats,
      payment_command: value.payment_command,
      optional_mining_command: value.optional_mining_command,
    });
    return Object.freeze({ ...value });
  }
  throw new Error('invalid Paykit reader worker status');
}

export async function writePaykitReaderWorkerStatus(
  value,
  path = paykitReaderWorkerStatusPath,
) {
  await writeAtomicStatus(validatePaykitReaderWorkerStatus(value), path);
}

export async function readPaykitReaderWorkerStatus(path = paykitReaderWorkerStatusPath) {
  try {
    const metadata = await lstat(path);
    if (!metadata.isFile() || metadata.isSymbolicLink() || (metadata.mode & 0o077) !== 0) return null;
    return validatePaykitReaderWorkerStatus(JSON.parse(await readFile(path, 'utf8')));
  } catch {
    return null;
  }
}

export function buildPaykitReaderBrowserStatus(worker, profile, { currentOwner = false } = {}) {
  if (!currentOwner) return { version: 1, state: 'starting' };
  if (!worker) return { version: 1, state: 'starting' };
  if (
    Object.hasOwn(worker, 'reader_pubky')
    && (profile?.role !== 'content-viewer' || profile.pubky !== worker.reader_pubky)
  ) {
    return { version: 1, state: 'failed', error: 'identity_mismatch' };
  }
  return { ...worker };
}
