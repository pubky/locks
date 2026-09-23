import { spawn } from 'node:child_process';
import { chmod, lstat, mkdir, open } from 'node:fs/promises';
import { dirname } from 'node:path';

import { runReaderOperation } from './paykit-reader-helper.mjs';
import { paykitReaderOwnershipPath } from './paths.mjs';
import { writePreparedReaderStatus } from './paykit-reader-status.mjs';

const RETRYABLE_ERRORS = new Set(['receive_timeout', 'protocol_failed']);
const INITIAL_RETRY_MS = 1_000;
const MAX_RETRY_MS = 30_000;
const OWNERSHIP_ACQUIRE_TIMEOUT_MS = 2_000;
const OWNERSHIP_RELEASE_TIMEOUT_MS = 2_000;
const OWNERSHIP_KILL_GRACE_MS = 1_000;
const OWNERSHIP_READY = 'locked\n';
const OWNERSHIP_HOLDER_PROGRAM = 'process.stdout.write("locked\\n");process.stdin.on("end",()=>process.exit(0));process.stdin.resume()';
const TERMINAL_ERRORS = new Set([
  'invalid_input',
  'invalid_config',
  'invalid_state',
  'output_failed',
  'prepare_timeout',
  'worker_failed',
]);

export function assertStandaloneReaderOperationAllowed(env = process.env) {
  if (env.PAYKIT_READER_WORKER_ENABLED === '1') {
    throw new Error('Paykit reader state is owned by the embedded reader-demo worker');
  }
}

export async function acquirePaykitReaderOwnership(
  path = paykitReaderOwnershipPath,
  { spawnProcess = spawn } = {},
) {
  const directory = dirname(path);
  await mkdir(directory, { recursive: true, mode: 0o700 });

  let existing;
  try {
    existing = await lstat(path);
  } catch (error) {
    if (error?.code !== 'ENOENT') throw error;
    existing = null;
  }
  if (existing && (!existing.isFile() || existing.isSymbolicLink())) {
    throw new Error('Paykit reader ownership lock must be a regular file');
  }
  if (!existing) {
    const created = await open(path, 'wx', 0o600);
    await created.close();
  }

  await chmod(directory, 0o700);
  const metadata = await lstat(path);
  if (!metadata.isFile() || metadata.isSymbolicLink() || (metadata.mode & 0o077) !== 0) {
    throw new Error('Paykit reader ownership lock permissions are unsafe');
  }
  await chmod(path, 0o600);

  const holder = spawnProcess('/usr/bin/flock', [
    '--nonblock',
    '--exclusive',
    '--no-fork',
    path,
    process.execPath,
    '-e',
    OWNERSHIP_HOLDER_PROGRAM,
  ], {
    stdio: ['pipe', 'pipe', 'pipe'],
    env: {},
    shell: false,
  });
  holder.stdin.on('error', () => {});
  const { closed } = await waitForOwnershipReady(holder);

  let releasing = false;
  let released = false;
  let resolveLost;
  const ownershipLost = new Promise((resolve) => { resolveLost = resolve; });
  void closed.then(() => {
    if (!releasing) resolveLost();
  });
  return {
    lost: ownershipLost,
    async release() {
      if (released) return;
      released = true;
      releasing = true;
      holder.stdin.end();
      await stopOwnershipHolder(holder, closed);
    },
  };
}

export async function supervisePaykitReaderWorker(task, { onTerminalFailure } = {}) {
  let result;
  try {
    result = await task;
  } catch {
    result = { status: 'failed', error: 'worker_failed' };
  }
  if (result?.status === 'failed' && typeof onTerminalFailure === 'function') {
    try {
      await onTerminalFailure(result.error ?? 'worker_failed');
    } catch {}
  }
  return result;
}

export async function waitForCreatorProfile({
  signal,
  readProfile,
  wait = waitForDelay,
} = {}) {
  if (!signal || typeof readProfile !== 'function') {
    throw new Error('Paykit reader creator wait dependencies are incomplete');
  }
  while (!signal.aborted) {
    let profile;
    try {
      profile = await readProfile();
    } catch (error) {
      if (error?.code !== 'ENOENT') throw error;
      profile = null;
    }
    if (profile === null) {
      await wait(INITIAL_RETRY_MS, signal);
      continue;
    }
    if (
      profile.role !== 'content-creator'
      || !/^pubky[ybndrfg8ejkmcpqxot1uwisza345h769]{52}$/.test(profile.pubky)
    ) {
      throw new Error('Paykit reader creator profile is invalid');
    }
    return profile;
  }
  return null;
}

export async function runPaykitReaderWorker({
  signal,
  runOperation = runReaderOperation,
  writePreparedStatus = writePreparedReaderStatus,
  writeWorkerStatus,
  wait = waitForDelay,
  acquireOwnership = acquirePaykitReaderOwnership,
  onOwnershipChange = () => {},
} = {}) {
  if (!signal || typeof writeWorkerStatus !== 'function') {
    throw new Error('Paykit reader worker dependencies are incomplete');
  }

  if (signal.aborted) return { status: 'stopped' };
  const ownership = await acquireOwnership();
  if (signal.aborted) {
    await ownership.release();
    return { status: 'stopped' };
  }
  const lifecycleController = new AbortController();
  let ownershipLost = false;
  let ownershipVisible = false;
  const setOwnershipVisible = (visible) => {
    if (ownershipVisible === visible) return;
    ownershipVisible = visible;
    onOwnershipChange(visible);
  };
  const assertOwnershipHeld = () => {
    if (ownershipLost) throw new Error('Paykit reader ownership was lost');
  };
  const abortLifecycle = () => lifecycleController.abort();
  signal.addEventListener('abort', abortLifecycle, { once: true });
  if (ownership.lost) {
    void ownership.lost.then(() => {
      ownershipLost = true;
      lifecycleController.abort();
      setOwnershipVisible(false);
    });
  }
  const publishWorkerStatus = async (status) => {
    assertOwnershipHeld();
    await writeWorkerStatus(status);
    assertOwnershipHeld();
  };
  try {
    await publishWorkerStatus({ version: 1, state: 'starting' });
    if (lifecycleController.signal.aborted) return { status: 'stopped' };
    setOwnershipVisible(true);
    const result = await runOwnedWorker({
      signal: lifecycleController.signal,
      runOperation,
      writePreparedStatus,
      writeWorkerStatus: publishWorkerStatus,
      wait,
      assertOwnershipHeld,
    });
    assertOwnershipHeld();
    return result;
  } finally {
    signal.removeEventListener('abort', abortLifecycle);
    setOwnershipVisible(false);
    await ownership.release();
  }
}

async function runOwnedWorker({
  signal,
  runOperation,
  writePreparedStatus,
  writeWorkerStatus,
  wait,
  assertOwnershipHeld,
}) {
  assertOwnershipHeld();
  if (signal.aborted) return { status: 'stopped' };
  let prepared;
  try {
    const result = await runOperation({ operation: 'prepare', signal });
    assertOwnershipHeld();
    if (signal.aborted) return { status: 'stopped' };
    if (result?.status !== 'success') {
      const failed = failedStatus(result);
      await writeWorkerStatus(failed);
      return { status: 'failed', error: failed.error };
    }
    prepared = result.value;
    assertOwnershipHeld();
    await writePreparedStatus(prepared);
    assertOwnershipHeld();
    if (signal.aborted) return { status: 'stopped' };
    await writeWorkerStatus({
      version: 1,
      state: 'waiting',
      reader_pubky: prepared.reader_pubky,
    });
  } catch {
    await writeWorkerStatus({ version: 1, state: 'failed', error: 'worker_failed' });
    return { status: 'failed', error: 'worker_failed' };
  }

  let retryMs = INITIAL_RETRY_MS;
  let lastPaymentRequestId = null;
  while (!signal.aborted) {
    let result;
    try {
      result = await runOperation({ operation: 'receive', signal });
    } catch {
      result = { status: 'failed', error: 'worker_failed' };
    }
    assertOwnershipHeld();
    if (signal.aborted) return { status: 'stopped' };
    if (result?.status === 'success') {
      if (result.value.payment_request_id === lastPaymentRequestId) {
        await wait(retryMs, signal);
        retryMs = Math.min(retryMs * 2, MAX_RETRY_MS);
        continue;
      }
      await writeWorkerStatus(receivedStatus(prepared.reader_pubky, result.value));
      lastPaymentRequestId = result.value.payment_request_id;
      retryMs = INITIAL_RETRY_MS;
      continue;
    }

    const error = result?.status === 'timeout'
      ? 'receive_timeout'
      : result?.error ?? 'worker_failed';
    if (!RETRYABLE_ERRORS.has(error)) {
      const terminalError = TERMINAL_ERRORS.has(error) ? error : 'worker_failed';
      await writeWorkerStatus({ version: 1, state: 'failed', error: terminalError });
      return { status: 'failed', error: terminalError };
    }
    if (lastPaymentRequestId === null) {
      await writeWorkerStatus({
        version: 1,
        state: 'retrying',
        reader_pubky: prepared.reader_pubky,
        error,
      });
    }
    await wait(retryMs, signal);
    retryMs = Math.min(retryMs * 2, MAX_RETRY_MS);
  }
  return { status: 'stopped' };
}

function failedStatus(result) {
  const candidate = result?.status === 'timeout'
    ? 'prepare_timeout'
    : result?.error ?? 'worker_failed';
  const error = TERMINAL_ERRORS.has(candidate) ? candidate : 'worker_failed';
  return { version: 1, state: 'failed', error };
}

function receivedStatus(readerPubky, value) {
  return {
    version: 1,
    state: 'request_received',
    reader_pubky: readerPubky,
    payment_request_id: value.payment_request_id,
    address: value.address,
    asset: value.asset,
    amount_sats: value.amount_sats,
    payment_command: value.payment_command,
    optional_mining_command: value.optional_mining_command,
  };
}

async function waitForOwnershipReady(holder) {
  let resolveClosed;
  const closed = new Promise((resolve) => { resolveClosed = resolve; });
  holder.once('close', resolveClosed);
  let stdout = '';
  let stderrBytes = 0;
  let timeout;
  const ready = new Promise((resolveReady, rejectReady) => {
    const rejectSpawn = () => rejectReady(new Error('Paykit reader ownership holder failed'));
    holder.once('error', rejectSpawn);
    holder.stderr.on('data', (chunk) => {
      stderrBytes += chunk.length;
      if (stderrBytes > 1_024) rejectReady(new Error('Paykit reader ownership holder failed'));
    });
    holder.stdout.on('data', (chunk) => {
      stdout += chunk.toString('utf8');
      if (stdout === OWNERSHIP_READY) {
        holder.removeListener('error', rejectSpawn);
        resolveReady();
      } else if (stdout.length > OWNERSHIP_READY.length || !OWNERSHIP_READY.startsWith(stdout)) {
        rejectReady(new Error('Paykit reader ownership holder failed'));
      }
    });
    timeout = setTimeout(
      () => rejectReady(new Error('Paykit reader ownership acquisition timed out')),
      OWNERSHIP_ACQUIRE_TIMEOUT_MS,
    );
  });
  try {
    await Promise.race([
      ready,
      closed.then(() => { throw new Error('Paykit reader state is already owned by another process'); }),
    ]);
    clearTimeout(timeout);
    return { closed };
  } catch (error) {
    clearTimeout(timeout);
    await stopOwnershipHolder(holder, closed);
    throw error;
  }
}

async function stopOwnershipHolder(holder, closed) {
  if (holder.exitCode !== null || holder.signalCode !== null) return;
  holder.stdin.end();
  if (await closesWithin(closed, OWNERSHIP_RELEASE_TIMEOUT_MS)) return;
  holder.kill('SIGTERM');
  if (await closesWithin(closed, OWNERSHIP_KILL_GRACE_MS)) return;
  if (holder.exitCode !== null || holder.signalCode !== null) return;
  holder.kill('SIGKILL');
  await closed;
}

async function closesWithin(closed, milliseconds) {
  let timer;
  const result = await Promise.race([
    closed.then(() => true),
    new Promise((resolve) => { timer = setTimeout(() => resolve(false), milliseconds); }),
  ]);
  clearTimeout(timer);
  return result;
}

function waitForDelay(milliseconds, signal) {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const timer = setTimeout(resolve, milliseconds);
    signal.addEventListener('abort', () => {
      clearTimeout(timer);
      resolve();
    }, { once: true });
  });
}
