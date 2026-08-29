#!/usr/bin/env node
import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { createInterface } from 'node:readline/promises';
import { stdin, stdout } from 'node:process';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';

import { parseArgs, requiredRole } from './lib/paths.mjs';
import { readDemoConfig, withInternalServiceUrls } from './lib/config.mjs';
import { loadRoleSecret } from './lib/pubky.mjs';

const DEFAULT_HELPER_PATH = '/usr/local/bin/paykit-companion-auth';
const COMPOSE_HELPER_PATH = fileURLToPath(new URL('./paykit-companion-auth-compose.sh', import.meta.url));
const DEFAULT_TIMEOUT_MS = 240_000;
const DEFAULT_KILL_GRACE_MS = 2_000;
const MAX_INPUT_BYTES = 16 * 1024;
const MAX_CAPTURE_BYTES = 4 * 1024;

function parseAccountIndex(value) {
  const text = String(value).trim();
  if (!/^(0|[1-9][0-9]*)$/.test(text)) {
    throw new Error('account index must be an unsigned decimal integer');
  }
  const accountIndex = Number(text);
  if (!Number.isSafeInteger(accountIndex) || accountIndex > 0xffff_ffff) {
    throw new Error('account index is outside the JSON u32 range');
  }
  return accountIndex;
}

function normalizeInput(companionHandle, accountXpub, accountIndex) {
  const normalizedCompanionHandle = String(companionHandle).trim();
  const normalizedAccountXpub = String(accountXpub).trim();
  let decodedHandle;
  try {
    decodedHandle = Buffer.from(normalizedCompanionHandle, 'base64url');
  } catch {
    decodedHandle = Buffer.alloc(0);
  }
  if (
    decodedHandle.length !== 32
    || decodedHandle.toString('base64url') !== normalizedCompanionHandle
    || !normalizedAccountXpub
  ) {
    decodedHandle.fill(0);
    throw new Error('Paykit input requires three ordered lines');
  }
  decodedHandle.fill(0);
  return {
    companionHandle: normalizedCompanionHandle,
    accountXpub: normalizedAccountXpub,
    accountIndex: parseAccountIndex(accountIndex),
  };
}

export function parsePaykitInputLines(value) {
  if (typeof value !== 'string' || Buffer.byteLength(value, 'utf8') > MAX_INPUT_BYTES) {
    throw new Error('Paykit input requires three ordered lines');
  }
  const lines = value.split(/\r?\n/);
  if (lines.at(-1) === '') lines.pop();
  if (lines.length !== 3) {
    throw new Error('Paykit input requires three ordered lines');
  }
  return normalizeInput(lines[0], lines[1], lines[2]);
}

async function readAll(stream) {
  const chunks = [];
  let size = 0;
  for await (const chunk of stream) {
    size += chunk.length;
    if (size > MAX_INPUT_BYTES) {
      chunk.fill(0);
      for (const buffered of chunks) buffered.fill(0);
      throw new Error('Paykit input requires three ordered lines');
    }
    chunks.push(chunk);
  }
  const bytes = Buffer.concat(chunks);
  try {
    return bytes.toString('utf8');
  } finally {
    bytes.fill(0);
    for (const chunk of chunks) chunk.fill(0);
  }
}

export async function collectPaykitInputs({
  isTTY = stdin.isTTY,
  question,
  readInput = () => readAll(stdin),
} = {}) {
  if (!isTTY) return parsePaykitInputLines(await readInput());
  if (typeof question !== 'function') throw new Error('interactive input is unavailable');
  return normalizeInput(
    await question('Paste Paykit companion handle: '),
    await question('Paste account xpub/tpub: '),
    await question('Account index: '),
  );
}

export function buildCompanionHelperInput({ companionHandle, accountXpub, accountIndex, creatorSecret }) {
  if (!(creatorSecret instanceof Uint8Array) || creatorSecret.length !== 32) {
    throw new Error('creator recovery file must contain a 32-byte secret');
  }
  const secretView = Buffer.from(
    creatorSecret.buffer,
    creatorSecret.byteOffset,
    creatorSecret.byteLength,
  );
  return {
    version: 1,
    companion_handle: companionHandle,
    creator_secret: secretView.toString('base64url'),
    account_xpub: accountXpub,
    account_index: accountIndex,
  };
}

export function requirePaykitCreatorRole(role) {
  if (role !== 'content-creator') {
    throw new Error('Paykit companion authentication requires --role content-creator');
  }
  return role;
}

export function companionResultCategory(result) {
  if (result?.status === 'approved') {
    return { exitCode: 0, stream: 'stdout', message: 'Paykit companion authentication approved.' };
  }
  if (result?.status === 'timeout') {
    return { exitCode: 1, stream: 'stderr', message: 'Paykit companion authentication timed out.' };
  }
  return { exitCode: 1, stream: 'stderr', message: 'Paykit companion authentication failed.' };
}

export function resolveCompanionHelperPath({
  env = process.env,
  nativeHelperPath = DEFAULT_HELPER_PATH,
  composeHelperPath = COMPOSE_HELPER_PATH,
  nativeHelperAvailable = existsSync,
} = {}) {
  if (typeof env.PAYKIT_COMPANION_AUTH_BIN === 'string' && env.PAYKIT_COMPANION_AUTH_BIN) {
    return env.PAYKIT_COMPANION_AUTH_BIN;
  }
  return nativeHelperAvailable(nativeHelperPath) ? nativeHelperPath : composeHelperPath;
}

export async function runBoundedHelper({
  helperPath,
  helperArgs = [],
  input,
  classifyClose,
  timeoutMs,
  killGraceMs = DEFAULT_KILL_GRACE_MS,
  signal,
  spawnProcess = spawn,
  spawnEnvironment,
}) {
  if (typeof helperPath !== 'string' || typeof classifyClose !== 'function') {
    throw new Error('invalid helper process contract');
  }
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0 || !Number.isFinite(killGraceMs) || killGraceMs <= 0) {
    throw new Error('invalid helper deadline');
  }
  if (!Array.isArray(helperArgs) || helperArgs.length > 8 || helperArgs.some((value) => typeof value !== 'string' || value.includes('\0'))) {
    throw new Error('invalid helper arguments');
  }
  if (signal?.aborted) return { status: 'failed' };

  const payload = Buffer.from(`${JSON.stringify(input)}\n`, 'utf8');
  if (payload.length > MAX_INPUT_BYTES) {
    payload.fill(0);
    throw new Error('helper input is too large');
  }

  return new Promise((resolveResult) => {
    let child;
    try {
      const spawnOptions = {
        stdio: ['pipe', 'pipe', 'pipe'],
        shell: false,
      };
      if (spawnEnvironment) spawnOptions.env = spawnEnvironment;
      child = spawnProcess(helperPath, helperArgs, spawnOptions);
    } catch {
      payload.fill(0);
      resolveResult({ status: 'failed' });
      return;
    }
    const stdoutChunks = [];
    const stderrChunks = [];
    let capturedBytes = 0;
    let timedOut = false;
    let aborted = false;
    let overflowed = false;
    let settled = false;
    let spawned = false;
    let terminating = false;
    let processErrored = false;
    let killTimer;
    let forceSettleTimer;

    function terminate() {
      if (terminating) return;
      terminating = true;
      try {
        child.kill('SIGTERM');
      } catch {}
      if (!killTimer) {
        killTimer = setTimeout(() => {
          try {
            child.kill('SIGKILL');
          } catch {}
          forceSettleTimer = setTimeout(() => {
            child.stdin.destroy();
            child.stdout.destroy();
            child.stderr.destroy();
            child.unref();
            settle(timedOut ? { status: 'timeout' } : { status: 'failed' });
          }, killGraceMs);
        }, killGraceMs);
      }
    }

    const deadline = setTimeout(() => {
      timedOut = true;
      terminate();
    }, timeoutMs);

    const abort = () => {
      aborted = true;
      terminate();
    };
    if (signal?.aborted) abort();
    else signal?.addEventListener('abort', abort, { once: true });

    function settle(result) {
      if (settled) return;
      settled = true;
      clearTimeout(deadline);
      clearTimeout(killTimer);
      clearTimeout(forceSettleTimer);
      signal?.removeEventListener('abort', abort);
      payload.fill(0);
      for (const chunk of [...stdoutChunks, ...stderrChunks]) chunk.fill(0);
      resolveResult(result);
    }

    function capture(target, chunk) {
      const bytes = Buffer.from(chunk);
      capturedBytes += bytes.length;
      if (capturedBytes > MAX_CAPTURE_BYTES) {
        bytes.fill(0);
        overflowed = true;
        terminate();
        return;
      }
      target.push(bytes);
    }

    child.stdout.on('data', (chunk) => capture(stdoutChunks, chunk));
    child.stderr.on('data', (chunk) => capture(stderrChunks, chunk));
    child.stdin.on('error', () => {});
    child.once('spawn', () => { spawned = true; });
    child.on('error', () => {
      if (!spawned) settle({ status: 'failed' });
      else {
        processErrored = true;
        terminate();
      }
    });
    child.on('close', (code, signal) => {
      const stdoutBytes = Buffer.concat(stdoutChunks);
      const stderrBytes = Buffer.concat(stderrChunks);
      let result = { status: 'failed' };
      if (!overflowed && !processErrored) {
        try {
          result = classifyClose({ code, signal, stdout: stdoutBytes, stderr: stderrBytes });
        } catch {}
      }
      stdoutBytes.fill(0);
      stderrBytes.fill(0);
      if (timedOut) settle({ status: 'timeout' });
      else if (aborted) settle({ status: 'failed' });
      else settle(result);
    });
    child.stdin.end(payload, () => payload.fill(0));
  });
}

export async function runCompanionHelper({
  helperPath = resolveCompanionHelperPath(),
  input,
  paykitServerUrl,
  timeoutMs = DEFAULT_TIMEOUT_MS,
  killGraceMs = DEFAULT_KILL_GRACE_MS,
  spawnProcess = spawn,
}) {
  const serverUrl = new URL(paykitServerUrl);
  if (
    !['http:', 'https:'].includes(serverUrl.protocol)
    || serverUrl.username
    || serverUrl.password
    || serverUrl.origin !== paykitServerUrl
  ) {
    throw new Error('invalid Paykit Server URL');
  }
  return runBoundedHelper({
    helperPath,
    input,
    timeoutMs,
    killGraceMs,
    spawnProcess,
    spawnEnvironment: { ...process.env, PAYKIT_SERVER_URL: paykitServerUrl },
    classifyClose: ({ code, signal, stdout, stderr }) => (
      code === 0
      && signal === null
      && stderr.length === 0
      && stdout.equals(Buffer.from('{"version":1,"status":"approved"}\n'))
        ? { status: 'approved' }
        : { status: 'failed' }
    ),
  });
}

async function main() {
  const args = parseArgs();
  const role = requirePaykitCreatorRole(requiredRole(args));

  let readline;
  let creatorSecret;
  let helperInput;
  try {
    const serviceConfig = withInternalServiceUrls(await readDemoConfig());
    if (stdin.isTTY) readline = createInterface({ input: stdin, output: stdout });
    const values = await collectPaykitInputs({
      isTTY: stdin.isTTY,
      question: readline ? (prompt) => readline.question(prompt) : undefined,
    });
    creatorSecret = await loadRoleSecret(role);
    helperInput = buildCompanionHelperInput({ ...values, creatorSecret });
    const result = await runCompanionHelper({
      input: helperInput,
      paykitServerUrl: serviceConfig.paykit.url,
    });
    const category = companionResultCategory(result);
    if (category.stream === 'stdout') console.log(category.message);
    else console.error(category.message);
    return category.exitCode;
  } finally {
    readline?.close();
    creatorSecret?.fill(0);
    if (helperInput) helperInput.creator_secret = '';
    if (helperInput) helperInput.companion_handle = '';
  }
}

const isMain = process.argv[1]
  && resolve(process.argv[1]) === resolve(fileURLToPath(import.meta.url));
if (isMain) {
  main()
    .then((code) => { process.exitCode = code; })
    .catch(() => {
      console.error('Paykit companion authentication could not start.');
      process.exitCode = 2;
    });
}
