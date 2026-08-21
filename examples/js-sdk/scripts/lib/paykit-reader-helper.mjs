import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import { runBoundedHelper } from '../authenticate-paykit.mjs';
import { readJson } from './paths.mjs';
import { readDemoConfig, withInternalServiceUrls } from './config.mjs';
import {
  homeserverPublicKey,
  loadRoleKeypair,
  loadRoleProfile,
  loadRoleSecret,
  pubkyForConfig,
  signupBestEffort,
} from './pubky.mjs';

const DEFAULT_HELPER_PATH = '/usr/local/bin/paykit-reader-demo';
const PREPARE_TIMEOUT_MS = 120_000;
const RECEIVE_TIMEOUT_MS = 310_000;
const REGISTRATION_TIMEOUT_MS = 30_000;
const REGISTRATION_SCRIPT = fileURLToPath(new URL('../register-paykit-reader.mjs', import.meta.url));
const COMPOSE_FILE = 'compose.paykit-local-demo.yaml';
const COMPOSE_COMMAND = `docker compose --file ${COMPOSE_FILE}`;
const OPERATOR_BITCOIN_CLI = 'bitcoin-cli -conf=/home/bitcoin/.bitcoin/bitcoin.conf -regtest -rpcwallet=miner';
const REQUIRED_ENV = [
  'PAYKIT_READER_STATE_PATH',
  'PAYKIT_READER_PUBKY_TESTNET_HOST',
  'PAYKIT_READER_RECEIVER_PATH',
  'PAYKIT_READER_SERVER_PUBKY',
  'PAYKIT_READER_SERVER_PATH',
];
const MINING_COMMAND = "docker compose exec -T bitcoin sh -ec 'bitcoin-cli -conf=\"$BITCOIN_DATA/bitcoin.conf\" -regtest -rpcwallet=miner generatetoaddress 6 \"$(bitcoin-cli -conf=\"$BITCOIN_DATA/bitcoin.conf\" -regtest -rpcwallet=miner getnewaddress)\"'";
const OPERATOR_MINING_COMMAND = `${COMPOSE_COMMAND} exec -T bitcoin sh -ec '${OPERATOR_BITCOIN_CLI} generatetoaddress 6 $(${OPERATOR_BITCOIN_CLI} getnewaddress)'`;
const FAILURE_CODES = new Set([
  'invalid_input',
  'invalid_config',
  'invalid_state',
  'protocol_failed',
  'receive_timeout',
  'output_failed',
]);

function exactKeys(value, expected) {
  return value
    && typeof value === 'object'
    && !Array.isArray(value)
    && Object.keys(value).length === expected.length
    && expected.every((key) => Object.hasOwn(value, key));
}

function parseOneJsonLine(stdout) {
  if (typeof stdout !== 'string' || !stdout.endsWith('\n')) {
    throw new Error('invalid reader helper output');
  }
  const body = stdout.slice(0, -1);
  if (!body || body.includes('\n') || body.includes('\r')) {
    throw new Error('invalid reader helper output');
  }
  try {
    return JSON.parse(body);
  } catch {
    throw new Error('invalid reader helper output');
  }
}

function isCanonicalPubky(value) {
  return typeof value === 'string'
    && /^pubky[ybndrfg8ejkmcpqxot1uwisza345h769]{52}$/.test(value);
}

function isReceiverPath(value) {
  return typeof value === 'string'
    && value.length <= 255
    && /^[A-Za-z0-9._-]+(?:\/[A-Za-z0-9._-]+)*$/.test(value);
}

function btcToSats(value) {
  if (!/^(0|[1-9][0-9]*)(?:\.[0-9]{1,8})?$/.test(value)) {
    throw new Error('invalid reader helper output');
  }
  const [whole, fraction = ''] = value.split('.');
  return (BigInt(whole) * 100_000_000n) + BigInt(fraction.padEnd(8, '0'));
}

export function buildReaderHelperInput({ operation, readerSecret }) {
  if (!['prepare', 'receive'].includes(operation)) {
    throw new Error('invalid reader helper operation');
  }
  if (!(readerSecret instanceof Uint8Array) || readerSecret.length !== 32) {
    throw new Error('reader recovery file must contain a 32-byte secret');
  }
  const secretView = Buffer.from(
    readerSecret.buffer,
    readerSecret.byteOffset,
    readerSecret.byteLength,
  );
  return {
    version: 1,
    operation,
    reader_secret: secretView.toString('base64url'),
  };
}

export function parseReaderHelperSuccess({ operation, stdout }) {
  const value = parseOneJsonLine(stdout);
  if (operation === 'prepare') {
    const keys = ['version', 'status', 'reader_pubky', 'receiver_path'];
    if (
      !exactKeys(value, keys)
      || value.version !== 1
      || value.status !== 'prepared'
      || !isCanonicalPubky(value.reader_pubky)
      || !isReceiverPath(value.receiver_path)
    ) {
      throw new Error('invalid reader helper output');
    }
    return value;
  }
  if (operation !== 'receive') throw new Error('invalid reader helper output');

  const keys = [
    'version',
    'status',
    'payment_request_id',
    'address',
    'asset',
    'amount_sats',
    'payment_command',
    'optional_mining_command',
  ];
  if (
    !exactKeys(value, keys)
    || value.version !== 1
    || value.status !== 'received'
    || !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value.payment_request_id)
    || !/^bcrt1[02-9ac-hj-np-z]{8,86}$/.test(value.address)
    || value.asset !== 'btc'
    || !/^[1-9][0-9]*$/.test(value.amount_sats)
    || value.optional_mining_command !== MINING_COMMAND
  ) {
    throw new Error('invalid reader helper output');
  }
  const paymentMatch = /^docker compose exec -T bitcoin sh -ec 'bitcoin-cli -conf="\$BITCOIN_DATA\/bitcoin\.conf" -regtest -rpcwallet=miner sendtoaddress "([^"]+)" "((?:0|[1-9][0-9]*)(?:\.[0-9]{1,8})?)"'$/.exec(value.payment_command);
  if (
    !paymentMatch
    || paymentMatch[1] !== value.address
    || btcToSats(paymentMatch[2]) !== BigInt(value.amount_sats)
  ) {
    throw new Error('invalid reader helper output');
  }
  return {
    ...value,
    asset: 'BTC',
    payment_command: `${COMPOSE_COMMAND} exec -T bitcoin sh -ec '${OPERATOR_BITCOIN_CLI} sendtoaddress ${value.address} ${paymentMatch[2]}'`,
    optional_mining_command: OPERATOR_MINING_COMMAND,
  };
}

export function validateReaderOperatorResult(value) {
  const payment = /^docker compose --file compose\.paykit-local-demo\.yaml exec -T bitcoin sh -ec 'bitcoin-cli -conf=\/home\/bitcoin\/\.bitcoin\/bitcoin\.conf -regtest -rpcwallet=miner sendtoaddress (bcrt1[02-9ac-hj-np-z]{8,86}) ((?:0|[1-9][0-9]*)(?:\.[0-9]{1,8})?)'$/.exec(value?.payment_command);
  if (
    !value
    || !payment
    || payment[1] !== value.address
    || btcToSats(payment[2]) !== BigInt(value.amount_sats)
    || value.optional_mining_command !== OPERATOR_MINING_COMMAND
  ) {
    throw new Error('invalid reader helper output');
  }
  return value;
}

function parseReaderFailure(stderr) {
  const value = parseOneJsonLine(stderr);
  if (!exactKeys(value, ['version', 'error']) || value.version !== 1 || !FAILURE_CODES.has(value.error)) {
    throw new Error('invalid reader helper output');
  }
  return value.error;
}

export function requireReaderEnvironment(env = process.env) {
  if (REQUIRED_ENV.some((name) => typeof env[name] !== 'string' || env[name].length === 0)) {
    throw new Error('Paykit reader helper environment is incomplete');
  }
  if (!env.PAYKIT_READER_STATE_PATH.replaceAll('\\', '/').endsWith('/.local/paykit-reader/state.v1')) {
    throw new Error('Paykit reader state path must end in .local/paykit-reader/state.v1');
  }
}

export async function readReaderCreatorProfile({
  env = process.env,
  readProfile = readJson,
  loadProfile = loadRoleProfile,
} = {}) {
  return env.PAYKIT_READER_CREATOR_PROFILE_PATH
    ? readProfile(env.PAYKIT_READER_CREATOR_PROFILE_PATH)
    : loadProfile('content-creator');
}

export async function resolveReaderEnvironment({
  env = process.env,
  loadProfile = loadRoleProfile,
} = {}) {
  const resolved = { ...env };
  if (!resolved.PAYKIT_READER_SERVER_PUBKY) {
    const profile = await readReaderCreatorProfile({ env: resolved, loadProfile }).catch(() => undefined);
    if (profile?.role !== 'content-creator' || !isCanonicalPubky(profile.pubky)) {
      throw new Error('valid content-creator profile is required for Paykit reader setup');
    }
    resolved.PAYKIT_READER_SERVER_PUBKY = profile.pubky;
  }
  requireReaderEnvironment(resolved);
  return resolved;
}

function readerHelperEnvironment(env) {
  const restricted = Object.fromEntries(REQUIRED_ENV.map((name) => [name, env[name]]));
  if (env.PATH) restricted.PATH = env.PATH;
  return restricted;
}

export async function signupReaderBestEffort({
  readConfig = readDemoConfig,
  normalizeConfig = withInternalServiceUrls,
  loadKeypair = loadRoleKeypair,
  pubkyFactory = pubkyForConfig,
  signup = signupBestEffort,
  getHomeserverPublicKey = homeserverPublicKey,
} = {}) {
  const config = normalizeConfig(await readConfig());
  const keypair = await loadKeypair('content-viewer');
  let signer;
  let session;
  try {
    signer = pubkyFactory(config).signer(keypair);
    session = await signup(signer, getHomeserverPublicKey(config));
  } finally {
    session?.free();
    signer?.free();
    keypair.free();
  }
}

export async function runReaderRegistration({
  signal,
  timeoutMs = REGISTRATION_TIMEOUT_MS,
  spawnProcess = spawn,
  env = process.env,
} = {}) {
  return runBoundedHelper({
    helperPath: process.execPath,
    helperArgs: [REGISTRATION_SCRIPT],
    input: { version: 1, operation: 'register' },
    timeoutMs,
    signal,
    spawnProcess,
    spawnEnvironment: env.PATH ? { PATH: env.PATH } : {},
    classifyClose: ({ code, signal: closeSignal, stdout, stderr }) => {
      if (
        code === 0
        && closeSignal === null
        && stderr.length === 0
        && stdout.toString('utf8') === '{"version":1,"status":"registered"}\n'
      ) {
        return { status: 'success' };
      }
      return { status: 'failed', error: 'protocol_failed' };
    },
  });
}

export function runRegistrationStep({
  ensureRegistered = runReaderRegistration,
  signal,
  timeoutMs = REGISTRATION_TIMEOUT_MS,
} = {}) {
  if (!signal || signal.aborted) return Promise.resolve({ status: 'failed' });
  return (async () => {
    const operationController = new AbortController();
    const operation = Promise.resolve()
      .then(() => ensureRegistered({ signal: operationController.signal, timeoutMs }))
      .then((result) => result ?? { status: 'success' })
      .catch(() => ({ status: 'failed', error: 'protocol_failed' }));
    let timer;
    let abortParent;
    const interrupted = new Promise((resolveInterrupted) => {
      abortParent = () => {
        operationController.abort();
        resolveInterrupted({ status: 'failed' });
      };
      signal.addEventListener('abort', abortParent, { once: true });
      timer = setTimeout(() => {
        operationController.abort();
        resolveInterrupted({ status: 'timeout' });
      }, timeoutMs);
    });
    const outcome = await Promise.race([
      operation.then((result) => ({ kind: 'operation', result })),
      interrupted.then((result) => ({ kind: 'interrupted', result })),
    ]);
    clearTimeout(timer);
    signal.removeEventListener('abort', abortParent);
    if (outcome.kind === 'operation') return outcome.result;
    if (!operationController.signal.aborted) {
      operationController.abort();
    }
    await operation;
    return outcome.result;
  })();
}

export async function runReaderOperation({
  operation,
  helperPath = process.env.PAYKIT_READER_DEMO_BIN || DEFAULT_HELPER_PATH,
  env = process.env,
  readerSecret,
  ensureRegistered = runReaderRegistration,
  spawnProcess = spawn,
  signal = new AbortController().signal,
  timeoutMs = operation === 'receive' ? RECEIVE_TIMEOUT_MS : PREPARE_TIMEOUT_MS,
  registrationTimeoutMs = REGISTRATION_TIMEOUT_MS,
} = {}) {
  if (!['prepare', 'receive'].includes(operation)) throw new Error('invalid reader helper operation');
  const resolvedEnvironment = await resolveReaderEnvironment({ env });
  if (operation === 'prepare') {
    const registration = await runRegistrationStep({
      ensureRegistered,
      signal,
      timeoutMs: registrationTimeoutMs,
    });
    if (registration.status !== 'success') return registration;
  }

  let secret = readerSecret;
  let input;
  try {
    secret ??= await loadRoleSecret('content-viewer');
    input = buildReaderHelperInput({ operation, readerSecret: secret });
    return await runBoundedHelper({
      helperPath,
      input,
      timeoutMs,
      signal,
      spawnProcess,
      spawnEnvironment: readerHelperEnvironment(resolvedEnvironment),
      classifyClose: ({ code, signal, stdout, stderr }) => {
        if (code === 0 && signal === null && stderr.length === 0) {
          return {
            status: 'success',
            value: parseReaderHelperSuccess({ operation, stdout: stdout.toString('utf8') }),
          };
        }
        if (code !== 0 && signal === null && stdout.length === 0) {
          try {
            return { status: 'failed', error: parseReaderFailure(stderr.toString('utf8')) };
          } catch {}
        }
        return { status: 'failed' };
      },
    });
  } finally {
    secret?.fill(0);
    if (input) input.reader_secret = '';
  }
}

export function readerResultCategory(operation, result) {
  if (result?.status === 'success') return { exitCode: 0, stream: 'stdout', value: result.value };
  if (result?.status === 'timeout') {
    return { exitCode: 1, stream: 'stderr', message: `Paykit reader ${operation} timed out.` };
  }
  const suffix = result?.error ? ` (${result.error})` : '';
  return { exitCode: 1, stream: 'stderr', message: `Paykit reader ${operation} failed${suffix}.` };
}

export function printReaderSuccess(operation, value, output = console.log) {
  if (operation === 'prepare') {
    output('Paykit reader prepared.');
    output(`Reader Pubky: ${value.reader_pubky}`);
    output(`Receiver path: ${value.receiver_path}`);
    return;
  }
  output('Paykit Payment Request received.');
  output(`Address: ${value.address}`);
  output(`Amount: ${value.amount_sats} sats`);
  output(`Payment request: ${value.payment_request_id}`);
  output(`Pay: ${value.payment_command}`);
  output(`Optional mining: ${value.optional_mining_command}`);
}
