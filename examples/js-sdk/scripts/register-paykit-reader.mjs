#!/usr/bin/env node
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { signupReaderBestEffort } from './lib/paykit-reader-helper.mjs';

const MAX_INPUT_BYTES = 1_024;

export async function main({ input = process.stdin, signup = signupReaderBestEffort } = {}) {
  const request = await readRequest(input);
  if (
    request?.version !== 1
    || request.operation !== 'register'
    || Object.keys(request).length !== 2
  ) {
    throw new Error('invalid registration input');
  }
  await signup();
  process.stdout.write('{"version":1,"status":"registered"}\n');
}

async function readRequest(input) {
  const chunks = [];
  let length = 0;
  for await (const chunk of input) {
    length += chunk.length;
    if (length > MAX_INPUT_BYTES) throw new Error('registration input is too large');
    chunks.push(chunk);
  }
  const body = Buffer.concat(chunks).toString('utf8');
  if (!body.endsWith('\n') || body.slice(0, -1).includes('\n') || body.includes('\r')) {
    throw new Error('invalid registration input');
  }
  return JSON.parse(body.slice(0, -1));
}

const isMain = process.argv[1]
  && resolve(process.argv[1]) === resolve(fileURLToPath(import.meta.url));
if (isMain) {
  main().catch(() => {
    process.stderr.write('{"version":1,"error":"registration_failed"}\n');
    process.exitCode = 1;
  });
}
