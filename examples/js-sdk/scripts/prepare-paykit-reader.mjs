#!/usr/bin/env node
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  printReaderSuccess,
  readerResultCategory,
  runReaderOperation,
} from './lib/paykit-reader-helper.mjs';
import { clearPreparedReaderStatus, writePreparedReaderStatus } from './lib/paykit-reader-status.mjs';
import {
  acquirePaykitReaderOwnership,
  assertStandaloneReaderOperationAllowed,
} from './lib/paykit-reader-worker.mjs';

const role = 'content-viewer';

export async function main({
  runOperation = runReaderOperation,
  clearStatus = clearPreparedReaderStatus,
  writeStatus = writePreparedReaderStatus,
  printSuccess = printReaderSuccess,
  printError = console.error,
  acquireOwnership = acquirePaykitReaderOwnership,
} = {}) {
  assertStandaloneReaderOperationAllowed();
  const ownership = await acquireOwnership();
  try {
    await clearStatus();
    const result = await runOperation({ operation: 'prepare' });
    const category = readerResultCategory('prepare', result);
    if (category.stream === 'stdout') {
      await writeStatus(category.value);
      printSuccess('prepare', category.value);
    } else {
      printError(category.message);
    }
    return category.exitCode;
  } finally {
    await ownership.release();
  }
}

const isMain = process.argv[1]
  && resolve(process.argv[1]) === resolve(fileURLToPath(import.meta.url));
if (isMain) {
  main()
    .then((code) => { process.exitCode = code; })
    .catch(() => {
      console.error(`Paykit reader prepare could not start for ${role}.`);
      process.exitCode = 2;
    });
}
