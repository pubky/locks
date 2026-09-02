#!/usr/bin/env node
import {
  printReaderSuccess,
  readerResultCategory,
  runReaderOperation,
} from './lib/paykit-reader-helper.mjs';
import {
  acquirePaykitReaderOwnership,
  assertStandaloneReaderOperationAllowed,
} from './lib/paykit-reader-worker.mjs';

const role = 'content-viewer';

export async function main({
  runOperation = runReaderOperation,
  printSuccess = printReaderSuccess,
  printError = console.error,
  acquireOwnership = acquirePaykitReaderOwnership,
} = {}) {
  assertStandaloneReaderOperationAllowed();
  const ownership = await acquireOwnership();
  try {
    const result = await runOperation({ operation: 'receive' });
    const category = readerResultCategory('receive', result);
    if (category.stream === 'stdout') printSuccess('receive', category.value);
    else printError(category.message);
    return category.exitCode;
  } finally {
    await ownership.release();
  }
}

main()
  .then((code) => { process.exitCode = code; })
  .catch(() => {
    console.error(`Paykit reader receive could not start for ${role}.`);
    process.exitCode = 2;
  });
