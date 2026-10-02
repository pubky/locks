import { readFileSync } from 'node:fs';

const compose = readFileSync(
  new URL('../../../../compose.paykit-local-demo.yaml', import.meta.url),
  'utf8',
);
const source = compose.match(
  /context: "\$\{PAYKIT_SERVER_CONTEXT:-(https:\/\/github\.com\/pubky\/paykit-server\.git#([0-9]+\.[0-9]+\.[0-9]+-rc[0-9]+))\}"/,
);
if (!source) throw new Error('Compose is missing the canonical Paykit Server source');

export const DEFAULT_PAYKIT_SERVER_CONTEXT = source[1];
export const PAYKIT_SERVER_REF = source[2];