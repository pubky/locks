import { PublicKey } from './node_modules/@synonymdev/pubky/index.js';

export function validateExternalReaderPubky(readerPubky, creatorPubky) {
  const normalized = typeof readerPubky === 'string' ? readerPubky.trim() : '';
  const canonicalReader = canonicalPubky(normalized);
  if (!canonicalReader || canonicalReader !== normalized) {
    throw new Error('Enter the canonical Bitkit reader Pubky.');
  }
  const canonicalCreator = canonicalPubky(creatorPubky);
  if (!canonicalCreator || canonicalReader === canonicalCreator) {
    throw new Error('Creator and reader require distinct Bitkit identities.');
  }
  return canonicalReader;
}

function canonicalPubky(value) {
  if (typeof value !== 'string') return null;
  let publicKey;
  try {
    publicKey = PublicKey.from(value);
    return publicKey.toString();
  } catch {
    return null;
  } finally {
    publicKey?.free();
  }
}

export async function checkExternalReaderPaykitData({
  readerPubky,
  creatorPubky,
  lookup,
}) {
  const validated = validateExternalReaderPubky(readerPubky, creatorPubky);
  try {
    const present = await lookup(validated);
    return present
      ? {
        state: 'present',
        readerPubky: validated,
        canSubmit: true,
        message: 'Paykit v0 data is present. Invoice creation will validate the usable Bitkit receiver.',
      }
      : {
        state: 'absent',
        readerPubky: validated,
        canSubmit: false,
        message: 'No Paykit v0 data found. Enable Paykit in Bitkit, then retry.',
      };
  } catch {
    return {
      state: 'unavailable',
      readerPubky: validated,
      canSubmit: false,
      message: 'Paykit data lookup is unavailable. Retry.',
    };
  }
}

export function createPaykitDataCheckController() {
  let generation = 0;
  return {
    invalidate() {
      generation += 1;
    },
    async check({
      incarnation,
      resource,
      creatorPubky,
      readerPubky,
      lookup,
      isCurrent,
    }) {
      const requestGeneration = ++generation;
      const snapshot = Object.freeze({
        incarnation,
        resource,
        creatorPubky,
        readerPubky,
      });
      const result = await checkExternalReaderPaykitData({
        readerPubky,
        creatorPubky,
        lookup,
      });
      if (requestGeneration !== generation || !isCurrent(snapshot)) return null;
      return result;
    },
  };
}
