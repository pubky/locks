const PUBLIC_KEYS = [
  'resource',
  'guardedResourcePath',
  'lockResources',
  'proofSatisfied',
  'verifierType',
  'loaded',
];

export function buildPersistedReaderState(state) {
  return Object.fromEntries(PUBLIC_KEYS.map((key) => [key, state[key]]));
}

export function restorePersistedReaderState(value) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return {};
  return Object.fromEntries(
    PUBLIC_KEYS
      .filter((key) => Object.hasOwn(value, key))
      .map((key) => [key, value[key]]),
  );
}
