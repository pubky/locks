export const PAYKIT_SETUP_CALLBACK_TYPE = 'paykit-setup-callback';

export function decidePaykitSetupReadiness(result) {
  if (
    result === null
    || typeof result !== 'object'
    || Array.isArray(result)
    || Object.keys(result).length !== 1
    || typeof result.status !== 'string'
  ) {
    throw new Error('invalid Paykit setup status');
  }
  switch (result.status) {
    case 'ready':
      return { setupComplete: true, openSetup: false, retry: false };
    case 'setup_required':
      return { setupComplete: false, openSetup: true, retry: false };
    case 'unavailable':
      return { setupComplete: false, openSetup: false, retry: true };
    default:
      throw new Error('invalid Paykit setup status');
  }
}

export function buildPaykitSetupRequest({ paykitUrl, returnTo, state }) {
  const paykit = parseHttpUrl(paykitUrl, 'Paykit URL');
  if (paykit.username || paykit.password) {
    throw new Error('Paykit URL must not contain credentials');
  }
  if (paykitUrl !== paykit.origin) {
    throw new Error('Paykit URL must be an exact origin');
  }

  const creatorOrigin = parseHttpUrl(returnTo, 'creator return origin');
  if (returnTo !== creatorOrigin.origin) {
    throw new Error('creator return_to must be an exact origin');
  }
  if (
    typeof state !== 'string'
    || state.length === 0
    || state.length > 512
    || /[\u0000-\u001f\u007f-\u009f]/u.test(state)
  ) {
    throw new Error('Paykit setup state must be a non-empty opaque string without control characters');
  }

  const setupUrl = new URL('/setup', paykit.origin);
  setupUrl.searchParams.set('return_to', creatorOrigin.origin);
  setupUrl.searchParams.set('state', state);
  return { url: setupUrl.toString(), origin: paykit.origin };
}

export function acceptPaykitSetupEvent({
  event,
  expectedOrigin,
  expectedSource,
  expectedState,
  setupCreator,
  currentCreator,
}) {
  if (!isConcreteHttpOrigin(expectedOrigin)) return null;
  if (!expectedSource) return null;
  if (event?.origin !== expectedOrigin || event.source !== expectedSource) return null;
  if (typeof expectedState !== 'string' || !expectedState) return null;
  if (
    typeof setupCreator !== 'string'
    || !setupCreator
    || setupCreator !== currentCreator
  ) return null;

  const data = event.data;
  if (!data || typeof data !== 'object' || Array.isArray(data)) return null;
  if (data.type !== PAYKIT_SETUP_CALLBACK_TYPE || data.state !== expectedState) return null;

  const keys = Object.keys(data).sort();
  if (keys.length === 2 && keys[0] === 'state' && keys[1] === 'type') {
    return { status: 'complete' };
  }
  if (
    keys.length === 3
    && keys[0] === 'error'
    && keys[1] === 'state'
    && keys[2] === 'type'
    && data.error === 'setup-failed'
  ) {
    return { status: 'error', error: 'setup-failed' };
  }
  return null;
}

function parseHttpUrl(value, label) {
  let url;
  try {
    url = new URL(value);
  } catch {
    throw new Error(`${label} must be a valid HTTP(S) URL`);
  }
  if (!['http:', 'https:'].includes(url.protocol) || !url.hostname) {
    throw new Error(`${label} must be a valid HTTP(S) URL`);
  }
  return url;
}

function isConcreteHttpOrigin(value) {
  if (typeof value !== 'string' || value === '*') return false;
  try {
    const url = parseHttpUrl(value, 'Paykit origin');
    return value === url.origin && !url.username && !url.password;
  } catch {
    return false;
  }
}
