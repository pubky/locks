export function pkarrRelaysForDemoConfig(config) {
  if (config?.mode === 'staging') return [];
  const relay = config?.testnet?.pkarrRelay;
  if (typeof relay !== 'string' || relay.length === 0) {
    throw new Error('demo config is missing PKARR relay');
  }
  return [relay];
}

export function demoAuthRelayForConfig(config) {
  if (config?.mode === 'staging') return undefined;
  const relay = config?.testnet?.httpRelay;
  if (typeof relay !== 'string' || relay.length === 0) {
    throw new Error('demo config is missing HTTP auth relay');
  }
  const url = new URL(relay);
  const normalizedPath = url.pathname.endsWith('/') ? url.pathname : `${url.pathname}/`;
  if (!normalizedPath.endsWith('/inbox/')) {
    url.pathname = `${normalizedPath}inbox/`.replace(/\/+/g, '/');
  }
  return url.toString();
}
