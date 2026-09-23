import { contentCreatorSessionPath, demoConfigPath } from './paths.mjs';

export function readDemoConfigPath(env = process.env) {
  const configured = env.LOCKS_DEMO_CONFIG_PATH?.trim();
  return configured || demoConfigPath;
}

export function resolveCreatorSessionOptions({
  mode,
  env = process.env,
  externalWallet = false,
} = {}) {
  if (mode === 'staging') {
    const sessionPath = env.LOCKS_DEMO_CREATOR_SESSION_PATH?.trim();
    if (!sessionPath) throw new Error('staging Creator session path is required');
    return { sessionPath, profilePath: null };
  }
  if (externalWallet) return { sessionPath: contentCreatorSessionPath, profilePath: null };
  return { sessionPath: contentCreatorSessionPath };
}
