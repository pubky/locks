export async function enforceCreatorIdentityMatch({ session, expectedCreatorPubky }) {
  const authenticatedCreatorPubky = session.creatorPubky();
  if (authenticatedCreatorPubky === expectedCreatorPubky) return;

  await session.signout();
  throw new Error('Lock Server creator does not match the demo creator; authenticate both flows with the same identity');
}

export async function commitIdentityScopedCreatorSession({
  state,
  sessionSecret,
  expectedCreatorPubky,
  expectedIdentityGeneration,
  expectedConnectState,
  revokeSession,
}) {
  const stale = state.creatorPubky !== expectedCreatorPubky
    || state.creatorIdentityGeneration !== expectedIdentityGeneration
    || state.pendingConnectState !== expectedConnectState;
  if (stale) {
    try {
      await revokeSession(sessionSecret);
      return { accepted: false, revoked: true };
    } catch {
      return { accepted: false, revoked: false };
    }
  }

  state.feLockSessionToken = sessionSecret;
  state.lockAuthenticated = true;
  return { accepted: true, revoked: false };
}

export async function invalidateIdentityScopedCreatorState({ state, revokeSession }) {
  const sessionSecret = state.feLockSessionToken;
  state.feLockSessionToken = null;
  state.lockAuthenticated = false;
  state.pendingConnectState = null;
  state.lockServerOrigin = null;
  state.lockAuthFrame = null;

  if (!sessionSecret) return { revoked: false };
  try {
    await revokeSession(sessionSecret);
    return { revoked: true };
  } catch {
    return { revoked: false };
  }
}
