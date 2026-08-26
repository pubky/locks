export async function enforceCreatorIdentityMatch({ session, expectedCreatorPubky }) {
  const authenticatedCreatorPubky = session.creatorPubky();
  if (authenticatedCreatorPubky === expectedCreatorPubky) return;

  await session.signout();
  throw new Error('Lock Server creator does not match the demo creator; authenticate both flows with the same identity');
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
