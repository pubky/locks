import { chmod, rm } from 'node:fs/promises';

import {
  contentCreatorSessionPath,
  readJson,
  roleProfilePath,
  writeJson,
} from './paths.mjs';

const defaultProfilePath = roleProfilePath('content-creator');

export async function clearCreatorDemoSession(path = contentCreatorSessionPath) {
  await rm(path, { force: true });
}

export async function readCreatorDemoSessionForCurrentRole({
  sessionPath = contentCreatorSessionPath,
  profilePath = defaultProfilePath,
} = {}) {
  let session;
  try {
    session = await readJson(sessionPath);
  } catch (error) {
    if (error.code === 'ENOENT') return null;
    throw error;
  }

  const profile = await readJson(profilePath);
  if (!creatorIdentitiesMatch(session, profile)) {
    await clearCreatorDemoSession(sessionPath);
    return null;
  }

  await chmod(sessionPath, 0o600);
  return session;
}

export async function writeCreatorDemoSessionForCurrentRole(
  session,
  {
    sessionPath = contentCreatorSessionPath,
    profilePath = defaultProfilePath,
  } = {},
) {
  const profileBeforeWrite = await readJson(profilePath);
  if (!creatorIdentitiesMatch(session, profileBeforeWrite)) {
    await clearCreatorDemoSession(sessionPath);
    throw new Error('creator identity changed during demo authentication');
  }

  await writeJson(sessionPath, session, { mode: 0o600 });
  await chmod(sessionPath, 0o600);

  const profileAfterWrite = await readJson(profilePath);
  if (!creatorIdentitiesMatch(session, profileAfterWrite)) {
    await clearCreatorDemoSession(sessionPath);
    throw new Error('creator identity changed during demo authentication');
  }
}

function creatorIdentitiesMatch(session, profile) {
  return session?.role === 'content-creator'
    && profile?.role === 'content-creator'
    && typeof session.pubky === 'string'
    && session.pubky === profile.pubky;
}
