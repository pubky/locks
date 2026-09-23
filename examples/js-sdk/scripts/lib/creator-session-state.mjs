import { chmod, lstat, rm } from 'node:fs/promises';
import { PublicKey } from '@synonymdev/pubky';

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
  try {
    const info = await lstat(sessionPath);
    const currentUid = typeof process.getuid === 'function' ? process.getuid() : info.uid;
    if (
      !info.isFile()
      || info.isSymbolicLink()
      || (info.mode & 0o077) !== 0
      || info.uid !== currentUid
    ) {
      await clearCreatorDemoSession(sessionPath);
      return null;
    }
  } catch (error) {
    if (error.code === 'ENOENT') return null;
    throw error;
  }
  let session;
  try {
    session = await readJson(sessionPath);
  } catch (error) {
    if (error.code === 'ENOENT') return null;
    throw error;
  }

  const profile = profilePath ? await readJson(profilePath) : null;
  if (!validCreatorSession(session) || (profile && !creatorIdentitiesMatch(session, profile))) {
    await clearCreatorDemoSession(sessionPath);
    return null;
  }

  return session;
}

export async function writeCreatorDemoSessionForCurrentRole(
  session,
  {
    sessionPath = contentCreatorSessionPath,
    profilePath = defaultProfilePath,
  } = {},
) {
  const profileBeforeWrite = profilePath ? await readJson(profilePath) : null;
  if (!validCreatorSession(session) || (profileBeforeWrite && !creatorIdentitiesMatch(session, profileBeforeWrite))) {
    await clearCreatorDemoSession(sessionPath);
    throw new Error('creator identity changed during demo authentication');
  }

  await writeJson(sessionPath, session, { mode: 0o600 });
  await chmod(sessionPath, 0o600);

  const profileAfterWrite = profilePath ? await readJson(profilePath) : null;
  if (profileAfterWrite && !creatorIdentitiesMatch(session, profileAfterWrite)) {
    await clearCreatorDemoSession(sessionPath);
    throw new Error('creator identity changed during demo authentication');
  }
}

function creatorIdentitiesMatch(session, profile) {
  return validCreatorSession(session)
    && profile?.role === 'content-creator'
    && session.pubky === profile.pubky;
}

function validCreatorSession(session) {
  if (session === null || typeof session !== 'object' || Array.isArray(session)) return false;
  const expectedKeys = ['authenticated_at', 'capabilities', 'exported_session', 'pubky', 'role'];
  const keys = Object.keys(session).sort();
  return keys.length === expectedKeys.length
    && keys.every((key, index) => key === expectedKeys[index])
    && session.role === 'content-creator'
    && isCanonicalPubky(session.pubky)
    && Array.isArray(session.capabilities)
    && session.capabilities.length > 0
    && session.capabilities.every((capability) => typeof capability === 'string' && capability.length > 0)
    && typeof session.exported_session === 'string'
    && session.exported_session.length > 0
    && typeof session.authenticated_at === 'string'
    && validCanonicalTimestamp(session.authenticated_at);
}

function isCanonicalPubky(value) {
  if (typeof value !== 'string') return false;
  let publicKey;
  try {
    publicKey = PublicKey.from(value);
    return publicKey.toString() === value;
  } catch {
    return false;
  } finally {
    publicKey?.free();
  }
}

function validCanonicalTimestamp(value) {
  try {
    return new Date(value).toISOString() === value;
  } catch {
    return false;
  }
}

export async function assertRestorableCreatorDemoSession(sessionRecord, { restore }) {
  if (!validCreatorSession(sessionRecord)) throw new Error('invalid persisted Creator session');
  const session = await restore(sessionRecord.exported_session);
  try {
    if (session.info.publicKey.toString() !== sessionRecord.pubky) {
      throw new Error('restored Creator session identity mismatch');
    }
    if (JSON.stringify(session.info.capabilities) !== JSON.stringify(sessionRecord.capabilities)) {
      throw new Error('restored Creator session capabilities mismatch');
    }
  } finally {
    session.free();
  }
}
