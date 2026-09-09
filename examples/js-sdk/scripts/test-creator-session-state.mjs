#!/usr/bin/env node
import assert from 'node:assert/strict';
import { chmod, lstat, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  assertRestorableCreatorDemoSession,
  readCreatorDemoSessionForCurrentRole,
  writeCreatorDemoSessionForCurrentRole,
} from './lib/creator-session-state.mjs';

const creator = 'pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy';
const record = {
  role: 'content-creator',
  pubky: creator,
  capabilities: ['/pub/locks.app/:rw', '/priv/locks.app/:rw'],
  exported_session: 'exported-session-secret',
  authenticated_at: '2026-09-02T00:00:00.000Z',
};
const root = await mkdtemp(join(tmpdir(), 'creator-session-state-'));
try {
  const sessionPath = join(root, 'session.json');
  await writeCreatorDemoSessionForCurrentRole(record, { sessionPath, profilePath: null });
  assert.equal((await lstat(sessionPath)).mode & 0o777, 0o600);
  assert.deepEqual(
    await readCreatorDemoSessionForCurrentRole({ sessionPath, profilePath: null }),
    record,
  );

  await chmod(sessionPath, 0o644);
  assert.equal(await readCreatorDemoSessionForCurrentRole({ sessionPath, profilePath: null }), null);
  await assert.rejects(readFile(sessionPath), { code: 'ENOENT' });

  const outside = join(root, 'outside.json');
  await writeFile(outside, JSON.stringify(record), { mode: 0o600 });
  await symlink(outside, sessionPath);
  assert.equal(await readCreatorDemoSessionForCurrentRole({ sessionPath, profilePath: null }), null);
  assert.equal(JSON.parse(await readFile(outside, 'utf8')).pubky, creator);

  for (const invalid of [
    { role: 'content-creator', pubky: creator },
    { ...record, unexpected: true },
    { ...record, pubky: `${creator.slice(0, -1)}b` },
    { ...record, exported_session: '' },
    { ...record, capabilities: 'not-an-array' },
  ]) {
    await writeFile(sessionPath, JSON.stringify(invalid), { mode: 0o600 });
    assert.equal(await readCreatorDemoSessionForCurrentRole({ sessionPath, profilePath: null }), null);
  }

  let freed = false;
  await assertRestorableCreatorDemoSession(record, {
    restore: async (secret) => {
      assert.equal(secret, record.exported_session);
      return {
        info: { publicKey: { toString: () => creator }, capabilities: record.capabilities },
        free: () => { freed = true; },
      };
    },
  });
  assert.equal(freed, true);

  await assert.rejects(
    assertRestorableCreatorDemoSession(record, {
      restore: async () => ({
        info: {
          publicKey: { toString: () => 'pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo' },
          capabilities: record.capabilities,
        },
        free() {},
      }),
    }),
    /restored Creator session identity mismatch/,
  );
  await assert.rejects(
    assertRestorableCreatorDemoSession(record, {
      restore: async () => ({
        info: { publicKey: { toString: () => creator }, capabilities: [] },
        free() {},
      }),
    }),
    /restored Creator session capabilities mismatch/,
  );
} finally {
  await rm(root, { recursive: true, force: true });
}

console.log('creator session state tests passed');
