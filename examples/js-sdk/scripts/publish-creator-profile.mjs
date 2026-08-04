#!/usr/bin/env node
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  creatorPublicProfilePath,
  readJson,
  roleProfilePath,
  writeJson,
} from './lib/paths.mjs';

const CANONICAL_PUBKY = /^pubky[ybndrfg8ejkmcpqxot1uwisza345h769]{52}$/;

export async function publishCreatorProfile({
  source = roleProfilePath('content-creator'),
  destination = creatorPublicProfilePath,
} = {}) {
  const profile = await readJson(source);
  if (profile?.role !== 'content-creator' || !CANONICAL_PUBKY.test(profile.pubky ?? '')) {
    throw new Error('valid content-creator profile is required');
  }
  const publicProfile = Object.freeze({ role: 'content-creator', pubky: profile.pubky });
  await writeJson(destination, publicProfile);
  return publicProfile;
}

async function main() {
  try {
    await publishCreatorProfile();
    process.stdout.write('Creator public profile published\n');
  } catch {
    process.stderr.write('Creator public profile publication failed\n');
    process.exitCode = 1;
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await main();
