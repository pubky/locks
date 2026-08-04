import { realpathSync } from 'node:fs';
import { normalize, resolve, sep } from 'node:path';

export function resolveCreatorStaticPath(pathname, { repoRoot, examplesRoot }) {
  let relative;
  if (pathname === '/') {
    relative = 'index.html';
  } else if (pathname.startsWith('/examples/js-sdk/')) {
    relative = pathname.slice('/examples/js-sdk/'.length);
    if (relative.length === 0) relative = 'index.html';
  } else if (pathname.startsWith('/pkg/')) {
    const packagePath = pathname.slice('/pkg/'.length);
    return resolveExistingPathWithin(resolve(repoRoot, 'locks-sdk/bindings/js/pkg'), packagePath);
  } else if (pathname.startsWith('/locks-sdk/bindings/js/pkg/')) {
    const packagePath = pathname.slice('/locks-sdk/bindings/js/pkg/'.length);
    return resolveExistingPathWithin(resolve(repoRoot, 'locks-sdk/bindings/js/pkg'), packagePath);
  } else {
    return null;
  }
  return resolveExistingPathWithin(examplesRoot, relative);
}

export function resolveExistingPathWithin(root, relative) {
  const normalized = normalize(relative).replace(/^[/\\]+/, '');
  const absoluteRoot = resolve(root);
  const candidate = resolve(absoluteRoot, normalized);
  if (candidate !== absoluteRoot && !candidate.startsWith(`${absoluteRoot}${sep}`)) return null;
  try {
    const canonicalRoot = realpathSync(absoluteRoot);
    const canonicalCandidate = realpathSync(candidate);
    if (canonicalCandidate !== canonicalRoot && !canonicalCandidate.startsWith(`${canonicalRoot}${sep}`)) return null;
    return canonicalCandidate;
  } catch {
    return null;
  }
}
