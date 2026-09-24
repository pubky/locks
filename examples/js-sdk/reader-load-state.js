const RETIRED_LOCKS_NAMESPACE = '/pub/locks.app/';
const CURRENT_LOCKS_NAMESPACE = '/pub/app.locks/';

export function validateContentLockResource(resource) {
  if (!resource) throw new Error('Paste a content lock resource.');
  if (resource.includes(RETIRED_LOCKS_NAMESPACE)) {
    throw new Error(
      `Content lock uses retired ${RETIRED_LOCKS_NAMESPACE} namespace. Republish it under ${CURRENT_LOCKS_NAMESPACE} from the current creator demo.`,
    );
  }
}

export function describeReaderLoadState({ loadingLock, loaded, resource, loadError }) {
  if (loadingLock) return { message: 'Loading content lock...', className: 'muted' };
  if (loadError) return { message: loadError, className: 'error' };
  if (loaded) return { message: 'Content lock loaded.', className: 'ok' };
  if (resource) return { message: 'Ready to load content lock.', className: 'muted' };
  return { message: 'Paste a content lock resource.', className: 'muted' };
}
