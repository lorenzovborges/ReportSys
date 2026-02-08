import { createHash } from 'node:crypto';

export const hashApiKey = (apiKey: string, pepper: string): string =>
  createHash('sha256').update(`${apiKey}:${pepper}`).digest('hex');

const SAFE_COLLECTION_NAME = /^[A-Za-z0-9_]+$/;

export const resolveAllowedSourceCollection = (
  sourceCollection: string | undefined,
  allowedSourceCollections: string[],
  fallbackCollection: string
): string => {
  const resolved = sourceCollection?.trim() || fallbackCollection;

  if (!SAFE_COLLECTION_NAME.test(resolved)) {
    throw new Error('sourceCollection must match ^[A-Za-z0-9_]+$');
  }

  if (!allowedSourceCollections.includes(resolved)) {
    throw new Error(`sourceCollection '${resolved}' is not allowed`);
  }

  return resolved;
};
