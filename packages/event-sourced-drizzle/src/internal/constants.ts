/** Default schema version for events on the wire. */
export const DEFAULT_EVENT_SCHEMA_VERSION = 1;

/** Default maximum push attempts before dead-lettering. */
export const DEFAULT_MAX_ATTEMPTS = 8;

/** Default first backoff delay (ms). */
export const DEFAULT_BASE_DELAY_MS = 1_000;

/** Default maximum backoff cap (ms). */
export const DEFAULT_MAX_DELAY_MS = 5 * 60 * 1_000;

/** Default events per push batch. */
export const DEFAULT_PUSH_BATCH_SIZE = 100;

/** Error code for server-side conflict rejections. */
export const CONFLICT_ERROR_CODE = "CONFLICT";

/** Sync-meta keys. */
export const SYNCMETA_KEY_CURSOR = "pullCursor";
export const SYNCMETA_KEY_CLIENT_ID = "clientId";
export const SYNCMETA_KEY_BACKEND_ID = "backendId";
export const SYNCMETA_KEY_LAST_SYNC_AT = "lastSyncAt";
export const SYNCMETA_KEY_LAST_ERROR = "lastError";
export const SYNCMETA_KEY_ROWVERSION_PREFIX = "rowversion:";

export function rowVersionMetaKey(collectionId: string, key: string): string {
  return `${SYNCMETA_KEY_ROWVERSION_PREFIX}${collectionId}:${key}`;
}
