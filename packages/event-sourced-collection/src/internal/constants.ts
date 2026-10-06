export const OUTBOX_ID = "outbox";
export const INBOX_ID = "inbox";
export const DEADLETTER_ID = "deadletter";
export const SYNCMETA_ID = "syncmeta";
export const ROWVERSIONS_ID = "rowversions";

/** `syncmeta` holds exactly one row; this is its key. */
export const SYNCMETA_KEY = "singleton";

export const RESERVED_IDS: ReadonlySet<string> = new Set([
  OUTBOX_ID,
  INBOX_ID,
  DEADLETTER_ID,
  SYNCMETA_ID,
  ROWVERSIONS_ID,
]);

export const DEFAULT_PUSH_BATCH_SIZE = 20;
export const MIN_PUSH_BATCH_SIZE = 1;
export const MAX_PUSH_BATCH_SIZE = 100;
export const PAYLOAD_TOO_LARGE_ERROR_CODE = "PAYLOAD_TOO_LARGE";
export const DEFAULT_MAX_ATTEMPTS = 8;
export const DEFAULT_BASE_DELAY_MS = 1_000;
export const DEFAULT_MAX_DELAY_MS = 5 * 60 * 1_000;
export const DEFAULT_EVENT_SCHEMA_VERSION = 1;

export const SYNC_LOCK_PREFIX = "event-sourced-sync";

/** Server error code that routes a rejected event to the dead-letter queue as a conflict. */
export const CONFLICT_ERROR_CODE = "CONFLICT";
