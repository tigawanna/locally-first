/**
 * Shared wire protocol types for the event-sourced sync system.
 *
 * These types define the contract between any client engine (event-sourced-collection,
 * event-sourced-drizzle, or future engines) and the sync backend. Both push and pull
 * endpoints speak this protocol.
 *
 * **Compatibility rule:** Any change to these types must be mirrored in
 * `event-sourced-collection/src/core/types.ts` and the backend implementation
 * (`apps/example/src/server/sync/remote.ts`). Consider extracting to a shared
 * package if drift becomes a maintenance burden.
 */

// --- Core Enums ---

/**
 * The three mutation verbs understood by the sync protocol.
 * Identical in both engine packages.
 */
export type MutationType = "insert" | "update" | "delete";

// --- Push (Client → Server) ---

/**
 * An event authored by the client, ready to be pushed to the sync server.
 * The server assigns a globalSeq and stores it in the event log.
 */
export type OutboundEvent = {
  /** Globally unique event identifier (UUID). Used for deduplication. */
  eventId: string;
  /** Which collection this event targets. */
  collectionId: string;
  /** The mutation verb. */
  type: MutationType;
  /** Primary key of the affected row. */
  key: string | number;
  /** For insert: full row. For update: partial patch. For delete: empty or full row. */
  payload: Record<string, unknown>;
  /** State before mutation. Present for update/delete, null for insert. */
  previous: Record<string, unknown> | null;
  /** Groups events from a single client transaction. Server commits atomically per txId. */
  txId: string;
  /** Stable client identity. Server uses this to skip echoes on pull. */
  clientId: string;
  /** Payload schema version for upcasting on replay. */
  schemaVersion: number;
  /**
   * Row version this mutation was authored against. Enables server-side
   * conflict detection. Null when conflict detection is disabled.
   */
  baseVersion: string | null;
  /** Client-side wall clock when the event was created. */
  timestamp: number;
};

/**
 * Server confirmation that an event was accepted and assigned a global sequence number.
 */
export type PushConfirmation = {
  eventId: string;
  globalSeq: number;
};

/**
 * Server rejection of an individual event (e.g. conflict, validation failure).
 */
export type PushFailure = {
  eventId: string;
  message: string;
  /** Machine-readable failure code (e.g. "CONFLICT", "TRANSIENT", "TX_ABORTED"). */
  code?: string;
  /** Whether the client may retry this event. */
  retryable?: boolean;
};

/**
 * The response envelope from the push endpoint.
 */
export type PushResponse = {
  confirmed: ReadonlyArray<PushConfirmation>;
  failed?: ReadonlyArray<PushFailure>;
};

// --- Pull (Server → Client) ---

/**
 * A server event returned during pull. Represents a mutation in the global log.
 */
export type ServerEvent = {
  /** Server-assigned monotonic sequence number. */
  globalSeq: number;
  /** Original event ID from the authoring client. */
  eventId: string;
  /** Which collection this event targets. */
  collectionId: string;
  /** The mutation verb. */
  type: MutationType;
  /** Primary key of the affected row. */
  key: string | number;
  /** Payload (insert: full row, update: patch, delete: empty or row snapshot). */
  payload: Record<string, unknown>;
  /** State before the mutation (optional, depends on backend). */
  previous?: Record<string, unknown> | null;
  /** Client that authored this event. Used to skip echoes. */
  clientId?: string;
  /** Schema version of the payload. */
  schemaVersion?: number;
  /** Server or client timestamp. */
  timestamp: number;
  /** Opaque cursor for resumption — typically stringified globalSeq. */
  cursor: string;
  /** Backend identity (for detecting database swaps). */
  backendId?: string;
};

/**
 * The response envelope from the pull endpoint.
 */
export type PullResponse = {
  events: ReadonlyArray<ServerEvent>;
  /** Cursor to pass as `since` on the next pull request. */
  cursor: string;
  /** Whether there are more events beyond this batch. */
  hasMore: boolean;
  /** Backend identity — clients compare to detect database resets. */
  backendId?: string;
};

// --- Transport Abstractions ---

/**
 * Function signature for pushing events to the server.
 * May return either a full PushResponse or a shorthand array of confirmations.
 */
export type PushEventsFn = (
  events: ReadonlyArray<OutboundEvent>,
) => Promise<PushResponse | ReadonlyArray<PushConfirmation>>;

/**
 * Function signature for pulling events from the server.
 */
export type PullEventsFn = (params: { since: number }) => Promise<PullResponse>;

/**
 * A fully normalized sync transport with push and pull functions.
 */
export type SyncTransport = {
  push: PushEventsFn;
  pull: (since: number) => Promise<PullResponse>;
};

/**
 * URL-based transport config — the engine builds HTTP fetch calls from these.
 */
export type SyncUrlConfig = {
  push: string;
  pull: string;
  headers?:
    | Record<string, string>
    | (() => Record<string, string> | Promise<Record<string, string>>);
};

/**
 * Flexible config that accepts handler functions, URLs, or a mix.
 */
export type SyncHandlersConfig = {
  pushEvents?: PushEventsFn;
  pullEvents?: PullEventsFn;
  pushUrl?: string;
  pullUrl?: string;
  headers?:
    | Record<string, string>
    | (() => Record<string, string> | Promise<Record<string, string>>);
};

/**
 * Internal normalized form — push/pull may be undefined if not configured.
 */
export type NormalizedSyncTransport = {
  push?: PushEventsFn;
  pull?: (since: number) => Promise<PullResponse>;
};
