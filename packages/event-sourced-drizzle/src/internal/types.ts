import type { EventSourcedLogger } from "../utils/logger";
import type { EmitHook } from "./hooks";

export type ResolvedRetryConfig = {
  maxAttempts: number;
  baseDelayMs: number;
  maxDelayMs: number;
};

export type MutationType = "insert" | "update" | "delete";
export type OutboxSyncStatus = "pending" | "synced" | "failed";

/** Drizzle-agnostic interface for running queries within the engine. */
export type DrizzleAdapter = {
  /** Run a function inside a transaction. */
  transaction: <T>(fn: (tx: unknown) => Promise<T>) => Promise<T>;
  /** Query outbox rows that are due for push. */
  queryDueOutbox: (now: number) => Promise<OutboxRow[]>;
  /** Update an outbox row by eventId. */
  updateOutbox: (eventId: string, patch: Partial<OutboxRow>) => Promise<void>;
  /** Mark an outbox row as synced. */
  markOutboxSynced: (eventId: string, globalSeq: number) => Promise<void>;
  /** Delete an outbox row (when dead-lettering). */
  deleteOutboxRow: (eventId: string) => Promise<void>;
  /** Insert an outbox row. */
  insertOutbox: (row: OutboxRow) => Promise<void>;
  /** Insert a dead-letter row. */
  insertDeadLetter: (row: DeadLetterRow) => Promise<void>;
  /** Insert an inbox row. */
  insertInbox: (row: InboxRow) => Promise<void>;
  /** Update an inbox row. */
  updateInbox: (eventId: string, patch: Partial<InboxRow>) => Promise<void>;
  /** Get an inbox row by eventId. */
  getInboxRow: (eventId: string) => Promise<InboxRow | undefined>;
  /** Query unresolved inbox rows ordered by globalSeq. */
  queryUnresolvedInbox: () => Promise<InboxRow[]>;
  /** Check if an eventId exists in the outbox. */
  outboxHas: (eventId: string) => Promise<boolean>;
  /** Read sync-meta value. */
  readMeta: (key: string) => Promise<string | null>;
  /** Write sync-meta value. */
  writeMeta: (key: string, value: string) => Promise<void>;
  /** Insert a row into a domain table. */
  domainInsert: (collectionId: string, row: Record<string, unknown>) => Promise<void>;
  /** Update a row in a domain table by key. */
  domainUpdate: (
    collectionId: string,
    key: string | number,
    patch: Record<string, unknown>,
  ) => Promise<void>;
  /** Delete a row from a domain table by key. */
  domainDelete: (collectionId: string, key: string | number) => Promise<void>;
};

export type OutboxRow = {
  eventId: string;
  collectionId: string;
  type: MutationType;
  key: string;
  payload: Record<string, unknown>;
  previous: Record<string, unknown> | null;
  txId: string;
  clientId: string;
  schemaVersion: number;
  baseVersion: string | null;
  timestamp: number;
  localSeq: number;
  globalSeq: number | null;
  sync: boolean;
  syncStatus: OutboxSyncStatus;
  attemptCount: number;
  lastAttemptAt: number | null;
  nextAttemptAt: number | null;
  lastError: string | null;
  lastErrorCode: string | null;
  retryable: boolean | null;
};

export type InboxRow = {
  eventId: string;
  globalSeq: number;
  collectionId: string;
  type: MutationType;
  key: string;
  payload: Record<string, unknown>;
  previous: Record<string, unknown> | null;
  clientId: string | null;
  schemaVersion: number;
  timestamp: number;
  sync: boolean;
  skipped: boolean;
  skipReason: string | null;
  attemptCount: number;
  lastError: string | null;
};

export type DeadLetterRow = {
  eventId: string;
  collectionId: string;
  type: MutationType;
  key: string;
  payload: Record<string, unknown>;
  previous: Record<string, unknown> | null;
  txId: string | null;
  clientId: string | null;
  schemaVersion: number;
  timestamp: number;
  localSeq: number | null;
  globalSeq: number | null;
  direction: "outbound" | "inbound";
  reason: DeadLetterReason;
  message: string;
  code: string | null;
  attemptCount: number;
  failedAt: number;
};

export type DeadLetterReason =
  | "rejected"
  | "maxAttemptsExceeded"
  | "conflict"
  | "manual"
  | "replayFailed";

export type ReplayContext = {
  adapter: DrizzleAdapter;
  collections: Record<string, { getKey: (row: never) => string | number }>;
  clientId: string;
  unknownEventHandling: "skip" | "fail";
  eventSchemaVersion: number;
  upcastEvent?: UpcastEventFn | undefined;
  maxReplayAttempts: number;
  emit: EmitHook;
  log: EventSourcedLogger;
};

export type UpcastableEvent = {
  eventId: string;
  collectionId: string;
  type: MutationType;
  key: string;
  payload: Record<string, unknown>;
  previous: Record<string, unknown> | null;
  schemaVersion: number;
};

export type UpcastEventFn = (event: UpcastableEvent) => UpcastableEvent | null;

export type ReplayOutcome =
  | { status: "applied" }
  | { status: "skipped"; reason: string }
  | { status: "halted"; reason: string }
  | { status: "failed"; error: Error };
