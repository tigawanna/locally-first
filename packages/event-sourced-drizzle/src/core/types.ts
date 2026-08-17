import type { SyncHandlersConfig, SyncTransport, SyncUrlConfig } from "./sync";
import type {
  BackendMismatchPolicy,
  EventSourcedHooks,
  ManualSyncResult,
  SyncResult,
} from "../internal/hooks";
import type {
  DeadLetterReason,
  DeadLetterRow,
  DrizzleAdapter,
  InboxRow,
  MutationType,
  OutboxRow,
  OutboxSyncStatus,
  UpcastEventFn,
} from "../internal/types";
import type { EventSourcedLogger } from "../utils/logger";
import type { TransactFn } from "./transaction";
import type { OptimisticStateTracker } from "./optimistic-state";

// Re-exports for public API.
export type {
  MutationType,
  OutboxSyncStatus,
  OutboxRow,
  InboxRow,
  DeadLetterRow,
  DeadLetterReason,
  DrizzleAdapter,
  UpcastEventFn,
};
export type { EventSourcedHooks, BackendMismatchPolicy, ManualSyncResult, SyncResult };
export type { EventSourcedLogger };

/** Minimal Drizzle table shape — tables satisfy this via `$inferSelect` / `$inferInsert`. */
export type TableLike = {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  $inferSelect: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  $inferInsert: any;
};

export type InferSelect<TTable> = TTable extends { $inferSelect: infer S } ? S : never;
export type InferInsert<TTable> = TTable extends { $inferInsert: infer I } ? I : never;

export type CollectionDef<
  TTable extends TableLike = TableLike,
  TKey extends string | number = string,
> = {
  table: TTable;
  getKey: (row: InferSelect<TTable>) => TKey;
};

export type CollectionMap = Record<string, CollectionDef>;

export type RetryConfig = {
  maxAttempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
};

export type SyncStatus = {
  isSyncing: boolean;
  isSynced: boolean;
  pendingCount: number;
  failedCount: number;
  deadLetterCount: number;
  pullCursor: number;
  backendId: string | null;
  lastSyncAt: number | null;
  lastError: string | null;
};

export type PruneOptions = {
  olderThanMs?: number;
  keepLast?: number;
};

export type PruneResult = {
  outbox: number;
  inbox: number;
};

/**
 * Full configuration for creating an event-sourced Drizzle sync engine.
 */
export type EventSourcedDrizzleConfig<TCollections extends CollectionMap> = {
  /**
   * A {@link DrizzleAdapter} that bridges the engine to your Drizzle instance.
   * Adapters for common setups can be built with helper factories (e.g.
   * `createSQLiteAdapter`, `createPgAdapter`).
   */
  adapter: DrizzleAdapter;
  /** Collection registry. Keys become collectionId on the wire. */
  collections: TCollections;
  /**
   * How to reach the sync server. Omit for offline-only (mutations still
   * accumulate in the outbox).
   */
  sync?: SyncHandlersConfig | SyncUrlConfig | SyncTransport;
  /** Whether sync may run. Defaults to true. Toggle with `setSyncEnabled()`. */
  syncEnabled?: boolean;
  /** Default schema version for local persistence. Defaults to 1. */
  schemaVersion?: number;
  /** Console logger or custom logger. Defaults to off. */
  debug?: boolean | EventSourcedLogger;
  /** Stable client ID. Auto-generated and persisted if not provided. */
  clientId?: string;
  /** What to do with events for unknown collections. Defaults to "skip". */
  unknownEventHandling?: "skip" | "fail";
  /** Cursor overlap for out-of-order sequence protection. Defaults to 0. */
  pullOverlap?: number;
  /** Version stamped on every authored outbox event. Defaults to 1. */
  eventSchemaVersion?: number;
  /** Migrates events from older schema versions. */
  upcastEvent?: UpcastEventFn;
  /** Retry / backoff for retryable push failures. */
  retry?: RetryConfig;
  /** Max events per push request. Defaults to 100. */
  pushBatchSize?: number;
  /** What to do when backend identity changes. Defaults to "resetCursor". */
  backendMismatch?: BackendMismatchPolicy;
  /** Lifecycle hooks. */
  hooks?: EventSourcedHooks;
};

export type MutateApi<TCollections extends CollectionMap> = {
  insert: <TId extends keyof TCollections & string>(
    collectionId: TId,
    row: InferInsert<TCollections[TId]["table"]>,
  ) => Promise<void>;
  update: <TId extends keyof TCollections & string>(
    collectionId: TId,
    key: ReturnType<TCollections[TId]["getKey"]>,
    patch: Partial<InferInsert<TCollections[TId]["table"]>>,
  ) => Promise<void>;
  delete: <TId extends keyof TCollections & string>(
    collectionId: TId,
    key: ReturnType<TCollections[TId]["getKey"]>,
  ) => Promise<void>;
};

/**
 * The returned engine handle.
 */
export type EventSourcedDrizzle<TCollections extends CollectionMap> = {
  /** Typed mutate API — writes domain table + appends outbox atomically. */
  mutate: MutateApi<TCollections>;
  /**
   * Group multiple mutations into a single atomic transaction.
   * All mutations inside `transact().run(...)` share a txId and commit together.
   *
   * @example
   * ```ts
   * const { run } = engine.transact()
   * await run(async () => {
   *   await engine.mutate.insert("todos", { id: "1", text: "Buy milk" })
   *   await engine.mutate.update("counters", "main", { count: 5 })
   * })
   * ```
   */
  transact: TransactFn;
  /**
   * In-memory tracker for optimistic (pending sync) mutations.
   * Use to check pending state, subscribe to changes, or get a summary.
   *
   * @example
   * ```ts
   * engine.optimistic.isPending("todos", "todo-1")
   * engine.optimistic.subscribe(() => updateUI())
   * ```
   */
  optimistic: OptimisticStateTracker;
  /** Push + pull sync cycle. */
  sync: () => Promise<SyncResult>;
  /** Push + pull + replay any pending inbox. */
  manualSync: () => Promise<ManualSyncResult>;
  /** Runtime sync toggle. */
  getSyncEnabled: () => boolean;
  setSyncEnabled: (enabled: boolean) => void;
  /** Releases internal resources. */
  dispose: () => void;
};
