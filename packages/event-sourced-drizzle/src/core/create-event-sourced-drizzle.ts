import {
  DEFAULT_BASE_DELAY_MS,
  DEFAULT_EVENT_SCHEMA_VERSION,
  DEFAULT_MAX_ATTEMPTS,
  DEFAULT_MAX_DELAY_MS,
  DEFAULT_PUSH_BATCH_SIZE,
} from "../internal/constants";
import { createHookEmitter } from "../internal/hooks";
import type { EmitHook, ManualSyncResult, SyncResult } from "../internal/hooks";
import { pullInbox } from "../internal/pull";
import { pushOutbox } from "../internal/push";
import { replayInbox } from "../internal/replay";
import { createSerialQueue } from "../internal/serial-queue";
import { readPullCursor, resolveClientId, writeSyncOutcome } from "../internal/sync-meta";
import type {
  DrizzleAdapter,
  OutboxRow,
  ReplayContext,
  ResolvedRetryConfig,
} from "../internal/types";
import { createSyncTransport } from "./sync";
import type { NormalizedSyncTransport } from "./sync";
import { createTransact, getActiveTransactionImpl } from "./transaction";
import type { TransactFn } from "./transaction";
import { createOptimisticStateTracker } from "./optimistic-state";
import type { OptimisticStateTracker } from "./optimistic-state";
import type {
  CollectionMap,
  EventSourcedDrizzle,
  EventSourcedDrizzleConfig,
  MutateApi,
} from "./types";
import type { EventSourcedLogger } from "../utils/logger";
import { createEventSourcedLogger } from "../utils/logger";
import { generateEventId } from "../utils/uuid";
import { pendingRowVersion, readRowVersion, writeRowVersion } from "../internal/row-versions";

function emptyResult(overrides: Partial<SyncResult> = {}): SyncResult {
  return {
    pushed: 0,
    pulled: 0,
    skipped: 0,
    deadLettered: 0,
    deferred: false,
    errors: [],
    ...overrides,
  };
}

/**
 * Creates a Drizzle-backed event-sourced sync engine.
 *
 * Inbox/outbox live in your Drizzle-managed SQL tables. Domain writes go
 * through `mutate` so outbox append stays atomic with the domain write.
 *
 * @param config — See {@link EventSourcedDrizzleConfig} for every option.
 *
 * @example
 * ```ts
 * import { createEventSourcedDrizzle } from "event-sourced-drizzle"
 * import { createSQLiteAdapter } from "event-sourced-drizzle/sqlite"
 *
 * const adapter = createSQLiteAdapter(db, {
 *   outbox,
 *   inbox,
 *   syncMeta,
 *   deadLetter,
 *   collections: { todos: { table: todos, keyColumn: todos.id } },
 * })
 *
 * const engine = await createEventSourcedDrizzle({
 *   adapter,
 *   collections: { todos: { table: todos, getKey: (row) => row.id } },
 *   sync: { pushUrl: "/api/sync/events", pullUrl: "/api/sync/events" },
 * })
 *
 * await engine.mutate.insert("todos", { id: "1", title: "Buy milk", done: false })
 * await engine.sync()
 * ```
 */
export async function createEventSourcedDrizzle<const TCollections extends CollectionMap>(
  config: EventSourcedDrizzleConfig<TCollections>,
): Promise<EventSourcedDrizzle<TCollections>> {
  const log: EventSourcedLogger = createEventSourcedLogger(config.debug);
  const emit: EmitHook = createHookEmitter(config.hooks, log);

  const transport: NormalizedSyncTransport | null = createSyncTransport(config.sync);
  let syncEnabled = config.syncEnabled ?? true;

  const adapter: DrizzleAdapter = config.adapter;
  const unknownEventHandling = config.unknownEventHandling ?? "skip";
  const pullOverlap = Math.max(0, config.pullOverlap ?? 0);
  const eventSchemaVersion = config.eventSchemaVersion ?? DEFAULT_EVENT_SCHEMA_VERSION;
  const pushBatchSize = Math.max(1, config.pushBatchSize ?? DEFAULT_PUSH_BATCH_SIZE);
  const backendMismatch = config.backendMismatch ?? "resetCursor";
  const conflictDetection = config.conflictDetection ?? false;
  const syncEnabledListeners = new Set<(enabled: boolean) => void>();

  const retry: ResolvedRetryConfig = {
    maxAttempts: Math.max(1, config.retry?.maxAttempts ?? DEFAULT_MAX_ATTEMPTS),
    baseDelayMs: Math.max(0, config.retry?.baseDelayMs ?? DEFAULT_BASE_DELAY_MS),
    maxDelayMs: Math.max(0, config.retry?.maxDelayMs ?? DEFAULT_MAX_DELAY_MS),
  };

  // Resolve stable client identity.
  const clientId = await resolveClientId(adapter, config.clientId, generateEventId);

  log.info("creating event-sourced drizzle engine", {
    collectionIds: Object.keys(config.collections),
    hasTransport: transport !== null,
    syncEnabled,
    unknownEventHandling,
    pullOverlap,
    pushBatchSize,
    backendMismatch,
    eventSchemaVersion,
    clientId,
  });

  const replayContext: ReplayContext = {
    adapter,
    collections: config.collections as unknown as Record<
      string,
      { getKey: (row: never) => string | number }
    >,
    clientId,
    unknownEventHandling,
    eventSchemaVersion,
    upcastEvent: config.upcastEvent,
    maxReplayAttempts: retry.maxAttempts,
    conflictDetection,
    emit,
    log,
  };

  // Replay any pending inbox rows from a previous session.
  await replayInbox(adapter, replayContext);

  const pullCursor = await readPullCursor(adapter);
  emit("onReady", { clientId, pullCursor });

  // Serial queue prevents overlapping sync cycles within this JS context.
  const runExclusive = createSerialQueue();

  // Optimistic state tracker — tracks pending local mutations in memory.
  const optimistic: OptimisticStateTracker = createOptimisticStateTracker();

  // --- Mutate API ---

  let localSeq = Date.now();

  function nextLocalSeq(): number {
    return ++localSeq;
  }

  function buildOutboxRow(params: {
    eventId: string;
    collectionId: string;
    type: "insert" | "update" | "delete";
    key: string;
    payload: Record<string, unknown>;
    previous: Record<string, unknown> | null;
    txId: string;
    timestamp: number;
    localSeq: number;
    baseVersion: string | null;
  }): OutboxRow {
    return {
      eventId: params.eventId,
      collectionId: params.collectionId,
      type: params.type,
      key: params.key,
      payload: params.payload,
      previous: params.previous,
      txId: params.txId,
      clientId,
      schemaVersion: eventSchemaVersion,
      baseVersion: params.baseVersion,
      timestamp: params.timestamp,
      localSeq: params.localSeq,
      globalSeq: null,
      sync: false,
      syncStatus: "pending",
      attemptCount: 0,
      lastAttemptAt: null,
      nextAttemptAt: null,
      lastError: null,
      lastErrorCode: null,
      retryable: null,
    };
  }

  async function resolveBaseVersion(collectionId: string, key: string): Promise<string | null> {
    if (!conflictDetection) return null;
    const ambientTx = getActiveTransactionImpl();
    if (ambientTx) {
      const pending = pendingRowVersion(ambientTx, collectionId, key);
      if (pending) return pending;
    }
    return readRowVersion(adapter, collectionId, key);
  }

  async function stampVersion(collectionId: string, key: string, eventId: string): Promise<void> {
    if (!conflictDetection) return;
    await writeRowVersion(adapter, collectionId, key, eventId);
  }

  const mutate: MutateApi<TCollections> = {
    async insert(collectionId, row) {
      const def = config.collections[collectionId];
      if (!def) throw new Error(`Unknown collection: ${collectionId}`);

      const key = String(def.getKey(row as never));
      const payload = row as Record<string, unknown>;
      const eventId = generateEventId();
      const now = Date.now();
      const seq = nextLocalSeq();

      // Check for ambient transaction
      const ambientTx = getActiveTransactionImpl();
      const txId = ambientTx ? ambientTx.id : generateEventId();
      const baseVersion = await resolveBaseVersion(collectionId, key);

      const outboxRow = buildOutboxRow({
        eventId,
        collectionId,
        type: "insert",
        key,
        payload,
        previous: null,
        txId,
        timestamp: now,
        localSeq: seq,
        baseVersion,
      });

      if (ambientTx) {
        // Defer execution — the ambient transaction will commit everything together.
        ambientTx.addEntry({
          outboxRow,
          domainOp: () => adapter.domainInsert(collectionId, payload),
        });
      } else {
        await adapter.transaction(async () => {
          await adapter.domainInsert(collectionId, payload);
          await adapter.insertOutbox(outboxRow);
        });
        await stampVersion(collectionId, key, eventId);

        optimistic.track({
          eventId,
          collectionId,
          type: "insert",
          key,
          syncStatus: "pending",
          timestamp: now,
          attemptCount: 0,
        });
        log.debug("mutate insert", { collectionId, key, eventId });
        emit("onMutation", outboxRow);
      }
    },

    async update(collectionId, key, patch) {
      const def = config.collections[collectionId];
      if (!def) throw new Error(`Unknown collection: ${collectionId}`);

      const payload = patch as Record<string, unknown>;
      const eventId = generateEventId();
      const now = Date.now();
      const seq = nextLocalSeq();

      // Check for ambient transaction
      const ambientTx = getActiveTransactionImpl();
      const txId = ambientTx ? ambientTx.id : generateEventId();
      const baseVersion = await resolveBaseVersion(collectionId, String(key));

      const outboxRow = buildOutboxRow({
        eventId,
        collectionId,
        type: "update",
        key: String(key),
        payload,
        previous: null,
        txId,
        timestamp: now,
        localSeq: seq,
        baseVersion,
      });

      if (ambientTx) {
        ambientTx.addEntry({
          outboxRow,
          domainOp: () => adapter.domainUpdate(collectionId, key as string | number, payload),
        });
      } else {
        await adapter.transaction(async () => {
          await adapter.domainUpdate(collectionId, key as string | number, payload);
          await adapter.insertOutbox(outboxRow);
        });
        await stampVersion(collectionId, String(key), eventId);

        optimistic.track({
          eventId,
          collectionId,
          type: "update",
          key: String(key),
          syncStatus: "pending",
          timestamp: now,
          attemptCount: 0,
        });
        log.debug("mutate update", { collectionId, key: String(key), eventId });
        emit("onMutation", outboxRow);
      }
    },

    async delete(collectionId, key) {
      const def = config.collections[collectionId];
      if (!def) throw new Error(`Unknown collection: ${collectionId}`);

      const eventId = generateEventId();
      const now = Date.now();
      const seq = nextLocalSeq();

      // Check for ambient transaction
      const ambientTx = getActiveTransactionImpl();
      const txId = ambientTx ? ambientTx.id : generateEventId();
      const baseVersion = await resolveBaseVersion(collectionId, String(key));

      const outboxRow = buildOutboxRow({
        eventId,
        collectionId,
        type: "delete",
        key: String(key),
        payload: {},
        previous: null,
        txId,
        timestamp: now,
        localSeq: seq,
        baseVersion,
      });

      if (ambientTx) {
        ambientTx.addEntry({
          outboxRow,
          domainOp: () => adapter.domainDelete(collectionId, key as string | number),
        });
      } else {
        await adapter.transaction(async () => {
          await adapter.domainDelete(collectionId, key as string | number);
          await adapter.insertOutbox(outboxRow);
        });
        await stampVersion(collectionId, String(key), eventId);

        optimistic.track({
          eventId,
          collectionId,
          type: "delete",
          key: String(key),
          syncStatus: "pending",
          timestamp: now,
          attemptCount: 0,
        });
        log.debug("mutate delete", { collectionId, key: String(key), eventId });
        emit("onMutation", outboxRow);
      }
    },
  };

  // --- Sync ---

  function recordError(phase: "push" | "pull" | "replay", label: string, err: unknown): Error {
    const error = err instanceof Error ? err : new Error(String(err));
    log.error(`${label} ${phase} failed`, { message: error.message });
    emit("onSyncError", { phase, error });
    return error;
  }

  async function pushPull(label: string): Promise<SyncResult> {
    const errors: Error[] = [];
    let pushed = 0;
    let pulled = 0;
    let skipped = 0;
    let deadLettered = 0;

    if (transport?.push) {
      try {
        const outcome = await pushOutbox({
          adapter,
          push: transport.push,
          batchSize: pushBatchSize,
          retry,
          now: Date.now(),
          emit,
          log,
          conflictDetection,
        });
        pushed += outcome.pushed;
        deadLettered += outcome.deadLettered;
        if (outcome.error) {
          errors.push(recordError("push", label, outcome.error));
        }
      } catch (err) {
        errors.push(recordError("push", label, err));
      }
    } else {
      log.debug(`${label} push skipped: no push transport configured`);
    }

    if (transport?.pull) {
      try {
        const outcome = await pullInbox({
          adapter,
          pull: transport.pull,
          clientId,
          pullOverlap,
          backendMismatch,
          context: replayContext,
          emit,
          log,
        });
        pulled = outcome.pulled;
        skipped = outcome.skipped;
        deadLettered += outcome.deadLettered;
      } catch (err) {
        errors.push(recordError("pull", label, err));
      }
    } else {
      log.debug(`${label} pull skipped: no pull transport configured`);
    }

    await writeSyncOutcome(adapter, Date.now(), errors[0]?.message ?? null);

    return { pushed, pulled, skipped, deadLettered, deferred: false, errors };
  }

  async function sync(): Promise<SyncResult> {
    return runExclusive(async () => {
      if (!syncEnabled) {
        log.debug("sync skipped: sync disabled");
        return emptyResult();
      }
      if (!transport) {
        log.warn("sync skipped: no transport configured");
        return emptyResult();
      }

      emit("onSyncStart", { trigger: "sync" });
      log.info("sync started");

      const result = await pushPull("sync");

      emit("onSyncComplete", { trigger: "sync", result });
      log.info("sync finished", {
        pushed: result.pushed,
        pulled: result.pulled,
        skipped: result.skipped,
        deadLettered: result.deadLettered,
        errorCount: result.errors.length,
      });

      return result;
    });
  }

  async function manualSync(): Promise<ManualSyncResult> {
    return runExclusive(async () => {
      emit("onSyncStart", { trigger: "manualSync" });
      log.info("manual sync started");

      let replayed = 0;
      try {
        const summary = await replayInbox(adapter, replayContext);
        replayed = summary.applied;
      } catch (err) {
        recordError("replay", "manualSync", err);
      }

      if (!syncEnabled || !transport) {
        const result: ManualSyncResult = { ...emptyResult(), replayed };
        emit("onSyncComplete", { trigger: "manualSync", result });
        return result;
      }

      const syncResult = await pushPull("manualSync");
      const result: ManualSyncResult = { ...syncResult, replayed };

      emit("onSyncComplete", { trigger: "manualSync", result });
      log.info("manual sync finished", {
        pushed: result.pushed,
        pulled: result.pulled,
        replayed: result.replayed,
        skipped: result.skipped,
        deadLettered: result.deadLettered,
        errorCount: result.errors.length,
      });

      return result;
    });
  }

  // --- Transact ---

  const transact = createTransact({
    adapter,
    emit,
    log,
    optimistic,
    onCommitted: async (entries) => {
      for (const entry of entries) {
        await stampVersion(
          entry.outboxRow.collectionId,
          entry.outboxRow.key,
          entry.outboxRow.eventId,
        );
      }
    },
  });

  return {
    mutate,
    transact,
    optimistic,
    sync,
    manualSync,
    getSyncEnabled: () => syncEnabled,
    setSyncEnabled: (enabled: boolean) => {
      syncEnabled = enabled;
      log.info("sync enabled changed", { syncEnabled: enabled });
      for (const listener of syncEnabledListeners) listener(enabled);
    },
    subscribeSyncEnabled: (listener) => {
      syncEnabledListeners.add(listener);
      return () => {
        syncEnabledListeners.delete(listener);
      };
    },
    dispose: () => {
      optimistic.clear();
      log.info("engine disposed");
    },
  };
}
