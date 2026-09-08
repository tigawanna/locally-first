import type { Collection } from "@tanstack/db";

import {
  DEADLETTER_ID,
  DEFAULT_BASE_DELAY_MS,
  DEFAULT_EVENT_SCHEMA_VERSION,
  DEFAULT_MAX_ATTEMPTS,
  DEFAULT_MAX_DELAY_MS,
  DEFAULT_PUSH_BATCH_SIZE,
  INBOX_ID,
  OUTBOX_ID,
  RESERVED_IDS,
  ROWVERSIONS_ID,
  SYNCMETA_ID,
  SYNC_LOCK_PREFIX,
} from "../internal/constants";
import { createHookEmitter } from "../internal/hooks";
import type { EmitHook } from "../internal/hooks";
import { pullInbox } from "../internal/pull";
import { pushOutbox, toOutboundEvent } from "../internal/push";
import { readRowVersion, recordRowVersion, replayInbox } from "../internal/replay";
import { createSerialQueue } from "../internal/serial-queue";
import {
  ensureSyncMeta,
  readPullCursor,
  readSyncMeta,
  resolveClientId,
  writePullCursor,
  writeSyncOutcome,
} from "../internal/sync-meta";
import type {
  AcceptMutationsCollection,
  MetaCollections,
  MutationHookParams,
  ReplayContext,
  ResolvedRetryConfig,
} from "../internal/types";
import { createSyncTransport, normalizePushResponse } from "./sync";
import type {
  CollectionMap,
  DeadLetterEntry,
  EventSourcedDB,
  EventSourcedDBConfig,
  InboxEntry,
  ManualSyncResult,
  MutationType,
  OutboundEvent,
  OutboxEntry,
  PruneOptions,
  PruneResult,
  RowVersionEntry,
  SyncMetaEntry,
  SyncResult,
  SyncStatus,
  SyncTrigger,
} from "./types";
import type { EventSourcedLogger } from "../utils/logger";
import { createEventSourcedLogger } from "../utils/logger";
import { generateEventId } from "../utils/uuid";

type CollectionDefConstraint = {
  getKey: (state: never) => string | number;
  schemaVersion?: number;
  indexes?: ReadonlyArray<{
    select: (row: never) => unknown;
    name?: string;
    indexType?: new (id: number, expression: never, name?: string, options?: never) => unknown;
  }>;
};

type SeqCounter = { value: number };

/**
 * Lets the mutation hooks, which are built before `syncmeta` has been
 * preloaded, read the persisted client id once it is known.
 */
type ClientIdRef = { value: string };

type MetaCollectionFactory = Pick<
  EventSourcedDBConfig<Record<string, CollectionDefConstraint>>,
  "createCollection" | "persistedCollectionOptions" | "persistence"
>;

type MutationContext = {
  outbox: Collection<OutboxEntry, string>;
  rowversions: Collection<RowVersionEntry, string>;
  collectionId: string;
  type: MutationType;
  seq: SeqCounter;
  clientId: ClientIdRef;
  eventSchemaVersion: number;
  conflictDetection: boolean;
  emit: EmitHook;
  log: EventSourcedLogger;
};

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
 * Low-level factory: wires persistence, collections, outbox/inbox, and sync.
 * Prefer a platform helper (`createBrowserEventSourcedDB`,
 * `createNodeEventSourcedDB`, `createReactNativeEventSourcedDB`) unless you are
 * supplying your own persistence.
 *
 * @param config - See {@link EventSourcedDBConfig} for every option.
 *
 * @example Node SQLite, opened immediately
 * ```ts
 * import { createCollection } from "@tanstack/db"
 * import {
 *   createNodeSQLitePersistence,
 *   persistedCollectionOptions,
 * } from "@tanstack/node-db-sqlite-persistence"
 * import Database from "better-sqlite3"
 * import { createEventSourcedDB } from "event-sourced-collection"
 *
 * type Todo = { id: string; title: string }
 *
 * const sqlite = new Database("app.sqlite")
 * const db = await createEventSourcedDB({
 *   persistence: createNodeSQLitePersistence({ database: sqlite }),
 *   createCollection,
 *   persistedCollectionOptions,
 *   collections: {
 *     todos: { getKey: (todo: Todo) => todo.id },
 *   },
 *   sync: {
 *     push: "/api/sync/events",
 *     pull: "/api/sync/events",
 *   },
 * })
 *
 * await db.collections.todos.insert({ id: "t1", title: "Ship" }).isPersisted.promise
 * await db.sync()
 * ```
 */
export async function createEventSourcedDB<
  const TDefs extends Record<string, CollectionDefConstraint>,
>(config: EventSourcedDBConfig<TDefs>): Promise<EventSourcedDB<TDefs>> {
  assertReservedNamesAvailable(config.collections);

  const log = createEventSourcedLogger(config.debug);
  const emit = createHookEmitter(config.hooks, log);

  const transport = createSyncTransport(config.sync);
  let syncEnabled = config.syncEnabled ?? true;

  // Provisional until `syncmeta` is preloaded and the persisted id is known.
  const clientId: ClientIdRef = { value: config.clientId ?? generateEventId() };
  const unknownEventHandling = config.unknownEventHandling ?? "skip";
  const pullOverlap = Math.max(0, config.pullOverlap ?? 0);
  const recordLocalEchoes = config.recordLocalEchoes ?? true;
  const eventSchemaVersion = config.eventSchemaVersion ?? DEFAULT_EVENT_SCHEMA_VERSION;
  const pushBatchSize = Math.max(1, config.pushBatchSize ?? DEFAULT_PUSH_BATCH_SIZE);
  const backendMismatch = config.backendMismatch ?? "resetCursor";
  const conflictDetection = config.conflictDetection ?? false;
  const lockName = `${SYNC_LOCK_PREFIX}:${config.lockName ?? "default"}`;

  const retry: ResolvedRetryConfig = {
    maxAttempts: Math.max(1, config.retry?.maxAttempts ?? DEFAULT_MAX_ATTEMPTS),
    baseDelayMs: Math.max(0, config.retry?.baseDelayMs ?? DEFAULT_BASE_DELAY_MS),
    maxDelayMs: Math.max(0, config.retry?.maxDelayMs ?? DEFAULT_MAX_DELAY_MS),
  };

  log.info("creating event-sourced db", {
    collectionIds: Object.keys(config.collections),
    hasTransport: transport !== null,
    hasLock: Boolean(config.lock),
    syncEnabled,
    unknownEventHandling,
    pullOverlap,
    recordLocalEchoes,
    pushBatchSize,
    backendMismatch,
    conflictDetection,
    eventSchemaVersion,
  });

  const defaultSchemaVersion = config.schemaVersion ?? 1;
  const seq: SeqCounter = { value: 0 };

  const meta: MetaCollections = {
    outbox: createMetaCollection<OutboxEntry>(
      config,
      OUTBOX_ID,
      (entry) => entry.eventId,
      defaultSchemaVersion,
    ),
    inbox: createMetaCollection<InboxEntry>(
      config,
      INBOX_ID,
      (entry) => entry.eventId,
      defaultSchemaVersion,
    ),
    deadletter: createMetaCollection<DeadLetterEntry>(
      config,
      DEADLETTER_ID,
      (entry) => entry.eventId,
      defaultSchemaVersion,
    ),
    syncmeta: createMetaCollection<SyncMetaEntry>(
      config,
      SYNCMETA_ID,
      (entry) => entry.id,
      defaultSchemaVersion,
    ),
    rowversions: createMetaCollection<RowVersionEntry>(
      config,
      ROWVERSIONS_ID,
      (entry) => entry.id,
      defaultSchemaVersion,
    ),
  };

  const { outbox, inbox, deadletter, syncmeta, rowversions } = meta;
  const userCollections = {} as CollectionMap<TDefs>;

  for (const collectionId of Object.keys(config.collections)) {
    const def = config.collections[collectionId]!;
    const getKey = def.getKey as (item: Record<string, unknown>) => string | number;

    const mutationContext = {
      outbox,
      rowversions,
      collectionId,
      seq,
      clientId,
      eventSchemaVersion,
      conflictDetection,
      emit,
      log,
    };

    const options = config.persistedCollectionOptions<Record<string, unknown>, string | number>({
      id: collectionId,
      getKey,
      persistence: config.persistence,
      schemaVersion: def.schemaVersion ?? defaultSchemaVersion,
      gcTime: Number.POSITIVE_INFINITY,
      onInsert: createMutationHook({ ...mutationContext, type: "insert" }),
      onUpdate: createMutationHook({ ...mutationContext, type: "update" }),
      onDelete: createMutationHook({ ...mutationContext, type: "delete" }),
    });

    const collection = config.createCollection(options);
    applyCollectionIndexes(collection, collectionId, def.indexes, log);

    log.debug("registered collection", {
      collectionId,
      hasAcceptMutations: Boolean((collection as AcceptMutationsCollection).utils?.acceptMutations),
    });

    (userCollections as Record<string, unknown>)[collectionId] = collection;
  }

  const collections = {
    ...(userCollections as CollectionMap<TDefs>),
    ...meta,
  } as EventSourcedDB<TDefs>["collections"];

  const replayTargets = collections as unknown as Record<string, AcceptMutationsCollection>;

  let isSyncing = false;
  const statusListeners = new Set<(status: SyncStatus) => void>();

  const notifyStatus = (): void => {
    if (statusListeners.size === 0) return;
    const status = getSyncStatus();
    for (const listener of statusListeners) listener(status);
  };

  const subscriptions = [
    outbox.subscribeChanges(notifyStatus),
    inbox.subscribeChanges(() => {}),
    deadletter.subscribeChanges(notifyStatus),
    syncmeta.subscribeChanges(notifyStatus),
  ];

  for (const collection of Object.values(meta)) {
    await collection.preload();
  }

  await ensureSyncMeta(syncmeta);

  clientId.value = await resolveClientId(syncmeta, config.clientId, generateEventId);

  seq.value = nextLocalSeq(outbox);

  // User collections must be hydrated before we hand the DB to the app.
  // Otherwise seeders like `ensureAppSettings` see an empty `get()` and
  // re-insert rows that already exist on disk / on the server.
  for (const collection of Object.values(userCollections)) {
    await collection.preload();
  }

  log.info("preloaded collections", {
    outboxCount: outbox.state.size,
    inboxCount: inbox.state.size,
    deadLetterCount: deadletter.state.size,
    userCollectionIds: Object.keys(userCollections),
    nextLocalSeq: seq.value,
    clientId: clientId.value,
    pullCursor: readPullCursor(syncmeta, inbox),
  });

  const replayContext: ReplayContext = {
    targets: replayTargets,
    rowversions,
    deadletter,
    unknownEventHandling,
    eventSchemaVersion,
    upcastEvent: config.upcastEvent,
    conflictDetection,
    maxReplayAttempts: retry.maxAttempts,
    emit,
    log,
  };

  await replayInbox(inbox, replayContext);

  emit("onReady", { clientId: clientId.value, pullCursor: readPullCursor(syncmeta, inbox) });

  // Serializes callers within this context; the optional lock extends that
  // across tabs so only one of them syncs at a time.
  const runExclusive = createSerialQueue();

  function recordError(phase: "push" | "pull" | "replay", label: string, err: unknown): Error {
    const error = toError(err);
    log.error(`${label} ${phase} failed`, { message: error.message });
    emit("onSyncError", { phase, error });
    return error;
  }

  async function pushPull(label: string): Promise<SyncResult> {
    await outbox.preload();
    await inbox.preload();

    const errors: Error[] = [];
    let pushed = 0;
    let pulled = 0;
    let skipped = 0;
    let deadLettered = 0;

    async function runPush(): Promise<void> {
      if (!transport?.push) {
        log.debug(`${label} push skipped: no push transport configured`);
        return;
      }

      try {
        const outcome = await pushOutbox({
          outbox,
          deadletter,
          rowversions,
          push: transport.push,
          batchSize: pushBatchSize,
          retry,
          now: Date.now(),
          emit,
          log,
        });
        pushed += outcome.pushed;
        deadLettered += outcome.deadLettered;
        if (outcome.error) {
          errors.push(recordError("push", label, outcome.error));
        }
      } catch (err) {
        errors.push(recordError("push", label, err));
      }
    }

    await runPush();

    let requeued = 0;

    try {
      if (transport?.pull) {
        const outcome = await pullInbox({
          outbox,
          inbox,
          syncmeta,
          pull: transport.pull,
          clientId: clientId.value,
          pullOverlap,
          recordLocalEchoes,
          backendMismatch,
          context: replayContext,
        });
        pulled = outcome.pulled;
        skipped = outcome.skipped;
        deadLettered += outcome.deadLettered;
        requeued = outcome.requeued;
      } else {
        log.debug(`${label} pull skipped: no pull transport configured`);
      }
    } catch (err) {
      errors.push(recordError("pull", label, err));
    }

    // A backend reset is only discovered during pull, by which point this
    // sync's push has already run. Upload the requeued history now so a single
    // sync is enough to recover rather than leaving the gap open until the next.
    if (requeued > 0) {
      log.info(`${label} re-pushing history after backend reset`, { requeued });
      await runPush();
    }

    await writeSyncOutcome(syncmeta, Date.now(), errors[0]?.message ?? null);

    return { pushed, pulled, skipped, deadLettered, deferred: false, errors };
  }

  /** Runs `fn` under the cross-context lock when one is configured. */
  async function withLock<T>(fn: () => Promise<T>, deferred: T): Promise<T> {
    if (!config.lock) return fn();

    const outcome = await config.lock.tryRun(lockName, fn);

    if (!outcome.acquired) {
      log.debug("sync deferred: lock held by another context", { lockName });
      return deferred;
    }

    return outcome.result;
  }

  async function tracked<T extends SyncResult>(
    trigger: SyncTrigger,
    fn: () => Promise<T>,
  ): Promise<T> {
    isSyncing = true;
    notifyStatus();
    emit("onSyncStart", { trigger });

    try {
      const result = await fn();
      emit("onSyncComplete", { trigger, result });
      return result;
    } finally {
      isSyncing = false;
      notifyStatus();
    }
  }

  function sync(): Promise<SyncResult> {
    return runExclusive(async () => {
      if (!syncEnabled) {
        log.debug("sync skipped: sync disabled");
        return emptyResult();
      }

      if (!transport) {
        log.warn("sync skipped: no transport configured");
        return emptyResult();
      }

      return withLock(
        () =>
          tracked("sync", async () => {
            log.info("sync started");
            const result = await pushPull("sync");
            log.info("sync finished", {
              pushed: result.pushed,
              pulled: result.pulled,
              skipped: result.skipped,
              deadLettered: result.deadLettered,
              errorCount: result.errors.length,
            });
            return result;
          }),
        emptyResult({ deferred: true }),
      );
    });
  }

  function manualSync(): Promise<ManualSyncResult> {
    return runExclusive(() =>
      withLock<ManualSyncResult>(
        () =>
          tracked("manualSync", async () => {
            log.info("manual sync started");

            await outbox.preload();
            await inbox.preload();

            let result: SyncResult = emptyResult();

            if (syncEnabled && transport) {
              result = await pushPull("manual sync");
            } else if (!syncEnabled) {
              log.debug("manual sync push/pull skipped: sync disabled");
            } else {
              log.warn("manual sync: no transport configured, skipping push/pull");
            }

            const errors = [...result.errors];
            let replayed = 0;

            try {
              const outcome = await replayInbox(inbox, replayContext);
              replayed = outcome.applied;
              result = { ...result, skipped: result.skipped + outcome.skipped };
            } catch (err) {
              errors.push(recordError("replay", "manual sync", err));
            }

            log.info("manual sync finished", {
              pushed: result.pushed,
              pulled: result.pulled,
              skipped: result.skipped,
              deadLettered: result.deadLettered,
              replayed,
              errorCount: errors.length,
            });

            return { ...result, replayed, errors };
          }),
        { ...emptyResult({ deferred: true }), replayed: 0 },
      ),
    );
  }

  function getSyncStatus(): SyncStatus {
    let pendingCount = 0;
    let failedCount = 0;

    for (const entry of outbox.state.values()) {
      if (entry.sync) continue;
      pendingCount++;
      if (entry.syncStatus === "failed") failedCount++;
    }

    const stored = readSyncMeta(syncmeta);

    return {
      isSyncing,
      isSynced: pendingCount === 0,
      pendingCount,
      failedCount,
      deadLetterCount: deadletter.state.size,
      pullCursor: readPullCursor(syncmeta, inbox),
      backendId: stored.backendId,
      lastSyncAt: stored.lastSyncAt,
      lastError: stored.lastError,
    };
  }

  function subscribeSyncStatus(listener: (status: SyncStatus) => void): () => void {
    statusListeners.add(listener);
    listener(getSyncStatus());
    return () => statusListeners.delete(listener);
  }

  /**
   * Retries dead-lettered events.
   * - Outbound: pushes directly to the sync transport (does not requeue the outbox).
   * - Inbound: requeues into the inbox and replays immediately.
   * Omit `eventId` to retry everything. Returns how many were successfully
   * cleared from the dead-letter queue.
   */
  function retryDeadLetter(eventId?: string): Promise<number> {
    return runExclusive(async () => {
      const targets = selectDeadLetters(deadletter, eventId);
      const outbound: DeadLetterEntry[] = [];
      let cleared = 0;
      let inboundRequeued = 0;

      for (const entry of targets) {
        if ((entry.direction ?? "outbound") === "inbound") {
          await requeueInboundDeadLetter(inbox, entry);
          await deadletter.delete(entry.eventId).isPersisted.promise;
          inboundRequeued++;
          cleared++;
        } else {
          outbound.push(entry);
        }
      }

      if (inboundRequeued > 0) {
        await replayInbox(inbox, replayContext);
      }

      if (outbound.length > 0) {
        if (!transport?.push) {
          throw new Error(
            "Cannot retry outbound dead-letter events: no push transport is configured.",
          );
        }

        const events: OutboundEvent[] = outbound.map((entry) =>
          toOutboundEvent({
            eventId: entry.eventId,
            collectionId: entry.collectionId,
            type: entry.type,
            key: entry.key,
            payload: entry.payload,
            previous: entry.previous,
            txId: entry.txId ?? entry.eventId,
            clientId: entry.clientId ?? clientId.value,
            schemaVersion: entry.schemaVersion,
            baseVersion: entry.baseVersion ?? null,
            timestamp: entry.timestamp,
            localSeq: entry.localSeq ?? 0,
            globalSeq: null,
            sync: false,
            syncStatus: "pending",
            attemptCount: entry.attemptCount,
            lastAttemptAt: null,
            nextAttemptAt: null,
            lastError: null,
            lastErrorCode: null,
            retryable: null,
          }),
        );

        const response = normalizePushResponse(await transport.push(events));
        const confirmed = new Set(response.confirmed.map((item) => item.eventId));
        const failedById = new Map((response.failed ?? []).map((item) => [item.eventId, item]));
        const now = Date.now();

        for (const confirmation of response.confirmed) {
          const entry = outbound.find((row) => row.eventId === confirmation.eventId);
          if (!entry) continue;

          if (deadletter.has(entry.eventId)) {
            await deadletter.delete(entry.eventId).isPersisted.promise;
          }

          if (conflictDetection) {
            await recordRowVersion(
              rowversions,
              entry.collectionId,
              entry.key,
              entry.eventId,
              confirmation.globalSeq,
            );
          }

          emit("onEventPushed", {
            eventId: confirmation.eventId,
            globalSeq: confirmation.globalSeq,
          });
          cleared++;
        }

        for (const entry of outbound) {
          if (confirmed.has(entry.eventId)) continue;

          const failure = failedById.get(entry.eventId);
          const message = failure?.message ?? "push failed during dead-letter retry";
          const code = failure?.code ?? null;

          if (deadletter.has(entry.eventId)) {
            await deadletter.update(entry.eventId, (draft) => {
              draft.attemptCount = (draft.attemptCount ?? 0) + 1;
              draft.message = message;
              draft.code = code;
              draft.failedAt = now;
            }).isPersisted.promise;
          }
        }

        log.info("outbound dead-letter events pushed", {
          attempted: outbound.length,
          confirmed: response.confirmed.length,
          failed: response.failed?.length ?? 0,
        });
      } else {
        log.info("dead-letter events retried", {
          count: targets.length,
          inbound: inboundRequeued,
        });
      }

      return cleared;
    });
  }

  function discardDeadLetter(eventId?: string): Promise<number> {
    return runExclusive(async () => {
      const targets = selectDeadLetters(deadletter, eventId);

      for (const entry of targets) {
        await deadletter.delete(entry.eventId).isPersisted.promise;
      }

      log.info("dead-letter events discarded", { count: targets.length });

      return targets.length;
    });
  }

  function pruneSyncedEvents(options: PruneOptions = {}): Promise<PruneResult> {
    return runExclusive(async () => {
      const olderThanMs = Math.max(0, options.olderThanMs ?? 0);
      const keepLast = Math.max(0, options.keepLast ?? 0);
      const cutoff = Date.now() - olderThanMs;

      // The cursor is derived partly from resolved inbox rows, so persist it
      // before those rows are removed.
      await writePullCursor(syncmeta, readPullCursor(syncmeta, inbox));

      const outboxTargets = trimKeepLast(
        [...outbox.state.values()]
          .filter((entry) => entry.sync && entry.timestamp <= cutoff)
          .sort((a, b) => a.localSeq - b.localSeq),
        keepLast,
      );

      const inboxTargets = trimKeepLast(
        [...inbox.state.values()]
          .filter((entry) => entry.sync && entry.timestamp <= cutoff)
          .sort((a, b) => a.globalSeq - b.globalSeq),
        keepLast,
      );

      for (const entry of outboxTargets) {
        await outbox.delete(entry.eventId).isPersisted.promise;
      }

      for (const entry of inboxTargets) {
        await inbox.delete(entry.eventId).isPersisted.promise;
      }

      log.info("pruned synced events", {
        outbox: outboxTargets.length,
        inbox: inboxTargets.length,
      });

      return { outbox: outboxTargets.length, inbox: inboxTargets.length };
    });
  }

  function getSyncEnabled(): boolean {
    return syncEnabled;
  }

  function setSyncEnabled(enabled: boolean): void {
    syncEnabled = enabled;
    log.debug("sync enabled updated", { syncEnabled: enabled });
  }

  function dispose(): void {
    log.debug("disposing event-sourced db");
    statusListeners.clear();
    for (const subscription of subscriptions) {
      subscription.unsubscribe();
    }
  }

  return {
    collections,
    sync,
    manualSync,
    getSyncEnabled,
    setSyncEnabled,
    getSyncStatus,
    subscribeSyncStatus,
    retryDeadLetter,
    discardDeadLetter,
    pruneSyncedEvents,
    dispose,
  };
}

function selectDeadLetters(
  deadletter: Collection<DeadLetterEntry, string>,
  eventId?: string,
): DeadLetterEntry[] {
  if (eventId === undefined) {
    return [...deadletter.state.values()].sort(
      (a, b) => (a.localSeq ?? a.globalSeq ?? 0) - (b.localSeq ?? b.globalSeq ?? 0),
    );
  }

  const entry = deadletter.get(eventId);
  return entry ? [entry] : [];
}

/** Puts a parked inbound event back in the inbox so replay can pick it up again. */
async function requeueInboundDeadLetter(
  inbox: Collection<InboxEntry, string>,
  entry: DeadLetterEntry,
): Promise<void> {
  if (inbox.has(entry.eventId)) {
    await inbox.update(entry.eventId, (draft) => {
      draft.sync = false;
      draft.skipped = false;
      draft.skipReason = null;
      draft.attemptCount = 0;
      draft.lastError = null;
    }).isPersisted.promise;
    return;
  }

  await inbox.insert({
    eventId: entry.eventId,
    globalSeq: entry.globalSeq ?? 0,
    collectionId: entry.collectionId,
    type: entry.type,
    key: entry.key,
    payload: entry.payload,
    previous: entry.previous,
    clientId: entry.clientId,
    schemaVersion: entry.schemaVersion,
    timestamp: entry.timestamp,
    sync: false,
    skipped: false,
    skipReason: null,
    attemptCount: 0,
    lastError: null,
  }).isPersisted.promise;
}

/** Drops the newest `keepLast` items from an ascending-sorted removal list. */
function trimKeepLast<T>(items: ReadonlyArray<T>, keepLast: number): T[] {
  if (keepLast <= 0) return [...items];
  return items.slice(0, Math.max(0, items.length - keepLast));
}

function createMetaCollection<TEntry extends object>(
  config: MetaCollectionFactory,
  id: string,
  getKey: (entry: TEntry) => string,
  schemaVersion: number,
): Collection<TEntry, string> {
  const options = config.persistedCollectionOptions<TEntry, string>({
    id,
    getKey,
    persistence: config.persistence,
    schemaVersion,
  });

  return config.createCollection(options);
}

type IndexableCollection = {
  createIndex: (
    indexCallback: (row: Record<string, unknown>) => unknown,
    config?: {
      name?: string;
      indexType?: new (id: number, expression: never, name?: string, options?: never) => unknown;
    },
  ) => unknown;
  getIndexMetadata?: () => ReadonlyArray<unknown>;
  on?: (event: "status:ready", callback: () => void) => () => void;
};

function applyCollectionIndexes(
  collection: Collection<Record<string, unknown>, string | number>,
  collectionId: string,
  indexes: CollectionDefConstraint["indexes"],
  log: EventSourcedLogger,
): void {
  if (!indexes?.length) return;

  const indexable = collection as IndexableCollection;

  const register = (): void => {
    if ((indexable.getIndexMetadata?.().length ?? 0) > 0) return;

    for (const indexDef of indexes) {
      indexable.createIndex(indexDef.select as (row: Record<string, unknown>) => unknown, {
        name: indexDef.name,
        indexType: indexDef.indexType,
      });

      log.debug("registered collection index", { collectionId, name: indexDef.name });
    }
  };

  register();

  indexable.on?.("status:ready", register);
}

function createMutationHook(context: MutationContext) {
  const {
    outbox,
    rowversions,
    collectionId,
    type,
    seq,
    clientId,
    eventSchemaVersion,
    conflictDetection,
    emit,
    log,
  } = context;

  return async (params: MutationHookParams): Promise<Record<string, unknown>> => {
    const txId = generateEventId();
    const timestamp = Date.now();

    for (const mutation of params.transaction.mutations) {
      const payload = type === "delete" ? mutation.original : mutation.modified;
      const eventId = generateEventId();

      const entry: OutboxEntry = {
        eventId,
        collectionId,
        type,
        key: mutation.key,
        payload,
        previous: type === "insert" ? null : mutation.original,
        txId,
        clientId: clientId.value,
        schemaVersion: eventSchemaVersion,
        baseVersion: conflictDetection
          ? readRowVersion(rowversions, collectionId, mutation.key)
          : null,
        timestamp,
        localSeq: allocateLocalSeq(outbox, seq),
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

      await outbox.insert(entry).isPersisted.promise;

      if (conflictDetection) {
        await recordRowVersion(rowversions, collectionId, mutation.key, eventId, null);
      }

      log.debug("outbox entry created", {
        eventId,
        collectionId,
        type,
        key: entry.key,
        localSeq: entry.localSeq,
        txId,
      });

      emit("onMutation", entry);
    }

    return {};
  };
}

/**
 * Reads the high-water mark from live outbox state rather than trusting the
 * cached counter alone, so a second tab writing to the same persisted store
 * cannot hand out a localSeq that has already been used.
 */
function allocateLocalSeq(outbox: Collection<OutboxEntry, string>, seq: SeqCounter): number {
  const allocated = Math.max(seq.value, nextLocalSeq(outbox));
  seq.value = allocated + 1;
  return allocated;
}

function nextLocalSeq(outbox: Collection<OutboxEntry, string>): number {
  let max = -1;

  for (const entry of outbox.state.values()) {
    if (entry.localSeq > max) max = entry.localSeq;
  }

  return max + 1;
}

function assertReservedNamesAvailable(collections: Record<string, unknown>): void {
  for (const id of Object.keys(collections)) {
    if (RESERVED_IDS.has(id)) {
      throw new Error(
        `Collection id "${id}" is reserved. Built-in collections are: ${[...RESERVED_IDS]
          .map((name) => `"${name}"`)
          .join(", ")}.`,
      );
    }
  }
}

function toError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err));
}
