import { BasicIndex, type Collection } from "@tanstack/db";

import {
  DEADLETTER_ID,
  DEFAULT_BASE_DELAY_MS,
  DEFAULT_EVENT_SCHEMA_VERSION,
  DEFAULT_MAX_ATTEMPTS,
  DEFAULT_MAX_DELAY_MS,
  INBOX_ID,
  OUTBOX_ID,
  ROWVERSIONS_ID,
  SYNCMETA_ID,
  SYNC_LOCK_PREFIX,
} from "../internal/constants";
import { pendingLocalOnlyOutbox } from "../internal/collection-queries";
import {
  anchorFoldedEvent,
  compactOutbox,
  foldedOutboxBody,
  nextLocalSeq,
} from "../internal/compact-outbox";
import { createOutboxWriteGate } from "../internal/outbox-gate";
import { createHookEmitter } from "../internal/hooks";
import { replayInbox } from "../internal/replay";
import { createSerialQueue } from "../internal/serial-queue";
import { ensureSyncMeta, readPullCursor, resolveClientId } from "../internal/sync-meta";
import type {
  AcceptMutationsCollection,
  MetaCollections,
  ReplayContext,
  ResolvedRetryConfig,
} from "../internal/types";
import { createEventSourcedLogger } from "../utils/logger";
import { generateEventId } from "../utils/uuid";
import { createSyncTransport } from "./sync";
import { resolvePushLimits } from "./sync-presets";
import type {
  CollectionMap,
  DeadLetterEntry,
  EventSourcedDB,
  EventSourcedDBConfig,
  InboxEntry,
  MutationType,
  OutboxEntry,
  RowVersionEntry,
  SyncMetaEntry,
  SyncPushLimits,
  SyncStatus,
} from "./types";
import { createMaintenance } from "./event-sourced-db/maintenance";
import { applyCollectionIndexes, createMetaCollection } from "./event-sourced-db/meta-collections";
import { createMutationHook, noOutboxHook } from "./event-sourced-db/mutation-hook";
import { buildUnsyncedOutboxEntry } from "./event-sourced-db/outbox-entry";
import type { SyncRuntime } from "./event-sourced-db/runtime";
import { createSyncCycle } from "./event-sourced-db/sync-cycle";
import { assertReservedNamesAvailable } from "./event-sourced-db/errors";
import type { ClientIdRef, SeqCounter } from "./event-sourced-db/refs";

export { SyncBatchLimitError, UnsyncedChangesError } from "./event-sourced-db/errors";

type CollectionDefConstraint = {
  getKey: (state: never) => string | number;
  schemaVersion?: number;
  localOnly?: boolean;
  indexes?: ReadonlyArray<{
    select: (row: never) => unknown;
    name?: string;
    indexType?: new (id: number, expression: never, name?: string, options?: never) => unknown;
  }>;
};

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
  let pushLimits: SyncPushLimits = resolvePushLimits(config);
  const { pushBatchSize, maxPushBytes } = pushLimits;
  const backendMismatch = config.backendMismatch ?? "resetCursor";
  const conflictDetection = config.conflictDetection ?? false;
  const compactOnWrite = config.compactOnWrite ?? true;
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
    maxPushBytes,
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
  outbox.createIndex((row) => row.collectionId, { name: "by-collection", indexType: BasicIndex });
  outbox.createIndex((row) => row.key, { name: "by-key", indexType: BasicIndex });
  const userCollections = {} as CollectionMap<TDefs>;
  const syncedCollections: Array<{
    collectionId: string;
    collection: Collection<Record<string, unknown>, string | number>;
  }> = [];
  const localOnlyIds = new Set(
    Object.keys(config.collections).filter((id) => config.collections[id]!.localOnly === true),
  );
  const outboxGate = createOutboxWriteGate();

  for (const collectionId of Object.keys(config.collections)) {
    const def = config.collections[collectionId]!;
    const getKey = def.getKey as (item: Record<string, unknown>) => string | number;
    const hook = (type: MutationType) =>
      def.localOnly ? noOutboxHook : createMutationHook({ ...mutationContext, type });

    const mutationContext = {
      outbox,
      rowversions,
      collectionId,
      seq,
      clientId,
      eventSchemaVersion,
      conflictDetection,
      compactOnWrite,
      outboxGate,
      emit,
      log,
    };

    const options = config.persistedCollectionOptions<Record<string, unknown>, string | number>({
      id: collectionId,
      getKey,
      persistence: config.persistence,
      schemaVersion: def.schemaVersion ?? defaultSchemaVersion,
      gcTime: Number.POSITIVE_INFINITY,
      onInsert: hook("insert"),
      onUpdate: hook("update"),
      onDelete: hook("delete"),
    });

    const collection = config.createCollection(options);
    applyCollectionIndexes(collection, collectionId, def.indexes, log);
    if (!def.localOnly) syncedCollections.push({ collectionId, collection });

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
  let readStatus: () => SyncStatus = () => {
    throw new Error("sync status is not ready");
  };

  const notifyStatus = (): void => {
    if (statusListeners.size === 0) return;
    const status = readStatus();
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

  // Outbox entries authored before a collection became local-only would
  // otherwise still be pushed.
  for (const entry of await pendingLocalOnlyOutbox(outbox, [...localOnlyIds])) {
    await outbox.delete(entry.eventId).isPersisted.promise;
  }

  const replayContext: ReplayContext = {
    targets: replayTargets,
    localOnlyIds,
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

  const runtime = {
    outbox,
    inbox,
    deadletter,
    syncmeta,
    rowversions,
    transport,
    clientId,
    replayContext,
    replayTargets,
    syncedCollections,
    emit,
    log,
    retry,
    pullOverlap,
    recordLocalEchoes,
    backendMismatch,
    conflictDetection,
    lock: config.lock,
    lockName,
    runExclusive,
    outboxGate,
    getSyncEnabled: () => syncEnabled,
    getPushLimits: () => pushLimits,
    syncBatch: config.syncBatch,
    isSyncing: () => isSyncing,
    setSyncing: (value: boolean) => {
      isSyncing = value;
    },
    notifyStatus,
    statusListeners,
  } satisfies SyncRuntime;

  const cycle = createSyncCycle(runtime);
  readStatus = cycle.getSyncStatus;
  const maintenance = createMaintenance(runtime, cycle.withLock);

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
    sync: cycle.sync,
    manualSync: cycle.manualSync,
    getSyncEnabled,
    setSyncEnabled,
    getSyncStatus: cycle.getSyncStatus,
    subscribeSyncStatus: cycle.subscribeSyncStatus,
    retryDeadLetter: maintenance.retryDeadLetter,
    discardDeadLetter: maintenance.discardDeadLetter,
    getPushLimits: () => pushLimits,
    setPushLimits(next: Partial<SyncPushLimits>) {
      pushLimits = resolvePushLimits({
        pushBatchSize: next.pushBatchSize ?? pushLimits.pushBatchSize,
        maxPushBytes:
          (next.maxPushBytes === undefined ? pushLimits.maxPushBytes : next.maxPushBytes) ??
          undefined,
      });
      log.info("push limits updated", pushLimits);
      return pushLimits;
    },
    pruneSyncedEvents: maintenance.pruneSyncedEvents,
    compact: () =>
      runExclusive(() =>
        cycle.withLock(
          () =>
            outboxGate.run(() =>
              compactOutbox(
                syncedCollections,
                outbox,
                {
                  squash(pending) {
                    const latest = pending[pending.length - 1]!;
                    const anchor = anchorFoldedEvent(pending, outbox);
                    const now = Date.now();
                    return buildUnsyncedOutboxEntry({
                      identity: latest,
                      body: foldedOutboxBody({
                        unpushedEvents: pending,
                        latest: latest.payload,
                        now,
                      }),
                      ids: {
                        outbox,
                        seq,
                        eventId: anchor.eventId,
                        txId: anchor.txId,
                        timestamp: anchor.timestamp,
                        localSeq: anchor.localSeq,
                      },
                    });
                  },
                  deleteRow(lastSynced, current) {
                    return buildUnsyncedOutboxEntry({
                      identity: {
                        collectionId: current.collectionId,
                        key: current.key,
                        clientId: current.clientId,
                        schemaVersion: current.schemaVersion,
                      },
                      body: {
                        type: "delete",
                        payload: lastSynced.payload,
                        previous: lastSynced.payload,
                        baseVersion: lastSynced.eventId,
                      },
                      ids: {
                        outbox,
                        seq,
                        eventId: current.eventId,
                        txId: current.txId,
                        timestamp: current.timestamp,
                        localSeq: current.localSeq,
                      },
                    });
                  },
                },
                (eventId) => outboxGate.isReserved(eventId),
              ),
            ),
          { compacted: 0, removed: 0, deferred: true },
        ),
      ),
    resetLocalReplica: maintenance.resetLocalReplica,
    dispose,
  };
}
