import type { Collection } from "@tanstack/db";

import type { NormalizedSyncTransport } from "../core/sync";
import type {
  BackendMismatchPolicy,
  InboxEntry,
  OutboxEntry,
  ServerEvent,
  SyncMetaEntry,
} from "../core/types";
import { DEFAULT_EVENT_SCHEMA_VERSION } from "./constants";
import { handleReplayFailure, replayEvent, resolveInboxEntry } from "./replay";
import { readBackendId, readPullCursor, writeBackendId, writePullCursor } from "./sync-meta";
import type { ReplayContext } from "./types";

export type PullOutcome = {
  pulled: number;
  skipped: number;
  deadLettered: number;
  /** Outbox events put back in the queue by a backend reset, awaiting a push. */
  requeued: number;
};

/**
 * Thrown when `backendMismatch` is `"fail"` and the server reports a different
 * `backendId` than this client last synced with (wiped or swapped event store).
 *
 * @example
 * ```ts
 * import { BackendMismatchError } from "event-sourced-collection"
 *
 * const result = await db.sync()
 * const mismatch = result.errors.find((error) => error instanceof BackendMismatchError)
 * if (mismatch) {
 *   console.error("backend changed", mismatch.expected, mismatch.received)
 * }
 * ```
 */
export class BackendMismatchError extends Error {
  constructor(
    readonly expected: string | null,
    readonly received: string,
  ) {
    super(
      `Sync backend identity changed (was "${expected ?? "unknown"}", now "${received}"). ` +
        `The local pull cursor no longer refers to this backend.`,
    );
    this.name = "BackendMismatchError";
  }
}

export type PullArgs = {
  outbox: Collection<OutboxEntry, string>;
  inbox: Collection<InboxEntry, string>;
  syncmeta: Collection<SyncMetaEntry, string>;
  pull: NonNullable<NormalizedSyncTransport["pull"]>;
  clientId: string;
  pullOverlap: number;
  /** When false, local-origin pulls advance the cursor without inbox rows. */
  recordLocalEchoes: boolean;
  backendMismatch: BackendMismatchPolicy;
  context: ReplayContext;
};

export async function pullInbox(args: PullArgs): Promise<PullOutcome> {
  const { outbox, inbox, syncmeta, pull, clientId, pullOverlap, recordLocalEchoes, context } =
    args;
  const { log } = context;

  let pulled = 0;
  let skipped = 0;
  let deadLettered = 0;
  let requeued = 0;
  let hasMore = true;
  let cursor = Math.max(0, readPullCursor(syncmeta, inbox) - pullOverlap);
  let identityChecked = false;

  while (hasMore) {
    log.debug("pull inbox page", { since: cursor });

    const response = await pull(cursor);

    log.debug("pull inbox response", {
      since: cursor,
      eventCount: response.events.length,
      hasMore: response.hasMore,
      cursor: response.cursor,
      backendId: response.backendId,
    });

    if (!identityChecked && response.backendId !== undefined) {
      identityChecked = true;

      const reconciled = await reconcileBackendIdentity(args, response.backendId);
      if (reconciled.reset) {
        requeued += reconciled.requeued;
        cursor = 0;
        continue;
      }
    }

    if (response.events.length === 0) break;

    const sorted = [...response.events].sort((a, b) => a.globalSeq - b.globalSeq);
    let halted = false;

    for (const event of sorted) {
      if (isLocalOrigin(event, outbox, clientId)) {
        if (recordLocalEchoes) {
          await markInboxEventResolved(inbox, event);
        }

        log.debug("pull skipped: event originated locally", {
          eventId: event.eventId,
          globalSeq: event.globalSeq,
          recorded: recordLocalEchoes,
        });
        continue;
      }

      const existing = inbox.get(event.eventId);
      if (existing?.sync) {
        log.debug("pull skipped: inbox already resolved", {
          eventId: event.eventId,
          globalSeq: event.globalSeq,
        });
        continue;
      }

      if (!existing) {
        await inbox.insert(toInboxEntry(event, false)).isPersisted.promise;
        log.debug("inbox entry inserted", {
          eventId: event.eventId,
          globalSeq: event.globalSeq,
          collectionId: event.collectionId,
        });
      }

      const outcome = await replayEvent(context, event);

      if (outcome.status === "halted") {
        halted = true;
        break;
      }

      if (outcome.status === "failed") {
        const resolution = await handleReplayFailure(
          context,
          inbox,
          event,
          outcome.error,
          Date.now(),
        );

        if (resolution === "retry") {
          halted = true;
          break;
        }

        deadLettered++;
        continue;
      }

      await resolveInboxEntry(inbox, event.eventId, outcome);

      if (outcome.status === "skipped") {
        skipped++;
        continue;
      }

      log.info("pull replay applied", {
        eventId: event.eventId,
        globalSeq: event.globalSeq,
        collectionId: event.collectionId,
        type: event.type,
        key: event.key,
      });

      pulled++;
    }

    // Leaving the cursor where it is means the halted event is retried on the
    // next sync rather than being stepped over.
    if (halted) break;

    const nextCursor = resolveNextCursor(cursor, sorted, response.cursor);

    if (nextCursor <= cursor) {
      log.warn("pull stopped: cursor did not advance", {
        cursor,
        nextCursor,
        eventCount: sorted.length,
      });
      break;
    }

    cursor = nextCursor;
    await writePullCursor(syncmeta, cursor);
    hasMore = response.hasMore;
  }

  log.info("pull inbox finished", { pulled, skipped, deadLettered, requeued, cursor });

  return { pulled, skipped, deadLettered, requeued };
}

type ReconcileOutcome = { reset: false } | { reset: true; requeued: number };

/**
 * Detects a wiped or swapped backend. Reports `reset` when the caller should
 * restart the pull from zero.
 */
async function reconcileBackendIdentity(
  args: PullArgs,
  received: string,
): Promise<ReconcileOutcome> {
  const { syncmeta, inbox, backendMismatch, context } = args;
  const { log } = context;

  const stored = readBackendId(syncmeta);

  if (stored === received) return { reset: false };

  if (stored === null) {
    await writeBackendId(syncmeta, received);
    log.info("sync backend identity recorded", { backendId: received });
    return { reset: false };
  }

  context.emit("onBackendMismatch", {
    expected: stored,
    received,
    policy: backendMismatch,
  });

  if (backendMismatch === "fail") {
    throw new BackendMismatchError(stored, received);
  }

  if (backendMismatch === "ignore") {
    log.warn("sync backend identity changed; ignoring per configuration", {
      expected: stored,
      received,
    });
    return { reset: false };
  }

  log.warn("sync backend identity changed; resetting pull cursor", {
    expected: stored,
    received,
  });

  for (const entry of inbox.state.values()) {
    await inbox.delete(entry.eventId).isPersisted.promise;
  }

  const requeued = await requeueSyncedOutbox(args.outbox);

  await writeBackendId(syncmeta, received);
  await writePullCursor(syncmeta, 0);

  log.info("sync backend identity reset complete", {
    backendId: received,
    inboxCleared: true,
    outboxRequeued: requeued,
  });

  return { reset: true, requeued };
}

/**
 * Marks previously confirmed outbox events as pending again. The new backend
 * has never seen them, so without this the client keeps local rows it believes
 * are safely synced and never uploads them again. Servers deduplicate by
 * `eventId`, so re-pushing to a *restored* backend is a no-op.
 *
 * `baseVersion` is cleared because it refers to event ids from the old
 * backend's history, which the new one cannot resolve.
 */
async function requeueSyncedOutbox(outbox: Collection<OutboxEntry, string>): Promise<number> {
  const synced = [...outbox.state.values()].filter((entry) => entry.sync);

  for (const entry of synced) {
    await outbox.update(entry.eventId, (draft) => {
      draft.sync = false;
      draft.syncStatus = "pending";
      draft.globalSeq = null;
      draft.baseVersion = null;
      draft.attemptCount = 0;
      draft.nextAttemptAt = null;
      draft.lastError = null;
      draft.lastErrorCode = null;
      draft.retryable = null;
    }).isPersisted.promise;
  }

  return synced.length;
}

function isLocalOrigin(
  event: ServerEvent,
  outbox: Collection<OutboxEntry, string>,
  clientId: string,
): boolean {
  // clientId keeps origin detection working after the outbox has been pruned.
  return outbox.has(event.eventId) || event.clientId === clientId;
}

function resolveNextCursor(
  cursor: number,
  events: ReadonlyArray<ServerEvent>,
  serverCursor: string | undefined,
): number {
  let next = cursor;

  for (const event of events) {
    if (event.globalSeq > next) next = event.globalSeq;
  }

  const parsed = Number(serverCursor);
  if (serverCursor !== undefined && Number.isFinite(parsed) && parsed > next) {
    next = parsed;
  }

  return next;
}

async function markInboxEventResolved(
  inbox: Collection<InboxEntry, string>,
  event: ServerEvent,
): Promise<void> {
  const existing = inbox.get(event.eventId);

  if (!existing) {
    await inbox.insert(toInboxEntry(event, true)).isPersisted.promise;
    return;
  }

  if (!existing.sync || existing.globalSeq !== event.globalSeq) {
    await inbox.update(event.eventId, (draft) => {
      draft.globalSeq = event.globalSeq;
      draft.sync = true;
    }).isPersisted.promise;
  }
}

export function toInboxEntry(event: ServerEvent, sync: boolean): InboxEntry {
  return {
    eventId: event.eventId,
    globalSeq: event.globalSeq,
    collectionId: event.collectionId,
    type: event.type,
    key: event.key,
    payload: event.payload,
    previous: event.previous ?? null,
    clientId: event.clientId ?? null,
    schemaVersion: event.schemaVersion ?? DEFAULT_EVENT_SCHEMA_VERSION,
    timestamp: event.timestamp,
    sync,
    skipped: false,
    skipReason: null,
    attemptCount: 0,
    lastError: null,
  };
}
