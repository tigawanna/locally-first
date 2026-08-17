import type { NormalizedSyncTransport, PullResponse, ServerEvent } from "../core/sync";
import type { EventSourcedLogger } from "../utils/logger";
import type { EmitHook, BackendMismatchPolicy } from "./hooks";
import type { DrizzleAdapter, InboxRow, ReplayContext } from "./types";
import { replayEvent, handleReplayFailure } from "./replay";
import { readBackendId, readPullCursor, writeBackendId, writePullCursor } from "./sync-meta";

export type PullOutcome = {
  pulled: number;
  skipped: number;
  deadLettered: number;
  requeued: number;
};

/**
 * Thrown when `backendMismatch` is `"fail"` and the server reports a different
 * `backendId` than this client last synced with (wiped or swapped event store).
 *
 * @example
 * ```ts
 * import { BackendMismatchError } from "event-sourced-drizzle"
 *
 * const result = await engine.sync()
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
  adapter: DrizzleAdapter;
  pull: NonNullable<NormalizedSyncTransport["pull"]>;
  clientId: string;
  pullOverlap: number;
  backendMismatch: BackendMismatchPolicy;
  context: ReplayContext;
  emit: EmitHook;
  log: EventSourcedLogger;
};

/**
 * Determines if a pulled event originated from this client.
 */
async function isLocalOrigin(
  adapter: DrizzleAdapter,
  event: ServerEvent,
  clientId: string,
): Promise<boolean> {
  return (await adapter.outboxHas(event.eventId)) || event.clientId === clientId;
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

function toInboxRow(event: ServerEvent): InboxRow {
  return {
    eventId: event.eventId,
    globalSeq: event.globalSeq,
    collectionId: event.collectionId,
    type: event.type,
    key: String(event.key),
    payload: event.payload,
    previous: event.previous ?? null,
    clientId: event.clientId ?? null,
    schemaVersion: event.schemaVersion ?? 1,
    timestamp: event.timestamp,
    sync: false,
    skipped: false,
    skipReason: null,
    attemptCount: 0,
    lastError: null,
  };
}

async function reconcileBackendIdentity(
  args: PullArgs,
  receivedId: string,
): Promise<{ reset: boolean; requeued: number }> {
  const { adapter, backendMismatch, emit, log } = args;
  const expected = await readBackendId(adapter);

  if (expected === receivedId || expected === null) {
    await writeBackendId(adapter, receivedId);
    return { reset: false, requeued: 0 };
  }

  emit("onBackendMismatch", { expected, received: receivedId, policy: backendMismatch });

  if (backendMismatch === "fail") {
    throw new BackendMismatchError(expected, receivedId);
  }

  if (backendMismatch === "ignore") {
    log.warn("backend mismatch ignored", { expected, received: receivedId });
    return { reset: false, requeued: 0 };
  }

  // resetCursor: wipe inbox, pull from zero, update backendId.
  log.warn("backend mismatch: resetting cursor", { expected, received: receivedId });
  await writeBackendId(adapter, receivedId);
  await writePullCursor(adapter, 0);

  return { reset: true, requeued: 0 };
}

/**
 * Pulls events from the server, inserts them into the inbox, and replays them
 * into domain tables.
 */
export async function pullInbox(args: PullArgs): Promise<PullOutcome> {
  const { adapter, pull, clientId, pullOverlap, context, log } = args;

  let pulled = 0;
  let skipped = 0;
  let deadLettered = 0;
  let requeued = 0;
  let hasMore = true;
  let cursor = Math.max(0, (await readPullCursor(adapter)) - pullOverlap);
  let identityChecked = false;

  while (hasMore) {
    log.debug("pull inbox page", { since: cursor });

    const response: PullResponse = await pull(cursor);

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
      // Skip events that originated locally.
      if (await isLocalOrigin(adapter, event, clientId)) {
        const existing = await adapter.getInboxRow(event.eventId);
        if (!existing) {
          await adapter.insertInbox({ ...toInboxRow(event), sync: true });
        } else if (!existing.sync) {
          await adapter.updateInbox(event.eventId, { sync: true });
        }
        log.debug("pull skipped: event originated locally", {
          eventId: event.eventId,
          globalSeq: event.globalSeq,
        });
        continue;
      }

      // Skip already-resolved inbox rows.
      const existing = await adapter.getInboxRow(event.eventId);
      if (existing?.sync) {
        log.debug("pull skipped: inbox already resolved", {
          eventId: event.eventId,
          globalSeq: event.globalSeq,
        });
        continue;
      }

      // Insert into inbox if new.
      if (!existing) {
        await adapter.insertInbox(toInboxRow(event));
        log.debug("inbox entry inserted", {
          eventId: event.eventId,
          globalSeq: event.globalSeq,
          collectionId: event.collectionId,
        });
      }

      // Replay into domain table.
      const inboxRow = (await adapter.getInboxRow(event.eventId))!;
      const outcome = await replayEvent(context, inboxRow);

      if (outcome.status === "halted") {
        halted = true;
        break;
      }

      if (outcome.status === "failed") {
        const resolution = await handleReplayFailure(context, inboxRow, outcome.error, Date.now());
        if (resolution === "retry") {
          halted = true;
          break;
        }
        deadLettered++;
        continue;
      }

      if (outcome.status === "skipped") {
        await adapter.updateInbox(event.eventId, {
          sync: true,
          skipped: true,
          skipReason: outcome.reason,
        });
        skipped++;
        continue;
      }

      // Applied.
      await adapter.updateInbox(event.eventId, { sync: true });
      pulled++;

      log.info("pull replay applied", {
        eventId: event.eventId,
        globalSeq: event.globalSeq,
        collectionId: event.collectionId,
        type: event.type,
        key: event.key,
      });
    }

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
    await writePullCursor(adapter, cursor);
    hasMore = response.hasMore;
  }

  return { pulled, skipped, deadLettered, requeued };
}
