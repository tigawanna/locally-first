import type { Collection } from "@tanstack/db";

import type { NormalizedSyncTransport } from "../core/sync";
import { normalizePushResponse } from "../core/sync";
import type {
  DeadLetterEntry,
  DeadLetterReason,
  OutboundEvent,
  OutboxEntry,
  PushConfirmation,
  PushFailure,
  RowVersionEntry,
} from "../core/types";
import type { EventSourcedLogger } from "../utils/logger";
import {
  CONFLICT_ERROR_CODE,
  DEFAULT_EVENT_SCHEMA_VERSION,
  PAYLOAD_TOO_LARGE_ERROR_CODE,
} from "./constants";
import type { EmitHook } from "./hooks";
import { restoreRowVersionAfterConflict } from "./replay";
import type { ResolvedRetryConfig } from "./types";

export type PushOutcome = {
  pushed: number;
  deadLettered: number;
  /**
   * Set when a batch failed at the transport level. Reported rather than thrown
   * so the counts from batches that already succeeded are not lost.
   */
  error?: Error;
};

export type PushArgs = {
  outbox: Collection<OutboxEntry, string>;
  deadletter: Collection<DeadLetterEntry, string>;
  rowversions: Collection<RowVersionEntry, string>;
  push: NonNullable<NormalizedSyncTransport["push"]>;
  batchSize: number;
  /** JSON byte cap per request; `null` means unlimited. */
  maxBytes: number | null;
  retry: ResolvedRetryConfig;
  now: number;
  emit: EmitHook;
  log: EventSourcedLogger;
};

/**
 * Backoff for attempt N, doubling from `baseDelayMs` and capped at `maxDelayMs`.
 * Deliberately jitter-free so retry scheduling stays reproducible in tests; if
 * you have many clients hitting one server, add jitter in your push handler.
 */
export function backoffDelay(attemptCount: number, retry: ResolvedRetryConfig): number {
  const exponent = Math.max(0, attemptCount - 1);
  const raw = retry.baseDelayMs * 2 ** exponent;
  return Math.min(retry.maxDelayMs, raw);
}

function isDue(entry: OutboxEntry, now: number): boolean {
  if (entry.sync) return false;
  // A permanent rejection is dead-lettered rather than parked, so anything still
  // marked failed here is retryable and just waiting for its backoff window.
  if (entry.syncStatus === "failed" && entry.retryable !== true) return false;
  return (entry.nextAttemptAt ?? 0) <= now;
}

function byLocalSeq(a: OutboxEntry, b: OutboxEntry): number {
  if (a.localSeq !== b.localSeq) return a.localSeq - b.localSeq;
  // localSeq can collide across tabs; eventId is a uuidv7 so it breaks the tie
  // in creation order.
  return a.eventId < b.eventId ? -1 : a.eventId > b.eventId ? 1 : 0;
}

export function toOutboundEvent(entry: OutboxEntry): OutboundEvent {
  return {
    eventId: entry.eventId,
    collectionId: entry.collectionId,
    type: entry.type,
    key: entry.key,
    payload: entry.payload,
    previous: entry.previous ?? null,
    txId: entry.txId,
    clientId: entry.clientId,
    // Rows written before these fields existed are still in persisted outboxes.
    schemaVersion: entry.schemaVersion ?? DEFAULT_EVENT_SCHEMA_VERSION,
    baseVersion: entry.baseVersion ?? null,
    timestamp: entry.timestamp,
  };
}

export async function deadLetter(
  deadletter: Collection<DeadLetterEntry, string>,
  outbox: Collection<OutboxEntry, string>,
  entry: OutboxEntry,
  reason: DeadLetterReason,
  message: string,
  code: string | null,
  now: number,
  emit: EmitHook,
  log: EventSourcedLogger,
  rowversions?: Collection<RowVersionEntry, string>,
): Promise<void> {
  const record: DeadLetterEntry = {
    eventId: entry.eventId,
    collectionId: entry.collectionId,
    type: entry.type,
    key: entry.key,
    payload: entry.payload,
    previous: entry.previous ?? null,
    txId: entry.txId,
    clientId: entry.clientId,
    schemaVersion: entry.schemaVersion ?? DEFAULT_EVENT_SCHEMA_VERSION,
    baseVersion: entry.baseVersion ?? null,
    timestamp: entry.timestamp,
    localSeq: entry.localSeq,
    globalSeq: entry.globalSeq ?? null,
    direction: "outbound",
    reason,
    message,
    code,
    attemptCount: entry.attemptCount,
    failedAt: now,
  };

  if (!deadletter.has(entry.eventId)) {
    await deadletter.insert(record).isPersisted.promise;
  }

  if (outbox.has(entry.eventId)) {
    await outbox.delete(entry.eventId).isPersisted.promise;
  }

  if (reason === "conflict" && rowversions) {
    await restoreRowVersionAfterConflict(
      rowversions,
      entry.collectionId,
      entry.key,
      entry.baseVersion ?? null,
    );
  }

  log.warn("event dead-lettered", {
    eventId: entry.eventId,
    collectionId: entry.collectionId,
    reason,
    message,
    code,
    attemptCount: entry.attemptCount,
  });

  emit("onDeadLetter", record);
}

/** All events of one transaction, plus the JSON bytes they add to a request body. */
type TxGroup = { entries: OutboxEntry[]; bytes: number };

type Batch = { groups: TxGroup[]; count: number; bytes: number };

const utf8 = new TextEncoder();

function jsonBytes(entry: OutboxEntry): number {
  return utf8.encode(JSON.stringify(toOutboundEvent(entry))).length;
}

/** Events are ordered by localSeq, so one transaction's events are adjacent. */
function groupByTransaction(events: ReadonlyArray<OutboxEntry>, measure: boolean): TxGroup[] {
  const groups: TxGroup[] = [];

  for (let index = 0; index < events.length;) {
    const txId = events[index]!.txId;
    const group: TxGroup = { entries: [], bytes: 0 };

    while (index < events.length && events[index]!.txId === txId) {
      const entry = events[index]!;
      group.entries.push(entry);
      // +1 for the comma (or the array bracket) separating it from its neighbour.
      if (measure) group.bytes += jsonBytes(entry) + 1;
      index++;
    }

    groups.push(group);
  }

  return groups;
}

/**
 * Packs transaction groups into batches of at most `size` events and
 * `maxBytes` of JSON, never separating events that share a `txId`: the server
 * commits each transaction atomically, which it cannot do if we deliver half
 * of one now and half later. A transaction with more than `size` events is
 * sent whole; one larger than `maxBytes` is returned in `oversized` instead.
 */
function packBatches(
  groups: ReadonlyArray<TxGroup>,
  size: number,
  maxBytes: number | null,
): { batches: Batch[]; oversized: TxGroup[] } {
  const batches: Batch[] = [];
  const oversized: TxGroup[] = [];
  let current: Batch = { groups: [], count: 0, bytes: 1 };

  const close = () => {
    if (current.count > 0) batches.push(current);
    current = { groups: [], count: 0, bytes: 1 };
  };

  for (const group of groups) {
    if (maxBytes !== null && group.bytes + 1 > maxBytes) {
      oversized.push(group);
      continue;
    }

    const overCount = current.count + group.entries.length > size;
    const overBytes = maxBytes !== null && current.bytes + group.bytes > maxBytes;
    if (current.count > 0 && (overCount || overBytes)) close();

    current.groups.push(group);
    current.count += group.entries.length;
    current.bytes += group.bytes;

    if (current.count >= size) close();
  }

  close();
  return { batches, oversized };
}

function isPayloadTooLarge(err: unknown): boolean {
  return typeof err === "object" && err !== null && "status" in err && err.status === 413;
}

type PostResult = {
  confirmed: PushConfirmation[];
  failed: PushFailure[];
  /** Transactions the server would not accept even when sent alone. */
  tooLarge: OutboxEntry[];
  /** Left unsent because the transport failed; `error` says why. */
  unsent: OutboxEntry[];
  error?: unknown;
};

/**
 * Posts `groups` as one request. On HTTP 413 the request is halved along
 * transaction boundaries and each half posted in turn, down to a single
 * transaction, so one oversized event cannot hold the rest hostage.
 */
async function postGroups(
  push: PushArgs["push"],
  groups: ReadonlyArray<TxGroup>,
  log: EventSourcedLogger,
): Promise<PostResult> {
  const entries = groups.flatMap((group) => group.entries);

  try {
    const response = normalizePushResponse(await push(entries.map(toOutboundEvent)));
    return {
      confirmed: [...response.confirmed],
      failed: [...(response.failed ?? [])],
      tooLarge: [],
      unsent: [],
    };
  } catch (err) {
    if (!isPayloadTooLarge(err)) {
      return { confirmed: [], failed: [], tooLarge: [], unsent: entries, error: err };
    }

    if (groups.length === 1) {
      log.warn("push transaction too large for the server", { events: entries.length });
      return { confirmed: [], failed: [], tooLarge: entries, unsent: [] };
    }

    const middle = Math.ceil(groups.length / 2);
    log.info("push batch hit HTTP 413, splitting", {
      groups: groups.length,
      events: entries.length,
    });

    const first = await postGroups(push, groups.slice(0, middle), log);
    if (first.error !== undefined) {
      const rest = groups.slice(middle).flatMap((group) => group.entries);
      return { ...first, unsent: [...first.unsent, ...rest] };
    }

    const second = await postGroups(push, groups.slice(middle), log);
    return {
      confirmed: [...first.confirmed, ...second.confirmed],
      failed: [...first.failed, ...second.failed],
      tooLarge: [...first.tooLarge, ...second.tooLarge],
      unsent: second.unsent,
      error: second.error,
    };
  }
}

async function deadLetterTooLarge(
  args: PushArgs,
  entries: ReadonlyArray<OutboxEntry>,
  message: string,
): Promise<number> {
  for (const entry of entries) {
    await deadLetter(
      args.deadletter,
      args.outbox,
      entry,
      "rejected",
      message,
      PAYLOAD_TOO_LARGE_ERROR_CODE,
      args.now,
      args.emit,
      args.log,
      args.rowversions,
    );
  }
  return entries.length;
}

/**
 * Pushes due outbox events in batches limited by event count and JSON bytes.
 * Each batch's result is persisted before the next is sent, so a failure
 * part-way through keeps the progress already made instead of restarting.
 */
export async function pushOutbox(args: PushArgs): Promise<PushOutcome> {
  const { outbox, push, batchSize, maxBytes, now, log } = args;

  const due = [...outbox.state.values()].filter((entry) => isDue(entry, now)).sort(byLocalSeq);

  log.debug("push outbox", { dueCount: due.length, batchSize, maxBytes });

  if (due.length === 0) return { pushed: 0, deadLettered: 0 };

  const { batches, oversized } = packBatches(
    groupByTransaction(due, maxBytes !== null),
    batchSize,
    maxBytes,
  );

  let pushed = 0;
  let deadLettered = 0;

  for (const group of oversized) {
    deadLettered += await deadLetterTooLarge(
      args,
      group.entries,
      `Transaction is ${group.bytes} bytes, over maxPushBytes (${maxBytes})`,
    );
  }

  for (const batch of batches) {
    const attempted: OutboxEntry[] = [];

    for (const group of batch.groups) {
      for (const entry of group.entries) {
        const attemptCount = (entry.attemptCount ?? 0) + 1;

        await outbox.update(entry.eventId, (draft) => {
          draft.syncStatus = "pending";
          draft.attemptCount = attemptCount;
          draft.lastAttemptAt = now;
          draft.nextAttemptAt = null;
          draft.lastError = null;
          draft.lastErrorCode = null;
          draft.retryable = null;
        }).isPersisted.promise;

        attempted.push({ ...entry, attemptCount });
      }
    }

    const byId = new Map(attempted.map((entry) => [entry.eventId, entry]));
    const attemptedGroups = batch.groups.map((group) => ({
      bytes: group.bytes,
      entries: group.entries.map((entry) => byId.get(entry.eventId)!),
    }));

    const response = await postGroups(push, attemptedGroups, log);

    log.info("push batch confirmed", {
      sent: attempted.length,
      bytes: batch.bytes,
      confirmed: response.confirmed.length,
      failed: response.failed.length,
      tooLarge: response.tooLarge.length,
    });

    for (const confirmation of response.confirmed) {
      if (!outbox.has(confirmation.eventId)) continue;

      await outbox.update(confirmation.eventId, (draft) => {
        draft.sync = true;
        draft.syncStatus = "synced";
        draft.globalSeq = confirmation.globalSeq;
        draft.nextAttemptAt = null;
        draft.lastError = null;
        draft.lastErrorCode = null;
        draft.retryable = null;
      }).isPersisted.promise;

      pushed++;
      args.emit("onEventPushed", {
        eventId: confirmation.eventId,
        globalSeq: confirmation.globalSeq,
      });
    }

    for (const failure of response.failed) {
      const entry = byId.get(failure.eventId);
      if (!entry) continue;

      deadLettered += await handleFailure(args, entry, failure);
    }

    deadLettered += await deadLetterTooLarge(
      args,
      response.tooLarge,
      "Server rejected the transaction as too large (HTTP 413)",
    );

    if (response.error !== undefined) {
      // Transport-level failure: back off what was not acknowledged and stop.
      // Everything confirmed so far stays synced.
      deadLettered += await backoffBatch(args, response.unsent, response.error);
      const error =
        response.error instanceof Error ? response.error : new Error(String(response.error));
      log.warn("push stopped after transport failure", {
        message: error.message,
        pushed,
        remaining: due.length - pushed,
      });
      return { pushed, deadLettered, error };
    }
  }

  return { pushed, deadLettered };
}

/** Returns 1 when the event was dead-lettered, 0 when it stays queued for retry. */
async function handleFailure(
  args: PushArgs,
  entry: OutboxEntry,
  failure: PushFailure,
): Promise<number> {
  const { outbox, deadletter, rowversions, retry, now, emit, log } = args;
  const code = failure.code ?? null;

  if (failure.retryable !== true) {
    const reason: DeadLetterReason = code === CONFLICT_ERROR_CODE ? "conflict" : "rejected";
    await deadLetter(
      deadletter,
      outbox,
      entry,
      reason,
      failure.message,
      code,
      now,
      emit,
      log,
      rowversions,
    );
    return 1;
  }

  if (entry.attemptCount >= retry.maxAttempts) {
    await deadLetter(
      deadletter,
      outbox,
      entry,
      "maxAttemptsExceeded",
      failure.message,
      code,
      now,
      emit,
      log,
      rowversions,
    );
    return 1;
  }

  const delay = backoffDelay(entry.attemptCount, retry);

  await outbox.update(entry.eventId, (draft) => {
    draft.sync = false;
    draft.syncStatus = "failed";
    draft.retryable = true;
    draft.lastError = failure.message;
    draft.lastErrorCode = code;
    draft.nextAttemptAt = now + delay;
  }).isPersisted.promise;

  log.warn("push retry scheduled", {
    eventId: entry.eventId,
    attemptCount: entry.attemptCount,
    retryInMs: delay,
    message: failure.message,
  });

  return 0;
}

/** Applies retry backoff to a whole batch after a transport-level failure. */
async function backoffBatch(
  args: PushArgs,
  batch: ReadonlyArray<OutboxEntry>,
  err: unknown,
): Promise<number> {
  const message = err instanceof Error ? err.message : String(err);
  let deadLettered = 0;

  for (const entry of batch) {
    deadLettered += await handleFailure(args, entry, {
      eventId: entry.eventId,
      message,
      retryable: true,
    });
  }

  return deadLettered;
}
