import type { NormalizedSyncTransport } from "../core/sync";
import type { EventSourcedLogger } from "../utils/logger";
import type { EmitHook } from "./hooks";
import { CONFLICT_ERROR_CODE } from "./constants";
import type {
  DeadLetterReason,
  DeadLetterRow,
  DrizzleAdapter,
  OutboxRow,
  ResolvedRetryConfig,
} from "./types";
import type { OutboundEvent, PushFailure, PushResponse } from "../core/sync";

export type PushOutcome = {
  pushed: number;
  deadLettered: number;
  error?: Error;
};

export type PushArgs = {
  adapter: DrizzleAdapter;
  push: NonNullable<NormalizedSyncTransport["push"]>;
  batchSize: number;
  retry: ResolvedRetryConfig;
  now: number;
  emit: EmitHook;
  log: EventSourcedLogger;
};

/**
 * Exponential backoff: doubles from baseDelayMs, capped at maxDelayMs.
 */
export function backoffDelay(attemptCount: number, retry: ResolvedRetryConfig): number {
  const exponent = Math.max(0, attemptCount - 1);
  const raw = retry.baseDelayMs * 2 ** exponent;
  return Math.min(retry.maxDelayMs, raw);
}

function toOutboundEvent(entry: OutboxRow): OutboundEvent {
  return {
    eventId: entry.eventId,
    collectionId: entry.collectionId,
    type: entry.type,
    key: entry.key,
    payload: entry.payload,
    previous: entry.previous ?? null,
    txId: entry.txId,
    clientId: entry.clientId,
    schemaVersion: entry.schemaVersion,
    baseVersion: entry.baseVersion ?? null,
    timestamp: entry.timestamp,
  };
}

/**
 * Splits events into batches without splitting a txId across requests.
 */
function batchByTransaction(events: OutboxRow[], size: number): OutboxRow[][] {
  const batches: OutboxRow[][] = [];
  let current: OutboxRow[] = [];

  for (let index = 0; index < events.length;) {
    const txId = events[index]!.txId;
    const group: OutboxRow[] = [];

    while (index < events.length && events[index]!.txId === txId) {
      group.push(events[index]!);
      index++;
    }

    if (current.length > 0 && current.length + group.length > size) {
      batches.push(current);
      current = [];
    }

    current.push(...group);

    if (current.length >= size) {
      batches.push(current);
      current = [];
    }
  }

  if (current.length > 0) batches.push(current);
  return batches;
}

function normalizePushResponse(
  response: PushResponse | ReadonlyArray<{ eventId: string; globalSeq: number }>,
): PushResponse {
  if (Array.isArray(response)) {
    return { confirmed: response };
  }
  return {
    confirmed: (response as PushResponse).confirmed,
    failed: (response as PushResponse).failed,
  };
}

async function deadLetter(
  adapter: DrizzleAdapter,
  entry: OutboxRow,
  reason: DeadLetterReason,
  message: string,
  code: string | null,
  now: number,
  emit: EmitHook,
  log: EventSourcedLogger,
): Promise<void> {
  const record: DeadLetterRow = {
    eventId: entry.eventId,
    collectionId: entry.collectionId,
    type: entry.type,
    key: entry.key,
    payload: entry.payload,
    previous: entry.previous,
    txId: entry.txId,
    clientId: entry.clientId,
    schemaVersion: entry.schemaVersion,
    timestamp: entry.timestamp,
    localSeq: entry.localSeq,
    globalSeq: entry.globalSeq,
    direction: "outbound",
    reason,
    message,
    code,
    attemptCount: entry.attemptCount,
    failedAt: now,
  };

  await adapter.insertDeadLetter(record);
  await adapter.deleteOutboxRow(entry.eventId);

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

async function handleFailure(
  args: PushArgs,
  entry: OutboxRow,
  failure: PushFailure,
): Promise<number> {
  const { adapter, retry, now, emit, log } = args;
  const code = failure.code ?? null;

  // Non-retryable → dead-letter immediately.
  if (failure.retryable !== true) {
    const reason: DeadLetterReason = code === CONFLICT_ERROR_CODE ? "conflict" : "rejected";
    await deadLetter(adapter, entry, reason, failure.message, code, now, emit, log);
    return 1;
  }

  // Retry budget exhausted → dead-letter.
  if (entry.attemptCount >= retry.maxAttempts) {
    await deadLetter(adapter, entry, "maxAttemptsExceeded", failure.message, code, now, emit, log);
    return 1;
  }

  // Schedule retry with backoff.
  const delay = backoffDelay(entry.attemptCount, retry);
  await adapter.updateOutbox(entry.eventId, {
    sync: false,
    syncStatus: "failed",
    retryable: true,
    lastError: failure.message,
    lastErrorCode: code,
    nextAttemptAt: now + delay,
  });

  log.warn("push retry scheduled", {
    eventId: entry.eventId,
    attemptCount: entry.attemptCount,
    retryInMs: delay,
    message: failure.message,
  });

  return 0;
}

async function backoffBatch(args: PushArgs, batch: OutboxRow[], err: unknown): Promise<number> {
  const { adapter, retry, now, emit, log } = args;
  let deadLettered = 0;

  for (const entry of batch) {
    if (entry.attemptCount >= retry.maxAttempts) {
      const error = err instanceof Error ? err : new Error(String(err));
      await deadLetter(adapter, entry, "maxAttemptsExceeded", error.message, null, now, emit, log);
      deadLettered++;
    } else {
      const delay = backoffDelay(entry.attemptCount, retry);
      await adapter.updateOutbox(entry.eventId, {
        sync: false,
        syncStatus: "failed",
        retryable: true,
        lastError: err instanceof Error ? err.message : String(err),
        lastErrorCode: null,
        nextAttemptAt: now + delay,
      });
    }
  }

  return deadLettered;
}

/**
 * Pushes due outbox events in transaction-aware batches.
 * Each batch's result is persisted before the next is sent.
 */
export async function pushOutbox(args: PushArgs): Promise<PushOutcome> {
  const { adapter, push, batchSize, now, log } = args;

  const due = await adapter.queryDueOutbox(now);

  log.debug("push outbox", { dueCount: due.length, batchSize });

  if (due.length === 0) return { pushed: 0, deadLettered: 0 };

  let pushed = 0;
  let deadLettered = 0;

  for (const batch of batchByTransaction(due, batchSize)) {
    // Mark as pending + increment attempt count.
    for (const entry of batch) {
      const attemptCount = (entry.attemptCount ?? 0) + 1;
      await adapter.updateOutbox(entry.eventId, {
        syncStatus: "pending",
        attemptCount,
        lastAttemptAt: now,
        nextAttemptAt: null,
        lastError: null,
        lastErrorCode: null,
        retryable: null,
      });
      entry.attemptCount = attemptCount;
    }

    let response: PushResponse;
    try {
      response = normalizePushResponse(await push(batch.map(toOutboundEvent)));
    } catch (err) {
      deadLettered += await backoffBatch(args, batch, err);
      const error = err instanceof Error ? err : new Error(String(err));
      log.warn("push stopped after transport failure", {
        message: error.message,
        pushed,
        remaining: due.length - pushed,
      });
      return { pushed, deadLettered, error };
    }

    log.info("push batch confirmed", {
      sent: batch.length,
      confirmed: response.confirmed.length,
      failed: response.failed?.length ?? 0,
    });

    for (const confirmation of response.confirmed) {
      await adapter.markOutboxSynced(confirmation.eventId, confirmation.globalSeq);
      pushed++;
      args.emit("onEventPushed", {
        eventId: confirmation.eventId,
        globalSeq: confirmation.globalSeq,
      });
    }

    const byId = new Map(batch.map((entry) => [entry.eventId, entry]));
    for (const failure of response.failed ?? []) {
      const entry = byId.get(failure.eventId);
      if (!entry) continue;
      deadLettered += await handleFailure(args, entry, failure);
    }
  }

  return { pushed, deadLettered };
}
