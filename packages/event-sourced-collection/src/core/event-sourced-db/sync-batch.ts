import type { SyncBatchOptions } from "../types";

export type SyncBatchVerdict =
  | { allowed: true }
  | {
      allowed: false;
      pendingCount: number;
      minEvents: number | null;
      delayRemainingMs: number | null;
      message: string;
    };

/**
 * A push may run when there is nothing to push, or when every configured limit
 * that can open the gate has been met. With both limits, either is enough:
 * 20 events push immediately, and a quieter stretch pushes whatever is waiting
 * once the delay has passed. Before the first successful push, that clock
 * starts at the oldest unpushed event. The pull half of a sync is not gated.
 */
export function decideSyncBatch(input: {
  batch: SyncBatchOptions | undefined;
  pendingCount: number;
  lastSyncAt: number | null;
  oldestPendingAt: number | null;
  now: number;
}): SyncBatchVerdict {
  const minEvents = positiveInt(input.batch?.minEvents);
  const delayMs = positiveMinutes(input.batch?.delayMinutes);
  if (minEvents === null && delayMs === null) return { allowed: true };
  if (input.pendingCount === 0) return { allowed: true };

  const eventsReady = minEvents !== null && input.pendingCount >= minEvents;
  const anchor = input.lastSyncAt ?? input.oldestPendingAt;
  const elapsedMs = anchor === null ? null : Math.max(0, input.now - anchor);
  const delayReady = delayMs !== null && elapsedMs !== null && elapsedMs >= delayMs;
  if (eventsReady || delayReady) return { allowed: true };

  const delayRemainingMs = delayMs === null || elapsedMs === null ? null : delayMs - elapsedMs;
  return {
    allowed: false,
    pendingCount: input.pendingCount,
    minEvents,
    delayRemainingMs,
    message: batchLimitMessage({
      pendingCount: input.pendingCount,
      minEvents,
      delayMs,
      delayRemainingMs,
    }),
  };
}

function positiveInt(value: number | undefined): number | null {
  if (value === undefined || !Number.isFinite(value)) return null;
  const rounded = Math.floor(value);
  return rounded >= 1 ? rounded : null;
}

function positiveMinutes(value: number | undefined): number | null {
  if (value === undefined || !Number.isFinite(value) || value <= 0) return null;
  return value * 60_000;
}

function batchLimitMessage(input: {
  pendingCount: number;
  minEvents: number | null;
  delayMs: number | null;
  delayRemainingMs: number | null;
}): string {
  const waiting = `${input.pendingCount} unpushed event(s)`;
  if (input.minEvents !== null && input.delayRemainingMs !== null) {
    const more = input.minEvents - input.pendingCount;
    return `Sync is limited to ${input.minEvents} events or ${minutes(input.delayMs ?? 0)}. ${waiting}; ${more} more event(s) or ${minutes(input.delayRemainingMs)} remaining.`;
  }
  if (input.minEvents !== null) {
    return `Sync is limited until ${input.minEvents} unpushed events are waiting. ${waiting}.`;
  }
  return `Sync is limited to once every ${minutes(input.delayMs ?? 0)}. ${minutes(input.delayRemainingMs ?? 0)} remaining, with ${waiting}.`;
}

function minutes(ms: number): string {
  const value = Math.max(0, ms) / 60_000;
  const rounded = value >= 1 ? Math.ceil(value) : Math.round(value * 10) / 10;
  return `${rounded} minute(s)`;
}
