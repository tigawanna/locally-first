import { RESERVED_IDS } from "../../internal/constants";
import type { SyncResult } from "../types";

/**
 * Reported as `SyncResult.batchHold` when `syncBatch` skips the push.
 * Either more unpushed events are required, or the delay since the last
 * successful push has not elapsed. Pull still runs. When both limits are set,
 * either one is enough to allow the push.
 */
export class SyncBatchLimitError extends Error {
  readonly pendingCount: number;
  readonly minEvents: number | null;
  readonly delayRemainingMs: number | null;

  constructor(input: {
    pendingCount: number;
    minEvents: number | null;
    delayRemainingMs: number | null;
    message: string;
  }) {
    super(input.message);
    this.name = "SyncBatchLimitError";
    this.pendingCount = input.pendingCount;
    this.minEvents = input.minEvents;
    this.delayRemainingMs = input.delayRemainingMs;
  }
}

/**
 * Thrown by `resetLocalReplica()` while the outbox still holds changes the
 * server has not accepted. Sync first, or pass `discardPending: true`.
 */
export class UnsyncedChangesError extends Error {
  constructor(readonly pendingCount: number) {
    super(`${pendingCount} local change(s) have not been pushed yet`);
    this.name = "UnsyncedChangesError";
  }
}

/** A zeroed `SyncResult`, with any counts or flags from `overrides` applied on top. */
export function emptyResult(overrides: Partial<SyncResult> = {}): SyncResult {
  return {
    pushed: 0,
    pulled: 0,
    skipped: 0,
    deadLettered: 0,
    deferred: false,
    batchHold: null,
    errors: [],
    ...overrides,
  };
}

/** Wraps a thrown value as an `Error` when it is not one already. */
export function toError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err));
}

/** Throws when a user collection id collides with a built-in collection such as the outbox. */
export function assertReservedNamesAvailable(collections: Record<string, unknown>): void {
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
