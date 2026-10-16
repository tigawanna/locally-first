import {
  and,
  eq,
  inArray,
  isNull,
  isUndefined,
  lte,
  not,
  or,
  queryOnce,
  type Collection,
} from "@tanstack/db";

import type { InboxEntry, OutboxEntry } from "../core/types";

function unwrapQueryRow<T>(row: unknown, isRow: (value: unknown) => value is T): T | null {
  if (isRow(row)) return row;
  if (typeof row === "object" && row !== null) {
    for (const value of Object.values(row)) {
      if (isRow(value)) return value;
    }
  }
  return null;
}

export function readOutboxEntry(row: unknown): OutboxEntry | null {
  return unwrapQueryRow(row, isOutboxEntry);
}

function isOutboxEntry(value: unknown): value is OutboxEntry {
  if (typeof value !== "object" || value === null) return false;
  const row = value as { eventId?: unknown; localSeq?: unknown; sync?: unknown };
  return (
    typeof row.eventId === "string" &&
    typeof row.localSeq === "number" &&
    typeof row.sync === "boolean"
  );
}

function isInboxEntry(value: unknown): value is InboxEntry {
  if (typeof value !== "object" || value === null) return false;
  const row = value as { eventId?: unknown; globalSeq?: unknown; sync?: unknown };
  return (
    typeof row.eventId === "string" &&
    typeof row.globalSeq === "number" &&
    typeof row.sync === "boolean"
  );
}

function takenRows<T>(rows: ReadonlyArray<unknown>, isRow: (value: unknown) => value is T): T[] {
  return rows.flatMap((row) => {
    const entry = unwrapQueryRow(row, isRow);
    return entry ? [entry] : [];
  });
}

/** Unsynced outbox rows whose backoff has elapsed, oldest `localSeq` first. */
export async function dueOutboxEntries(
  outbox: Collection<OutboxEntry, string>,
  now: number,
): Promise<OutboxEntry[]> {
  const rows: ReadonlyArray<unknown> = await queryOnce((q) =>
    q
      .from({ event: outbox })
      .where(({ event }) =>
        and(
          eq(event.sync, false),
          or(not(eq(event.syncStatus, "failed")), eq(event.retryable, true)),
          or(
            isNull(event.nextAttemptAt),
            isUndefined(event.nextAttemptAt),
            lte(event.nextAttemptAt, now),
          ),
        ),
      )
      .orderBy(({ event }) => event.localSeq, "asc")
      .orderBy(({ event }) => event.eventId, "asc"),
  );
  return takenRows(rows, isOutboxEntry);
}

/** Unsynced inbox rows in `globalSeq` order, ready to replay. */
export async function pendingInboxEntries(
  inbox: Collection<InboxEntry, string>,
): Promise<InboxEntry[]> {
  const rows: ReadonlyArray<unknown> = await queryOnce((q) =>
    q
      .from({ event: inbox })
      .where(({ event }) => eq(event.sync, false))
      .orderBy(({ event }) => event.globalSeq, "asc"),
  );
  return takenRows(rows, isInboxEntry);
}

/** Outbox rows the server has already accepted. */
export async function syncedOutboxEntries(
  outbox: Collection<OutboxEntry, string>,
): Promise<OutboxEntry[]> {
  const rows: ReadonlyArray<unknown> = await queryOnce((q) =>
    q.from({ event: outbox }).where(({ event }) => eq(event.sync, true)),
  );
  return takenRows(rows, isOutboxEntry);
}

/** Synced outbox rows at or before `cutoff`, oldest `localSeq` first. */
export async function syncedOutboxBefore(
  outbox: Collection<OutboxEntry, string>,
  cutoff: number,
): Promise<OutboxEntry[]> {
  const rows: ReadonlyArray<unknown> = await queryOnce((q) =>
    q
      .from({ event: outbox })
      .where(({ event }) => and(eq(event.sync, true), lte(event.timestamp, cutoff)))
      .orderBy(({ event }) => event.localSeq, "asc"),
  );
  return takenRows(rows, isOutboxEntry);
}

/** Synced inbox rows at or before `cutoff`, oldest `globalSeq` first. */
export async function syncedInboxBefore(
  inbox: Collection<InboxEntry, string>,
  cutoff: number,
): Promise<InboxEntry[]> {
  const rows: ReadonlyArray<unknown> = await queryOnce((q) =>
    q
      .from({ event: inbox })
      .where(({ event }) => and(eq(event.sync, true), lte(event.timestamp, cutoff)))
      .orderBy(({ event }) => event.globalSeq, "asc"),
  );
  return takenRows(rows, isInboxEntry);
}

/** Outbox rows that have not been pushed. */
export async function unsyncedOutboxEntries(
  outbox: Collection<OutboxEntry, string>,
): Promise<OutboxEntry[]> {
  const rows: ReadonlyArray<unknown> = await queryOnce((q) =>
    q.from({ event: outbox }).where(({ event }) => eq(event.sync, false)),
  );
  return takenRows(rows, isOutboxEntry);
}

/** Unsynced outbox rows for collections that no longer sync. */
export async function pendingLocalOnlyOutbox(
  outbox: Collection<OutboxEntry, string>,
  collectionIds: readonly string[],
): Promise<OutboxEntry[]> {
  if (collectionIds.length === 0) return [];
  const rows: ReadonlyArray<unknown> = await queryOnce((q) =>
    q
      .from({ event: outbox })
      .where(({ event }) =>
        and(eq(event.sync, false), inArray(event.collectionId, [...collectionIds])),
      ),
  );
  return takenRows(rows, isOutboxEntry);
}
