import { and, eq, queryOnce, type Collection } from "@tanstack/db";

import type { CompactOutboxResult, OutboxEntry } from "../core/types";

type SeqCounter = { value: number };

/**
 * Pending events for one row, oldest first. Synced rows stay; the server
 * already has them, and a later compaction has to merge with them upstream.
 */
export async function pendingForKey(
  outbox: Collection<OutboxEntry, string>,
  collectionId: string,
  key: string | number,
): Promise<OutboxEntry[]> {
  const rows: ReadonlyArray<unknown> = await queryOnce((q) =>
    q
      .from({ event: outbox })
      .where(({ event }) =>
        and(eq(event.sync, false), eq(event.collectionId, collectionId), eq(event.key, key)),
      )
      .orderBy(({ event }) => event.localSeq, "asc"),
  );
  return rows.flatMap((row) => {
    const entry = outboxEntryFromQuery(row);
    return entry ? [entry] : [];
  });
}

/**
 * Replaces a pending insert/update chain with one event carrying the latest
 * row. A chain that still contains the insert stays an insert, because the
 * server has never seen the row. Otherwise it stays an update whose `previous`
 * is the state before the first unsynced edit.
 */
export function squashedEntry(
  pending: readonly OutboxEntry[],
  latest: Record<string, unknown>,
  now: number,
): Pick<
  OutboxEntry,
  "type" | "payload" | "previous" | "baseVersion" | "compaction" | "compactedAt"
> {
  const oldest = pending[0];
  const hasInsert = pending.some((entry) => entry.type === "insert");
  const seenByServer = pending.some((entry) => entry.sync);
  const type = !seenByServer && hasInsert ? "insert" : "update";
  return {
    type,
    payload: latest,
    previous: type === "insert" ? null : (oldest?.previous ?? oldest?.payload ?? null),
    baseVersion: oldest?.baseVersion ?? null,
    compaction: true,
    compactedAt: now,
  };
}

/** True when this pending chain can be represented by a single insert or update. */
export function canSquash(pending: readonly OutboxEntry[]): boolean {
  return (
    pending.length > 1 &&
    pending.every((entry) => entry.type === "insert" || entry.type === "update")
  );
}

export async function replacePending(
  outbox: Collection<OutboxEntry, string>,
  pending: readonly OutboxEntry[],
  entry: OutboxEntry,
): Promise<void> {
  for (const existing of pending) {
    await outbox.delete(existing.eventId).isPersisted.promise;
  }
  await outbox.insert(entry).isPersisted.promise;
}

export type CompactableCollection = {
  collectionId: string;
  collection: { state: { keys: () => Iterable<string | number> } };
};

/**
 * One row at a time. The query is this collection and this key only. Events
 * before the latest `compactedAt` are already folded into that event, so the
 * chain to squash is that compaction plus anything newer.
 */
function outboxEntryFromQuery(row: unknown): OutboxEntry | null {
  if (isOutboxEntry(row)) return row;
  if (typeof row === "object" && row !== null && "event" in row) {
    const wrapped = row.event;
    if (isOutboxEntry(wrapped)) return wrapped;
  }
  return null;
}

function isOutboxEntry(value: unknown): value is OutboxEntry {
  if (typeof value !== "object" || value === null) return false;
  const row = value as {
    eventId?: unknown;
    localSeq?: unknown;
    collectionId?: unknown;
    type?: unknown;
  };
  return (
    typeof row.eventId === "string" &&
    typeof row.localSeq === "number" &&
    typeof row.collectionId === "string" &&
    typeof row.type === "string"
  );
}

export async function eventsSinceLastCompaction(
  outbox: Collection<OutboxEntry, string>,
  collectionId: string,
  key: string | number,
): Promise<OutboxEntry[]> {
  const rows: ReadonlyArray<unknown> = await queryOnce((q) =>
    q
      .from({ event: outbox })
      .where(({ event }) => and(eq(event.collectionId, collectionId), eq(event.key, key))),
  );
  const events = rows.flatMap((row) => {
    const entry = outboxEntryFromQuery(row);
    return entry ? [entry] : [];
  });
  events.sort((a, b) => a.localSeq - b.localSeq);
  let start = 0;
  for (let index = events.length - 1; index >= 0; index--) {
    if (events[index]?.compactedAt != null) {
      start = index;
      break;
    }
  }
  return events.slice(start);
}

/**
 * Walks each synced collection's current row ids and compacts that row's
 * outbox chain, including events that have already been pushed.
 */
export async function compactOutbox(
  collections: readonly CompactableCollection[],
  outbox: Collection<OutboxEntry, string>,
  createEntry: (pending: OutboxEntry[]) => OutboxEntry,
): Promise<CompactOutboxResult> {
  let compacted = 0;
  let removed = 0;
  for (const { collectionId, collection } of collections) {
    for (const key of collection.state.keys()) {
      const chain = await eventsSinceLastCompaction(outbox, collectionId, key);
      if (!canSquash(chain)) continue;
      await replacePending(outbox, chain, createEntry(chain));
      compacted += 1;
      removed += chain.length - 1;
    }
  }
  return { compacted, removed };
}

export function allocateLocalSeq(outbox: Collection<OutboxEntry, string>, seq: SeqCounter): number {
  const allocated = Math.max(seq.value, nextLocalSeq(outbox));
  seq.value = allocated + 1;
  return allocated;
}

export function nextLocalSeq(outbox: Collection<OutboxEntry, string>): number {
  let max = -1;
  for (const entry of outbox.state.values()) {
    if (entry.localSeq > max) max = entry.localSeq;
  }
  return max + 1;
}
