import { and, eq, queryOnce, type Collection } from "@tanstack/db";

import type { SeqCounter } from "../core/event-sourced-db/refs";
import type { CompactOutboxResult, OutboxEntry } from "../core/types";
import { readOutboxEntry } from "./collection-queries";

/**
 * Outbox events for one row that have not been pushed yet, oldest first.
 * Synced rows stay; the server already has them, and a later compaction has
 * to merge with them upstream.
 */
export async function unpushedOutboxEventsForKey(
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
    const entry = readOutboxEntry(row);
    return entry ? [entry] : [];
  });
}

/**
 * Body of one event that replaces an unpushed insert/update chain.
 * A chain that still contains the insert, and that the server has not seen,
 * stays an insert. Otherwise it stays an update whose `previous` and
 * `baseVersion` come from the oldest event. `payload` is the whole latest
 * row, so a field set by an earlier edit stays even when later edits change
 * something else.
 *
 * Insert "Task", then set color to "red", then title "Task 1", then "Task 2",
 * none synced. The replacement is one insert whose payload is
 * `{ title: "Task 2", color: "red" }`. The color edit is not dropped.
 *
 * Insert already synced, then title "Task 3" (`baseVersion` "server-event-9",
 * `previous` "Task 2"), then title "Task 4". The replacement is one update
 * from "Task 2" to "Task 4", still based on "server-event-9".
 */
export function foldedOutboxBody(input: {
  unpushedEvents: readonly OutboxEntry[];
  latest: Record<string, unknown>;
  now: number;
}): Pick<
  OutboxEntry,
  "type" | "payload" | "previous" | "baseVersion" | "compaction" | "compactedAt"
> {
  const { unpushedEvents, latest, now } = input;
  const oldest = unpushedEvents[0];
  const hasInsert = unpushedEvents.some((event) => event.type === "insert");
  const seenByServer = unpushedEvents.some((event) => event.sync);
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

/**
 * Events after the latest delete.
 * A delete ends one span of this row's history; the next insert starts another.
 * See `splitLives`.
 */
export function eventsAfterLastDelete(events: readonly OutboxEntry[]): OutboxEntry[] {
  for (let index = events.length - 1; index >= 0; index--) {
    if (events[index]?.type === "delete") return events.slice(index + 1);
  }
  return [...events];
}

/** True when this pending chain can be represented by a single insert or update. */
export function canSquash(pending: readonly OutboxEntry[]): boolean {
  return (
    pending.length > 1 &&
    pending.every((entry) => entry.type === "insert" || entry.type === "update")
  );
}

/** Deletes the unpushed outbox events and inserts the row that replaces them. */
export async function replaceUnpushedOutboxEvents(
  outbox: Collection<OutboxEntry, string>,
  unpushedEvents: readonly OutboxEntry[],
  outboxRow: OutboxEntry,
): Promise<void> {
  for (const existing of unpushedEvents) {
    await outbox.delete(existing.eventId).isPersisted.promise;
  }
  await outbox.insert(outboxRow).isPersisted.promise;
}

export type CompactableCollection = {
  collectionId: string;
};

/**
 * Contiguous unsynced suffix. Stops at a synced event or one reserved for an
 * in-flight push, so a fold never rewrites a payload the server already has
 * or a payload that has already left the device.
 */
export function foldableTail(
  events: readonly OutboxEntry[],
  isReserved: (eventId: string) => boolean = () => false,
): OutboxEntry[] {
  const tail: OutboxEntry[] = [];
  for (let index = events.length - 1; index >= 0; index--) {
    const event = events[index];
    if (!event || event.sync || isReserved(event.eventId)) break;
    tail.push(event);
  }
  tail.reverse();
  return tail;
}

/**
 * The event whose identity the replacement keeps.
 * A fold that shared its `txId` with another row stays in that transaction,
 * preferring the newest such event so the latest multi-row write stays atomic.
 * Otherwise the oldest event wins, which keeps the chain's place in `localSeq`
 * order instead of moving the folded row behind everything queued after it.
 */
export function anchorFoldedEvent(
  chain: readonly OutboxEntry[],
  outbox: Collection<OutboxEntry, string>,
): OutboxEntry {
  const folding = new Set(chain.map((event) => event.eventId));
  const siblingTxIds = new Set<string>();
  for (const entry of outbox.state.values()) {
    if (!folding.has(entry.eventId)) siblingTxIds.add(entry.txId);
  }
  for (let index = chain.length - 1; index >= 0; index--) {
    const event = chain[index];
    if (event && siblingTxIds.has(event.txId)) return event;
  }
  return chain[0]!;
}

/**
 * One "life" is the outbox events for a single row from its insert until its
 * delete. The delete is the last event of that span. Events after it belong
 * to the next span. `closed` spans already end in a delete. `open` is the
 * span that has not been deleted yet.
 *
 * insert, update, delete, insert, update
 * → closed: [insert, update, delete]
 * → open:   [insert, update]
 */
function splitLives(events: readonly OutboxEntry[]): {
  closed: OutboxEntry[][];
  open: OutboxEntry[];
} {
  const closed: OutboxEntry[][] = [];
  let current: OutboxEntry[] = [];
  for (const event of events) {
    current.push(event);
    if (event.type === "delete") {
      closed.push(current);
      current = [];
    }
  }
  return { closed, open: current };
}

async function eventsForKey(
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
    const entry = readOutboxEntry(row);
    return entry ? [entry] : [];
  });
  events.sort((a, b) => a.localSeq - b.localSeq);
  return events;
}

/**
 * One delete-ended span is dropped when the server never saw it, or reduced
 * to one delete when it did. The span that is still live folds only its
 * unsynced insert/update tail. "Life" is defined on `splitLives`.
 */
export type CompactFactory = {
  /** One insert or update covering the span that has not been deleted yet. */
  squash: (chain: OutboxEntry[]) => OutboxEntry;
  /** One delete of the row the server already has. `current` is the delete being replaced. */
  deleteRow: (lastSynced: OutboxEntry, current: OutboxEntry) => OutboxEntry;
};

async function dropEvents(
  outbox: Collection<OutboxEntry, string>,
  events: readonly OutboxEntry[],
): Promise<void> {
  for (const event of events) {
    await outbox.delete(event.eventId).isPersisted.promise;
  }
}

/**
 * A span that ends in a delete (`splitLives`). Nothing synced: drop the whole
 * span. Otherwise keep the synced prefix and replace the unsynced tail with
 * one delete.
 */
async function collapseClosedLife(
  outbox: Collection<OutboxEntry, string>,
  life: readonly OutboxEntry[],
  factory: CompactFactory,
  isReserved: (eventId: string) => boolean,
): Promise<{ compacted: number; removed: number }> {
  const suffixStart = life.findIndex((event) => !event.sync);
  if (suffixStart === -1) return { compacted: 0, removed: 0 };

  const prefix = life.slice(0, suffixStart);
  const suffix = life.slice(suffixStart);
  if (suffix.some((event) => event.sync || isReserved(event.eventId))) {
    return { compacted: 0, removed: 0 };
  }
  if (suffix.at(-1)?.type !== "delete") return { compacted: 0, removed: 0 };

  if (prefix.length === 0) {
    await dropEvents(outbox, suffix);
    return { compacted: 0, removed: suffix.length };
  }

  if (suffix.length === 1) return { compacted: 0, removed: 0 };

  const lastSynced = prefix.at(-1)!;
  const current = suffix.at(-1)!;
  await replaceUnpushedOutboxEvents(outbox, suffix, factory.deleteRow(lastSynced, current));
  return { compacted: 1, removed: suffix.length - 1 };
}

export async function compactOutbox(
  collections: readonly CompactableCollection[],
  outbox: Collection<OutboxEntry, string>,
  factory: CompactFactory,
  isReserved: (eventId: string) => boolean = () => false,
): Promise<CompactOutboxResult> {
  const allowed = new Set(collections.map((item) => item.collectionId));
  const keysByCollection = new Map<string, Set<string | number>>();
  for (const entry of outbox.state.values()) {
    if (!allowed.has(entry.collectionId)) continue;
    const keys = keysByCollection.get(entry.collectionId) ?? new Set<string | number>();
    keys.add(entry.key);
    keysByCollection.set(entry.collectionId, keys);
  }

  let compacted = 0;
  let removed = 0;
  for (const [collectionId, keys] of keysByCollection) {
    for (const key of keys) {
      const events = await eventsForKey(outbox, collectionId, key);
      const { closed, open } = splitLives(events);

      for (const life of closed) {
        const outcome = await collapseClosedLife(outbox, life, factory, isReserved);
        compacted += outcome.compacted;
        removed += outcome.removed;
      }

      const chain = foldableTail(open, isReserved);
      if (!canSquash(chain)) continue;
      await replaceUnpushedOutboxEvents(outbox, chain, factory.squash(chain));
      compacted += 1;
      removed += chain.length - 1;
    }
  }
  return { compacted, removed, deferred: false };
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
