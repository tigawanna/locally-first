import { createCollection } from "@tanstack/db";
import {
  createNodeSQLitePersistence,
  persistedCollectionOptions,
} from "@tanstack/node-db-sqlite-persistence";
import Database from "better-sqlite3";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { InboxEntry, OutboxEntry } from "../src/core/types";
import {
  dueOutboxEntries,
  pendingInboxEntries,
  pendingLocalOnlyOutbox,
  syncedInboxBefore,
  syncedOutboxBefore,
  syncedOutboxEntries,
  unsyncedOutboxEntries,
} from "../src/internal/collection-queries";
import { trimKeepLast } from "../src/core/event-sourced-db/trim";
import { createNodeEventSourcedDB } from "../src/node";

const NOW = 1_000;

function outboxRow(overrides: Partial<OutboxEntry> & Pick<OutboxEntry, "eventId">): OutboxEntry {
  return {
    collectionId: "todos",
    type: "insert",
    key: overrides.eventId,
    payload: { id: overrides.eventId },
    previous: null,
    txId: overrides.eventId,
    clientId: "device",
    schemaVersion: 1,
    baseVersion: null,
    timestamp: 0,
    localSeq: 0,
    globalSeq: null,
    sync: false,
    syncStatus: "pending",
    attemptCount: 0,
    lastAttemptAt: null,
    nextAttemptAt: null,
    lastError: null,
    lastErrorCode: null,
    retryable: null,
    ...overrides,
  };
}

function inboxRow(
  overrides: Partial<InboxEntry> & Pick<InboxEntry, "eventId" | "globalSeq">,
): InboxEntry {
  return {
    collectionId: "todos",
    type: "insert",
    key: overrides.eventId,
    payload: { id: overrides.eventId },
    previous: null,
    clientId: null,
    schemaVersion: 1,
    timestamp: 0,
    sync: false,
    skipped: false,
    skipReason: null,
    ...overrides,
  };
}

function ids(rows: ReadonlyArray<{ eventId: string }>): string[] {
  return rows.map((row) => row.eventId);
}

function sameIds(
  label: string,
  query: readonly { eventId: string }[],
  memory: readonly { eventId: string }[],
) {
  const left = ids(query);
  const right = ids(memory);
  const same = left.length === right.length && left.every((id, index) => id === right[index]);
  console.log(label, { query: left, memory: right, same });
  if (!same) throw new Error(`${label} diverged`);
}

function isDue(entry: OutboxEntry, now: number): boolean {
  if (entry.sync) return false;
  if (entry.syncStatus === "failed" && entry.retryable !== true) return false;
  return (entry.nextAttemptAt ?? 0) <= now;
}

function byLocalSeq(a: OutboxEntry, b: OutboxEntry): number {
  if (a.localSeq !== b.localSeq) return a.localSeq - b.localSeq;
  return a.eventId < b.eventId ? -1 : a.eventId > b.eventId ? 1 : 0;
}

const dir = mkdtempSync(join(tmpdir(), "esc-bricks-"));
const sqlite = new Database(join(dir, "app.sqlite"));
const handle = createNodeEventSourcedDB({
  collections: {
    todos: { getKey: (todo: { id: string }) => todo.id },
    drafts: { getKey: (draft: { id: string }) => draft.id, localOnly: true },
  },
  modules: {
    database: sqlite,
    createNodeSQLitePersistence,
    createCollection,
    persistedCollectionOptions,
  },
});

try {
  const db = await handle.ensureDb();
  const outbox = db.collections.outbox;
  const inbox = db.collections.inbox;

  const outboxRows = [
    outboxRow({ eventId: "synced", sync: true, localSeq: 1, timestamp: 10 }),
    outboxRow({ eventId: "due-null", localSeq: 4, nextAttemptAt: null }),
    outboxRow({ eventId: "future", localSeq: 2, nextAttemptAt: NOW + 50 }),
    outboxRow({ eventId: "past", localSeq: 3, nextAttemptAt: NOW - 1 }),
    outboxRow({ eventId: "failed-closed", syncStatus: "failed", retryable: false, localSeq: 5 }),
    outboxRow({ eventId: "failed-null", syncStatus: "failed", retryable: null, localSeq: 6 }),
    outboxRow({ eventId: "b-tie", localSeq: 7, nextAttemptAt: 0 }),
    outboxRow({ eventId: "a-tie", localSeq: 7, nextAttemptAt: 0 }),
    outboxRow({
      eventId: "failed-open",
      syncStatus: "failed",
      retryable: true,
      localSeq: 8,
      nextAttemptAt: NOW,
    }),
    outboxRow({ eventId: "old-synced", sync: true, localSeq: 0, timestamp: 10 }),
    outboxRow({ eventId: "new-synced", sync: true, localSeq: 9, timestamp: 500 }),
    outboxRow({ eventId: "local-pending", collectionId: "drafts", localSeq: 10 }),
    outboxRow({ eventId: "local-synced", collectionId: "drafts", sync: true, localSeq: 11 }),
  ];
  for (const row of outboxRows) {
    await outbox.insert(row).isPersisted.promise;
  }

  const due = await dueOutboxEntries(outbox, NOW);
  sameIds(
    "due outbox",
    due,
    [...outbox.state.values()].filter((entry) => isDue(entry, NOW)).sort(byLocalSeq),
  );

  const synced = await syncedOutboxEntries(outbox);
  sameIds(
    "synced outbox",
    [...synced].sort(byLocalSeq),
    [...outbox.state.values()].filter((entry) => entry.sync).sort(byLocalSeq),
  );

  const unsynced = await unsyncedOutboxEntries(outbox);
  sameIds(
    "unsynced outbox",
    [...unsynced].sort(byLocalSeq),
    [...outbox.state.values()].filter((entry) => !entry.sync).sort(byLocalSeq),
  );

  const cutoff = 100;
  const outboxPrune = await syncedOutboxBefore(outbox, cutoff);
  sameIds(
    "prune outbox",
    trimKeepLast(outboxPrune, 1),
    trimKeepLast(
      [...outbox.state.values()]
        .filter((entry) => entry.sync && entry.timestamp <= cutoff)
        .sort((a, b) => a.localSeq - b.localSeq),
      1,
    ),
  );

  const localOnly = await pendingLocalOnlyOutbox(outbox, ["drafts"]);
  sameIds(
    "local-only pending",
    [...localOnly].sort(byLocalSeq),
    [...outbox.state.values()]
      .filter((entry) => !entry.sync && entry.collectionId === "drafts")
      .sort(byLocalSeq),
  );

  const inboxRows = [
    inboxRow({ eventId: "applied", globalSeq: 1, sync: true, timestamp: 10 }),
    inboxRow({ eventId: "later", globalSeq: 5, timestamp: 20 }),
    inboxRow({ eventId: "sooner", globalSeq: 2, timestamp: 400 }),
    inboxRow({ eventId: "old-applied", globalSeq: 3, sync: true, timestamp: 30 }),
  ];
  for (const row of inboxRows) {
    await inbox.insert(row).isPersisted.promise;
  }

  const pending = await pendingInboxEntries(inbox);
  sameIds(
    "pending inbox",
    pending,
    [...inbox.state.values()]
      .filter((entry) => !entry.sync)
      .sort((a, b) => a.globalSeq - b.globalSeq),
  );

  const inboxPrune = await syncedInboxBefore(inbox, cutoff);
  sameIds(
    "prune inbox",
    inboxPrune,
    [...inbox.state.values()]
      .filter((entry) => entry.sync && entry.timestamp <= cutoff)
      .sort((a, b) => a.globalSeq - b.globalSeq),
  );

  console.log("all bricks matched");
} catch (error) {
  console.error(error);
  process.exitCode = 1;
} finally {
  await handle.close();
}
