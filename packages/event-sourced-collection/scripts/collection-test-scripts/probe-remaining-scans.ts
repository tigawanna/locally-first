/**
 * Measures the five full-collection walks against queryOnce.
 * The recorded result lives in ARCHITECTURE.md under "queryOnce — One-Shot Reads".
 * Re-run with: pnpm exec tsx scripts/probe-remaining-scans.ts
 *
 * Last run (15 Oct 2026, 5500 outbox rows, 2000 inbox rows, 1500 todos):
 * the in-memory loop was faster for nextLocalSeq, getSyncStatus, and
 * readPullCursor. queryOnce copied every row for delete-all (~2.8MB) and for
 * compact()'s key walk (~171KB). Leave those five on the map.
 */
import { and, BasicIndex, count, createCollection, eq, max, queryOnce } from "@tanstack/db";
import {
  createNodeSQLitePersistence,
  persistedCollectionOptions,
} from "@tanstack/node-db-sqlite-persistence";
import Database from "better-sqlite3";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { InboxEntry, OutboxEntry } from "../../src/core/types";
import { nextLocalSeq } from "../../src/internal/compact-outbox";
import { readPullCursor } from "../../src/internal/sync-meta";
import { createNodeEventSourcedDB } from "../../src/node";

const OUTBOX_ROWS = 4000;
const INBOX_ROWS = 2000;
const TODOS = 1500;

function outboxRow(index: number): OutboxEntry {
  const failed = index % 17 === 0;
  return {
    eventId: `e${index}`,
    collectionId: "todos",
    type: "update",
    key: `t${index % TODOS}`,
    payload: { id: `t${index % TODOS}`, title: `title ${index}`, done: false },
    previous: null,
    txId: `e${index}`,
    clientId: "device",
    schemaVersion: 1,
    baseVersion: null,
    timestamp: index,
    localSeq: index,
    globalSeq: null,
    sync: index % 3 === 0,
    syncStatus: failed ? "failed" : "pending",
    attemptCount: 0,
    lastAttemptAt: null,
    nextAttemptAt: null,
    lastError: null,
    lastErrorCode: null,
    retryable: failed ? index % 2 === 0 : null,
  };
}

function inboxRow(index: number): InboxEntry {
  return {
    eventId: `i${index}`,
    globalSeq: index + 1,
    collectionId: "todos",
    type: "insert",
    key: `t${index}`,
    payload: { id: `t${index}` },
    previous: null,
    clientId: null,
    schemaVersion: 1,
    timestamp: index,
    sync: index % 2 === 0,
    skipped: false,
    skipReason: null,
  };
}

function bytes(value: unknown): number {
  return Buffer.byteLength(JSON.stringify(value));
}

async function time<T>(label: string, fn: () => Promise<T> | T): Promise<T> {
  const start = performance.now();
  const value = await fn();
  console.log(label, `${(performance.now() - start).toFixed(1)}ms`);
  return value;
}

const dir = mkdtempSync(join(tmpdir(), "esc-probe-"));
const sqlite = new Database(join(dir, "app.sqlite"));
const handle = createNodeEventSourcedDB({
  collections: {
    todos: { getKey: (todo: { id: string; title: string; done: boolean }) => todo.id },
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
  for (let index = 0; index < TODOS; index++) {
    await db.collections.todos.insert({ id: `t${index}`, title: `t${index}`, done: false })
      .isPersisted.promise;
  }
  for (let index = 0; index < OUTBOX_ROWS; index++) {
    await db.collections.outbox.insert(outboxRow(index)).isPersisted.promise;
  }
  for (let index = 0; index < INBOX_ROWS; index++) {
    await db.collections.inbox.insert(inboxRow(index)).isPersisted.promise;
  }

  db.collections.outbox.createIndex((row) => row.localSeq, {
    name: "by-local-seq",
    indexType: BasicIndex,
  });
  db.collections.inbox.createIndex((row) => row.globalSeq, {
    name: "by-global-seq",
    indexType: BasicIndex,
  });

  const loopSeq = await time("nextLocalSeq loop", () => nextLocalSeq(db.collections.outbox));
  const seqRows = await time("nextLocalSeq query", () =>
    queryOnce((q) =>
      q
        .from({ event: db.collections.outbox })
        .orderBy(({ event }) => event.localSeq, "desc")
        .limit(1),
    ),
  );
  console.log("nextLocalSeq", {
    loop: loopSeq,
    queryRows: seqRows.length,
    queryBytes: bytes(seqRows),
  });

  const counted = await time("status loop", () => {
    let pendingCount = 0;
    let failedCount = 0;
    for (const entry of db.collections.outbox.state.values()) {
      if (entry.sync) continue;
      pendingCount++;
      if (entry.syncStatus === "failed") failedCount++;
    }
    return { pendingCount, failedCount };
  });
  const { pendingCount, failedCount } = counted;
  const pendingRows = await time("pending count query", () =>
    queryOnce((q) =>
      q
        .from({ event: db.collections.outbox })
        .where(({ event }) => eq(event.sync, false))
        .select(({ event }) => ({ pending: count(event.eventId) })),
    ),
  );
  const failedRows = await time("failed count query", () =>
    queryOnce((q) =>
      q
        .from({ event: db.collections.outbox })
        .where(({ event }) => and(eq(event.sync, false), eq(event.syncStatus, "failed")))
        .select(({ event }) => ({ failed: count(event.eventId) })),
    ),
  );
  console.log("getSyncStatus", {
    loop: { pendingCount, failedCount },
    pendingRows,
    failedRows,
    queryBytes: bytes(pendingRows) + bytes(failedRows),
  });

  const loopCursor = await time("pull cursor loop", () =>
    readPullCursor(db.collections.syncmeta, db.collections.inbox),
  );
  const cursorRows = await time("pull cursor query", () =>
    queryOnce((q) =>
      q
        .from({ event: db.collections.inbox })
        .where(({ event }) => eq(event.sync, true))
        .select(({ event }) => ({ high: max(event.globalSeq) })),
    ),
  );
  console.log("readPullCursor", {
    loop: loopCursor,
    query: cursorRows,
    queryBytes: bytes(cursorRows),
  });

  const allOutbox = await time("delete-all query", () =>
    queryOnce((q) => q.from({ event: db.collections.outbox })),
  );
  let keyCount = 0;
  for (const _key of db.collections.outbox.state.keys()) keyCount++;
  console.log("delete-all", {
    iteratorKeys: keyCount,
    queryRows: allOutbox.length,
    queryBytes: bytes(allOutbox),
  });

  const allTodos = await time("compact keys query", () =>
    queryOnce((q) => q.from({ row: db.collections.todos })),
  );
  let todoKeys = 0;
  for (const _key of db.collections.todos.state.keys()) todoKeys++;
  console.log("compact keys", {
    iteratorKeys: todoKeys,
    queryRows: allTodos.length,
    queryBytes: bytes(allTodos),
  });
} catch (error) {
  console.error(error);
  process.exitCode = 1;
} finally {
  await handle.close();
}
