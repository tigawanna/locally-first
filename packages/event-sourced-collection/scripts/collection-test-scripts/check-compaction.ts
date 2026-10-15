/**
 * Prints the outbox after a short write sequence so you can see how
 * compactOnWrite and compact() fold unsynced edits.
 *
 * Re-run with: pnpm exec tsx scripts/check-compaction.ts
 *
 * It opens a temp SQLite database twice: once with compactOnWrite on, then
 * once with it off. The first pass updates the same todo several times, marks
 * an insert synced, edits again, deletes a row, recreates it, and inserts a
 * local-only note. The second pass writes three events and calls compact()
 * twice. Each dump is the outbox in localSeq order.
 */
import { createCollection } from "@tanstack/db";
import {
  createNodeSQLitePersistence,
  persistedCollectionOptions,
} from "@tanstack/node-db-sqlite-persistence";
import Database from "better-sqlite3";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createEventSourcedDB } from "../../src/core/create-event-sourced-db";
import type { EventSourcedDB, OutboxEntry } from "../../src/core/types";

type Todo = { id: string; title: string; done: boolean };
type Note = { id: string; body: string };

function openDb(compactOnWrite: boolean) {
  const dir = mkdtempSync(join(tmpdir(), "esc-compact-"));
  const sqlite = new Database(join(dir, "app.sqlite"));
  return createEventSourcedDB<{
    todos: { getKey: (todo: Todo) => string };
    notes: { getKey: (note: Note) => string; localOnly: true };
  }>({
    compactOnWrite,
    persistence: createNodeSQLitePersistence({ database: sqlite }),
    createCollection,
    persistedCollectionOptions,
    collections: {
      todos: { getKey: (todo: Todo) => todo.id },
      notes: { getKey: (note: Note) => note.id, localOnly: true },
    },
  }).then((db) => ({
    db,
    close: () => {
      db.dispose();
      sqlite.close();
    },
  }));
}

function brief(entry: OutboxEntry) {
  return {
    type: entry.type,
    key: entry.key,
    sync: entry.sync,
    title: entry.payload.title,
    previousTitle:
      entry.previous && "title" in entry.previous ? entry.previous.title : entry.previous,
    compaction: entry.compaction === true,
  };
}

function dump(
  label: string,
  db: EventSourcedDB<{
    todos: { getKey: (todo: Todo) => string };
    notes: { getKey: (note: Note) => string; localOnly: true };
  }>,
) {
  const events = [...db.collections.outbox.state.values()].sort((a, b) => a.localSeq - b.localSeq);
  console.log(label, events.map(brief));
  return events;
}

async function main() {
  const on = await openDb(true);
  const todos = on.db.collections.todos;
  await todos.insert({ id: "t1", title: "Task", done: false }).isPersisted.promise;
  for (let step = 1; step <= 5; step++) {
    await todos.update("t1", (draft) => {
      draft.title = `Task ${step}`;
    }).isPersisted.promise;
  }
  await todos.insert({ id: "t2", title: "Other", done: false }).isPersisted.promise;
  await todos.update("t2", (draft) => {
    draft.title = "Other 1";
  }).isPersisted.promise;
  dump("compactOnWrite two rows", on.db);

  const created = [...on.db.collections.outbox.state.values()].find((row) => row.key === "t1")!;
  await on.db.collections.outbox.update(created.eventId, (draft) => {
    draft.sync = true;
  }).isPersisted.promise;
  await todos.update("t1", (draft) => {
    draft.title = "After sync";
  }).isPersisted.promise;
  await todos.update("t1", (draft) => {
    draft.title = "After sync 2";
  }).isPersisted.promise;
  dump("after synced insert, two more edits", on.db);

  await todos.delete("t2").isPersisted.promise;
  dump("after deleting t2", on.db);
  const deleted = await on.db.compact();
  dump(`compact() while t2 is gone ${JSON.stringify(deleted)}`, on.db);

  await todos.insert({ id: "t2", title: "Reborn", done: false }).isPersisted.promise;
  await todos.update("t2", (draft) => {
    draft.title = "Reborn 2";
  }).isPersisted.promise;
  const reborn = await on.db.compact();
  dump(`compact() after delete and recreate ${JSON.stringify(reborn)}`, on.db);

  await on.db.collections.notes.insert({ id: "n1", body: "local" }).isPersisted.promise;
  dump("after local-only note", on.db);
  on.close();

  const off = await openDb(false);
  await off.db.collections.todos.insert({ id: "t1", title: "Task", done: false }).isPersisted
    .promise;
  await off.db.collections.todos.update("t1", (draft) => {
    draft.title = "Once";
  }).isPersisted.promise;
  await off.db.collections.todos.update("t1", (draft) => {
    draft.title = "Twice";
  }).isPersisted.promise;
  dump("compactOnWrite off", off.db);
  const first = await off.db.compact();
  dump(`first compact() ${JSON.stringify(first)}`, off.db);
  const second = await off.db.compact();
  dump(`second compact() ${JSON.stringify(second)}`, off.db);
  off.close();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
