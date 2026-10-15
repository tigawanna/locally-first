import { createCollection } from "@tanstack/db";
import {
  createNodeSQLitePersistence,
  persistedCollectionOptions,
} from "@tanstack/node-db-sqlite-persistence";
import Database from "better-sqlite3";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { pendingForKey } from "../src/internal/compact-outbox";
import { createNodeEventSourcedDB } from "../src/node";

const dir = mkdtempSync(join(tmpdir(), "esc-pending-"));
const sqlite = new Database(join(dir, "app.sqlite"));

const handle = createNodeEventSourcedDB({
  collections: {
    todos: {
      getKey: (todo: { id: string; title: string; done: boolean }) => todo.id,
    },
  },
  modules: {
    database: sqlite,
    createNodeSQLitePersistence,
    createCollection,
    persistedCollectionOptions,
  },
});

function brief(entry: {
  type: string;
  sync: boolean;
  key: string | number;
  localSeq: number;
  payload: Record<string, unknown>;
}) {
  const title = entry.payload.title;
  return { type: entry.type, sync: entry.sync, key: entry.key, localSeq: entry.localSeq, title };
}

try {
  const db = await handle.ensureDb();
  const todos = db.collections.todos;

  for (const id of ["keep", "edit", "edit-twice", "drop", "edit-then-drop", "synced"]) {
    await todos.insert({ id, title: id, done: false }).isPersisted.promise;
  }

  await todos.update("edit", (draft) => {
    draft.title = "edit-1";
  }).isPersisted.promise;

  await todos.update("edit-twice", (draft) => {
    draft.title = "twice-1";
  }).isPersisted.promise;
  await todos.update("edit-twice", (draft) => {
    draft.title = "twice-2";
  }).isPersisted.promise;

  await todos.update("edit-then-drop", (draft) => {
    draft.title = "about-to-drop";
  }).isPersisted.promise;

  const synced = [...db.collections.outbox.state.values()].find((row) => row.key === "synced");
  if (!synced) throw new Error("missing insert for synced");
  await db.collections.outbox.update(synced.eventId, (draft) => {
    draft.sync = true;
  }).isPersisted.promise;
  await todos.update("synced", (draft) => {
    draft.title = "after-sync";
  }).isPersisted.promise;
  await todos.update("synced", (draft) => {
    draft.title = "after-sync-2";
  }).isPersisted.promise;

  await todos.delete("drop").isPersisted.promise;
  await todos.delete("edit-then-drop").isPersisted.promise;

  console.log(
    "outbox",
    [...db.collections.outbox.state.values()].sort((a, b) => a.localSeq - b.localSeq).map(brief),
  );

  for (const key of ["keep", "edit", "edit-twice", "drop", "edit-then-drop", "synced", "missing"]) {
    const pending = await pendingForKey(db.collections.outbox, "todos", key);
    const inMemory = [...db.collections.outbox.state.values()]
      .filter((row) => !row.sync && row.collectionId === "todos" && row.key === key)
      .sort((a, b) => a.localSeq - b.localSeq);
    const same =
      pending.length === inMemory.length &&
      pending.every((row, index) => row.eventId === inMemory[index]?.eventId);
    console.log(key, {
      query: pending.map(brief),
      memory: inMemory.map(brief),
      same,
    });
  }
} catch (error) {
  console.error("failed");
  console.error(error);
  process.exitCode = 1;
} finally {
  await handle.close();
}
