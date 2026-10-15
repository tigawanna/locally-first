/**
 * Prints what an update actually stores: the whole row, or only the keys touched.
 *
 * Re-run with: pnpm exec tsx scripts/collection-test-scripts/probe-update-payloads.ts
 *
 * compactOnWrite is off so each edit stays its own outbox event. Tries a normal
 * draft edit, a returned partial, a deleted key, Object.assign of a partial,
 * and a hand-inserted outbox event. Then foldedOutboxBody runs on the normal
 * edit chain.
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
import { foldedOutboxBody } from "../../src/internal/compact-outbox";
import type { OutboxEntry } from "../../src/core/types";

type Todo = { id: string; title: string; color: string; done: boolean };

const dir = mkdtempSync(join(tmpdir(), "esc-update-payload-"));
const sqlite = new Database(join(dir, "app.sqlite"));

const db = await createEventSourcedDB<{
  todos: { getKey: (todo: Todo) => string };
}>({
  compactOnWrite: false,
  persistence: createNodeSQLitePersistence({ database: sqlite }),
  createCollection,
  persistedCollectionOptions,
  collections: {
    todos: { getKey: (todo: Todo) => todo.id },
  },
});

const todos = db.collections.todos;

function eventsFor(key: string): OutboxEntry[] {
  return [...db.collections.outbox.state.values()]
    .filter((row) => row.key === key)
    .sort((a, b) => a.localSeq - b.localSeq);
}

function dump(label: string, key: string) {
  console.log(label);
  console.log("  collection", todos.get(key));
  for (const event of eventsFor(key)) {
    console.log("  outbox", { type: event.type, payload: event.payload, previous: event.previous });
  }
}

try {
  await todos.insert({ id: "draft", title: "Task", color: "blue", done: false }).isPersisted
    .promise;
  await todos.update("draft", (row) => {
    row.title = "Task 1";
  }).isPersisted.promise;
  await todos.update("draft", (row) => {
    row.title = "Task 2";
  }).isPersisted.promise;
  dump("draft mutation, title only", "draft");

  const draftEvents = eventsFor("draft").filter((event) => !event.sync);
  console.log(
    "folded body",
    foldedOutboxBody({
      unpushedEvents: draftEvents,
      latest: draftEvents.at(-1)?.payload ?? {},
      now: 1,
    }),
  );

  await todos.insert({ id: "returned", title: "Task", color: "blue", done: false }).isPersisted
    .promise;
  await todos.update("returned", () => ({ id: "returned", title: "only title" }) as Todo)
    .isPersisted.promise;
  dump("callback returns a partial object", "returned");

  await todos.insert({ id: "deleted-key", title: "Task", color: "blue", done: false }).isPersisted
    .promise;
  await todos.update("deleted-key", (row) => {
    delete (row as { color?: string }).color;
  }).isPersisted.promise;
  dump("draft deletes color", "deleted-key");

  await todos.insert({ id: "assign", title: "Task", color: "blue", done: false }).isPersisted
    .promise;
  await todos.update("assign", (row) => {
    Object.assign(row, { title: "assigned" });
  }).isPersisted.promise;
  dump("Object.assign a partial onto the draft", "assign");

  await todos.insert({ id: "hand", title: "Task", color: "blue", done: false }).isPersisted.promise;
  const handInsert = eventsFor("hand")[0];
  if (!handInsert) throw new Error("missing insert");
  await db.collections.outbox.insert({
    ...handInsert,
    eventId: "hand-partial",
    type: "update",
    payload: { title: "partial" },
    previous: handInsert.payload,
    localSeq: handInsert.localSeq + 1,
    txId: "hand-partial",
  }).isPersisted.promise;
  dump("outbox insert with a partial payload", "hand");
} catch (error) {
  console.error(error);
  process.exitCode = 1;
} finally {
  db.dispose();
  sqlite.close();
}
