import { createCollection } from "@tanstack/db";
import {
  createNodeSQLitePersistence,
  persistedCollectionOptions,
} from "@tanstack/node-db-sqlite-persistence";
import Database from "better-sqlite3";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach } from "vite-plus/test";

import { createEventSourcedDB } from "../../core/create-event-sourced-db";
import { createNodeEventSourcedDB } from "../../node";
import type { CreateCollectionFn } from "../../core/persisted-collection";
import type {
  EventSourcedDB,
  EventSourcedDBConfig,
  EventSourcedSharedOptions,
  SyncTransport,
} from "../../core/types";

export type Todo = {
  id: string;
  title: string;
  done: boolean;
};

export type TodoDefs = {
  todos: { getKey: (todo: Todo) => string };
};

type Closeable = { close: () => Promise<void> | void };

/** Every sqlite / event-sourced handle opened in a test is closed here. */
const openHandles: Closeable[] = [];

afterEach(async () => {
  while (openHandles.length > 0) {
    await openHandles.pop()?.close();
  }
});

/** Tiny fixtures so tests don't rebuild Todo rows by hand. */
export function makeTodo(id: string, title = "Task"): Todo {
  return { id, title, done: false };
}

export function pickTodo(
  row: { id: string; title: string; done: boolean } | undefined,
): Todo | undefined {
  if (!row) return undefined;
  return { id: row.id, title: row.title, done: row.done };
}

export function todoRows(collection: { state: Map<string, Todo> }): Todo[] {
  return [...collection.state.values()]
    .map((row) => pickTodo(row)!)
    .sort((a, b) => a.id.localeCompare(b.id));
}

/** Fresh on-disk sqlite file — TanStack persistence needs a real Database. */
export function openTempSqlite(): { sqlite: Database.Database; filePath: string } {
  const dir = mkdtempSync(join(tmpdir(), "esc-node-"));
  const filePath = join(dir, "app.sqlite");
  return { sqlite: new Database(filePath), filePath };
}

/** Register sqlite + optional EventSourcedDB.dispose() for afterEach. */
function trackSqlite(sqlite: Database.Database, db?: { dispose: () => void }): void {
  let closed = false;
  openHandles.push({
    close: () => {
      if (closed) return;
      closed = true;
      db?.dispose();
      try {
        sqlite.close();
      } catch {
        // already closed by the test
      }
    },
  });
}

/**
 * Node entry: createNodeEventSourcedDB. `modules` is the TanStack stack —
 * sqlite handle, persistence factory, createCollection, persistedCollectionOptions.
 */
export function createNodeTodoHandle(
  sqlite: Database.Database,
  options: EventSourcedSharedOptions & { sync?: SyncTransport } = {},
) {
  const handle = createNodeEventSourcedDB<TodoDefs>({
    collections: {
      todos: { getKey: (todo: Todo) => todo.id },
    },
    modules: {
      database: sqlite,
      createNodeSQLitePersistence,
      createCollection,
      persistedCollectionOptions,
    },
    ...options,
  });
  openHandles.push(handle);
  return handle;
}

/**
 * Lower-level: createEventSourcedDB with TanStack persistence already built.
 * Strips collections/persistence from `options` so tests can't override the todos wiring.
 */
export async function openTodoDbOnSqlite(
  sqlite: Database.Database,
  options: Partial<EventSourcedDBConfig<TodoDefs>> = {},
): Promise<EventSourcedDB<TodoDefs>> {
  const {
    collections: _ignored,
    persistence: _p,
    persistedCollectionOptions: _o,
    ...rest
  } = options;
  const db = await createEventSourcedDB<TodoDefs>({
    ...rest,
    persistence: createNodeSQLitePersistence({ database: sqlite }),
    createCollection: (options.createCollection ?? createCollection) as CreateCollectionFn,
    persistedCollectionOptions,
    collections: {
      todos: { getKey: (todo: Todo) => todo.id },
    },
  });
  trackSqlite(sqlite, db);
  return db;
}

/** Same as openTodoDbOnSqlite, but opens a throwaway sqlite file first. */
export async function openTodoDb(
  options: Partial<EventSourcedDBConfig<TodoDefs>> = {},
): Promise<EventSourcedDB<TodoDefs>> {
  return openTodoDbOnSqlite(openTempSqlite().sqlite, options);
}

/** Generic harness: any collection defs, default TanStack sqlite persistence. */
export async function openEventSourcedDb<
  TDefs extends Record<string, { getKey: (state: never) => string | number }>,
>(
  config: Omit<
    EventSourcedDBConfig<TDefs>,
    "persistence" | "createCollection" | "persistedCollectionOptions"
  > &
    Partial<
      Pick<
        EventSourcedDBConfig<TDefs>,
        "persistence" | "createCollection" | "persistedCollectionOptions"
      >
    >,
): Promise<EventSourcedDB<TDefs>> {
  const { sqlite } = openTempSqlite();
  const db = await createEventSourcedDB<TDefs>({
    ...config,
    persistence: config.persistence ?? createNodeSQLitePersistence({ database: sqlite }),
    createCollection: (config.createCollection ?? createCollection) as CreateCollectionFn,
    persistedCollectionOptions: config.persistedCollectionOptions ?? persistedCollectionOptions,
  });
  trackSqlite(sqlite, db);
  return db;
}

/** Wraps the real TanStack factory so replay can refuse selected keys. */
export function createRejectingCollectionFactory(
  reject: (key: string | number) => string | undefined,
): CreateCollectionFn {
  return ((options: Parameters<typeof createCollection>[0]) => {
    const collection = createCollection(options);
    const accept = collection.utils.acceptMutations.bind(collection.utils);
    collection.utils.acceptMutations = async (tx: {
      mutations: Array<{ key: string | number }>;
    }) => {
      for (const mutation of tx.mutations) {
        const message = reject(mutation.key);
        if (message !== undefined) throw new Error(message);
      }
      return accept(tx);
    };
    return collection;
  }) as CreateCollectionFn;
}
