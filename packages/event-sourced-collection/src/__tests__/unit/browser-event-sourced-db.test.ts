import { createCollection } from "@tanstack/db";
import {
  createNodeSQLitePersistence,
  persistedCollectionOptions,
} from "@tanstack/node-db-sqlite-persistence";
import { describe, expect, it, vi } from "vitest";

import { createBrowserEventSourcedDB } from "../../platforms/browser-event-sourced-db";
import type { BrowserEventSourcedModules } from "../../platforms/browser-event-sourced-db";
import { makeTodo, openTempSqlite, type TodoDefs } from "../helpers/node-db";

const collections: TodoDefs = { todos: { getKey: (todo) => todo.id } };

function createMemoryStorage(): Storage {
  const items = new Map<string, string>();
  return {
    get length() {
      return items.size;
    },
    clear: () => items.clear(),
    getItem: (key) => items.get(key) ?? null,
    key: (index) => [...items.keys()][index] ?? null,
    removeItem: (key) => void items.delete(key),
    setItem: (key, value) => void items.set(key, String(value)),
  };
}

async function withWindow<T>(run: () => T | Promise<T>): Promise<T> {
  vi.stubGlobal("window", {});
  // TanStack DB reads localStorage once `window` exists. Node 25 ships a global
  // localStorage without getItem unless --localstorage-file is set.
  vi.stubGlobal("localStorage", createMemoryStorage());
  try {
    return await run();
  } finally {
    vi.unstubAllGlobals();
  }
}

function createModules() {
  let openCount = 0;
  let coordinatorDisposeCount = 0;
  let databaseCloseCount = 0;

  const modules = (): BrowserEventSourcedModules => {
    openCount += 1;
    const { sqlite } = openTempSqlite();
    return {
      openBrowserWASQLiteOPFSDatabase: async () => ({
        execute: async () => [],
        close: () => {
          databaseCloseCount += 1;
          try {
            sqlite.close();
          } catch {
            // already closed
          }
        },
      }),
      createBrowserWASQLitePersistence: () => createNodeSQLitePersistence({ database: sqlite }),
      BrowserCollectionCoordinator: class {
        dispose() {
          coordinatorDisposeCount += 1;
        }
      },
      createCollection,
      persistedCollectionOptions,
    } as unknown as BrowserEventSourcedModules;
  };

  return {
    modules,
    counts: () => ({ openCount, coordinatorDisposeCount, databaseCloseCount }),
  };
}

describe("createBrowserEventSourcedDB", () => {
  it("initializes the database once and exposes user and reserved collections", async () => {
    await withWindow(async () => {
      const { modules, counts } = createModules();
      const { ensureDb, db } = createBrowserEventSourcedDB<TodoDefs>({
        databaseName: "test.sqlite",
        collections,
        modules,
        lock: null,
      });

      const database = await ensureDb();

      expect(counts().openCount).toBe(1);
      expect(database.collections.todos).toBeDefined();
      expect(database.collections.outbox).toBeDefined();
      expect(database.collections.inbox).toBeDefined();

      await db.collections.todos.insert(makeTodo("t1", "A")).isPersisted.promise;
      expect(db.collections.todos.get("t1")).toMatchObject({ id: "t1", title: "A" });
    });
  });

  it("deduplicates concurrent ensureDb calls", async () => {
    await withWindow(async () => {
      const { modules, counts } = createModules();
      const { ensureDb } = createBrowserEventSourcedDB<TodoDefs>({
        databaseName: "test.sqlite",
        collections,
        modules,
        lock: null,
      });

      await Promise.all([ensureDb(), ensureDb()]);
      expect(counts().openCount).toBe(1);
    });
  });

  it("throws when db is accessed before ensureDb", async () => {
    await withWindow(() => {
      const { modules } = createModules();
      const { db } = createBrowserEventSourcedDB<TodoDefs>({
        databaseName: "test.sqlite",
        collections,
        modules,
        lock: null,
      });

      expect(() => db.collections).toThrow(/Call ensureDb/);
    });
  });

  it("closes the platform and re-initializes on the next ensureDb", async () => {
    await withWindow(async () => {
      const { modules, counts } = createModules();
      const { ensureDb, close } = createBrowserEventSourcedDB<TodoDefs>({
        databaseName: "test.sqlite",
        collections,
        modules,
        lock: null,
      });

      await ensureDb();
      await close();

      expect(counts().coordinatorDisposeCount).toBe(1);
      expect(counts().databaseCloseCount).toBe(1);

      await ensureDb();
      expect(counts().openCount).toBe(2);
    });
  });

  it("rejects initialization outside a browser environment", async () => {
    const { modules, counts } = createModules();
    const { ensureDb } = createBrowserEventSourcedDB<TodoDefs>({
      databaseName: "test.sqlite",
      collections,
      modules,
      lock: null,
    });

    await expect(ensureDb()).rejects.toThrow(/browser environment/);
    expect(counts().openCount).toBe(0);
  });
});
