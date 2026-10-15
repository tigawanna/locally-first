import Database from "better-sqlite3";
import { describe, expect, it } from "vite-plus/test";

import { createMockSyncBackend } from "../../testing/mock-sync-backend";
import { createNodeTodoHandle, makeTodo, openTempSqlite, todoRows } from "../helpers/node-db";

describe("node event-sourced collection (sqlite)", () => {
  it("inserts, updates, and deletes rows through the live collection", async () => {
    const { sqlite } = openTempSqlite();
    const handle = createNodeTodoHandle(sqlite);

    await handle.ensureDb();

    await handle.db.collections.todos.insert(makeTodo("t1", "Write tests")).isPersisted.promise;
    expect(todoRows(handle.db.collections.todos)).toEqual([
      { id: "t1", title: "Write tests", done: false },
    ]);

    await handle.db.collections.todos.update("t1", (draft) => {
      draft.done = true;
    }).isPersisted.promise;
    expect(handle.db.collections.todos.get("t1")?.done).toBe(true);

    await handle.db.collections.todos.delete("t1").isPersisted.promise;
    expect(handle.db.collections.todos.size).toBe(0);
  });

  it("records local mutations in the outbox", async () => {
    const { sqlite } = openTempSqlite();
    const handle = createNodeTodoHandle(sqlite, { clientId: "device-1" });

    await handle.ensureDb();
    await handle.db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;

    const outbox = [...handle.db.collections.outbox.state.values()];
    expect(outbox).toHaveLength(1);
    expect(outbox[0]).toMatchObject({
      collectionId: "todos",
      type: "insert",
      key: "t1",
      clientId: "device-1",
      syncStatus: "pending",
    });
  });

  it("reloads persisted rows after close and reopen on the same file", async () => {
    // Write through a live handle, wait until TanStack flushed to sqlite, then tear it down.
    const { sqlite, filePath } = openTempSqlite();
    const first = createNodeTodoHandle(sqlite, { clientId: "device-1" });
    await first.ensureDb();
    await first.db.collections.todos.insert(makeTodo("t1", "Persisted")).isPersisted.promise;
    await first.close();

    // New connection + new EventSourcedDB on the same file — same device coming back.
    const sqlite2 = new Database(filePath);
    const second = createNodeTodoHandle(sqlite2, { clientId: "device-1" });
    await second.ensureDb();

    expect(todoRows(second.db.collections.todos)).toEqual([
      { id: "t1", title: "Persisted", done: false },
    ]);
  });

  it("throws if the proxy is used before ensureDb", () => {
    const { sqlite } = openTempSqlite();
    const handle = createNodeTodoHandle(sqlite);

    expect(() => handle.db.collections).toThrow(/Call ensureDb/);
  });

  it("pushes local events and pulls remote events through a real sync backend", async () => {
    const backend = createMockSyncBackend({ backendId: "server-a" });
    const { sqlite } = openTempSqlite();
    const handle = createNodeTodoHandle(sqlite, {
      clientId: "device-1",
      sync: backend,
    });

    await handle.ensureDb();
    await handle.db.collections.todos.insert(makeTodo("local")).isPersisted.promise;

    const pushResult = await handle.db.sync();
    expect(pushResult.pushed).toBe(1);
    expect(backend.events).toHaveLength(1);

    backend.seed({
      collectionId: "todos",
      key: "remote",
      payload: { id: "remote", title: "From server", done: false },
    });

    const pullResult = await handle.db.sync();
    expect(pullResult.pulled).toBeGreaterThanOrEqual(1);
    expect(handle.db.collections.todos.get("remote")?.title).toBe("From server");
  });

  it("does not sync while sync is disabled", async () => {
    const backend = createMockSyncBackend();
    const { sqlite } = openTempSqlite();
    const handle = createNodeTodoHandle(sqlite, {
      sync: backend,
      syncEnabled: false,
    });

    await handle.ensureDb();
    await handle.db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;

    const result = await handle.db.sync();
    expect(result.pushed).toBe(0);
    expect(backend.pushCalls).toBe(0);
    expect(handle.db.collections.outbox.size).toBe(1);
  });
});
