import { describe, expect, it } from "vitest";

import { UnsyncedChangesError } from "../../core/create-event-sourced-db";
import { BackendMismatchError } from "../../internal/pull";
import { createMockSyncBackend } from "../../testing/mock-sync-backend";
import {
  makeTodo,
  openEventSourcedDb,
  openTempSqlite,
  openTodoDb,
  openTodoDbOnSqlite,
  todoRows,
} from "../helpers/node-db";

describe("resetLocalReplica", () => {
  it("empties the local copy without authoring delete events, then sync restores it", async () => {
    const backend = createMockSyncBackend({ backendId: "reset-test" });
    const db = await openTodoDb({ sync: backend });

    await db.collections.todos.insert(makeTodo("mine", "Authored here")).isPersisted.promise;
    backend.seed({ collectionId: "todos", key: "theirs", payload: makeTodo("theirs", "Remote") });
    await db.sync();
    expect(todoRows(db.collections.todos).map((row) => row.id)).toEqual(["mine", "theirs"]);
    const oldClientId = db.collections.syncmeta.toArray[0]?.clientId;

    const result = await db.resetLocalReplica();

    expect(result).toMatchObject({ deferred: false, removedRows: 2, discardedPending: 0 });
    expect(result.clientId).not.toBe(oldClientId);
    expect(db.collections.todos.size).toBe(0);
    expect(db.collections.outbox.size).toBe(0);
    expect(db.collections.inbox.size).toBe(0);
    expect(db.collections.syncmeta.toArray[0]?.pullCursor).toBe(0);

    await db.sync();

    expect(backend.events.filter((event) => event.type === "delete")).toEqual([]);
    expect(backend.events).toHaveLength(2);
    expect(todoRows(db.collections.todos)).toEqual([
      makeTodo("mine", "Authored here"),
      makeTodo("theirs", "Remote"),
    ]);
  });

  it("removes the rows from persistence, not just memory", async () => {
    const backend = createMockSyncBackend();
    const { sqlite } = openTempSqlite();
    const db = await openTodoDbOnSqlite(sqlite, { sync: backend });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    await db.sync();
    await db.resetLocalReplica();
    db.dispose();

    const reopened = await openTodoDbOnSqlite(sqlite, { sync: backend, syncEnabled: false });
    expect(reopened.collections.todos.size).toBe(0);
    expect(reopened.collections.outbox.size).toBe(0);
  });

  it("refuses while changes are waiting to be pushed", async () => {
    const backend = createMockSyncBackend();
    const db = await openTodoDb({ sync: backend });

    await db.collections.todos.insert(makeTodo("unsent")).isPersisted.promise;

    await expect(db.resetLocalReplica()).rejects.toBeInstanceOf(UnsyncedChangesError);
    expect(db.collections.todos.get("unsent")).toBeDefined();
    expect(db.getSyncStatus().pendingCount).toBe(1);

    const result = await db.resetLocalReplica({ discardPending: true });
    expect(result.discardedPending).toBe(1);
    expect(db.collections.todos.size).toBe(0);

    await db.sync();
    expect(backend.events).toEqual([]);
  });

  it("forgets the backend id so a 'fail' client can adopt a rebuilt backend", async () => {
    const backend = createMockSyncBackend({ backendId: "before" });
    const db = await openTodoDb({ sync: backend, backendMismatch: "fail" });
    backend.seed({ collectionId: "todos", key: "t1", payload: makeTodo("t1") });
    await db.sync();

    backend.setBackendId("after");
    const mismatched = await db.sync();
    expect(mismatched.errors.some((err) => err instanceof BackendMismatchError)).toBe(true);

    await db.resetLocalReplica();
    const resynced = await db.sync();

    expect(resynced.errors).toEqual([]);
    expect(db.collections.syncmeta.toArray[0]?.backendId).toBe("after");
    expect(todoRows(db.collections.todos).map((row) => row.id)).toEqual(["t1"]);
  });

  it("keeps local-only collections", async () => {
    const backend = createMockSyncBackend();
    const db = await openEventSourcedDb({
      sync: backend,
      collections: {
        todos: { getKey: (row: { id: string }) => row.id },
        settings: { getKey: (row: { id: string }) => row.id, localOnly: true },
      },
    });

    await db.collections.todos.insert({ id: "t1" }).isPersisted.promise;
    await db.collections.settings.insert({ id: "app" }).isPersisted.promise;
    await db.sync();

    const result = await db.resetLocalReplica();

    expect(result.removedRows).toBe(1);
    expect(db.collections.todos.size).toBe(0);
    expect(db.collections.settings.get("app")).toBeDefined();
  });
});
