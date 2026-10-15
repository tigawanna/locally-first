import { describe, expect, it } from "vite-plus/test";

import { createMockSyncBackend } from "../../testing/mock-sync-backend";
import type { EventSourcedDB } from "../../core/types";
import { openTodoDb, type TodoDefs } from "../helpers/node-db";

function createDb(
  sync: ReturnType<typeof createMockSyncBackend>,
  overrides: { clientId?: string; pushBatchSize?: number } = {},
): Promise<EventSourcedDB<TodoDefs>> {
  return openTodoDb({
    sync,
    ...overrides,
  });
}

describe("createMockSyncBackend", () => {
  it("round-trips a local mutation to a second client", async () => {
    const backend = createMockSyncBackend();

    const a = await createDb(backend, { clientId: "a" });
    const b = await createDb(backend, { clientId: "b" });

    await a.collections.todos.insert({ id: "t1", title: "Shared", done: false }).isPersisted
      .promise;
    await a.sync();

    const result = await b.sync();

    expect(result.pulled).toBe(1);
    expect(b.collections.todos.get("t1")).toMatchObject({ title: "Shared" });
  });

  it("deduplicates a replayed push by eventId", async () => {
    const backend = createMockSyncBackend();
    const db = await createDb(backend);

    await db.collections.todos.insert({ id: "t1", title: "One", done: false }).isPersisted.promise;
    await db.sync();
    await db.retryDeadLetter();
    await db.sync();

    expect(backend.events).toHaveLength(1);
  });

  it("paginates according to pageSize", async () => {
    const backend = createMockSyncBackend({ pageSize: 1 });

    for (const id of ["a", "b", "c"]) {
      backend.seed({ collectionId: "todos", key: id, payload: { id, title: id, done: false } });
    }

    const db = await createDb(backend);
    const result = await db.sync();

    expect(result.pulled).toBe(3);
    expect(backend.pullCalls).toBeGreaterThanOrEqual(3);
  });

  it("reports batch sizes so push batching can be asserted", async () => {
    const backend = createMockSyncBackend();
    const db = await createDb(backend, { pushBatchSize: 2 });

    for (const id of ["a", "b", "c", "d", "e"]) {
      await db.collections.todos.insert({ id, title: id, done: false }).isPersisted.promise;
    }
    await db.sync();

    expect(backend.pushBatchSizes).toEqual([2, 2, 1]);
  });

  it("injects transport outages", async () => {
    const backend = createMockSyncBackend();
    const db = await createDb(backend);

    backend.failNextPushes(1, "offline");

    await db.collections.todos.insert({ id: "t1", title: "One", done: false }).isPersisted.promise;
    const failed = await db.sync();

    expect(failed.errors[0]?.message).toBe("offline");
    expect(backend.events).toHaveLength(0);
  });

  it("injects per-event rejections", async () => {
    const backend = createMockSyncBackend({
      rejectPush: (event) =>
        event.key === "bad" ? { message: "invalid", retryable: false } : undefined,
    });

    const db = await createDb(backend);

    await db.collections.todos.insert({ id: "ok", title: "Fine", done: false }).isPersisted.promise;
    await db.collections.todos.insert({ id: "bad", title: "Nope", done: false }).isPersisted
      .promise;

    const result = await db.sync();

    expect(result.pushed).toBe(1);
    expect(result.deadLettered).toBe(1);
    expect([...db.collections.deadletter.state.values()][0]).toMatchObject({ key: "bad" });
  });

  it("simulates a wiped backend", async () => {
    const backend = createMockSyncBackend({ backendId: "one" });
    backend.seed({
      collectionId: "todos",
      key: "t1",
      payload: { id: "t1", title: "A", done: false },
    });

    const db = await createDb(backend);
    await db.sync();
    expect(db.getSyncStatus().pullCursor).toBe(1);

    backend.reset();
    backend.setBackendId("two");
    backend.seed({
      collectionId: "todos",
      key: "t2",
      payload: { id: "t2", title: "B", done: false },
    });

    const result = await db.sync();

    expect(result.pulled).toBe(1);
    expect(db.collections.todos.get("t2")).toBeDefined();
  });
});
