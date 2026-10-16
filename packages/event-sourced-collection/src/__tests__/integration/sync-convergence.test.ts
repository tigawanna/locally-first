import { describe, expect, it } from "vite-plus/test";
import Database from "better-sqlite3";

import { createMockSyncBackend } from "../../testing/mock-sync-backend";
import type { EventSourcedDB, EventSourcedDBConfig } from "../../core/types";
import {
  createRejectingCollectionFactory,
  makeTodo,
  openTempSqlite,
  openTodoDb,
  openTodoDbOnSqlite,
  todoRows,
  type Todo,
  type TodoDefs,
} from "../helpers/node-db";

type Overrides = Partial<
  Pick<
    EventSourcedDBConfig<TodoDefs>,
    | "clientId"
    | "pullOverlap"
    | "retry"
    | "pushBatchSize"
    | "createCollection"
    | "backendMismatch"
    | "compactOnWrite"
  >
>;

function createDb(
  sync: EventSourcedDBConfig<TodoDefs>["sync"],
  overrides: Overrides = {},
): Promise<EventSourcedDB<TodoDefs>> {
  return openTodoDb({
    sync,
    clientId: overrides.clientId,
    pullOverlap: overrides.pullOverlap,
    retry: overrides.retry,
    pushBatchSize: overrides.pushBatchSize,
    createCollection: overrides.createCollection,
    backendMismatch: overrides.backendMismatch,
    compactOnWrite: overrides.compactOnWrite,
  });
}

function todosOf(db: EventSourcedDB<TodoDefs>): Todo[] {
  return todoRows(db.collections.todos);
}

describe("cross-device convergence", () => {
  it("carries inserts, updates and deletes across the boundary and converges", async () => {
    const backend = createMockSyncBackend();
    const a = await createDb(backend, { clientId: "a", compactOnWrite: false });
    const b = await createDb(backend, { clientId: "b", compactOnWrite: false });

    for (let index = 0; index < 10; index++) {
      await a.collections.todos.insert(makeTodo(`t${index}`)).isPersisted.promise;
    }
    await a.collections.todos.update("t3", (draft) => {
      draft.done = true;
    }).isPersisted.promise;
    await a.collections.todos.delete("t7").isPersisted.promise;

    const pushResult = await a.sync();
    const pullResult = await b.sync();

    // 10 inserts + 1 update + 1 delete, every one of them crossing once.
    expect(pushResult.pushed).toBe(12);
    expect(backend.events).toHaveLength(12);
    expect(pullResult.pulled).toBe(12);
    expect(pullResult.skipped).toBe(0);

    expect(todosOf(b)).toHaveLength(9);
    expect(b.collections.todos.get("t7")).toBeUndefined();
    expect(b.collections.todos.get("t3")).toMatchObject({ done: true });
    expect(todosOf(b)).toEqual(todosOf(a));
  });

  it("converges when both devices write different rows", async () => {
    const backend = createMockSyncBackend();
    const a = await createDb(backend, { clientId: "a" });
    const b = await createDb(backend, { clientId: "b" });

    await a.collections.todos.insert(makeTodo("from-a")).isPersisted.promise;
    await b.collections.todos.insert(makeTodo("from-b")).isPersisted.promise;

    await a.sync();
    await b.sync();
    await a.sync();

    expect(todosOf(a).map((todo) => todo.id)).toEqual(["from-a", "from-b"]);
    expect(todosOf(b)).toEqual(todosOf(a));
  });

  it("never applies a device's own events twice when pages overlap", async () => {
    const backend = createMockSyncBackend();
    // Rewinds far enough that every sync re-delivers the entire history.
    const a = await createDb(backend, {
      clientId: "a",
      pullOverlap: 1_000,
      compactOnWrite: false,
    });
    const b = await createDb(backend, {
      clientId: "b",
      pullOverlap: 1_000,
      compactOnWrite: false,
    });

    await a.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    await a.collections.todos.update("t1", (draft) => {
      draft.title = "updated";
    }).isPersisted.promise;
    await a.sync();
    await b.sync();

    // Re-deliver the same history several times over.
    for (let round = 0; round < 3; round++) {
      const resultA = await a.sync();
      const resultB = await b.sync();
      expect(resultA.pulled).toBe(0);
      expect(resultB.pulled).toBe(0);
    }

    expect(backend.events).toHaveLength(2);
    expect(a.collections.todos.get("t1")).toMatchObject({ title: "updated" });
    expect(todosOf(b)).toEqual(todosOf(a));
  });

  it("keeps three devices in agreement under randomised traffic", async () => {
    const backend = createMockSyncBackend({ pageSize: 3 });
    const names = ["a", "b", "c"];
    const devices = [
      await createDb(backend, { clientId: "a", pushBatchSize: 2 }),
      await createDb(backend, { clientId: "b", pushBatchSize: 2 }),
      await createDb(backend, { clientId: "c", pushBatchSize: 2 }),
    ];

    // Deterministic PRNG so any failure is reproducible from the seed.
    let seed = 1337;
    const random = (bound: number): number => {
      seed = (seed * 1103515245 + 12345) % 2147483648;
      return seed % bound;
    };

    // Each row is written only by the device that created it. Concurrent edits
    // to one row are excluded deliberately — see the "concurrent edits to the
    // same row" test below for why that case does not converge yet.
    let nextId = 0;

    for (let round = 0; round < 60; round++) {
      const index = random(devices.length);
      const device = devices[index]!;
      const prefix = `${names[index]}-`;
      const owned = todosOf(device).filter((todo) => todo.id.startsWith(prefix));
      const action = random(4);

      if (action === 0 || owned.length === 0) {
        await device.collections.todos.insert(makeTodo(`${prefix}${nextId++}`)).isPersisted.promise;
      } else if (action === 1) {
        const target = owned[random(owned.length)]!;
        await device.collections.todos.update(target.id, (draft) => {
          draft.title = `${target.title}+${round}`;
        }).isPersisted.promise;
      } else if (action === 2) {
        const target = owned[random(owned.length)]!;
        await device.collections.todos.delete(target.id).isPersisted.promise;
      } else {
        await device.sync();
      }
    }

    // Sync to quiescence.
    for (let round = 0; round < 4; round++) {
      for (const device of devices) await device.sync();
    }

    const [first, ...rest] = devices;
    const expected = todosOf(first!);

    expect(expected.length).toBeGreaterThan(0);
    for (const device of rest) {
      expect(todosOf(device)).toEqual(expected);
    }
    for (const device of devices) {
      expect(device.getSyncStatus().isSynced).toBe(true);
      expect(device.collections.deadletter.state.size).toBe(0);
    }
  });

  /**
   * Known limitation, pinned so a future change to the ordering rules is a
   * visible decision rather than an accident.
   *
   * A device skips replaying its own events, because recognising them is what
   * stops it from re-applying its own pruned history. The cost is that when two
   * devices edit one row concurrently, the later author never re-applies its own
   * winning event on top of the earlier one it pulls, so the two disagree.
   * Resolving it needs a per-row applied-sequence watermark rather than an
   * origin check.
   */
  it("does not yet converge when two devices edit the same row concurrently", async () => {
    const backend = createMockSyncBackend();
    const a = await createDb(backend, { clientId: "a" });
    const b = await createDb(backend, { clientId: "b" });

    await a.collections.todos.insert(makeTodo("t1", "base")).isPersisted.promise;
    await a.sync();
    await b.sync();

    await a.collections.todos.update("t1", (draft) => {
      draft.title = "from-a";
    }).isPersisted.promise;
    await b.collections.todos.update("t1", (draft) => {
      draft.title = "from-b";
    }).isPersisted.promise;

    for (let round = 0; round < 2; round++) {
      await a.sync();
      await b.sync();
    }

    // `b` authored the later event, so server order makes "from-b" the winner.
    expect(backend.events.at(-1)?.payload).toMatchObject({ title: "from-b" });
    expect(a.collections.todos.get("t1")).toMatchObject({ title: "from-b" });
    // ...but `b` skipped its own event and kept the earlier one it pulled.
    expect(b.collections.todos.get("t1")).toMatchObject({ title: "from-a" });
  });
});

describe("client identity", () => {
  it("persists the generated client id across a reload", async () => {
    const { sqlite, filePath } = openTempSqlite();
    const backend = createMockSyncBackend();

    const first = await openTodoDbOnSqlite(sqlite, { sync: backend });
    await first.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    const authored = [...first.collections.outbox.state.values()][0]!.clientId;
    first.dispose();
    sqlite.close();

    const second = await openTodoDbOnSqlite(new Database(filePath), { sync: backend });
    await second.collections.todos.insert(makeTodo("t2")).isPersisted.promise;
    const reloaded = [...second.collections.outbox.state.values()].find(
      (entry) => entry.key === "t2",
    )!.clientId;

    expect(reloaded).toBe(authored);
  });

  it("does not replay its own pruned history after a reload", async () => {
    const { sqlite, filePath } = openTempSqlite();
    const backend = createMockSyncBackend();

    const first = await openTodoDbOnSqlite(sqlite, { sync: backend, pullOverlap: 1_000 });
    await first.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    await first.sync();
    await first.collections.todos.delete("t1").isPersisted.promise;
    await first.sync();
    await first.pruneSyncedEvents();
    first.dispose();
    sqlite.close();

    const second = await openTodoDbOnSqlite(new Database(filePath), {
      sync: backend,
      pullOverlap: 1_000,
    });
    await second.collections.todos.insert(makeTodo("t1", "recreated")).isPersisted.promise;

    const result = await second.sync();

    expect(result.pulled).toBe(0);
    expect(second.collections.todos.get("t1")).toMatchObject({ title: "recreated" });
  });

  it("still honours an explicitly configured client id", async () => {
    const { sqlite, filePath } = openTempSqlite();
    const backend = createMockSyncBackend();

    const first = await openTodoDbOnSqlite(sqlite, { sync: backend, clientId: "device-a" });
    await first.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    expect([...first.collections.outbox.state.values()][0]?.clientId).toBe("device-a");
    first.dispose();
    sqlite.close();

    const second = await openTodoDbOnSqlite(new Database(filePath), {
      sync: backend,
      clientId: "device-b",
    });
    await second.collections.todos.insert(makeTodo("t2")).isPersisted.promise;
    const entry = [...second.collections.outbox.state.values()].find((row) => row.key === "t2");

    expect(entry?.clientId).toBe("device-b");
  });
});

describe("backend reset recovery", () => {
  it("re-uploads retained history to a wiped backend in a single sync", async () => {
    const backend = createMockSyncBackend({ backendId: "one" });
    const db = await createDb(backend, { clientId: "a" });

    for (const id of ["t1", "t2", "t3"]) {
      await db.collections.todos.insert(makeTodo(id)).isPersisted.promise;
    }
    await db.sync();
    expect(backend.events).toHaveLength(3);

    backend.reset();
    backend.setBackendId("two");

    const result = await db.sync();

    expect(backend.events).toHaveLength(3);
    expect(result.pushed).toBe(3);
    expect(db.getSyncStatus().backendId).toBe("two");
    expect(db.getSyncStatus().isSynced).toBe(true);
    expect(todosOf(db)).toHaveLength(3);
  });

  it("lets a peer see the restored history", async () => {
    const backend = createMockSyncBackend({ backendId: "one" });
    const a = await createDb(backend, { clientId: "a" });

    await a.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    await a.sync();

    backend.reset();
    backend.setBackendId("two");
    await a.sync();

    const b = await createDb(backend, { clientId: "b" });
    const result = await b.sync();

    expect(result.pulled).toBe(1);
    expect(todosOf(b)).toEqual(todosOf(a));
  });

  it("leaves the outbox alone when the mismatch policy is ignore", async () => {
    const backend = createMockSyncBackend({ backendId: "one" });
    const db = await createDb(backend, { clientId: "a", backendMismatch: "ignore" });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    await db.sync();

    backend.reset();
    backend.setBackendId("two");
    const result = await db.sync();

    expect(result.pushed).toBe(0);
    expect(backend.events).toHaveLength(0);
  });
});

describe("inbound dead-lettering", () => {
  const createPoisonedCollectionFactory = (poisonKey: string) =>
    createRejectingCollectionFactory((key) =>
      key === poisonKey ? "constraint violation" : undefined,
    );

  it("parks an unapplicable event and drains the ones behind it", async () => {
    const backend = createMockSyncBackend();
    backend.seed({ collectionId: "todos", key: "bad", payload: makeTodo("bad") });
    backend.seed({ collectionId: "todos", key: "good", payload: makeTodo("good") });

    const db = await createDb(backend, {
      clientId: "a",
      retry: { maxAttempts: 2, baseDelayMs: 0 },
      createCollection: createPoisonedCollectionFactory("bad"),
    });

    // First sync burns an attempt and holds position behind the poison event.
    const first = await db.sync();
    expect(first.pulled).toBe(0);
    expect(first.deadLettered).toBe(0);
    expect(db.getSyncStatus().pullCursor).toBe(0);

    // Second exhausts the budget, parks it, and lets the queue move on.
    const second = await db.sync();

    expect(second.deadLettered).toBe(1);
    expect(second.pulled).toBe(1);
    expect(db.collections.todos.get("good")).toBeDefined();
    expect(db.getSyncStatus().pullCursor).toBe(2);

    const parked = [...db.collections.deadletter.state.values()];
    expect(parked).toHaveLength(1);
    expect(parked[0]).toMatchObject({
      eventId: expect.any(String),
      direction: "inbound",
      reason: "replayFailed",
      message: "constraint violation",
      globalSeq: 1,
      attemptCount: 2,
    });
  });

  it("does not push a parked inbound event back to the server on retry", async () => {
    const backend = createMockSyncBackend();
    backend.seed({ collectionId: "todos", key: "bad", payload: makeTodo("bad") });

    const db = await createDb(backend, {
      clientId: "a",
      retry: { maxAttempts: 1, baseDelayMs: 0 },
      createCollection: createPoisonedCollectionFactory("bad"),
    });

    await db.sync();
    expect(db.collections.deadletter.state.size).toBe(1);

    expect(await db.retryDeadLetter()).toBe(1);

    // Requeued into the inbox for another replay, never into the outbox.
    expect(db.collections.outbox.state.size).toBe(0);
    expect(backend.pushBatchSizes).toEqual([]);
    // Replay failed again immediately, so it is parked once more rather than lost.
    expect(db.collections.deadletter.state.size).toBe(1);
  });

  it("applies a parked event once retry can succeed", async () => {
    const backend = createMockSyncBackend();
    backend.seed({ collectionId: "todos", key: "flaky", payload: makeTodo("flaky") });

    let poisoned = true;
    const createCollection = createRejectingCollectionFactory((key) =>
      poisoned && key === "flaky" ? "temporarily unapplicable" : undefined,
    );

    const db = await createDb(backend, {
      clientId: "a",
      retry: { maxAttempts: 1, baseDelayMs: 0 },
      createCollection,
    });

    await db.sync();
    expect(db.collections.deadletter.state.size).toBe(1);
    expect(db.collections.todos.get("flaky")).toBeUndefined();

    poisoned = false;
    await db.retryDeadLetter();

    expect(db.collections.deadletter.state.size).toBe(0);
    expect(db.collections.todos.get("flaky")).toBeDefined();
  });
});

describe("transaction batching", () => {
  it("keeps a multi-event transaction inside one push batch", async () => {
    const backend = createMockSyncBackend();
    const db = await createDb(backend, { clientId: "a", pushBatchSize: 2 });

    await db.collections.todos.insert([makeTodo("a1"), makeTodo("a2"), makeTodo("a3")]).isPersisted
      .promise;
    await db.collections.todos.insert(makeTodo("b1")).isPersisted.promise;

    await db.sync();

    const batches = backend.pushBatchSizes;
    expect(batches.reduce((total, size) => total + size, 0)).toBe(4);
    // The three-event transaction exceeds the batch size but is still sent whole.
    expect(batches).toEqual([3, 1]);
  });
});

describe("local mutation durability", () => {
  it("rolls the row back when the outbox append fails", async () => {
    const db = await openTodoDb({ clientId: "a" });

    const outbox = db.collections.outbox as unknown as {
      insert: (entry: unknown) => { isPersisted: { promise: Promise<void> } };
    };
    const originalInsert = outbox.insert.bind(outbox);
    outbox.insert = () => {
      const promise = Promise.reject(new Error("disk full"));
      promise.catch(() => undefined);
      return { isPersisted: { promise } };
    };

    await expect(db.collections.todos.insert(makeTodo("t1")).isPersisted.promise).rejects.toThrow(
      /disk full/,
    );

    expect(db.collections.todos.get("t1")).toBeUndefined();

    outbox.insert = originalInsert;
    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    expect(db.collections.todos.get("t1")).toBeDefined();
    expect(db.collections.outbox.state.size).toBe(1);
  });
});
