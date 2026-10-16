import { describe, expect, it } from "vite-plus/test";

import { makeTodo, openTodoDb } from "../helpers/node-db";

describe("compactOnWrite", () => {
  it("folds twenty updates into the unsynced insert", async () => {
    const db = await openTodoDb();
    await db.collections.todos.insert(makeTodo("t1", "Task")).isPersisted.promise;
    const created = [...db.collections.outbox.state.values()][0]!;
    for (let step = 1; step <= 20; step++) {
      await db.collections.todos.update("t1", (draft) => {
        draft.title = `Task ${step}`;
      }).isPersisted.promise;
    }

    const events = [...db.collections.outbox.state.values()];
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      eventId: created.eventId,
      localSeq: created.localSeq,
      txId: created.txId,
      type: "insert",
      key: "t1",
      compaction: true,
      payload: { id: "t1", title: "Task 20", done: false },
      previous: null,
    });
    expect(events[0]?.compactedAt).toEqual(expect.any(Number));
    expect(db.collections.todos.get("t1")?.title).toBe("Task 20");
  });

  it("keeps each update when compactOnWrite is off", async () => {
    const db = await openTodoDb({ compactOnWrite: false });
    await db.collections.todos.insert(makeTodo("t1", "Task")).isPersisted.promise;
    await db.collections.todos.update("t1", (draft) => {
      draft.title = "Edited";
    }).isPersisted.promise;

    const events = [...db.collections.outbox.state.values()].sort(
      (a, b) => a.localSeq - b.localSeq,
    );
    expect(events.map((event) => event.type)).toEqual(["insert", "update"]);
    expect(events.every((event) => event.compaction !== true)).toBe(true);
  });

  it("folds later edits into one update once the insert has synced", async () => {
    const db = await openTodoDb();
    await db.collections.todos.insert(makeTodo("t1", "Task")).isPersisted.promise;
    const created = [...db.collections.outbox.state.values()][0]!;
    await db.collections.outbox.update(created.eventId, (draft) => {
      draft.sync = true;
    }).isPersisted.promise;

    await db.collections.todos.update("t1", (draft) => {
      draft.title = "Once";
    }).isPersisted.promise;
    await db.collections.todos.update("t1", (draft) => {
      draft.title = "Twice";
    }).isPersisted.promise;

    const pending = [...db.collections.outbox.state.values()].filter((event) => !event.sync);
    expect(pending).toHaveLength(1);
    expect(pending[0]).toMatchObject({
      type: "update",
      compaction: true,
      payload: { title: "Twice" },
      previous: { id: "t1", title: "Task", done: false },
    });
  });
});

describe("compact", () => {
  it("folds an outbox that was written with compactOnWrite off", async () => {
    const db = await openTodoDb({ compactOnWrite: false });
    await db.collections.todos.insert(makeTodo("t1", "Task")).isPersisted.promise;
    await db.collections.todos.update("t1", (draft) => {
      draft.title = "Once";
    }).isPersisted.promise;
    await db.collections.todos.update("t1", (draft) => {
      draft.title = "Twice";
    }).isPersisted.promise;

    const result = await db.compact();
    expect(result).toEqual({ compacted: 1, removed: 2, deferred: false });

    const events = [...db.collections.outbox.state.values()];
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      type: "insert",
      compaction: true,
      payload: { title: "Twice" },
    });
    expect(events[0]?.compactedAt).toEqual(expect.any(Number));
  });

  it("folds a recreated row without swallowing the earlier delete", async () => {
    const db = await openTodoDb();
    await db.collections.todos.insert(makeTodo("t1", "Task")).isPersisted.promise;
    await db.collections.todos.delete("t1").isPersisted.promise;
    await db.collections.todos.insert(makeTodo("t1", "Reborn")).isPersisted.promise;
    await db.collections.todos.update("t1", (draft) => {
      draft.title = "Reborn 2";
    }).isPersisted.promise;

    const events = [...db.collections.outbox.state.values()].sort(
      (a, b) => a.localSeq - b.localSeq,
    );
    expect(events.map((event) => event.type)).toEqual(["insert", "delete", "insert"]);
    expect(events[2]).toMatchObject({
      compaction: true,
      payload: { title: "Reborn 2" },
    });
  });

  it("leaves a chain the server has already accepted", async () => {
    const db = await openTodoDb({ compactOnWrite: false });
    await db.collections.todos.insert(makeTodo("t1", "Task")).isPersisted.promise;
    await db.collections.todos.update("t1", (draft) => {
      draft.title = "Edited";
    }).isPersisted.promise;
    for (const event of db.collections.outbox.state.values()) {
      await db.collections.outbox.update(event.eventId, (draft) => {
        draft.sync = true;
      }).isPersisted.promise;
    }

    const result = await db.compact();
    expect(result).toEqual({ compacted: 0, removed: 0, deferred: false });
    const events = [...db.collections.outbox.state.values()];
    expect(events).toHaveLength(2);
    expect(events.every((event) => event.sync)).toBe(true);
  });

  it("folds only the unsynced tail onto a synced insert", async () => {
    const db = await openTodoDb({ compactOnWrite: false });
    await db.collections.todos.insert(makeTodo("t1", "Task")).isPersisted.promise;
    const created = [...db.collections.outbox.state.values()][0]!;
    await db.collections.outbox.update(created.eventId, (draft) => {
      draft.sync = true;
    }).isPersisted.promise;
    await db.collections.todos.update("t1", (draft) => {
      draft.title = "Once";
    }).isPersisted.promise;
    await db.collections.todos.update("t1", (draft) => {
      draft.title = "Twice";
    }).isPersisted.promise;

    const result = await db.compact();
    expect(result).toEqual({ compacted: 1, removed: 1, deferred: false });
    const events = [...db.collections.outbox.state.values()].sort(
      (a, b) => a.localSeq - b.localSeq,
    );
    expect(events.map((event) => ({ type: event.type, sync: event.sync }))).toEqual([
      { type: "insert", sync: true },
      { type: "update", sync: false },
    ]);
    expect(events[1]).toMatchObject({
      compaction: true,
      payload: { title: "Twice" },
      previous: { title: "Task" },
    });
  });

  it("drops a life the server never saw", async () => {
    const db = await openTodoDb({ compactOnWrite: false });
    await db.collections.todos.insert(makeTodo("t1", "Task")).isPersisted.promise;
    await db.collections.todos.update("t1", (draft) => {
      draft.title = "Edited";
    }).isPersisted.promise;
    await db.collections.todos.delete("t1").isPersisted.promise;

    const result = await db.compact();
    expect(result).toEqual({ compacted: 0, removed: 3, deferred: false });
    expect(db.collections.outbox.state.size).toBe(0);
  });

  it("keeps the synced insert and one delete after later edits", async () => {
    const db = await openTodoDb({ compactOnWrite: false });
    await db.collections.todos.insert(makeTodo("t1", "Task")).isPersisted.promise;
    const created = [...db.collections.outbox.state.values()][0]!;
    await db.collections.outbox.update(created.eventId, (draft) => {
      draft.sync = true;
    }).isPersisted.promise;
    await db.collections.todos.update("t1", (draft) => {
      draft.title = "Edited";
    }).isPersisted.promise;
    await db.collections.todos.delete("t1").isPersisted.promise;

    const result = await db.compact();
    expect(result).toEqual({ compacted: 1, removed: 1, deferred: false });
    const events = [...db.collections.outbox.state.values()].sort(
      (a, b) => a.localSeq - b.localSeq,
    );
    expect(events.map((event) => ({ type: event.type, sync: event.sync }))).toEqual([
      { type: "insert", sync: true },
      { type: "delete", sync: false },
    ]);
    expect(events[1]).toMatchObject({
      payload: { title: "Task" },
      previous: { title: "Task" },
    });
  });

  it("keeps a folded edit in the transaction it already shared", async () => {
    const db = await openTodoDb();
    await db.collections.todos.insert(makeTodo("t1", "Task")).isPersisted.promise;
    await db.collections.todos.insert(makeTodo("t2", "Other")).isPersisted.promise;
    const [first, second] = [...db.collections.outbox.state.values()].sort(
      (a, b) => a.localSeq - b.localSeq,
    );
    await db.collections.outbox.update(second!.eventId, (draft) => {
      draft.txId = first!.txId;
    }).isPersisted.promise;

    await db.collections.todos.update("t1", (draft) => {
      draft.title = "Edited";
    }).isPersisted.promise;

    const events = [...db.collections.outbox.state.values()].sort(
      (a, b) => a.localSeq - b.localSeq,
    );
    expect(events.map((event) => event.key)).toEqual(["t1", "t2"]);
    expect(events[0]).toMatchObject({
      eventId: first!.eventId,
      txId: first!.txId,
      localSeq: first!.localSeq,
      compaction: true,
      payload: { title: "Edited" },
    });
    expect(events[1]?.txId).toBe(first!.txId);
  });
});

describe("compaction and push", () => {
  it("sends compaction fields on the pushed event", async () => {
    const seen: Array<{ compaction?: boolean; compactedAt?: number | null; type: string }> = [];
    const db = await openTodoDb({
      sync: {
        push: async (events) => {
          seen.push(
            ...events.map((event) => ({
              type: event.type,
              compaction: event.compaction,
              compactedAt: event.compactedAt,
            })),
          );
          return {
            confirmed: events.map((event) => ({ eventId: event.eventId, globalSeq: 1 })),
          };
        },
        pull: async () => ({ events: [], cursor: "0", hasMore: false }),
      },
    });

    await db.collections.todos.insert(makeTodo("t1", "Task")).isPersisted.promise;
    await db.collections.todos.update("t1", (draft) => {
      draft.title = "Edited";
    }).isPersisted.promise;
    await db.sync();

    expect(seen).toHaveLength(1);
    expect(seen[0]).toMatchObject({ type: "insert", compaction: true });
    expect(seen[0]?.compactedAt).toEqual(expect.any(Number));
  });

  it("does not rewrite an insert a push has already sent", async () => {
    let release: () => void = () => {};
    const released = new Promise<void>((resolve) => {
      release = resolve;
    });
    let markStarted: () => void = () => {};
    const pushStarted = new Promise<void>((resolve) => {
      markStarted = resolve;
    });
    const sent: Array<{ type: string; title: unknown }> = [];

    const db = await openTodoDb({
      sync: {
        push: async (events) => {
          sent.push(...events.map((event) => ({ type: event.type, title: event.payload.title })));
          markStarted();
          await released;
          return {
            confirmed: events.map((event) => ({ eventId: event.eventId, globalSeq: 1 })),
          };
        },
        pull: async () => ({ events: [], cursor: "0", hasMore: false }),
      },
    });

    await db.collections.todos.insert(makeTodo("t1", "Task")).isPersisted.promise;
    const syncing = db.sync();
    await pushStarted;
    await db.collections.todos.update("t1", (draft) => {
      draft.title = "Edited";
    }).isPersisted.promise;
    release();
    await syncing;

    expect(sent).toEqual([{ type: "insert", title: "Task" }]);
    const events = [...db.collections.outbox.state.values()].sort(
      (a, b) => a.localSeq - b.localSeq,
    );
    expect(events.map((event) => ({ type: event.type, sync: event.sync }))).toEqual([
      { type: "insert", sync: true },
      { type: "update", sync: false },
    ]);
    expect(events[1]?.payload).toMatchObject({ title: "Edited" });
  });
});
