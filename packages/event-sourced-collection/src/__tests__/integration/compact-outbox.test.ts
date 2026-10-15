import { describe, expect, it } from "vitest";

import { makeTodo, openTodoDb } from "../helpers/node-db";

describe("compactOnWrite", () => {
  it("folds twenty updates into the unsynced insert", async () => {
    const db = await openTodoDb();
    await db.collections.todos.insert(makeTodo("t1", "Task")).isPersisted.promise;
    for (let step = 1; step <= 20; step++) {
      await db.collections.todos.update("t1", (draft) => {
        draft.title = `Task ${step}`;
      }).isPersisted.promise;
    }

    const events = [...db.collections.outbox.state.values()];
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
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
    expect(result).toEqual({ compacted: 1, removed: 2 });

    const events = [...db.collections.outbox.state.values()];
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      type: "insert",
      compaction: true,
      payload: { title: "Twice" },
    });
    expect(events[0]?.compactedAt).toEqual(expect.any(Number));
  });

  it("folds events that have already synced", async () => {
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
    expect(result).toEqual({ compacted: 1, removed: 1 });
    const events = [...db.collections.outbox.state.values()];
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      type: "update",
      sync: false,
      compaction: true,
      payload: { title: "Edited" },
    });
  });
});
