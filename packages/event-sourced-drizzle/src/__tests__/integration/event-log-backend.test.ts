import { afterEach, describe, expect, it } from "vitest";

import { createSqliteTestContext } from "../helpers/db";
import { createSqliteEventLogBackend } from "../../testing/sqlite-event-log-backend";

const backends: Array<{ close: () => void }> = [];

afterEach(() => {
  while (backends.length > 0) {
    backends.pop()?.close();
  }
});

describe("sqlite event log backend", () => {
  it("persists pushed events in the backend database", async () => {
    const backend = createSqliteEventLogBackend({ backendId: "lab" });
    backends.push(backend);

    const ctx = await createSqliteTestContext({ sync: backend });
    await ctx.engine.mutate.insert("todos", { id: "t1", title: "From client", done: false });
    const result = await ctx.engine.sync();

    expect(result.pushed).toBe(1);
    expect(result.errors).toHaveLength(0);

    const log = backend.rawQuery<{
      event_id: string;
      collection_id: string;
      key: string;
      type: string;
    }>("SELECT event_id, collection_id, key, type FROM sync_events ORDER BY global_seq");
    expect(log).toHaveLength(1);
    expect(log[0]).toMatchObject({ collection_id: "todos", key: "t1", type: "insert" });
  });

  it("pulls seeded remote events into the client domain table", async () => {
    const backend = createSqliteEventLogBackend({ backendId: "lab" });
    backends.push(backend);

    backend.seed({
      collectionId: "todos",
      key: "remote-1",
      payload: { id: "remote-1", title: "From server", done: true },
    });

    const ctx = await createSqliteTestContext({ sync: backend });
    const result = await ctx.engine.sync();

    expect(result.pulled).toBe(1);
    const todos = ctx.rawQuery<{ id: string; title: string }>("SELECT id, title FROM todos");
    expect(todos).toEqual([{ id: "remote-1", title: "From server" }]);

    const log = backend.rawQuery<{ key: string }>("SELECT key FROM sync_events");
    expect(log).toEqual([{ key: "remote-1" }]);
  });
});
