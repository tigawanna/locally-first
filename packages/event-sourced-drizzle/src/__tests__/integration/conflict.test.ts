import { afterEach, describe, expect, it } from "vitest";

import { createSqliteTestContext } from "../helpers/db";
import { createSqliteEventLogBackend } from "../../testing/sqlite-event-log-backend";

const backends: Array<{ close: () => void }> = [];

afterEach(() => {
  while (backends.length > 0) {
    backends.pop()?.close();
  }
});

describe("conflict detection", () => {
  it("stamps baseVersion from the previous local event", async () => {
    const backend = createSqliteEventLogBackend();
    backends.push(backend);
    const ctx = await createSqliteTestContext({ sync: backend, conflictDetection: true });

    await ctx.engine.mutate.insert("todos", { id: "t1", title: "one", done: false });
    await ctx.engine.mutate.update("todos", "t1", { title: "two" });

    const outbox = ctx.rawQuery<{ type: string; base_version: string | null }>(
      "SELECT type, base_version FROM sync_outbox ORDER BY local_seq",
    );
    expect(outbox[0]?.base_version).toBeNull();
    expect(outbox[1]?.base_version).toBeTruthy();
  });

  it("rejects a stale update against the event log", async () => {
    const backend = createSqliteEventLogBackend();
    backends.push(backend);
    const ctx = await createSqliteTestContext({
      sync: backend,
      conflictDetection: true,
      retry: { maxAttempts: 1 },
    });

    await ctx.engine.mutate.insert("todos", { id: "t1", title: "one", done: false });
    await ctx.engine.sync();

    backend.seed({
      collectionId: "todos",
      key: "t1",
      payload: { id: "t1", title: "remote wins", done: false },
    });

    await ctx.engine.mutate.update("todos", "t1", { title: "stale" });
    const result = await ctx.engine.sync();

    expect(result.deadLettered).toBeGreaterThanOrEqual(1);
    const dead = ctx.rawQuery<{ reason: string }>("SELECT reason FROM sync_dead_letter");
    expect(dead.some((row) => row.reason === "conflict")).toBe(true);
  });
});
