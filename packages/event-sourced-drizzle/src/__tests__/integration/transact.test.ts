import { describe, expect, it } from "vitest";
import { createSqliteTestContext, createPgliteTestContext, type TestContext } from "../helpers/db";

type CreateCtx = () => Promise<TestContext>;

function transactTests(createCtx: CreateCtx) {
  it("groups mutations under a single txId", async () => {
    const ctx = await createCtx();
    const { run } = ctx.engine.transact();

    await run(async () => {
      await ctx.engine.mutate.insert("todos", { id: "t1", title: "First", done: false });
      await ctx.engine.mutate.insert("todos", { id: "t2", title: "Second", done: false });
    });

    if (ctx.dbType === "sqlite") {
      const outbox = ctx.rawQuery<{ tx_id: string }>("SELECT tx_id FROM sync_outbox");
      expect(outbox).toHaveLength(2);
      // Both events share the same txId
      expect(outbox[0]!.tx_id).toBe(outbox[1]!.tx_id);
    }

    // Both tracked in optimistic state
    expect(ctx.engine.optimistic.isPending("todos", "t1")).toBe(true);
    expect(ctx.engine.optimistic.isPending("todos", "t2")).toBe(true);
  });

  it("commits domain writes atomically", async () => {
    const ctx = await createCtx();
    const { run } = ctx.engine.transact();

    await run(async () => {
      await ctx.engine.mutate.insert("todos", { id: "t1", title: "One", done: false });
      await ctx.engine.mutate.insert("todos", { id: "t2", title: "Two", done: false });
    });

    if (ctx.dbType === "sqlite") {
      const rows = ctx.rawQuery<{ id: string }>("SELECT * FROM todos ORDER BY id");
      expect(rows).toHaveLength(2);
    }
  });

  it("rolls back on error — no domain writes persisted", async () => {
    const ctx = await createCtx();
    const { run } = ctx.engine.transact();

    await expect(
      run(async () => {
        await ctx.engine.mutate.insert("todos", { id: "t1", title: "One", done: false });
        throw new Error("abort!");
      }),
    ).rejects.toThrow("abort!");

    if (ctx.dbType === "sqlite") {
      const rows = ctx.rawQuery<{ id: string }>("SELECT * FROM todos");
      expect(rows).toHaveLength(0);

      const outbox = ctx.rawQuery<{ event_id: string }>("SELECT * FROM sync_outbox");
      expect(outbox).toHaveLength(0);
    }
  });

  it("standalone mutations outside transact get their own txId", async () => {
    const ctx = await createCtx();

    await ctx.engine.mutate.insert("todos", { id: "t1", title: "Solo", done: false });

    if (ctx.dbType === "sqlite") {
      const outbox = ctx.rawQuery<{ tx_id: string }>("SELECT tx_id FROM sync_outbox");
      expect(outbox).toHaveLength(1);
      // txId is a UUID
      expect(outbox[0]!.tx_id).toMatch(/^[0-9a-f-]+$/);
    }
  });

  it("allows a custom txId", async () => {
    const ctx = await createCtx();
    const { run, txId } = ctx.engine.transact({ txId: "custom-tx-123" });

    await run(async () => {
      await ctx.engine.mutate.insert("todos", { id: "t1", title: "X", done: false });
    });

    expect(txId).toBe("custom-tx-123");

    if (ctx.dbType === "sqlite") {
      const outbox = ctx.rawQuery<{ tx_id: string }>("SELECT tx_id FROM sync_outbox");
      expect(outbox[0]!.tx_id).toBe("custom-tx-123");
    }
  });
}

describe("transact [sqlite]", () => {
  transactTests(createSqliteTestContext);
});

describe("transact [pglite]", () => {
  transactTests(createPgliteTestContext);
});
