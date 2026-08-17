import { describe, expect, it } from "vitest";
import { createSqliteTestContext, createPgliteTestContext, type TestContext } from "../helpers/db";

type CreateCtx = () => Promise<TestContext>;

function mutateTests(createCtx: CreateCtx) {
  describe("insert", () => {
    it("inserts a row into the domain table and outbox", async () => {
      const ctx = await createCtx();
      await ctx.engine.mutate.insert("todos", { id: "t1", title: "Buy milk", done: false });

      // Verify domain table
      if (ctx.dbType === "sqlite") {
        const rows = ctx.rawQuery<{ id: string; title: string }>("SELECT * FROM todos");
        expect(rows).toHaveLength(1);
        expect(rows[0]!.title).toBe("Buy milk");
      }

      // Verify outbox
      if (ctx.dbType === "sqlite") {
        const outbox = ctx.rawQuery<{
          event_id: string;
          collection_id: string;
          type: string;
          key: string;
        }>("SELECT * FROM sync_outbox");
        expect(outbox).toHaveLength(1);
        expect(outbox[0]!.collection_id).toBe("todos");
        expect(outbox[0]!.type).toBe("insert");
        expect(outbox[0]!.key).toBe("t1");
      }
    });

    it("inserts multiple rows", async () => {
      const ctx = await createCtx();
      await ctx.engine.mutate.insert("todos", { id: "t1", title: "First", done: false });
      await ctx.engine.mutate.insert("todos", { id: "t2", title: "Second", done: true });

      if (ctx.dbType === "sqlite") {
        const rows = ctx.rawQuery<{ id: string }>("SELECT * FROM todos ORDER BY id");
        expect(rows).toHaveLength(2);
      }
    });

    it("tracks optimistic state after insert", async () => {
      const ctx = await createCtx();
      await ctx.engine.mutate.insert("todos", { id: "t1", title: "Hello", done: false });

      expect(ctx.engine.optimistic.isPending("todos", "t1")).toBe(true);
      expect(ctx.engine.optimistic.size).toBe(1);
    });

    it("throws on unknown collection", async () => {
      const ctx = await createCtx();
      await expect((ctx.engine.mutate as any).insert("widgets", { id: "w1" })).rejects.toThrow(
        /[Uu]nknown collection/,
      );
    });
  });

  describe("update", () => {
    it("updates an existing row in the domain table", async () => {
      const ctx = await createCtx();
      await ctx.engine.mutate.insert("todos", { id: "t1", title: "Original", done: false });
      await ctx.engine.mutate.update("todos", "t1", { title: "Updated" });

      if (ctx.dbType === "sqlite") {
        const rows = ctx.rawQuery<{ title: string }>("SELECT title FROM todos WHERE id = 't1'");
        expect(rows[0]!.title).toBe("Updated");
      }
    });

    it("appends a second outbox event", async () => {
      const ctx = await createCtx();
      await ctx.engine.mutate.insert("todos", { id: "t1", title: "X", done: false });
      await ctx.engine.mutate.update("todos", "t1", { done: true });

      if (ctx.dbType === "sqlite") {
        const outbox = ctx.rawQuery<{ type: string }>(
          "SELECT type FROM sync_outbox ORDER BY local_seq",
        );
        expect(outbox).toHaveLength(2);
        expect(outbox[0]!.type).toBe("insert");
        expect(outbox[1]!.type).toBe("update");
      }
    });

    it("tracks optimistic state for update", async () => {
      const ctx = await createCtx();
      await ctx.engine.mutate.insert("todos", { id: "t1", title: "X", done: false });
      await ctx.engine.mutate.update("todos", "t1", { title: "Y" });

      expect(ctx.engine.optimistic.isPending("todos", "t1")).toBe(true);
      // 2 entries: one for insert, one for update
      expect(ctx.engine.optimistic.size).toBe(2);
    });
  });

  describe("delete", () => {
    it("removes a row from the domain table", async () => {
      const ctx = await createCtx();
      await ctx.engine.mutate.insert("todos", { id: "t1", title: "X", done: false });
      await ctx.engine.mutate.delete("todos", "t1");

      if (ctx.dbType === "sqlite") {
        const rows = ctx.rawQuery<{ id: string }>("SELECT * FROM todos");
        expect(rows).toHaveLength(0);
      }
    });

    it("appends delete event to outbox", async () => {
      const ctx = await createCtx();
      await ctx.engine.mutate.insert("todos", { id: "t1", title: "X", done: false });
      await ctx.engine.mutate.delete("todos", "t1");

      if (ctx.dbType === "sqlite") {
        const outbox = ctx.rawQuery<{ type: string }>(
          "SELECT type FROM sync_outbox ORDER BY local_seq",
        );
        expect(outbox[outbox.length - 1]!.type).toBe("delete");
      }
    });
  });
}

describe("mutate [sqlite]", () => {
  mutateTests(createSqliteTestContext);
});

describe("mutate [pglite]", () => {
  mutateTests(createPgliteTestContext);
});
