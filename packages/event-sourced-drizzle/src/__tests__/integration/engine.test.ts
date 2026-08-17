import { describe, expect, it } from "vitest";
import { createSqliteTestContext, createPgliteTestContext, type TestContext } from "../helpers/db";

type CreateCtx = (options?: any) => Promise<TestContext>;

function engineTests(createCtx: CreateCtx) {
  it("resolves a stable clientId on creation", async () => {
    const ctx = await createCtx();

    if (ctx.dbType === "sqlite") {
      const meta = ctx.rawQuery<{ key: string; value: string }>(
        "SELECT * FROM sync_meta WHERE key = 'clientId'",
      );
      expect(meta).toHaveLength(1);
      expect(meta[0]!.value).toMatch(/^[0-9a-f-]+$/); // UUID format
    }
  });

  it("uses provided clientId", async () => {
    const ctx = await createCtx({ clientId: "my-custom-client" });

    if (ctx.dbType === "sqlite") {
      const meta = ctx.rawQuery<{ value: string }>(
        "SELECT value FROM sync_meta WHERE key = 'clientId'",
      );
      expect(meta[0]!.value).toBe("my-custom-client");
    }
  });

  it("fires onReady hook on creation", async () => {
    let readyCtx: { clientId: string; pullCursor: number } | null = null;
    await createCtx({
      hooks: {
        onReady: (ctx: any) => {
          readyCtx = ctx;
        },
      },
    });

    expect(readyCtx).not.toBeNull();
    expect(readyCtx!.clientId).toBeTruthy();
    expect(readyCtx!.pullCursor).toBe(0);
  });

  it("fires onMutation hook on insert", async () => {
    const mutations: any[] = [];
    const ctx = await createCtx({
      hooks: { onMutation: (entry: any) => mutations.push(entry) },
    });

    await ctx.engine.mutate.insert("todos", { id: "t1", title: "X", done: false });
    expect(mutations).toHaveLength(1);
    expect(mutations[0].collectionId).toBe("todos");
    expect(mutations[0].type).toBe("insert");
  });

  it("optimistic tracker reflects pending mutations", async () => {
    const ctx = await createCtx();

    expect(ctx.engine.optimistic.size).toBe(0);

    await ctx.engine.mutate.insert("todos", { id: "t1", title: "X", done: false });
    expect(ctx.engine.optimistic.size).toBe(1);
    expect(ctx.engine.optimistic.isPending("todos", "t1")).toBe(true);
    expect(ctx.engine.optimistic.isPending("todos", "t2")).toBe(false);

    const summary = ctx.engine.optimistic.summary();
    expect(summary.pendingCount).toBe(1);
    expect(summary.collections["todos"]!.pendingKeys.has("t1")).toBe(true);
  });

  it("dispose clears optimistic state", async () => {
    const ctx = await createCtx();
    await ctx.engine.mutate.insert("todos", { id: "t1", title: "X", done: false });

    ctx.engine.dispose();
    expect(ctx.engine.optimistic.size).toBe(0);
  });
}

describe("engine [sqlite]", () => {
  engineTests(createSqliteTestContext);
});

describe("engine [pglite]", () => {
  engineTests(createPgliteTestContext);
});
