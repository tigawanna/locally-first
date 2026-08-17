import { describe, expect, it } from "vitest";
import {
  createSqliteTestContext,
  createPgliteTestContext,
  createTestTransport,
  type TestContext,
} from "../helpers/db";
import type { PullResponse, PushResponse, OutboundEvent } from "../../core/protocol";

type CreateCtx = (options?: any) => Promise<TestContext>;

function syncTests(createCtx: CreateCtx) {
  it("pushes outbox events via sync transport", async () => {
    const { transport, pushed } = createTestTransport();
    const ctx = await createCtx({ sync: transport });

    await ctx.engine.mutate.insert("todos", { id: "t1", title: "Push me", done: false });
    const result = await ctx.engine.sync();

    expect(result.pushed).toBe(1);
    expect(pushed).toHaveLength(1);
    expect(pushed[0]!.events[0].collectionId).toBe("todos");
    expect(pushed[0]!.events[0].type).toBe("insert");
    expect(pushed[0]!.events[0].key).toBe("t1");
  });

  it("marks outbox as synced after successful push", async () => {
    const { transport } = createTestTransport();
    const ctx = await createCtx({ sync: transport });

    await ctx.engine.mutate.insert("todos", { id: "t1", title: "X", done: false });
    await ctx.engine.sync();

    if (ctx.dbType === "sqlite") {
      const outbox = ctx.rawQuery<{ sync: number; sync_status: string }>(
        "SELECT sync, sync_status FROM sync_outbox",
      );
      expect(outbox[0]!.sync).toBe(1); // true in sqlite
      expect(outbox[0]!.sync_status).toBe("synced");
    }
  });

  it("does not push when sync is disabled", async () => {
    const { transport, pushed } = createTestTransport();
    const ctx = await createCtx({ sync: transport, syncEnabled: false });

    await ctx.engine.mutate.insert("todos", { id: "t1", title: "X", done: false });
    const result = await ctx.engine.sync();

    expect(result.pushed).toBe(0);
    expect(pushed).toHaveLength(0);
  });

  it("toggle syncEnabled at runtime", async () => {
    const { transport, pushed } = createTestTransport();
    const ctx = await createCtx({ sync: transport });

    ctx.engine.setSyncEnabled(false);
    expect(ctx.engine.getSyncEnabled()).toBe(false);

    await ctx.engine.mutate.insert("todos", { id: "t1", title: "X", done: false });
    await ctx.engine.sync();
    expect(pushed).toHaveLength(0);

    ctx.engine.setSyncEnabled(true);
    await ctx.engine.sync();
    expect(pushed).toHaveLength(1);
  });

  it("reports errors from failed push", async () => {
    const transport = {
      push: async () => {
        throw new Error("network failure");
      },
      pull: async () => ({ events: [], cursor: "0", hasMore: false }),
    };
    const ctx = await createCtx({ sync: transport });

    await ctx.engine.mutate.insert("todos", { id: "t1", title: "X", done: false });
    const result = await ctx.engine.sync();

    expect(result.errors).toHaveLength(1);
    expect(result.errors[0]!.message).toContain("network failure");
  });

  it("handles push with partial failures", async () => {
    let callCount = 0;
    const transport = {
      push: async (events: ReadonlyArray<OutboundEvent>): Promise<PushResponse> => {
        callCount++;
        return {
          confirmed: [],
          failed: events.map((e) => ({
            eventId: e.eventId,
            message: "rejected",
            code: "CONFLICT",
            retryable: false,
          })),
        };
      },
      pull: async (): Promise<PullResponse> => ({ events: [], cursor: "0", hasMore: false }),
    };
    const ctx = await createCtx({ sync: transport });

    await ctx.engine.mutate.insert("todos", { id: "t1", title: "X", done: false });
    await ctx.engine.sync();

    expect(callCount).toBe(1);
    // Event should still be in outbox (failed, possibly dead-lettered depending on retry config)
  });

  it("calls onSyncStart and onSyncComplete hooks", async () => {
    const { transport } = createTestTransport();
    const hookCalls: string[] = [];

    const ctx = await createCtx({
      sync: transport,
      hooks: {
        onSyncStart: () => hookCalls.push("start"),
        onSyncComplete: () => hookCalls.push("complete"),
      },
    });

    await ctx.engine.mutate.insert("todos", { id: "t1", title: "X", done: false });
    await ctx.engine.sync();

    expect(hookCalls).toContain("start");
    expect(hookCalls).toContain("complete");
  });
}

describe("sync [sqlite]", () => {
  syncTests(createSqliteTestContext);
});

describe("sync [pglite]", () => {
  syncTests(createPgliteTestContext);
});
