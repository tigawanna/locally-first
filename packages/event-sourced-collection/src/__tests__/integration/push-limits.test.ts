import { describe, expect, it } from "vitest";

import { SyncPushError } from "../../core/sync";
import { SYNC_PUSH_PRESETS, resolvePushLimits } from "../../core/sync-presets";
import type { OutboundEvent, PullResponse, PushResponse } from "../../core/types";
import { makeTodo, openTodoDb } from "../helpers/node-db";

const noPull = async (): Promise<PullResponse> => ({ events: [], cursor: "0", hasMore: false });

const confirmAll = (events: ReadonlyArray<OutboundEvent>): PushResponse => ({
  confirmed: events.map((event, index) => ({ eventId: event.eventId, globalSeq: index + 1 })),
});

function bodyBytes(events: ReadonlyArray<OutboundEvent>): number {
  return new TextEncoder().encode(JSON.stringify(events)).length;
}

describe("resolvePushLimits", () => {
  it("defaults to 20 events and no byte cap", () => {
    expect(resolvePushLimits({})).toEqual({ pushBatchSize: 20, maxPushBytes: null });
  });

  it("clamps pushBatchSize to 1–100", () => {
    expect(resolvePushLimits({ pushBatchSize: 0 }).pushBatchSize).toBe(1);
    expect(resolvePushLimits({ pushBatchSize: 500 }).pushBatchSize).toBe(100);
  });

  it("lets explicit values override a preset", () => {
    expect(resolvePushLimits({ syncPreset: "vercel" })).toEqual(SYNC_PUSH_PRESETS.vercel);
    expect(resolvePushLimits({ syncPreset: "vercel", pushBatchSize: 4 })).toEqual({
      pushBatchSize: 4,
      maxPushBytes: SYNC_PUSH_PRESETS.vercel.maxPushBytes,
    });
  });
});

describe("push limits", () => {
  it("drains the whole outbox in one sync, batch by batch", async () => {
    const batches: number[] = [];
    const db = await openTodoDb({
      pushBatchSize: 3,
      sync: {
        push: async (events) => {
          batches.push(events.length);
          return confirmAll(events);
        },
        pull: noPull,
      },
    });

    for (let i = 0; i < 8; i++) {
      await db.collections.todos.insert(makeTodo(`t${i}`)).isPersisted.promise;
    }

    const result = await db.sync();

    expect(batches).toEqual([3, 3, 2]);
    expect(result.pushed).toBe(8);
    expect(db.getSyncStatus().pendingCount).toBe(0);
  });

  it("applies setPushLimits to the next sync", async () => {
    const batches: number[] = [];
    const db = await openTodoDb({
      pushBatchSize: 50,
      sync: {
        push: async (events) => {
          batches.push(events.length);
          return confirmAll(events);
        },
        pull: noPull,
      },
    });

    expect(db.setPushLimits({ pushBatchSize: 2 })).toEqual({
      pushBatchSize: 2,
      maxPushBytes: null,
    });
    for (let i = 0; i < 5; i++) {
      await db.collections.todos.insert(makeTodo(`t${i}`)).isPersisted.promise;
    }
    await db.sync();

    expect(batches).toEqual([2, 2, 1]);
    expect(db.setPushLimits({ pushBatchSize: 0 }).pushBatchSize).toBe(1);
  });

  it("keeps every request body under maxPushBytes", async () => {
    const maxPushBytes = 2_000;
    const sizes: number[] = [];
    const db = await openTodoDb({
      pushBatchSize: 100,
      maxPushBytes,
      sync: {
        push: async (events) => {
          sizes.push(bodyBytes(events));
          return confirmAll(events);
        },
        pull: noPull,
      },
    });

    for (let i = 0; i < 12; i++) {
      await db.collections.todos.insert(makeTodo(`t${i}`, "x".repeat(300))).isPersisted.promise;
    }

    const result = await db.sync();

    expect(result.pushed).toBe(12);
    expect(sizes.length).toBeGreaterThan(1);
    for (const size of sizes) expect(size).toBeLessThanOrEqual(maxPushBytes);
  });

  it("dead-letters an event that can never fit without sending it", async () => {
    const sent: string[] = [];
    const db = await openTodoDb({
      maxPushBytes: 1_000,
      sync: {
        push: async (events) => {
          sent.push(...events.map((event) => event.key.toString()));
          return confirmAll(events);
        },
        pull: noPull,
      },
    });

    await db.collections.todos.insert(makeTodo("small")).isPersisted.promise;
    await db.collections.todos.insert(makeTodo("huge", "x".repeat(5_000))).isPersisted.promise;
    await db.collections.todos.insert(makeTodo("after")).isPersisted.promise;

    const result = await db.sync();

    expect(sent).toEqual(["small", "after"]);
    expect(result.pushed).toBe(2);
    const dead = [...db.collections.deadletter.state.values()];
    expect(dead.map((row) => [row.key, row.code])).toEqual([["huge", "PAYLOAD_TOO_LARGE"]]);
  });

  it("splits a batch the server rejects with 413 down to single events", async () => {
    const requests: string[][] = [];
    const db = await openTodoDb({
      pushBatchSize: 4,
      sync: {
        push: async (events) => {
          const keys = events.map((event) => event.key.toString());
          requests.push(keys);
          if (keys.includes("huge")) throw new SyncPushError(413, "Request Entity Too Large");
          return confirmAll(events);
        },
        pull: noPull,
      },
    });

    for (const id of ["a", "b", "huge", "c"]) {
      await db.collections.todos.insert(makeTodo(id)).isPersisted.promise;
    }

    const result = await db.sync();

    expect(requests).toEqual([["a", "b", "huge", "c"], ["a", "b"], ["huge", "c"], ["huge"], ["c"]]);
    expect(result.pushed).toBe(3);
    expect(result.errors).toEqual([]);
    expect([...db.collections.deadletter.state.values()].map((row) => row.key)).toEqual(["huge"]);
    expect(db.getSyncStatus().pendingCount).toBe(0);
  });

  it("counts one attempt per batch even when 413 splits it", async () => {
    let calls = 0;
    const db = await openTodoDb({
      pushBatchSize: 4,
      sync: {
        push: async (events) => {
          calls += 1;
          if (events.length > 1) throw new SyncPushError(413, "too large");
          return confirmAll(events);
        },
        pull: noPull,
      },
    });

    for (const id of ["a", "b", "c", "d"]) {
      await db.collections.todos.insert(makeTodo(id)).isPersisted.promise;
    }

    await db.sync();

    expect(calls).toBe(7);
    for (const row of db.collections.outbox.state.values()) {
      expect(row.sync).toBe(true);
      expect(row.attemptCount).toBe(1);
    }
  });

  it("backs off only the unsent half after a transport failure mid-split", async () => {
    const db = await openTodoDb({
      pushBatchSize: 4,
      sync: {
        push: async (events) => {
          if (events.length > 2) throw new SyncPushError(413, "too large");
          if (events.some((event) => event.key === "c")) throw new Error("offline");
          return confirmAll(events);
        },
        pull: noPull,
      },
    });

    for (const id of ["a", "b", "c", "d"]) {
      await db.collections.todos.insert(makeTodo(id)).isPersisted.promise;
    }

    const result = await db.sync();

    expect(result.pushed).toBe(2);
    expect(result.errors[0]?.message).toBe("offline");
    const byKey = new Map([...db.collections.outbox.state.values()].map((row) => [row.key, row]));
    expect(byKey.get("a")?.sync).toBe(true);
    expect(byKey.get("b")?.sync).toBe(true);
    expect(byKey.get("c")?.syncStatus).toBe("failed");
    expect(byKey.get("d")?.syncStatus).toBe("failed");
  });
});
