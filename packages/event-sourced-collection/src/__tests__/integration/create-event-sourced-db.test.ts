import { BasicIndex } from "@tanstack/db";
import { describe, expect, it } from "vitest";

import Database from "better-sqlite3";
import type {
  BackendMismatchPolicy,
  DeadLetterEntry,
  EventSourcedDB,
  EventSourcedHooks,
  InboxEntry,
  OutboundEvent,
  OutboxEntry,
  PullResponse,
  PushResponse,
  RetryConfig,
  ServerEvent,
  SyncLock,
  UnknownEventHandling,
  UpcastEventFn,
} from "../../core/types";
import {
  makeTodo,
  openEventSourcedDb,
  openTempSqlite,
  openTodoDb,
  openTodoDbOnSqlite,
  type TodoDefs,
} from "../helpers/node-db";
import type { CreateCollectionFn } from "../../core/persisted-collection";

type TestTransport = {
  push: (events: ReadonlyArray<OutboundEvent>) => Promise<PushResponse>;
  pull: (since: number) => Promise<PullResponse>;
};

type CreateDbOptions = {
  sync?: TestTransport;
  clientId?: string;
  unknownEventHandling?: UnknownEventHandling;
  pullOverlap?: number;
  eventSchemaVersion?: number;
  upcastEvent?: UpcastEventFn;
  retry?: RetryConfig;
  pushBatchSize?: number;
  backendMismatch?: BackendMismatchPolicy;
  conflictDetection?: boolean;
  lock?: SyncLock;
  hooks?: EventSourcedHooks;
  syncEnabled?: boolean;
  createCollection?: CreateCollectionFn;
};

function createDb(options: CreateDbOptions = {}): Promise<EventSourcedDB<TodoDefs>> {
  return openTodoDb(options);
}

function outboxRows(db: EventSourcedDB<TodoDefs>): OutboxEntry[] {
  return [...db.collections.outbox.state.values()];
}

function inboxRows(db: EventSourcedDB<TodoDefs>): InboxEntry[] {
  return [...db.collections.inbox.state.values()];
}

function deadLetterRows(db: EventSourcedDB<TodoDefs>): DeadLetterEntry[] {
  return [...db.collections.deadletter.state.values()];
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function makeServerEvent(
  globalSeq: number,
  key: string,
  eventId = `s${globalSeq}`,
  overrides: Partial<ServerEvent> = {},
): ServerEvent {
  return {
    globalSeq,
    eventId,
    collectionId: "todos",
    type: "insert",
    key,
    payload: { id: key, title: "FromServer", done: false },
    timestamp: 0,
    cursor: String(globalSeq),
    ...overrides,
  };
}

const noPull = async (): Promise<PullResponse> => ({ events: [], cursor: "0", hasMore: false });
const noPush = async (): Promise<PushResponse> => ({ confirmed: [] });

describe("syncEnabled", () => {
  it("skips push and pull when sync is disabled", async () => {
    let pushCalls = 0;
    const db = await createDb({
      syncEnabled: false,
      sync: {
        push: async () => {
          pushCalls += 1;
          return { confirmed: [] };
        },
        pull: async () => ({ events: [], cursor: "0", hasMore: false }),
      },
    });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    const result = await db.sync();

    expect(pushCalls).toBe(0);
    expect(result).toEqual({
      pushed: 0,
      pulled: 0,
      skipped: 0,
      deadLettered: 0,
      deferred: false,
      errors: [],
    });
  });

  it("can be toggled at runtime with setSyncEnabled", async () => {
    let pushCalls = 0;
    const db = await createDb({
      sync: {
        push: async (events) => {
          pushCalls += 1;
          return {
            confirmed: events.map((event) => ({ eventId: event.eventId, globalSeq: 1 })),
          };
        },
        pull: async () => ({ events: [], cursor: "0", hasMore: false }),
      },
    });

    expect(db.getSyncEnabled()).toBe(true);

    db.setSyncEnabled(false);
    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    await db.sync();
    expect(pushCalls).toBe(0);

    db.setSyncEnabled(true);
    await db.sync();
    expect(pushCalls).toBe(1);
  });
});

describe("collection indexes", () => {
  it("registers indexes declared on the collection definition", async () => {
    const db = await openEventSourcedDb({
      collections: {
        todos: {
          getKey: (todo: { id: string; done: boolean }) => todo.id,
          indexes: [
            { select: (todo: { id: string }) => todo.id, name: "by-id", indexType: BasicIndex },
            {
              select: (todo: { done: boolean }) => todo.done,
              name: "by-done",
              indexType: BasicIndex,
            },
          ],
        },
      },
    });

    const indexes = db.collections.todos.getIndexMetadata?.() ?? [];

    expect(indexes).toHaveLength(2);
    expect(indexes.map((index) => index.name)).toEqual(["by-id", "by-done"]);
  });
});

describe("mutation logging", () => {
  it("appends an outbox entry for an insert", async () => {
    const db = await createDb();

    await db.collections.todos.insert(makeTodo("t1", "Buy milk")).isPersisted.promise;

    const rows = outboxRows(db);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      collectionId: "todos",
      type: "insert",
      key: "t1",
      sync: false,
      syncStatus: "pending",
      localSeq: 0,
    });
    expect(rows[0]?.payload).toEqual({ id: "t1", title: "Buy milk", done: false });
  });

  it("increments localSeq across successive mutations", async () => {
    const db = await createDb();

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    await db.collections.todos.insert(makeTodo("t2")).isPersisted.promise;

    const seqs = outboxRows(db)
      .map((row) => row.localSeq)
      .sort((a, b) => a - b);
    expect(seqs).toEqual([0, 1]);
  });

  it("logs the modified payload for an update", async () => {
    const db = await createDb();

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    await db.collections.todos.update("t1", (draft) => {
      draft.done = true;
    }).isPersisted.promise;

    const updateRow = outboxRows(db).find((row) => row.type === "update");
    expect(updateRow?.payload).toEqual({ id: "t1", title: "Task", done: true });
  });

  it("logs the original payload for a delete", async () => {
    const db = await createDb();

    await db.collections.todos.insert(makeTodo("t1", "Keep")).isPersisted.promise;
    await db.collections.todos.delete("t1").isPersisted.promise;

    const deleteRow = outboxRows(db).find((row) => row.type === "delete");
    expect(deleteRow?.payload).toEqual({ id: "t1", title: "Keep", done: false });
  });

  it("rejects collections that use reserved ids", async () => {
    await expect(
      openEventSourcedDb({
        collections: {
          outbox: { getKey: (item: { id: string }) => item.id },
        },
      }),
    ).rejects.toThrow(/reserved/i);
  });
});

describe("offline behavior", () => {
  it("treats sync as a no-op when no transport is configured", async () => {
    const db = await createDb();

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    const result = await db.sync();

    expect(result).toEqual({
      pushed: 0,
      pulled: 0,
      skipped: 0,
      deadLettered: 0,
      deferred: false,
      errors: [],
    });
    expect(outboxRows(db)[0]?.sync).toBe(false);
  });
});

describe("push", () => {
  it("marks confirmed outbox rows as synced with their globalSeq", async () => {
    const db = await createDb({
      sync: {
        push: async (events) => ({
          confirmed: events.map((event) => ({ eventId: event.eventId, globalSeq: 100 })),
        }),
        pull: noPull,
      },
    });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    const result = await db.sync();

    expect(result.pushed).toBe(1);
    expect(outboxRows(db)[0]).toMatchObject({
      sync: true,
      syncStatus: "synced",
      globalSeq: 100,
    });
  });

  it("dead-letters permanently rejected events with their diagnostics", async () => {
    const db = await createDb({
      sync: {
        push: async (events) => ({
          confirmed: [],
          failed: events.map((event) => ({
            eventId: event.eventId,
            message: "Validation failed",
            code: "VALIDATION_ERROR",
            retryable: false,
          })),
        }),
        pull: noPull,
      },
    });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    const result = await db.sync();

    expect(result.deadLettered).toBe(1);
    expect(outboxRows(db)).toHaveLength(0);
    expect(deadLetterRows(db)[0]).toMatchObject({
      collectionId: "todos",
      key: "t1",
      reason: "rejected",
      message: "Validation failed",
      code: "VALIDATION_ERROR",
    });
  });

  it("does not resend dead-lettered or already-synced rows", async () => {
    const batches: number[] = [];
    const db = await createDb({
      sync: {
        push: async (events) => {
          batches.push(events.length);
          return {
            confirmed: [],
            failed: events.map((event) => ({ eventId: event.eventId, message: "nope" })),
          };
        },
        pull: noPull,
      },
    });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    await db.sync();
    await db.sync();

    expect(batches).toEqual([1]);
  });

  it("splits large pushes into batches and keeps earlier progress on failure", async () => {
    const batches: number[] = [];
    let call = 0;

    const db = await createDb({
      pushBatchSize: 2,
      sync: {
        push: async (events) => {
          call += 1;
          batches.push(events.length);
          if (call === 2) throw new Error("connection lost");
          return { confirmed: events.map((e) => ({ eventId: e.eventId, globalSeq: call })) };
        },
        pull: noPull,
      },
    });

    for (const id of ["t1", "t2", "t3", "t4", "t5"]) {
      await db.collections.todos.insert(makeTodo(id)).isPersisted.promise;
    }

    const result = await db.sync();

    expect(batches).toEqual([2, 2]);
    expect(result.pushed).toBe(2);
    expect(result.errors[0]?.message).toBe("connection lost");
    expect(outboxRows(db).filter((row) => row.sync)).toHaveLength(2);
  });
});

describe("push retry", () => {
  it("retries a retryable failure once its backoff window has elapsed", async () => {
    const attempts: number[] = [];
    let call = 0;

    const db = await createDb({
      retry: { baseDelayMs: 5, maxAttempts: 5 },
      sync: {
        push: async (events) => {
          call += 1;
          attempts.push(events.length);
          if (call === 1) {
            return {
              confirmed: [],
              failed: events.map((e) => ({
                eventId: e.eventId,
                message: "server busy",
                retryable: true,
              })),
            };
          }
          return { confirmed: events.map((e) => ({ eventId: e.eventId, globalSeq: 1 })) };
        },
        pull: noPull,
      },
    });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;

    const first = await db.sync();
    expect(first.pushed).toBe(0);
    expect(outboxRows(db)[0]).toMatchObject({
      syncStatus: "failed",
      retryable: true,
      attemptCount: 1,
    });

    await sleep(10);

    const second = await db.sync();
    expect(second.pushed).toBe(1);
    expect(attempts).toEqual([1, 1]);
  });

  it("holds a retryable failure back until its backoff window opens", async () => {
    let call = 0;

    const db = await createDb({
      retry: { baseDelayMs: 60_000 },
      sync: {
        push: async (events) => {
          call += 1;
          return {
            confirmed: [],
            failed: events.map((e) => ({
              eventId: e.eventId,
              message: "server busy",
              retryable: true,
            })),
          };
        },
        pull: noPull,
      },
    });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    await db.sync();
    await db.sync();

    expect(call).toBe(1);
  });

  it("dead-letters an event that exhausts its retry budget", async () => {
    const db = await createDb({
      retry: { baseDelayMs: 0, maxAttempts: 3 },
      sync: {
        push: async (events) => ({
          confirmed: [],
          failed: events.map((e) => ({
            eventId: e.eventId,
            message: "still busy",
            retryable: true,
          })),
        }),
        pull: noPull,
      },
    });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;

    await db.sync();
    await db.sync();
    const third = await db.sync();

    expect(third.deadLettered).toBe(1);
    expect(outboxRows(db)).toHaveLength(0);
    expect(deadLetterRows(db)[0]).toMatchObject({
      reason: "maxAttemptsExceeded",
      attemptCount: 3,
    });
  });
});

describe("dead-letter queue", () => {
  it("requeues dead-lettered events with a fresh retry budget", async () => {
    let reject = true;

    const db = await createDb({
      sync: {
        push: async (events) => {
          if (reject) {
            return {
              confirmed: [],
              failed: events.map((e) => ({
                eventId: e.eventId,
                message: "schema mismatch",
                retryable: false,
              })),
            };
          }
          return { confirmed: events.map((e) => ({ eventId: e.eventId, globalSeq: 7 })) };
        },
        pull: noPull,
      },
    });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    await db.sync();
    expect(deadLetterRows(db)).toHaveLength(1);

    reject = false;
    expect(await db.retryDeadLetter()).toBe(1);
    expect(deadLetterRows(db)).toHaveLength(0);
    // Outbound retry pushes upstream immediately — it does not requeue the outbox.
    expect(outboxRows(db)).toHaveLength(0);
  });

  it("discards dead-lettered events permanently", async () => {
    const db = await createDb({
      sync: {
        push: async (events) => ({
          confirmed: [],
          failed: events.map((e) => ({ eventId: e.eventId, message: "nope", retryable: false })),
        }),
        pull: noPull,
      },
    });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    await db.sync();

    expect(await db.discardDeadLetter()).toBe(1);
    expect(deadLetterRows(db)).toHaveLength(0);
    expect(outboxRows(db)).toHaveLength(0);
  });

  it("tags conflict rejections so they can be surfaced separately", async () => {
    const db = await createDb({
      sync: {
        push: async (events) => ({
          confirmed: [],
          failed: events.map((e) => ({
            eventId: e.eventId,
            message: "row moved on",
            code: "CONFLICT",
            retryable: false,
          })),
        }),
        pull: noPull,
      },
    });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    await db.sync();

    expect(deadLetterRows(db)[0]?.reason).toBe("conflict");
    expect(deadLetterRows(db)[0]?.baseVersion).toBeNull();
  });

  it("restores rowversions after a CONFLICT dead-letter", async () => {
    const db = await createDb({
      conflictDetection: true,
      sync: {
        push: async (events) => ({
          confirmed: [],
          failed: events.map((e) => ({
            eventId: e.eventId,
            message: "row moved on",
            code: "CONFLICT",
            retryable: false,
          })),
        }),
        pull: noPull,
      },
    });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    const insertId = outboxRows(db)[0]!.eventId;

    await db.collections.todos.update("t1", (draft) => {
      draft.done = true;
    }).isPersisted.promise;

    const updateId = outboxRows(db).find((row) => row.type === "update")!.eventId;
    expect(db.collections.rowversions.get("todos::t1")?.version).toBe(updateId);

    await db.sync();

    expect(deadLetterRows(db)).toHaveLength(2);
    // Rejected update must not leave its eventId as the optimistic head.
    expect(db.collections.rowversions.get("todos::t1")?.version).toBe(insertId);
  });
});

describe("preload", () => {
  it("hydrates user collections from persistence before ready", async () => {
    const { sqlite, filePath } = openTempSqlite();
    const first = await openTodoDbOnSqlite(sqlite);
    await first.collections.todos.insert(makeTodo("t1", "already-there")).isPersisted.promise;
    first.dispose();
    sqlite.close();

    const db = await openTodoDbOnSqlite(new Database(filePath));

    expect(db.collections.todos.get("t1")).toMatchObject({
      id: "t1",
      title: "already-there",
    });
  });
});

describe("pull", () => {
  it("inserts an inbox row, replays into the target collection, and marks it synced", async () => {
    let pulled = false;
    const db = await createDb({
      sync: {
        push: noPush,
        pull: async () => {
          if (pulled) return { events: [], cursor: "1", hasMore: false };
          pulled = true;
          return { events: [makeServerEvent(1, "t1")], cursor: "1", hasMore: false };
        },
      },
    });

    const result = await db.sync();

    expect(result.pulled).toBe(1);
    expect(db.collections.todos.get("t1")).toMatchObject({
      id: "t1",
      title: "FromServer",
      done: false,
    });
    expect(inboxRows(db).find((row) => row.eventId === "s1")?.sync).toBe(true);
  });

  it("skips events that originated locally without re-applying them", async () => {
    let echoEventId: string | null = null;
    let echoed = false;
    const db = await createDb({
      sync: {
        push: async (events) => ({
          confirmed: events.map((event) => ({ eventId: event.eventId, globalSeq: 1 })),
        }),
        pull: async () => {
          if (echoed || !echoEventId) return { events: [], cursor: "1", hasMore: false };
          echoed = true;
          return {
            events: [makeServerEvent(1, "t1", echoEventId)],
            cursor: "1",
            hasMore: false,
          };
        },
      },
    });

    await db.collections.todos.insert(makeTodo("t1", "Local")).isPersisted.promise;
    echoEventId = outboxRows(db)[0]!.eventId;

    const result = await db.sync();

    expect(result.pulled).toBe(0);
    expect(inboxRows(db).find((row) => row.eventId === echoEventId)?.sync).toBe(true);
    expect(db.collections.todos.get("t1")?.title).toBe("Local");
  });

  it("advances the pull cursor to the highest synced globalSeq", async () => {
    const sinceCalls: number[] = [];
    const db = await createDb({
      sync: {
        push: noPush,
        pull: async (since) => {
          sinceCalls.push(since);
          if (since < 5) {
            return { events: [makeServerEvent(5, "t5")], cursor: "5", hasMore: false };
          }
          return { events: [], cursor: "5", hasMore: false };
        },
      },
    });

    await db.sync();
    await db.sync();

    expect(sinceCalls).toEqual([0, 5]);
  });

  it("follows pagination until hasMore is false", async () => {
    let call = 0;
    const db = await createDb({
      sync: {
        push: noPush,
        pull: async () => {
          call += 1;
          if (call === 1) return { events: [makeServerEvent(1, "a")], cursor: "1", hasMore: true };
          if (call === 2) return { events: [makeServerEvent(2, "b")], cursor: "2", hasMore: false };
          return { events: [], cursor: "2", hasMore: false };
        },
      },
    });

    const result = await db.sync();

    expect(result.pulled).toBe(2);
    expect(call).toBe(2);
    expect(db.collections.todos.get("a")).toBeDefined();
    expect(db.collections.todos.get("b")).toBeDefined();
  });
});

describe("pull resilience", () => {
  it("stops paginating when a page yields no cursor progress", async () => {
    let calls = 0;
    const db = await createDb({
      sync: {
        push: noPush,
        // Reports hasMore forever while replaying the same already-applied page.
        pull: async () => {
          calls += 1;
          return { events: [makeServerEvent(1, "t1")], cursor: "1", hasMore: true };
        },
      },
    });

    const result = await db.sync();

    expect(result.pulled).toBe(1);
    expect(calls).toBe(2);
  });

  it("records an unknown collection as skipped and keeps draining later events", async () => {
    let call = 0;
    const db = await createDb({
      sync: {
        push: noPush,
        pull: async () => {
          call += 1;
          if (call > 1) return { events: [], cursor: "2", hasMore: false };
          return {
            events: [
              makeServerEvent(1, "x1", "s1", { collectionId: "unknown_collection" }),
              makeServerEvent(2, "t2"),
            ],
            cursor: "2",
            hasMore: false,
          };
        },
      },
    });

    const result = await db.sync();

    expect(result.skipped).toBe(1);
    expect(result.pulled).toBe(1);
    expect(db.collections.todos.get("t2")).toBeDefined();

    const skippedRow = inboxRows(db).find((row) => row.eventId === "s1");
    expect(skippedRow).toMatchObject({ sync: true, skipped: true });
    expect(skippedRow?.skipReason).toMatch(/unknown collection/i);
  });

  it("does not advance past an unknown collection when configured to fail", async () => {
    const sinceCalls: number[] = [];
    const db = await createDb({
      unknownEventHandling: "fail",
      sync: {
        push: noPush,
        pull: async (since) => {
          sinceCalls.push(since);
          return {
            events: [makeServerEvent(1, "x1", "s1", { collectionId: "unknown_collection" })],
            cursor: "1",
            hasMore: false,
          };
        },
      },
    });

    const result = await db.sync();

    expect(result).toMatchObject({ pulled: 0, skipped: 0 });
    expect(inboxRows(db).find((row) => row.eventId === "s1")?.sync).toBe(false);

    await db.sync();
    expect(sinceCalls).toEqual([0, 0]);
  });

  it("rewinds the cursor by pullOverlap to recover events that committed late", async () => {
    const sinceCalls: number[] = [];
    const db = await createDb({
      pullOverlap: 5,
      sync: {
        push: noPush,
        pull: async (since) => {
          sinceCalls.push(since);
          if (sinceCalls.length === 1) {
            return { events: [makeServerEvent(10, "t10")], cursor: "10", hasMore: false };
          }
          return { events: [], cursor: "10", hasMore: false };
        },
      },
    });

    await db.sync();
    await db.sync();

    expect(sinceCalls).toEqual([0, 5]);
  });

  it("treats events from this client as local origin even when the outbox is empty", async () => {
    const db = await createDb({
      clientId: "device-a",
      sync: {
        push: noPush,
        pull: async (since) => {
          if (since > 0) return { events: [], cursor: "1", hasMore: false };
          return {
            events: [makeServerEvent(1, "t1", "pruned-event", { clientId: "device-a" })],
            cursor: "1",
            hasMore: false,
          };
        },
      },
    });

    const result = await db.sync();

    expect(result.pulled).toBe(0);
    expect(db.collections.todos.get("t1")).toBeUndefined();
    expect(inboxRows(db).find((row) => row.eventId === "pruned-event")?.sync).toBe(true);
  });

  it("omits inbox rows for local-origin pulls when recordLocalEchoes is false", async () => {
    const db = await createDb({
      clientId: "device-a",
      recordLocalEchoes: false,
      sync: {
        push: noPush,
        pull: async (since) => {
          if (since > 0) return { events: [], cursor: "1", hasMore: false };
          return {
            events: [makeServerEvent(1, "t1", "echo-event", { clientId: "device-a" })],
            cursor: "1",
            hasMore: false,
          };
        },
      },
    });

    const result = await db.sync();

    expect(result.pulled).toBe(0);
    expect(db.collections.todos.get("t1")).toBeUndefined();
    expect(inboxRows(db).find((row) => row.eventId === "echo-event")).toBeUndefined();
    expect(db.getSyncStatus().pullCursor).toBe(1);
  });
});

describe("concurrent sync", () => {
  it("serializes overlapping sync calls so pending events are pushed once", async () => {
    const pushedIds: string[] = [];
    const db = await createDb({
      sync: {
        push: async (events) => {
          for (const event of events) pushedIds.push(event.eventId);
          await new Promise((resolve) => setTimeout(resolve, 10));
          return { confirmed: events.map((e) => ({ eventId: e.eventId, globalSeq: 1 })) };
        },
        pull: noPull,
      },
    });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    const eventId = outboxRows(db)[0]!.eventId;

    await Promise.all([db.sync(), db.sync(), db.sync()]);

    expect(pushedIds).toEqual([eventId]);
  });
});

describe("event metadata", () => {
  it("stamps clientId and a shared txId on logged mutations", async () => {
    const db = await createDb({ clientId: "device-a" });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;

    const row = outboxRows(db)[0]!;
    expect(row.clientId).toBe("device-a");
    expect(row.txId).toEqual(expect.any(String));
  });

  it("records the pre-mutation state so updates and deletes are invertible", async () => {
    const db = await createDb();

    await db.collections.todos.insert(makeTodo("t1", "Before")).isPersisted.promise;
    await db.collections.todos.update("t1", (draft) => {
      draft.title = "After";
    }).isPersisted.promise;

    const insertRow = outboxRows(db).find((row) => row.type === "insert");
    const updateRow = outboxRows(db).find((row) => row.type === "update");

    expect(insertRow?.previous).toBeNull();
    expect(updateRow?.previous).toEqual({ id: "t1", title: "Before", done: false });
    expect(updateRow?.payload).toEqual({ id: "t1", title: "After", done: false });
  });
});

describe("startup replay", () => {
  it("applies pending inbox events when the database initializes", async () => {
    const { sqlite, filePath } = openTempSqlite();
    const first = await openTodoDbOnSqlite(sqlite);
    await first.collections.inbox.insert({
      eventId: "s1",
      globalSeq: 1,
      collectionId: "todos",
      type: "insert",
      key: "t1",
      payload: { id: "t1", title: "Persisted", done: false },
      previous: null,
      clientId: null,
      schemaVersion: 1,
      timestamp: 0,
      sync: false,
      skipped: false,
      skipReason: null,
    }).isPersisted.promise;
    first.dispose();
    sqlite.close();

    const db = await openTodoDbOnSqlite(new Database(filePath));

    expect(db.collections.todos.get("t1")).toMatchObject({
      id: "t1",
      title: "Persisted",
      done: false,
    });
    expect(inboxRows(db).find((row) => row.eventId === "s1")?.sync).toBe(true);
  });
});

describe("manualSync", () => {
  it("returns push, pull, and replay counts", async () => {
    let pulled = false;
    const db = await createDb({
      sync: {
        push: noPush,
        pull: async () => {
          if (pulled) return { events: [], cursor: "1", hasMore: false };
          pulled = true;
          return { events: [makeServerEvent(1, "t1")], cursor: "1", hasMore: false };
        },
      },
    });

    const result = await db.manualSync();

    expect(result).toMatchObject({ pushed: 0, pulled: 1, replayed: 0 });
    expect(result.errors).toEqual([]);
  });
});

describe("backend identity", () => {
  it("records the backend identity on first pull", async () => {
    const db = await createDb({
      sync: {
        push: noPush,
        pull: async () => ({ events: [], cursor: "0", hasMore: false, backendId: "backend-1" }),
      },
    });

    await db.sync();

    expect(db.getSyncStatus().backendId).toBe("backend-1");
  });

  it("resets the cursor when the backend is replaced", async () => {
    let backendId = "backend-1";
    const sinceCalls: number[] = [];

    const db = await createDb({
      sync: {
        push: noPush,
        pull: async (since) => {
          sinceCalls.push(since);
          if (backendId === "backend-1") {
            return since >= 5
              ? { events: [], cursor: "5", hasMore: false, backendId }
              : { events: [makeServerEvent(5, "t5")], cursor: "5", hasMore: false, backendId };
          }
          return since >= 1
            ? { events: [], cursor: "1", hasMore: false, backendId }
            : {
                events: [makeServerEvent(1, "fresh", "fresh-1")],
                cursor: "1",
                hasMore: false,
                backendId,
              };
        },
      },
    });

    await db.sync();
    expect(db.getSyncStatus().pullCursor).toBe(5);

    backendId = "backend-2";
    const result = await db.sync();

    // Rewound to 0 and re-pulled against the new backend.
    expect(sinceCalls).toEqual([0, 5, 0]);
    expect(result.pulled).toBe(1);
    expect(db.collections.todos.get("fresh")).toBeDefined();
    expect(db.getSyncStatus().backendId).toBe("backend-2");
  });

  it("surfaces an error instead of resetting when configured to fail", async () => {
    let backendId = "backend-1";

    const db = await createDb({
      backendMismatch: "fail",
      sync: {
        push: noPush,
        pull: async () => ({ events: [], cursor: "0", hasMore: false, backendId }),
      },
    });

    await db.sync();
    backendId = "backend-2";
    const result = await db.sync();

    expect(result.errors[0]?.name).toBe("BackendMismatchError");
    expect(db.getSyncStatus().backendId).toBe("backend-1");
  });
});

describe("pruning", () => {
  it("removes synced rows while preserving the pull cursor", async () => {
    const db = await createDb({
      sync: {
        push: async (events) => ({
          confirmed: events.map((e) => ({ eventId: e.eventId, globalSeq: 3 })),
        }),
        pull: async (since) =>
          since >= 9
            ? { events: [], cursor: "9", hasMore: false }
            : { events: [makeServerEvent(9, "remote")], cursor: "9", hasMore: false },
      },
    });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    await db.sync();

    expect(outboxRows(db)).toHaveLength(1);
    expect(inboxRows(db)).toHaveLength(1);

    const pruned = await db.pruneSyncedEvents();

    expect(pruned).toEqual({ outbox: 1, inbox: 1 });
    expect(outboxRows(db)).toHaveLength(0);
    expect(inboxRows(db)).toHaveLength(0);
    // The cursor survives because it lives in syncmeta, not in the inbox.
    expect(db.getSyncStatus().pullCursor).toBe(9);
    // Replayed state is untouched by pruning the log.
    expect(db.collections.todos.get("remote")).toBeDefined();
  });

  it("keeps unsynced rows and honours keepLast", async () => {
    const db = await createDb({
      sync: {
        push: async (events) => ({
          confirmed: events.map((e) => ({ eventId: e.eventId, globalSeq: 1 })),
        }),
        pull: noPull,
      },
    });

    for (const id of ["t1", "t2", "t3"]) {
      await db.collections.todos.insert(makeTodo(id)).isPersisted.promise;
    }
    await db.sync();
    await db.collections.todos.insert(makeTodo("t4")).isPersisted.promise;

    const pruned = await db.pruneSyncedEvents({ keepLast: 1 });

    expect(pruned.outbox).toBe(2);
    // The retained synced row plus the still-pending one.
    expect(outboxRows(db)).toHaveLength(2);
    expect(outboxRows(db).filter((row) => !row.sync)).toHaveLength(1);
  });
});

describe("sync status", () => {
  it("reports pending work and clears once pushed", async () => {
    const db = await createDb({
      sync: {
        push: async (events) => ({
          confirmed: events.map((e) => ({ eventId: e.eventId, globalSeq: 1 })),
        }),
        pull: noPull,
      },
    });

    expect(db.getSyncStatus()).toMatchObject({ isSynced: true, pendingCount: 0 });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    expect(db.getSyncStatus()).toMatchObject({ isSynced: false, pendingCount: 1 });

    await db.sync();
    expect(db.getSyncStatus()).toMatchObject({
      isSynced: true,
      pendingCount: 0,
      isSyncing: false,
    });
    expect(db.getSyncStatus().lastSyncAt).toEqual(expect.any(Number));
  });

  it("notifies subscribers and stops after unsubscribe", async () => {
    const db = await createDb();
    const seen: number[] = [];

    const unsubscribe = db.subscribeSyncStatus((status) => seen.push(status.pendingCount));

    expect(seen).toEqual([0]);

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    expect(seen.at(-1)).toBe(1);

    unsubscribe();
    const countAfterUnsubscribe = seen.length;
    await db.collections.todos.insert(makeTodo("t2")).isPersisted.promise;

    expect(seen).toHaveLength(countAfterUnsubscribe);
  });

  it("records the last error", async () => {
    const db = await createDb({
      sync: {
        push: noPush,
        pull: async () => {
          throw new Error("pull exploded");
        },
      },
    });

    await db.sync();

    expect(db.getSyncStatus().lastError).toBe("pull exploded");
  });
});

describe("event schema versions", () => {
  it("stamps the configured version on authored events", async () => {
    const db = await createDb({ eventSchemaVersion: 3 });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;

    expect(outboxRows(db)[0]?.schemaVersion).toBe(3);
  });

  it("upcasts an older event before replaying it", async () => {
    const db = await createDb({
      eventSchemaVersion: 2,
      upcastEvent: (event) => ({
        ...event,
        payload: { ...event.payload, done: Boolean(event.payload.completed) },
        schemaVersion: 2,
      }),
      sync: {
        push: noPush,
        pull: async (since) =>
          since >= 1
            ? { events: [], cursor: "1", hasMore: false }
            : {
                events: [
                  makeServerEvent(1, "t1", "s1", {
                    schemaVersion: 1,
                    payload: { id: "t1", title: "Legacy", completed: 1 },
                  }),
                ],
                cursor: "1",
                hasMore: false,
              },
      },
    });

    const result = await db.sync();

    expect(result.pulled).toBe(1);
    expect(db.collections.todos.get("t1")).toMatchObject({ title: "Legacy", done: true });
  });

  it("refuses an event authored by a newer schema than it supports", async () => {
    const db = await createDb({
      eventSchemaVersion: 1,
      sync: {
        push: noPush,
        pull: async (since) =>
          since >= 1
            ? { events: [], cursor: "1", hasMore: false }
            : {
                events: [makeServerEvent(1, "t1", "s1", { schemaVersion: 9 })],
                cursor: "1",
                hasMore: false,
              },
      },
    });

    const result = await db.sync();

    expect(result).toMatchObject({ pulled: 0, skipped: 1 });
    expect(db.collections.todos.get("t1")).toBeUndefined();
    expect(inboxRows(db)[0]?.skipReason).toMatch(/newer than supported/i);
  });

  it("drops an event when the upcaster returns null", async () => {
    const db = await createDb({
      eventSchemaVersion: 2,
      upcastEvent: () => null,
      sync: {
        push: noPush,
        pull: async (since) =>
          since >= 1
            ? { events: [], cursor: "1", hasMore: false }
            : {
                events: [makeServerEvent(1, "t1", "s1", { schemaVersion: 1 })],
                cursor: "1",
                hasMore: false,
              },
      },
    });

    const result = await db.sync();

    expect(result.skipped).toBe(1);
    expect(db.collections.todos.get("t1")).toBeUndefined();
  });
});

describe("conflict detection", () => {
  it("stamps baseVersion from the previously applied event", async () => {
    const db = await createDb({ conflictDetection: true });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    const insertId = outboxRows(db)[0]!.eventId;

    await db.collections.todos.update("t1", (draft) => {
      draft.done = true;
    }).isPersisted.promise;

    const updateRow = outboxRows(db).find((row) => row.type === "update");

    expect(outboxRows(db).find((row) => row.type === "insert")?.baseVersion).toBeNull();
    expect(updateRow?.baseVersion).toBe(insertId);
  });

  it("leaves baseVersion null when disabled", async () => {
    const db = await createDb();

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    await db.collections.todos.update("t1", (draft) => {
      draft.done = true;
    }).isPersisted.promise;

    expect(outboxRows(db).every((row) => row.baseVersion === null)).toBe(true);
  });

  it("bases a local edit on the last remote event once one has been applied", async () => {
    const db = await createDb({
      conflictDetection: true,
      sync: {
        push: noPush,
        pull: async (since) =>
          since >= 1
            ? { events: [], cursor: "1", hasMore: false }
            : { events: [makeServerEvent(1, "t1", "remote-1")], cursor: "1", hasMore: false },
      },
    });

    await db.sync();
    await db.collections.todos.update("t1", (draft) => {
      draft.done = true;
    }).isPersisted.promise;

    expect(outboxRows(db)[0]?.baseVersion).toBe("remote-1");
  });
});

describe("leader election", () => {
  it("defers when the lock is held by another context", async () => {
    let pushCalls = 0;
    const lock: SyncLock = {
      tryRun: async () => ({ acquired: false }),
    };

    const db = await createDb({
      lock,
      sync: {
        push: async () => {
          pushCalls += 1;
          return { confirmed: [] };
        },
        pull: noPull,
      },
    });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    const result = await db.sync();

    expect(result.deferred).toBe(true);
    expect(pushCalls).toBe(0);
    expect(outboxRows(db)[0]?.sync).toBe(false);
  });

  it("runs normally when the lock is acquired", async () => {
    const names: string[] = [];
    const lock: SyncLock = {
      tryRun: async (name, fn) => {
        names.push(name);
        return { acquired: true, result: await fn() };
      },
    };

    const db = await createDb({
      lock,
      sync: {
        push: async (events) => ({
          confirmed: events.map((e) => ({ eventId: e.eventId, globalSeq: 1 })),
        }),
        pull: noPull,
      },
    });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    const result = await db.sync();

    expect(result).toMatchObject({ pushed: 1, deferred: false });
    expect(names).toEqual(["event-sourced-sync:default"]);
  });
});

describe("lifecycle hooks", () => {
  it("reports the local mutation, push, and sync lifecycle in order", async () => {
    const calls: string[] = [];

    const db = await createDb({
      hooks: {
        onReady: () => calls.push("ready"),
        onMutation: (entry) => calls.push(`mutation:${entry.type}`),
        onSyncStart: ({ trigger }) => calls.push(`start:${trigger}`),
        onEventPushed: ({ globalSeq }) => calls.push(`pushed:${globalSeq}`),
        onSyncComplete: ({ result }) => calls.push(`complete:${result.pushed}`),
      },
      sync: {
        push: async (events) => ({
          confirmed: events.map((e) => ({ eventId: e.eventId, globalSeq: 11 })),
        }),
        pull: noPull,
      },
    });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    await db.sync();

    expect(calls).toEqual(["ready", "mutation:insert", "start:sync", "pushed:11", "complete:1"]);
  });

  it("reports applied and skipped remote events", async () => {
    const applied: string[] = [];
    const skipped: string[] = [];

    const db = await createDb({
      hooks: {
        onEventApplied: ({ eventId }) => applied.push(eventId),
        onEventSkipped: ({ eventId, reason }) => skipped.push(`${eventId}:${reason}`),
      },
      sync: {
        push: noPush,
        pull: async (since) =>
          since >= 2
            ? { events: [], cursor: "2", hasMore: false }
            : {
                events: [
                  makeServerEvent(1, "x", "s1", { collectionId: "nope" }),
                  makeServerEvent(2, "t2", "s2"),
                ],
                cursor: "2",
                hasMore: false,
              },
      },
    });

    await db.sync();

    expect(applied).toEqual(["s2"]);
    expect(skipped[0]).toMatch(/^s1:unknown collection/);
  });

  it("reports dead-letters, sync errors, and backend mismatches", async () => {
    const events: string[] = [];
    let backendId = "one";

    const db = await createDb({
      hooks: {
        onDeadLetter: (entry) => events.push(`dead:${entry.reason}`),
        onSyncError: ({ phase, error }) => events.push(`error:${phase}:${error.message}`),
        onBackendMismatch: ({ policy }) => events.push(`mismatch:${policy}`),
      },
      sync: {
        push: async (pushed) => ({
          confirmed: [],
          failed: pushed.map((e) => ({ eventId: e.eventId, message: "no", retryable: false })),
        }),
        pull: async () => ({ events: [], cursor: "0", hasMore: false, backendId }),
      },
    });

    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    await db.sync();

    backendId = "two";
    await db.collections.todos.insert(makeTodo("t2")).isPersisted.promise;
    await db.sync();

    expect(events).toEqual(["dead:rejected", "dead:rejected", "mismatch:resetCursor"]);

    const failing = await createDb({
      hooks: { onSyncError: ({ phase, error }) => events.push(`error:${phase}:${error.message}`) },
      sync: {
        push: noPush,
        pull: async () => {
          throw new Error("boom");
        },
      },
    });

    await failing.sync();

    expect(events.at(-1)).toBe("error:pull:boom");
  });

  it("swallows a throwing hook so it cannot break sync", async () => {
    const db = await createDb({
      hooks: {
        onMutation: () => {
          throw new Error("hook blew up");
        },
      },
      sync: {
        push: async (events) => ({
          confirmed: events.map((e) => ({ eventId: e.eventId, globalSeq: 1 })),
        }),
        pull: noPull,
      },
    });

    await expect(
      db.collections.todos.insert(makeTodo("t1")).isPersisted.promise,
    ).resolves.not.toThrow();

    const result = await db.sync();
    expect(result.pushed).toBe(1);
  });
});

describe("transaction batching", () => {
  it("never splits events that share a txId across push batches", async () => {
    const batches: Array<Array<string>> = [];

    const db = await createDb({
      pushBatchSize: 2,
      sync: {
        push: async (events) => {
          batches.push(events.map((e) => e.txId));
          return { confirmed: events.map((e) => ({ eventId: e.eventId, globalSeq: 1 })) };
        },
        pull: noPull,
      },
    });

    // A single collection call produces one txId; three calls produce three.
    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    await db.collections.todos.insert(makeTodo("t2")).isPersisted.promise;
    await db.collections.todos.insert(makeTodo("t3")).isPersisted.promise;

    await db.sync();

    for (const batch of batches) {
      expect(batch.length).toBeLessThanOrEqual(2);
    }

    // Every txId appears in exactly one batch.
    const seen = new Map<string, number>();
    for (const batch of batches) {
      for (const txId of new Set(batch)) {
        seen.set(txId, (seen.get(txId) ?? 0) + 1);
      }
    }
    expect([...seen.values()].every((count) => count === 1)).toBe(true);
  });
});

describe("dispose", () => {
  it("can be called without throwing", async () => {
    const db = await createDb();
    expect(() => db.dispose()).not.toThrow();
  });

  it("stops notifying status subscribers", async () => {
    const db = await createDb();
    const seen: number[] = [];

    db.subscribeSyncStatus((status) => seen.push(status.pendingCount));
    await db.collections.todos.insert(makeTodo("t1")).isPersisted.promise;
    expect(seen.length).toBeGreaterThan(1);

    db.dispose();
    const countAtDispose = seen.length;

    await db.collections.todos.insert(makeTodo("t2")).isPersisted.promise;

    expect(seen).toHaveLength(countAtDispose);
  });
});
