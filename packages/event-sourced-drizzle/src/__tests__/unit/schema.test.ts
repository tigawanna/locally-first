import { getTableColumns, getTableName } from "drizzle-orm";
import { integer, text } from "drizzle-orm/sqlite-core";
import { describe, expect, it } from "vitest";

import {
  defineDeadLetterTable as definePgDeadLetterTable,
  defineInboxTable as definePgInboxTable,
  defineOutboxTable as definePgOutboxTable,
  defineSyncMetaTable as definePgSyncMetaTable,
  pgDeadLetterColumns,
  pgInboxRequiredColumns,
  pgOutboxRequiredColumns,
  pgSyncMetaColumns,
} from "../../schema/pg";
import {
  defineDeadLetterTable,
  defineInboxTable,
  defineOutboxTable,
  defineSyncMetaTable,
  sqliteDeadLetterColumns,
  sqliteInboxRequiredColumns,
  sqliteOutboxRequiredColumns,
  sqliteSyncMetaColumns,
} from "../../schema/sqlite";

type Table = Parameters<typeof getTableName>[0];

function sqlNameOf(builder: unknown): string {
  const name = (builder as { config?: { name?: string } }).config?.name;
  if (!name) throw new Error("expected a Drizzle column builder with config.name");
  return name;
}

function expectTableMatchesColumns(table: Table, name: string, columns: Record<string, unknown>) {
  expect(getTableName(table)).toBe(name);

  const actual = getTableColumns(table);
  expect(Object.keys(actual).sort()).toEqual(Object.keys(columns).sort());

  for (const [key, builder] of Object.entries(columns)) {
    const col = actual[key];
    if (!col) throw new Error(`missing column ${key}`);
    expect(col.name, `${key} SQL name`).toBe(sqlNameOf(builder));
  }
}

describe("sqlite schema builders", () => {
  it("spreads the required column maps onto default table names", () => {
    expectTableMatchesColumns(defineOutboxTable(), "sync_outbox", sqliteOutboxRequiredColumns);
    expectTableMatchesColumns(defineInboxTable(), "sync_inbox", sqliteInboxRequiredColumns);
    expectTableMatchesColumns(defineSyncMetaTable(), "sync_meta", sqliteSyncMetaColumns);
    expectTableMatchesColumns(defineDeadLetterTable(), "sync_dead_letter", sqliteDeadLetterColumns);
  });

  it("keeps sqlite-specific types for payload, booleans, and the primary key", () => {
    const outbox = getTableColumns(defineOutboxTable());
    expect(outbox.eventId.primary).toBe(true);
    expect(outbox.payload.columnType).toBe("SQLiteTextJson");
    expect(outbox.payload.dataType).toBe("json");
    expect(outbox.sync.columnType).toBe("SQLiteBoolean");
    expect(outbox.retryable.columnType).toBe("SQLiteBoolean");
    expect(outbox.retryable.notNull).toBe(false);
    expect(outbox.timestamp.columnType).toBe("SQLiteInteger");
  });

  it("uses custom table names and extra columns", () => {
    const outbox = defineOutboxTable("app_outbox", {
      deviceId: text("device_id"),
      priority: integer("priority").default(0),
    });
    const inbox = defineInboxTable("app_inbox", {
      receivedAt: integer("received_at"),
    });

    expect(getTableName(outbox)).toBe("app_outbox");
    expect(getTableName(inbox)).toBe("app_inbox");

    const outboxCols = getTableColumns(outbox);
    expect(Object.keys(outboxCols)).toEqual(
      expect.arrayContaining([...Object.keys(sqliteOutboxRequiredColumns), "deviceId", "priority"]),
    );
    expect(outboxCols.deviceId.name).toBe("device_id");
    expect(outboxCols.priority.name).toBe("priority");

    const inboxCols = getTableColumns(inbox);
    expect(inboxCols.receivedAt.name).toBe("received_at");
  });

  it("preserves extra columns in inferred select types", () => {
    const outbox = defineOutboxTable("sync_outbox", {
      deviceId: text("device_id"),
      priority: integer("priority").default(0),
    });
    const inbox = defineInboxTable("sync_inbox", {
      receivedAt: integer("received_at"),
    });

    type OutboxRow = typeof outbox.$inferSelect;
    type InboxRow = typeof inbox.$inferSelect;

    const sampleOutbox = {
      eventId: "e1",
      collectionId: "todos",
      type: "insert" as const,
      key: "t1",
      payload: {},
      timestamp: 1,
      localSeq: 1,
      globalSeq: null,
      sync: false,
      syncStatus: "pending" as const,
      attemptCount: 0,
      lastAttemptAt: null,
      lastError: null,
      lastErrorCode: null,
      retryable: null,
      deviceId: "dev-1",
      priority: 1,
    } satisfies OutboxRow;

    const sampleInbox = {
      eventId: "e2",
      globalSeq: 10,
      collectionId: "todos",
      type: "update" as const,
      key: "t1",
      payload: { title: "x" },
      timestamp: 2,
      sync: false,
      receivedAt: 3,
    } satisfies InboxRow;

    expect(sampleOutbox.deviceId).toBe("dev-1");
    expect(sampleInbox.receivedAt).toBe(3);
  });
});

describe("pg schema builders", () => {
  it("spreads the required column maps onto default table names", () => {
    expectTableMatchesColumns(definePgOutboxTable(), "sync_outbox", pgOutboxRequiredColumns);
    expectTableMatchesColumns(definePgInboxTable(), "sync_inbox", pgInboxRequiredColumns);
    expectTableMatchesColumns(definePgSyncMetaTable(), "sync_meta", pgSyncMetaColumns);
    expectTableMatchesColumns(definePgDeadLetterTable(), "sync_dead_letter", pgDeadLetterColumns);
  });

  it("keeps pg-specific types for payload, booleans, and timestamps", () => {
    const outbox = getTableColumns(definePgOutboxTable());
    expect(outbox.eventId.primary).toBe(true);
    expect(outbox.payload.columnType).toBe("PgJsonb");
    expect(outbox.payload.dataType).toBe("json");
    expect(outbox.sync.columnType).toBe("PgBoolean");
    expect(outbox.retryable.columnType).toBe("PgBoolean");
    expect(outbox.retryable.notNull).toBe(false);
    expect(outbox.timestamp.columnType).toBe("PgBigInt53");
    expect(outbox.globalSeq.columnType).toBe("PgBigInt53");
  });

  it("uses custom table names", () => {
    expect(getTableName(definePgOutboxTable("events_outbox"))).toBe("events_outbox");
    expect(getTableName(definePgInboxTable("events_inbox"))).toBe("events_inbox");
    expect(getTableName(definePgSyncMetaTable("events_meta"))).toBe("events_meta");
    expect(getTableName(definePgDeadLetterTable("events_dead_letter"))).toBe("events_dead_letter");
  });
});
