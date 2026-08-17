/**
 * Sync backend backed by a real SQLite event log (not an in-memory array).
 * Tests can `backend.rawQuery("SELECT * FROM sync_events")` to confirm writes.
 */
import Database from "better-sqlite3";
import { eq, gt } from "drizzle-orm";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";

import type {
  MutationType,
  OutboundEvent,
  PullResponse,
  PushConfirmation,
  PushFailure,
  PushResponse,
  ServerEvent,
  SyncTransport,
} from "../core/protocol";
import { CONFLICT_ERROR_CODE } from "../internal/constants";

export const eventLogEvents = sqliteTable("sync_events", {
  globalSeq: integer("global_seq").primaryKey(),
  eventId: text("event_id").notNull().unique(),
  collectionId: text("collection_id").notNull(),
  type: text("type").notNull(),
  key: text("key").notNull(),
  payload: text("payload", { mode: "json" }).$type<Record<string, unknown>>().notNull(),
  previous: text("previous", { mode: "json" }).$type<Record<string, unknown> | null>(),
  txId: text("tx_id"),
  clientId: text("client_id"),
  schemaVersion: integer("schema_version"),
  baseVersion: text("base_version"),
  timestamp: integer("timestamp").notNull(),
});

const EVENT_LOG_DDL = `
  CREATE TABLE IF NOT EXISTS sync_events (
    global_seq INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    collection_id TEXT NOT NULL,
    type TEXT NOT NULL,
    key TEXT NOT NULL,
    payload TEXT NOT NULL,
    previous TEXT,
    tx_id TEXT,
    client_id TEXT,
    schema_version INTEGER,
    base_version TEXT,
    timestamp INTEGER NOT NULL
  );
`;

export type SqliteEventLogBackendOptions = {
  backendId?: string;
  pageSize?: number;
  /** Reject stale writes when the client sends baseVersion. Defaults to true. */
  conflictDetection?: boolean;
};

export type SqliteEventLogBackend = SyncTransport & {
  readonly sqlite: InstanceType<typeof Database>;
  readonly backendId: string;
  rawQuery: <T>(sql: string) => T[];
  seed: (event: {
    collectionId: string;
    type?: MutationType;
    key: string | number;
    payload: Record<string, unknown>;
    eventId?: string;
    clientId?: string;
  }) => ServerEvent;
  close: () => void;
};

type EventLogRow = typeof eventLogEvents.$inferSelect;

function toServerEvent(row: EventLogRow, backendId: string): ServerEvent {
  return {
    globalSeq: row.globalSeq,
    eventId: row.eventId,
    collectionId: row.collectionId,
    type: row.type as MutationType,
    key: row.key,
    payload: row.payload,
    previous: row.previous,
    clientId: row.clientId ?? undefined,
    schemaVersion: row.schemaVersion ?? undefined,
    timestamp: row.timestamp,
    cursor: String(row.globalSeq),
    backendId,
  };
}

/**
 * Push/pull transport persisted in SQLite. Pass `sync: backend` into
 * `createEventSourcedDrizzle`.
 */
export function createSqliteEventLogBackend(
  options: SqliteEventLogBackendOptions = {},
): SqliteEventLogBackend {
  const sqlite = new Database(":memory:");
  sqlite.exec(EVENT_LOG_DDL);
  const db = drizzle(sqlite, { schema: { eventLogEvents } });
  const backendId = options.backendId ?? "event-log";
  const conflictDetection = options.conflictDetection ?? true;

  function nextSeq(): number {
    const row = sqlite
      .prepare("SELECT COALESCE(MAX(global_seq), 0) AS m FROM sync_events")
      .get() as {
      m: number;
    };
    return row.m + 1;
  }

  function findByEventId(eventId: string): EventLogRow | undefined {
    return db.select().from(eventLogEvents).where(eq(eventLogEvents.eventId, eventId)).get();
  }

  function headEventId(collectionId: string, key: string): string | null {
    const row = sqlite
      .prepare(
        "SELECT event_id FROM sync_events WHERE collection_id = ? AND key = ? ORDER BY global_seq DESC LIMIT 1",
      )
      .get(collectionId, key) as { event_id: string } | undefined;
    return row?.event_id ?? null;
  }

  function insertEvent(
    event: Omit<ServerEvent, "globalSeq" | "cursor" | "backendId"> & {
      baseVersion?: string | null;
    },
  ): ServerEvent {
    const existing = findByEventId(event.eventId);
    if (existing) return toServerEvent(existing, backendId);

    const globalSeq = nextSeq();
    db.insert(eventLogEvents)
      .values({
        globalSeq,
        eventId: event.eventId,
        collectionId: event.collectionId,
        type: event.type,
        key: String(event.key),
        payload: event.payload,
        previous: event.previous ?? null,
        txId: null,
        clientId: event.clientId ?? null,
        schemaVersion: event.schemaVersion ?? null,
        baseVersion: event.baseVersion ?? null,
        timestamp: event.timestamp,
      })
      .run();

    return toServerEvent(findByEventId(event.eventId)!, backendId);
  }

  return {
    sqlite,
    backendId,

    rawQuery: <T>(sql: string) => sqlite.prepare(sql).all() as T[],

    push: async (batch: ReadonlyArray<OutboundEvent>): Promise<PushResponse> => {
      const confirmed: PushConfirmation[] = [];
      const failed: PushFailure[] = [];

      for (const event of batch) {
        if (conflictDetection && event.baseVersion) {
          const head = headEventId(event.collectionId, String(event.key));
          if (head !== null && head !== event.baseVersion) {
            failed.push({
              eventId: event.eventId,
              message: `stale write: row is at ${head}, client sent ${event.baseVersion}`,
              code: CONFLICT_ERROR_CODE,
              retryable: false,
            });
            continue;
          }
        }

        const stored = insertEvent({
          eventId: event.eventId,
          collectionId: event.collectionId,
          type: event.type,
          key: event.key,
          payload: event.payload,
          previous: event.previous,
          clientId: event.clientId,
          schemaVersion: event.schemaVersion,
          timestamp: event.timestamp,
          baseVersion: event.baseVersion,
        });
        confirmed.push({ eventId: stored.eventId, globalSeq: stored.globalSeq });
      }

      return { confirmed, failed };
    },

    pull: async (since: number): Promise<PullResponse> => {
      const rows = db
        .select()
        .from(eventLogEvents)
        .where(gt(eventLogEvents.globalSeq, since))
        .orderBy(eventLogEvents.globalSeq)
        .all();

      const limit = options.pageSize ?? rows.length;
      const page = limit > 0 ? rows.slice(0, limit) : rows;
      const events = page.map((row) => toServerEvent(row, backendId));
      const cursor = events.length > 0 ? events[events.length - 1]!.cursor : String(since);

      return {
        events,
        cursor,
        hasMore: page.length < rows.length,
        backendId,
      };
    },

    seed: (event) =>
      insertEvent({
        eventId: event.eventId ?? `seed-${nextSeq()}`,
        collectionId: event.collectionId,
        type: event.type ?? "insert",
        key: event.key,
        payload: event.payload,
        previous: null,
        clientId: event.clientId ?? "remote-client",
        timestamp: Date.now(),
      }),

    close: () => sqlite.close(),
  };
}
