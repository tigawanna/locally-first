/**
 * Test database helpers: creates real SQLite and PGlite databases,
 * runs schema migrations, and returns a fully wired engine.
 *
 * No mocks — all tests run against real databases.
 */

import { PGlite } from "@electric-sql/pglite";
import Database from "better-sqlite3";
import { eq } from "drizzle-orm";
import { drizzle as drizzleSqlite } from "drizzle-orm/better-sqlite3";
import { drizzle as drizzlePg } from "drizzle-orm/pglite";
import { afterEach } from "vitest";

import { createSQLiteAdapter } from "../../adapters/sqlite";
import { createEventSourcedDrizzle } from "../../core/create-event-sourced-drizzle";
import type { DrizzleAdapter } from "../../internal/types";
import type { SyncTransport } from "../../core/protocol";
import type { EventSourcedDrizzle, EventSourcedDrizzleConfig } from "../../core/types";

import {
  pgDeadLetter,
  pgInbox,
  pgOutbox,
  pgSyncMeta,
  pgTodos,
  sqliteDeadLetter,
  sqliteInbox,
  sqliteOutbox,
  sqliteSyncMeta,
  sqliteTodos,
} from "./test-schema";

// --- Types ---

export type Todo = {
  id: string;
  title: string;
  done: boolean;
};

export type TodoCollections = {
  todos: { table: typeof sqliteTodos; getKey: (row: Todo) => string };
};

export type TestEngine = EventSourcedDrizzle<TodoCollections>;

export type TestContext = {
  engine: TestEngine;
  adapter: DrizzleAdapter;
  /** Raw DB access for assertions. */
  rawQuery: <T>(query: string) => T[];
  cleanup: () => Promise<void>;
  dbType: "sqlite" | "pglite";
};

// --- Cleanup tracking ---

type Closeable = { close: () => Promise<void> | void };
const openHandles: Closeable[] = [];

afterEach(async () => {
  while (openHandles.length > 0) {
    await openHandles.pop()?.close();
  }
});

// --- SQLite ---

const SQLITE_DDL = `
  CREATE TABLE IF NOT EXISTS sync_outbox (
    event_id TEXT PRIMARY KEY,
    collection_id TEXT NOT NULL,
    type TEXT NOT NULL,
    key TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    previous TEXT,
    tx_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    base_version TEXT,
    timestamp INTEGER NOT NULL,
    local_seq INTEGER NOT NULL,
    global_seq INTEGER,
    sync INTEGER NOT NULL DEFAULT 0,
    sync_status TEXT NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_attempt_at INTEGER,
    next_attempt_at INTEGER,
    last_error TEXT,
    last_error_code TEXT,
    retryable INTEGER
  );

  CREATE TABLE IF NOT EXISTS sync_inbox (
    event_id TEXT PRIMARY KEY,
    global_seq INTEGER NOT NULL,
    collection_id TEXT NOT NULL,
    type TEXT NOT NULL,
    key TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    previous TEXT,
    client_id TEXT,
    schema_version INTEGER NOT NULL DEFAULT 1,
    timestamp INTEGER NOT NULL,
    sync INTEGER NOT NULL DEFAULT 0,
    skipped INTEGER NOT NULL DEFAULT 0,
    skip_reason TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
  );

  CREATE TABLE IF NOT EXISTS sync_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS sync_dead_letter (
    event_id TEXT PRIMARY KEY,
    collection_id TEXT NOT NULL,
    type TEXT NOT NULL,
    key TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    previous TEXT,
    tx_id TEXT,
    client_id TEXT,
    schema_version INTEGER NOT NULL DEFAULT 1,
    timestamp INTEGER NOT NULL,
    local_seq INTEGER,
    global_seq INTEGER,
    direction TEXT NOT NULL,
    reason TEXT NOT NULL,
    message TEXT NOT NULL,
    code TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    failed_at INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS todos (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    done INTEGER NOT NULL DEFAULT 0
  );
`;

export async function createSqliteTestContext(
  options: Partial<
    Omit<EventSourcedDrizzleConfig<TodoCollections>, "adapter" | "collections">
  > = {},
): Promise<TestContext> {
  const sqlite = new Database(":memory:");
  sqlite.pragma("journal_mode = WAL");
  sqlite.exec(SQLITE_DDL);

  const db = drizzleSqlite(sqlite) as any;

  // better-sqlite3's transaction() rejects async callbacks. Since all SQLite operations
  // are synchronous in practice, we use raw BEGIN/COMMIT/ROLLBACK for the adapter.
  const originalTransaction = db.transaction;
  db.transaction = async <T>(fn: (tx: any) => Promise<T>): Promise<T> => {
    sqlite.exec("BEGIN");
    try {
      const result = await fn(db);
      sqlite.exec("COMMIT");
      return result;
    } catch (err) {
      sqlite.exec("ROLLBACK");
      throw err;
    }
  };

  const adapter = createSQLiteAdapter(db, {
    outbox: sqliteOutbox,
    inbox: sqliteInbox,
    syncMeta: sqliteSyncMeta,
    deadLetter: sqliteDeadLetter,
    collections: {
      todos: { table: sqliteTodos, keyColumn: sqliteTodos.id },
    },
  });

  const engine = await createEventSourcedDrizzle<TodoCollections>({
    adapter,
    collections: {
      todos: { table: sqliteTodos, getKey: (row: Todo) => row.id },
    },
    ...options,
  });

  const ctx: TestContext = {
    engine,
    adapter,
    rawQuery: <T>(query: string) => sqlite.prepare(query).all() as T[],
    cleanup: async () => {
      engine.dispose();
      sqlite.close();
    },
    dbType: "sqlite",
  };

  openHandles.push({ close: ctx.cleanup });
  return ctx;
}

// --- PGlite ---

const PG_DDL = `
  CREATE TABLE IF NOT EXISTS sync_outbox (
    event_id TEXT PRIMARY KEY,
    collection_id TEXT NOT NULL,
    type TEXT NOT NULL,
    key TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    previous JSONB,
    tx_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    base_version TEXT,
    timestamp BIGINT NOT NULL,
    local_seq BIGINT NOT NULL,
    global_seq BIGINT,
    sync BOOLEAN NOT NULL DEFAULT false,
    sync_status TEXT NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_attempt_at BIGINT,
    next_attempt_at BIGINT,
    last_error TEXT,
    last_error_code TEXT,
    retryable BOOLEAN
  );

  CREATE TABLE IF NOT EXISTS sync_inbox (
    event_id TEXT PRIMARY KEY,
    global_seq BIGINT NOT NULL,
    collection_id TEXT NOT NULL,
    type TEXT NOT NULL,
    key TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    previous JSONB,
    client_id TEXT,
    schema_version INTEGER NOT NULL DEFAULT 1,
    timestamp BIGINT NOT NULL,
    sync BOOLEAN NOT NULL DEFAULT false,
    skipped BOOLEAN NOT NULL DEFAULT false,
    skip_reason TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
  );

  CREATE TABLE IF NOT EXISTS sync_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS sync_dead_letter (
    event_id TEXT PRIMARY KEY,
    collection_id TEXT NOT NULL,
    type TEXT NOT NULL,
    key TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    previous JSONB,
    tx_id TEXT,
    client_id TEXT,
    schema_version INTEGER NOT NULL DEFAULT 1,
    timestamp BIGINT NOT NULL,
    local_seq INTEGER,
    global_seq BIGINT,
    direction TEXT NOT NULL,
    reason TEXT NOT NULL,
    message TEXT NOT NULL,
    code TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    failed_at BIGINT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS todos (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    done BOOLEAN NOT NULL DEFAULT false
  );
`;

export async function createPgliteTestContext(
  options: Partial<
    Omit<EventSourcedDrizzleConfig<TodoCollections>, "adapter" | "collections">
  > = {},
): Promise<TestContext> {
  const client = new PGlite();
  await client.exec(PG_DDL);

  const db = drizzlePg(client) as any;

  // PGlite requires using the `tx` handle inside transactions — you can't use
  // the parent `db` for queries while a transaction is open. We track the active
  // tx and route all operations through it.
  let activeTx: any = null;
  const getDb = () => activeTx ?? db;

  const adapter: DrizzleAdapter = {
    transaction: async <T>(fn: (tx: unknown) => Promise<T>): Promise<T> => {
      return db.transaction(async (tx: any) => {
        activeTx = tx;
        try {
          return await fn(tx);
        } finally {
          activeTx = null;
        }
      });
    },

    async queryDueOutbox(now: number): Promise<any[]> {
      const rows = await getDb()
        .select()
        .from(pgOutbox)
        .where(eq(pgOutbox.sync, false))
        .orderBy(pgOutbox.localSeq);
      return rows.filter(
        (r: any) => !r.sync && (r.nextAttemptAt === null || r.nextAttemptAt <= now),
      );
    },

    async updateOutbox(eventId: string, patch: any): Promise<void> {
      await getDb().update(pgOutbox).set(patch).where(eq(pgOutbox.eventId, eventId));
    },

    async markOutboxSynced(eventId: string, globalSeq: number): Promise<void> {
      await getDb()
        .update(pgOutbox)
        .set({
          sync: true,
          syncStatus: "synced",
          globalSeq,
          nextAttemptAt: null,
          lastError: null,
          lastErrorCode: null,
          retryable: null,
        })
        .where(eq(pgOutbox.eventId, eventId));
    },

    async deleteOutboxRow(eventId: string): Promise<void> {
      await getDb().delete(pgOutbox).where(eq(pgOutbox.eventId, eventId));
    },

    async insertOutbox(row: any): Promise<void> {
      await getDb().insert(pgOutbox).values(row);
    },

    async insertDeadLetter(row: any): Promise<void> {
      await getDb().insert(pgDeadLetter).values(row);
    },

    async insertInbox(row: any): Promise<void> {
      await getDb().insert(pgInbox).values(row);
    },

    async updateInbox(eventId: string, patch: any): Promise<void> {
      await getDb().update(pgInbox).set(patch).where(eq(pgInbox.eventId, eventId));
    },

    async getInboxRow(eventId: string): Promise<any> {
      const rows = await getDb()
        .select()
        .from(pgInbox)
        .where(eq(pgInbox.eventId, eventId))
        .limit(1);
      return rows[0];
    },

    async queryUnresolvedInbox(): Promise<any[]> {
      return getDb()
        .select()
        .from(pgInbox)
        .where(eq(pgInbox.sync, false))
        .orderBy(pgInbox.globalSeq);
    },

    async outboxHas(eventId: string): Promise<boolean> {
      const rows = await getDb()
        .select()
        .from(pgOutbox)
        .where(eq(pgOutbox.eventId, eventId))
        .limit(1);
      return rows.length > 0;
    },

    async readMeta(key: string): Promise<string | null> {
      const rows = await getDb().select().from(pgSyncMeta).where(eq(pgSyncMeta.key, key)).limit(1);
      return (rows[0] as any)?.value ?? null;
    },

    async writeMeta(key: string, value: string): Promise<void> {
      await getDb()
        .insert(pgSyncMeta)
        .values({ key, value })
        .onConflictDoUpdate({ target: pgSyncMeta.key, set: { value } });
    },

    async domainInsert(collectionId: string, row: Record<string, unknown>): Promise<void> {
      if (collectionId !== "todos") throw new Error(`Unknown collection: ${collectionId}`);
      await getDb().insert(pgTodos).values(row);
    },

    async domainUpdate(
      collectionId: string,
      key: string | number,
      patch: Record<string, unknown>,
    ): Promise<void> {
      if (collectionId !== "todos") throw new Error(`Unknown collection: ${collectionId}`);
      await getDb()
        .update(pgTodos)
        .set(patch)
        .where(eq(pgTodos.id, key as string));
    },

    async domainDelete(collectionId: string, key: string | number): Promise<void> {
      if (collectionId !== "todos") throw new Error(`Unknown collection: ${collectionId}`);
      await getDb()
        .delete(pgTodos)
        .where(eq(pgTodos.id, key as string));
    },
  };

  const engine = await createEventSourcedDrizzle<TodoCollections>({
    adapter,
    collections: {
      todos: { table: pgTodos, getKey: (row: Todo) => row.id },
    } as any,
    ...options,
  });

  const ctx: TestContext = {
    engine,
    adapter,
    rawQuery: <T>(_query: string) => {
      throw new Error("Use rawQueryAsync for PGlite");
    },
    cleanup: async () => {
      engine.dispose();
      await client.close();
    },
    dbType: "pglite",
  };

  openHandles.push({ close: ctx.cleanup });
  return ctx;
}

// --- Dual-DB test runner ---

type DbFactory = (
  options?: Partial<Omit<EventSourcedDrizzleConfig<TodoCollections>, "adapter" | "collections">>,
) => Promise<TestContext>;

/**
 * Runs the same test suite against both SQLite and PGlite.
 * Use this in integration tests to verify behavior across both backends.
 *
 * @example
 * ```ts
 * import { describeDualDb } from "../helpers/db"
 *
 * describeDualDb("mutate.insert", (createCtx) => {
 *   it("inserts a row into the domain table", async () => {
 *     const ctx = await createCtx()
 *     await ctx.engine.mutate.insert("todos", { id: "1", title: "hi", done: false })
 *     // assert...
 *   })
 * })
 * ```
 */
export function describeDualDb(
  name: string,
  fn: (createCtx: DbFactory, dbType: "sqlite" | "pglite") => void,
): void {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { describe } = require("vitest");

  describe(`${name} [sqlite]`, () => {
    fn(createSqliteTestContext, "sqlite");
  });

  describe(`${name} [pglite]`, () => {
    fn(createPgliteTestContext, "pglite");
  });
}

// --- Test utilities ---

export function makeTodo(id: string, title = "Task"): Todo {
  return { id, title, done: false };
}

/** Simple sync transport that captures push calls and returns empty pulls. */
export function createTestTransport(): {
  transport: SyncTransport;
  pushed: Array<{ events: ReadonlyArray<any> }>;
} {
  const pushed: Array<{ events: ReadonlyArray<any> }> = [];

  const transport: SyncTransport = {
    push: async (events) => {
      pushed.push({ events });
      return {
        confirmed: events.map((e, i) => ({ eventId: e.eventId, globalSeq: i + 1 })),
      };
    },
    pull: async () => ({ events: [], cursor: "0", hasMore: false }),
  };

  return { transport, pushed };
}
