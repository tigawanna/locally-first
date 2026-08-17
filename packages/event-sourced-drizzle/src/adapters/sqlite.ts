/**
 * Reference SQLite adapter for `createEventSourcedDrizzle`.
 *
 * Requires your Drizzle schema to include:
 * - An outbox table (from `defineOutboxTable`)
 * - An inbox table (from `defineInboxTable`)
 * - A sync_meta table (from `defineSyncMetaTable`)
 * - A dead_letter table (from `defineDeadLetterTable`)
 * - Your domain tables (registered in `collections`)
 *
 * Usage:
 * ```ts
 * import { createSQLiteAdapter } from "event-sourced-drizzle/sqlite";
 * const adapter = createSQLiteAdapter(db, { outbox, inbox, syncMeta, deadLetter, collections });
 * ```
 */
import { eq } from "drizzle-orm";
import type { SQLiteTableWithColumns } from "drizzle-orm/sqlite-core";

import type { DeadLetterRow, DrizzleAdapter, InboxRow, OutboxRow } from "../internal/types";

type SQLiteDb = {
  // Minimal Drizzle SQLite database shape.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  select: (...args: any[]) => any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  insert: (table: any) => any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  update: (table: any) => any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  delete: (table: any) => any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  transaction: <T>(fn: (tx: any) => Promise<T>) => Promise<T>;
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyTable = SQLiteTableWithColumns<any>;

export type SQLiteAdapterConfig = {
  /** The outbox table created by `defineOutboxTable`. */
  outbox: AnyTable;
  /** The inbox table created by `defineInboxTable`. */
  inbox: AnyTable;
  /** A key-value table for sync metadata (pull cursor, clientId, etc.). */
  syncMeta: AnyTable;
  /** The dead-letter table. */
  deadLetter: AnyTable;
  /**
   * Map of collectionId → { table, keyColumn }.
   * `keyColumn` is the Drizzle column reference used for WHERE clauses.
   */
  collections: Record<string, { table: AnyTable; keyColumn: AnyTable["_"]["columns"][string] }>;
};

/**
 * Creates a DrizzleAdapter backed by a Drizzle SQLite database.
 */
export function createSQLiteAdapter(db: SQLiteDb, config: SQLiteAdapterConfig): DrizzleAdapter {
  const { outbox, inbox, syncMeta, deadLetter, collections } = config;

  return {
    transaction: <T>(fn: (tx: unknown) => Promise<T>) => db.transaction(fn),

    async queryDueOutbox(now: number): Promise<OutboxRow[]> {
      const rows = await db
        .select()
        .from(outbox)
        .where(eq(outbox.sync, false))
        .orderBy(outbox.localSeq);
      // Filter in JS for nextAttemptAt logic (Drizzle doesn't have a great <= or null check combo).
      return (rows as OutboxRow[]).filter(
        (r) => !r.sync && (r.nextAttemptAt === null || r.nextAttemptAt <= now),
      );
    },

    async updateOutbox(eventId: string, patch: Partial<OutboxRow>): Promise<void> {
      await db.update(outbox).set(patch).where(eq(outbox.eventId, eventId));
    },

    async markOutboxSynced(eventId: string, globalSeq: number): Promise<void> {
      await db
        .update(outbox)
        .set({
          sync: true,
          syncStatus: "synced",
          globalSeq,
          nextAttemptAt: null,
          lastError: null,
          lastErrorCode: null,
          retryable: null,
        })
        .where(eq(outbox.eventId, eventId));
    },

    async deleteOutboxRow(eventId: string): Promise<void> {
      await db.delete(outbox).where(eq(outbox.eventId, eventId));
    },

    async insertOutbox(row: OutboxRow): Promise<void> {
      await db.insert(outbox).values(row);
    },

    async insertDeadLetter(row: DeadLetterRow): Promise<void> {
      await db.insert(deadLetter).values(row);
    },

    async insertInbox(row: InboxRow): Promise<void> {
      await db.insert(inbox).values(row);
    },

    async updateInbox(eventId: string, patch: Partial<InboxRow>): Promise<void> {
      await db.update(inbox).set(patch).where(eq(inbox.eventId, eventId));
    },

    async getInboxRow(eventId: string): Promise<InboxRow | undefined> {
      const rows = await db.select().from(inbox).where(eq(inbox.eventId, eventId)).limit(1);
      return rows[0] as InboxRow | undefined;
    },

    async queryUnresolvedInbox(): Promise<InboxRow[]> {
      const rows = await db
        .select()
        .from(inbox)
        .where(eq(inbox.sync, false))
        .orderBy(inbox.globalSeq);
      return rows as InboxRow[];
    },

    async outboxHas(eventId: string): Promise<boolean> {
      const rows = await db.select().from(outbox).where(eq(outbox.eventId, eventId)).limit(1);
      return rows.length > 0;
    },

    async readMeta(key: string): Promise<string | null> {
      const rows = await db.select().from(syncMeta).where(eq(syncMeta.key, key)).limit(1);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return (rows[0] as any)?.value ?? null;
    },

    async writeMeta(key: string, value: string): Promise<void> {
      await db
        .insert(syncMeta)
        .values({ key, value })
        .onConflictDoUpdate({ target: syncMeta.key, set: { value } });
    },

    async domainInsert(collectionId: string, row: Record<string, unknown>): Promise<void> {
      const col = collections[collectionId];
      if (!col) throw new Error(`Adapter: unknown collection "${collectionId}"`);
      await db.insert(col.table).values(row);
    },

    async domainUpdate(
      collectionId: string,
      key: string | number,
      patch: Record<string, unknown>,
    ): Promise<void> {
      const col = collections[collectionId];
      if (!col) throw new Error(`Adapter: unknown collection "${collectionId}"`);
      await db.update(col.table).set(patch).where(eq(col.keyColumn, key));
    },

    async domainDelete(collectionId: string, key: string | number): Promise<void> {
      const col = collections[collectionId];
      if (!col) throw new Error(`Adapter: unknown collection "${collectionId}"`);
      await db.delete(col.table).where(eq(col.keyColumn, key));
    },
  };
}
