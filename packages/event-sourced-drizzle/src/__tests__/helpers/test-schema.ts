/**
 * Full test schemas for SQLite and PG that include ALL OutboxRow/InboxRow fields.
 * The published schemas are intentionally minimal — these test schemas prove
 * the engine works with the complete field set.
 */

// --- SQLite Schema ---
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";
import {
  bigint,
  boolean,
  integer as pgInteger,
  jsonb,
  pgTable,
  text as pgText,
} from "drizzle-orm/pg-core";

// SQLite test outbox — includes all OutboxRow fields
export const sqliteOutbox = sqliteTable("sync_outbox", {
  eventId: text("event_id").primaryKey(),
  collectionId: text("collection_id").notNull(),
  type: text("type").notNull(),
  key: text("key").notNull(),
  payload: text("payload", { mode: "json" }).$type<Record<string, unknown>>().notNull(),
  previous: text("previous", { mode: "json" }).$type<Record<string, unknown> | null>(),
  txId: text("tx_id").notNull(),
  clientId: text("client_id").notNull(),
  schemaVersion: integer("schema_version").notNull().default(1),
  baseVersion: text("base_version"),
  timestamp: integer("timestamp").notNull(),
  localSeq: integer("local_seq").notNull(),
  globalSeq: integer("global_seq"),
  sync: integer("sync", { mode: "boolean" }).notNull().default(false),
  syncStatus: text("sync_status").notNull().default("pending"),
  attemptCount: integer("attempt_count").notNull().default(0),
  lastAttemptAt: integer("last_attempt_at"),
  nextAttemptAt: integer("next_attempt_at"),
  lastError: text("last_error"),
  lastErrorCode: text("last_error_code"),
  retryable: integer("retryable", { mode: "boolean" }),
});

// SQLite test inbox — all InboxRow fields
export const sqliteInbox = sqliteTable("sync_inbox", {
  eventId: text("event_id").primaryKey(),
  globalSeq: integer("global_seq").notNull(),
  collectionId: text("collection_id").notNull(),
  type: text("type").notNull(),
  key: text("key").notNull(),
  payload: text("payload", { mode: "json" }).$type<Record<string, unknown>>().notNull(),
  previous: text("previous", { mode: "json" }).$type<Record<string, unknown> | null>(),
  clientId: text("client_id"),
  schemaVersion: integer("schema_version").notNull().default(1),
  timestamp: integer("timestamp").notNull(),
  sync: integer("sync", { mode: "boolean" }).notNull().default(false),
  skipped: integer("skipped", { mode: "boolean" }).notNull().default(false),
  skipReason: text("skip_reason"),
  attemptCount: integer("attempt_count").notNull().default(0),
  lastError: text("last_error"),
});

// SQLite sync meta
export const sqliteSyncMeta = sqliteTable("sync_meta", {
  key: text("key").primaryKey(),
  value: text("value").notNull(),
});

// SQLite dead letter
export const sqliteDeadLetter = sqliteTable("sync_dead_letter", {
  eventId: text("event_id").primaryKey(),
  collectionId: text("collection_id").notNull(),
  type: text("type").notNull(),
  key: text("key").notNull(),
  payload: text("payload", { mode: "json" }).$type<Record<string, unknown>>().notNull(),
  previous: text("previous", { mode: "json" }).$type<Record<string, unknown> | null>(),
  txId: text("tx_id"),
  clientId: text("client_id"),
  schemaVersion: integer("schema_version").notNull().default(1),
  timestamp: integer("timestamp").notNull(),
  localSeq: integer("local_seq"),
  globalSeq: integer("global_seq"),
  direction: text("direction").notNull(),
  reason: text("reason").notNull(),
  message: text("message").notNull(),
  code: text("code"),
  attemptCount: integer("attempt_count").notNull().default(0),
  failedAt: integer("failed_at").notNull(),
});

// SQLite domain table: todos
export const sqliteTodos = sqliteTable("todos", {
  id: text("id").primaryKey(),
  title: text("title").notNull(),
  done: integer("done", { mode: "boolean" }).notNull().default(false),
});

// --- PG Schema ---

export const pgOutbox = pgTable("sync_outbox", {
  eventId: pgText("event_id").primaryKey(),
  collectionId: pgText("collection_id").notNull(),
  type: pgText("type").notNull(),
  key: pgText("key").notNull(),
  payload: jsonb("payload").$type<Record<string, unknown>>().notNull(),
  previous: jsonb("previous").$type<Record<string, unknown> | null>(),
  txId: pgText("tx_id").notNull(),
  clientId: pgText("client_id").notNull(),
  schemaVersion: pgInteger("schema_version").notNull().default(1),
  baseVersion: pgText("base_version"),
  timestamp: bigint("timestamp", { mode: "number" }).notNull(),
  localSeq: bigint("local_seq", { mode: "number" }).notNull(),
  globalSeq: bigint("global_seq", { mode: "number" }),
  sync: boolean("sync").notNull().default(false),
  syncStatus: pgText("sync_status").notNull().default("pending"),
  attemptCount: pgInteger("attempt_count").notNull().default(0),
  lastAttemptAt: bigint("last_attempt_at", { mode: "number" }),
  nextAttemptAt: bigint("next_attempt_at", { mode: "number" }),
  lastError: pgText("last_error"),
  lastErrorCode: pgText("last_error_code"),
  retryable: boolean("retryable"),
});

export const pgInbox = pgTable("sync_inbox", {
  eventId: pgText("event_id").primaryKey(),
  globalSeq: bigint("global_seq", { mode: "number" }).notNull(),
  collectionId: pgText("collection_id").notNull(),
  type: pgText("type").notNull(),
  key: pgText("key").notNull(),
  payload: jsonb("payload").$type<Record<string, unknown>>().notNull(),
  previous: jsonb("previous").$type<Record<string, unknown> | null>(),
  clientId: pgText("client_id"),
  schemaVersion: pgInteger("schema_version").notNull().default(1),
  timestamp: bigint("timestamp", { mode: "number" }).notNull(),
  sync: boolean("sync").notNull().default(false),
  skipped: boolean("skipped").notNull().default(false),
  skipReason: pgText("skip_reason"),
  attemptCount: pgInteger("attempt_count").notNull().default(0),
  lastError: pgText("last_error"),
});

export const pgSyncMeta = pgTable("sync_meta", {
  key: pgText("key").primaryKey(),
  value: pgText("value").notNull(),
});

export const pgDeadLetter = pgTable("sync_dead_letter", {
  eventId: pgText("event_id").primaryKey(),
  collectionId: pgText("collection_id").notNull(),
  type: pgText("type").notNull(),
  key: pgText("key").notNull(),
  payload: jsonb("payload").$type<Record<string, unknown>>().notNull(),
  previous: jsonb("previous").$type<Record<string, unknown> | null>(),
  txId: pgText("tx_id"),
  clientId: pgText("client_id"),
  schemaVersion: pgInteger("schema_version").notNull().default(1),
  timestamp: bigint("timestamp", { mode: "number" }).notNull(),
  localSeq: pgInteger("local_seq"),
  globalSeq: bigint("global_seq", { mode: "number" }),
  direction: pgText("direction").notNull(),
  reason: pgText("reason").notNull(),
  message: pgText("message").notNull(),
  code: pgText("code"),
  attemptCount: pgInteger("attempt_count").notNull().default(0),
  failedAt: bigint("failed_at", { mode: "number" }).notNull(),
});

export const pgTodos = pgTable("todos", {
  id: pgText("id").primaryKey(),
  title: pgText("title").notNull(),
  done: boolean("done").notNull().default(false),
});
