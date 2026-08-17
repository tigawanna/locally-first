/**
 * SQLite example.
 *
 * Writes MUST go through `engine.mutate` (outbox + domain in one transaction).
 * Reads use the Drizzle `db` instance as usual — `db.select()`, joins, etc.
 *
 *   pnpm --filter event-sourced-drizzle example:generate:sqlite
 *   pnpm --filter event-sourced-drizzle example:sqlite
 */
import { fileURLToPath } from "node:url";
import path from "node:path";

import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { migrate } from "drizzle-orm/better-sqlite3/migrator";
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";

import { createEventSourcedDrizzle } from "../src/index";
import type { CollectionDef, EventSourcedDrizzle } from "../src/index";
import {
  createSQLiteAdapter,
  defineDeadLetterTable,
  defineInboxTable,
  defineOutboxTable,
  defineSyncMetaTable,
} from "../src/sqlite";

const playgroundDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../.playground");
const dbPath = path.join(playgroundDir, "app.sqlite");
const migrationsFolder = path.join(playgroundDir, "drizzle-sqlite");

export const todos = sqliteTable("todos", {
  id: text("id").primaryKey(),
  title: text("title").notNull(),
  status: text("status").$type<"pending" | "complete">().notNull().default("pending"),
  createdAt: integer("created_at").notNull(),
  updatedAt: integer("updated_at").notNull(),
});

export const outbox = defineOutboxTable("sync_outbox", {
  deviceId: text("device_id"),
});

export const inbox = defineInboxTable("sync_inbox", {
  receivedAt: integer("received_at"),
});

export const syncMeta = defineSyncMetaTable("sync_meta");
export const deadLetter = defineDeadLetterTable("sync_dead_letter");

type AppCollections = {
  todos: CollectionDef<typeof todos, string>;
};

const collections: AppCollections = {
  todos: { table: todos, getKey: (row) => row.id },
};

function createExampleDb() {
  const sqlite = new Database(dbPath);
  const db = drizzle(sqlite, { schema: { todos, outbox, inbox, syncMeta, deadLetter } });
  migrate(db, { migrationsFolder });

  // better-sqlite3's Drizzle `transaction` is sync-only; the engine always awaits.
  const adapterDb = {
    select: db.select.bind(db),
    insert: db.insert.bind(db),
    update: db.update.bind(db),
    delete: db.delete.bind(db),
    transaction: async <T>(fn: (tx: unknown) => Promise<T>): Promise<T> => {
      sqlite.exec("BEGIN");
      try {
        const result = await fn(db);
        sqlite.exec("COMMIT");
        return result;
      } catch (error) {
        sqlite.exec("ROLLBACK");
        throw error;
      }
    },
  };

  return { sqlite, db, adapterDb };
}

async function createExampleEngine(
  adapterDb: ReturnType<typeof createExampleDb>["adapterDb"],
): Promise<EventSourcedDrizzle<AppCollections>> {
  return createEventSourcedDrizzle({
    adapter: createSQLiteAdapter(adapterDb, {
      outbox,
      inbox,
      syncMeta,
      deadLetter,
      collections: {
        todos: { table: todos, keyColumn: todos.id },
      },
    }),
    collections,
    syncEnabled: true,
    debug: true,
  });
}

async function main() {
  const { sqlite, db, adapterDb } = createExampleDb();
  const engine = await createExampleEngine(adapterDb);

  // Correct: mutate writes the row AND an outbox event.
  await engine.mutate.insert("todos", {
    id: crypto.randomUUID(),
    title: "Buy groceries",
    status: "pending",
    createdAt: Date.now(),
    updatedAt: Date.now(),
  });

  // Correct: queries go through Drizzle.
  const rows = db.select().from(todos).all();
  console.log("todos via drizzle", rows);

  // Wrong: db.insert(todos).values(...) / update / delete skip the outbox.
  // Those writes never sync.

  engine.dispose();
  sqlite.close();
}

const invoked = process.argv[1] ? path.resolve(process.argv[1]) : "";
if (invoked === fileURLToPath(import.meta.url)) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
