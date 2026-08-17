/**
 * PGlite example — same pattern as `sqlite.ts`, Postgres dialect.
 *
 * Writes MUST go through `engine.mutate`. Reads use the Drizzle `db` instance.
 *
 *   pnpm --filter event-sourced-drizzle example:generate:pglite
 *   pnpm --filter event-sourced-drizzle example:pglite
 */
import { fileURLToPath } from "node:url";
import path from "node:path";

import { PGlite } from "@electric-sql/pglite";
import { bigint, pgTable, text } from "drizzle-orm/pg-core";
import { drizzle } from "drizzle-orm/pglite";
import { migrate } from "drizzle-orm/pglite/migrator";

import { createEventSourcedDrizzle } from "../src/index";
import type { CollectionDef, EventSourcedDrizzle } from "../src/index";
import {
  createPgAdapter,
  defineDeadLetterTable,
  defineInboxTable,
  defineOutboxTable,
  defineSyncMetaTable,
} from "../src/pg";

const playgroundDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../.playground");
const dataDir = path.join(playgroundDir, "pglite");
const migrationsFolder = path.join(playgroundDir, "drizzle-pg");

export const todos = pgTable("todos", {
  id: text("id").primaryKey(),
  title: text("title").notNull(),
  status: text("status").$type<"pending" | "complete">().notNull().default("pending"),
  createdAt: bigint("created_at", { mode: "number" }).notNull(),
  updatedAt: bigint("updated_at", { mode: "number" }).notNull(),
});

export const outbox = defineOutboxTable("sync_outbox", {
  deviceId: text("device_id"),
});

export const inbox = defineInboxTable("sync_inbox", {
  receivedAt: bigint("received_at", { mode: "number" }),
});

export const syncMeta = defineSyncMetaTable("sync_meta");
export const deadLetter = defineDeadLetterTable("sync_dead_letter");

type AppCollections = {
  todos: CollectionDef<typeof todos, string>;
};

const collections: AppCollections = {
  todos: { table: todos, getKey: (row) => row.id },
};

async function createExampleDb() {
  const client = new PGlite(dataDir);
  const db = drizzle(client, { schema: { todos, outbox, inbox, syncMeta, deadLetter } });
  await migrate(db, { migrationsFolder });

  // PGlite requires queries inside a transaction to use the `tx` handle, not `db`.
  let activeTx: typeof db | null = null;
  const current = () => activeTx ?? db;
  const adapterDb = {
    select: (...args: Parameters<typeof db.select>) => current().select(...args),
    insert: (...args: Parameters<typeof db.insert>) => current().insert(...args),
    update: (...args: Parameters<typeof db.update>) => current().update(...args),
    delete: (...args: Parameters<typeof db.delete>) => current().delete(...args),
    transaction: async <T>(fn: (tx: unknown) => Promise<T>): Promise<T> => {
      return db.transaction(async (tx) => {
        activeTx = tx as unknown as typeof db;
        try {
          return await fn(tx);
        } finally {
          activeTx = null;
        }
      });
    },
  };

  return { client, db, adapterDb };
}

async function createExampleEngine(
  adapterDb: Awaited<ReturnType<typeof createExampleDb>>["adapterDb"],
): Promise<EventSourcedDrizzle<AppCollections>> {
  return createEventSourcedDrizzle({
    adapter: createPgAdapter(adapterDb, {
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
  const { client, db, adapterDb } = await createExampleDb();
  const engine = await createExampleEngine(adapterDb);

  await engine.mutate.insert("todos", {
    id: crypto.randomUUID(),
    title: "Buy groceries",
    status: "pending",
    createdAt: Date.now(),
    updatedAt: Date.now(),
  });

  const rows = await db.select().from(todos);
  console.log("todos via drizzle", rows);

  engine.dispose();
  await client.close();
}

const invoked = process.argv[1] ? path.resolve(process.argv[1]) : "";
if (invoked === fileURLToPath(import.meta.url)) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
