# event-sourced-drizzle

Event-sourced local-first sync engine for **Drizzle ORM**. Same push/pull wire protocol as [event-sourced-collection](https://github.com/tigawanna/locally-first/tree/main/packages/event-sourced-drizzle), but all state lives in your SQL database (SQLite, PGlite, Postgres) — managed by Drizzle, not TanStack DB memory.

Every `insert`, `update`, and `delete` goes through a typed `mutate` API that atomically writes the domain table AND appends an outbox event. Sync pushes outbox events to your server and pulls remote events into an inbox, replaying them into domain tables.

## Install

```bash
npm install event-sourced-drizzle drizzle-orm
```

## Quick Start (SQLite)

Schema, adapter, mutate, and Drizzle reads — same pattern as [`examples/sqlite.ts`](./examples/sqlite.ts).

```ts
import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";
import { createEventSourcedDrizzle } from "event-sourced-drizzle";
import {
  createSQLiteAdapter,
  defineDeadLetterTable,
  defineInboxTable,
  defineOutboxTable,
  defineSyncMetaTable,
} from "event-sourced-drizzle/sqlite";

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

const sqlite = new Database("app.sqlite");
const db = drizzle(sqlite, { schema: { todos, outbox, inbox, syncMeta, deadLetter } });

const engine = await createEventSourcedDrizzle({
  adapter: createSQLiteAdapter(db, {
    outbox,
    inbox,
    syncMeta,
    deadLetter,
    collections: {
      todos: { table: todos, keyColumn: todos.id },
    },
  }),
  collections: {
    todos: { table: todos, getKey: (row) => row.id },
  },
});

// Writes: engine.mutate (domain row + outbox event in one transaction)
await engine.mutate.insert("todos", {
  id: crypto.randomUUID(),
  title: "Buy groceries",
  status: "pending",
  createdAt: Date.now(),
  updatedAt: Date.now(),
});

// Reads: your Drizzle instance
const rows = db.select().from(todos).all();
```

> [!WARNING]
> **Do not** `db.insert()`, `db.update()`, or `db.delete()` on synced tables. Those skip the outbox, so the change never syncs and a later pull can overwrite it. **Do** query with Drizzle as usual (`select`, joins, aggregates).

> [!NOTE]
> better-sqlite3’s Drizzle `transaction` is sync-only. If mutate throws `Transaction function cannot return a promise`, wrap `BEGIN`/`COMMIT` around the adapter like [`examples/sqlite.ts`](./examples/sqlite.ts).

PGlite uses the same engine API with `event-sourced-drizzle/pg` — see [`examples/pglite.ts`](./examples/pglite.ts).

## Migrations

> [!NOTE]
> This package does **not** create or migrate tables. You own the schema (`defineOutboxTable`, `defineInboxTable`, domain tables, `sync_meta`, dead-letter) and you apply it with whatever Drizzle pipeline you already use.

**Node / servers:** drizzle-kit as usual — `drizzle-kit generate`, then `migrate()` from `drizzle-orm/*/migrator`, or `drizzle-kit push` in development.

**Browser / Vite local-first:** drizzle-kit cannot run in the browser. Generate SQL with drizzle-kit, then apply those files at runtime. [proj-airi/drizzle-orm-browser](https://github.com/proj-airi/drizzle-orm-browser) bundles journaled migrations into `virtual:drizzle-migrations.sql` and applies them with a browser migrator (PGlite, SQLite, DuckDB WASM):

```ts
import { migrate } from "@proj-airi/drizzle-orm-browser-migrator/pglite";
import { drizzle } from "drizzle-orm/pglite";
import migrations from "virtual:drizzle-migrations.sql";

const db = drizzle({ client: pgLite });
await migrate(db, migrations);
```

Create the engine only after migrations have succeeded. Outbox, inbox, sync meta, dead-letter, and every domain table the adapter registers must exist first.

## Examples

Runnable copies live in [`examples/`](./examples):

| File                                         | Dialect           |
| -------------------------------------------- | ----------------- |
| [`examples/sqlite.ts`](./examples/sqlite.ts) | better-sqlite3    |
| [`examples/pglite.ts`](./examples/pglite.ts) | PGlite (Postgres) |

```bash
pnpm --filter event-sourced-drizzle example:sqlite
pnpm --filter event-sourced-drizzle example:pglite
```

Generate migrations with `example:generate:sqlite` / `example:generate:pglite`. See [`examples/README.md`](./examples/README.md).

## Postgres / PGlite

Same engine and `mutate` / Drizzle-read split. Import schema helpers and `createPgAdapter` from `event-sourced-drizzle/pg` (jsonb / boolean / bigint). Full wiring is in [`examples/pglite.ts`](./examples/pglite.ts).

## Architecture

This package mirrors the architecture of `event-sourced-collection`:

```
src/
├── internal/
│   ├── constants.ts       # Defaults, error codes, sync-meta keys
│   ├── hooks.ts           # Lifecycle hook emitter
│   ├── push.ts            # Outbox → server (batching, retry, dead-letter)
│   ├── pull.ts            # Server → inbox (cursor, overlap, local-origin filtering)
│   ├── replay.ts          # Inbox → domain tables (upcast, apply, dead-letter)
│   ├── serial-queue.ts    # In-process mutual exclusion
│   ├── sync-meta.ts       # Pull cursor, clientId, backendId persistence
│   └── types.ts           # Internal types (DrizzleAdapter, OutboxRow, InboxRow, etc.)
├── schema/
│   ├── pg.ts              # Postgres schema builder with extensible columns
│   └── sqlite.ts          # SQLite schema builder with extensible columns
├── utils/
│   ├── logger.ts          # Structured logger
│   └── uuid.ts            # UUIDv7 event ID generation
├── create-event-sourced-drizzle.ts   # Main factory
├── sync.ts                # Transport normalization (URL/handlers/raw)
├── types.ts               # Public API types
├── index.ts               # Barrel export (main entry)
├── sqlite.ts              # Barrel for schema/sqlite
└── pg.ts                  # Barrel for schema/pg
```

### Key Differences from `event-sourced-collection`

| Aspect        | event-sourced-collection                           | event-sourced-drizzle                               |
| ------------- | -------------------------------------------------- | --------------------------------------------------- |
| State storage | TanStack DB in-memory collections backed by SQLite | SQL tables via Drizzle ORM                          |
| Query engine  | TanStack DB live queries                           | Drizzle's query builder (limit/offset, joins, etc.) |
| Schema        | Fixed internal structure                           | Extensible — add custom columns to outbox/inbox     |
| Persistence   | Automatic via TanStack DB                          | You manage migrations with drizzle-kit              |
| Platform      | Browser OPFS, React Native                         | Anywhere Drizzle runs (Node, Bun, edge, mobile)     |
| Wire protocol | Same                                               | Same (servers are interchangeable)                  |

### DrizzleAdapter

The engine communicates with your database through a `DrizzleAdapter` interface. This keeps the core pipeline database-agnostic — you can implement it for any Drizzle dialect:

```ts
type DrizzleAdapter = {
  transaction: <T>(fn: (tx: unknown) => Promise<T>) => Promise<T>;
  queryDueOutbox: (now: number) => Promise<OutboxRow[]>;
  updateOutbox: (eventId: string, patch: Partial<OutboxRow>) => Promise<void>;
  markOutboxSynced: (eventId: string, globalSeq: number) => Promise<void>;
  deleteOutboxRow: (eventId: string) => Promise<void>;
  insertDeadLetter: (row: DeadLetterRow) => Promise<void>;
  insertInbox: (row: InboxRow) => Promise<void>;
  updateInbox: (eventId: string, patch: Partial<InboxRow>) => Promise<void>;
  getInboxRow: (eventId: string) => Promise<InboxRow | undefined>;
  queryUnresolvedInbox: () => Promise<InboxRow[]>;
  outboxHas: (eventId: string) => Promise<boolean>;
  readMeta: (key: string) => Promise<string | null>;
  writeMeta: (key: string, value: string) => Promise<void>;
  domainInsert: (collectionId: string, row: Record<string, unknown>) => Promise<void>;
  domainUpdate: (
    collectionId: string,
    key: string | number,
    patch: Record<string, unknown>,
  ) => Promise<void>;
  domainDelete: (collectionId: string, key: string | number) => Promise<void>;
};
```

### Extensible Inbox/Outbox Tables

The schema builders enforce required columns at compile time but let you add your own:

```ts
const outbox = defineOutboxTable("sync_outbox", {
  deviceId: text("device_id"), // your custom column
  priority: integer("priority"), // your custom column
});
// TypeScript enforces all required columns exist + your extras are typed
```

This means hooks like `onAppendOutbox` and `onPullInbox` can work with your extended row types without casts.

## Sync Transport

Same three config styles as `event-sourced-collection`:

```ts
// URL strings — built-in HTTP adapter
sync: { pushUrl: "/api/events", pullUrl: "/api/events" }

// Handler functions
sync: { pushEvents: myPushFn, pullEvents: myPullFn }

// Raw transport
sync: { push: fn, pull: fn }
```

## Configuration

| Option                 | Default                       | Description                                                  |
| ---------------------- | ----------------------------- | ------------------------------------------------------------ |
| `adapter`              | required                      | DrizzleAdapter bridging engine to your DB                    |
| `collections`          | required                      | Collection registry — keys become `collectionId` on the wire |
| `sync`                 | none                          | Transport config. Omit for offline-only                      |
| `syncEnabled`          | `true`                        | Whether sync runs. Toggle with `setSyncEnabled()`            |
| `clientId`             | auto-generated                | Stable device identity                                       |
| `unknownEventHandling` | `"skip"`                      | What to do with events for unknown collections               |
| `pullOverlap`          | `0`                           | Cursor overlap for out-of-order sequence protection          |
| `eventSchemaVersion`   | `1`                           | Stamped on every authored event                              |
| `upcastEvent`          | none                          | Migrates events from older schema versions                   |
| `retry`                | 8 attempts, 1s base, 5min cap | Backoff for retryable push failures                          |
| `pushBatchSize`        | `100`                         | Max events per push request                                  |
| `backendMismatch`      | `"resetCursor"`               | What to do when server identity changes                      |
| `conflictDetection`    | `false`                       | Stamp `baseVersion` for stale-write rejection                |
| `hooks`                | none                          | Lifecycle hooks                                              |
| `debug`                | `false`                       | Logger config                                                |

## Lifecycle Hooks

Same set as `event-sourced-collection`:

| Hook                | Fires when                                 |
| ------------------- | ------------------------------------------ |
| `onReady`           | Engine initialized, pending inbox replayed |
| `onMutation`        | Local mutation appended to outbox          |
| `onSyncStart`       | Sync cycle begins                          |
| `onSyncComplete`    | Sync cycle finishes                        |
| `onSyncError`       | A phase (push/pull/replay) fails           |
| `onEventPushed`     | Server confirms an outbound event          |
| `onEventApplied`    | Remote event replayed into a domain table  |
| `onEventSkipped`    | Event recorded but not applied             |
| `onDeadLetter`      | Event moved to dead-letter queue           |
| `onBackendMismatch` | Server reports different backend identity  |

## Exports

- `event-sourced-drizzle` — `createEventSourcedDrizzle`, `createSqliteEventLogBackend`, protocol types, logger, UUID
- `event-sourced-drizzle/sqlite` — `defineOutboxTable`, `defineInboxTable`, `createSQLiteAdapter`
- `event-sourced-drizzle/pg` — Postgres/PGlite equivalents
- `event-sourced-drizzle/react` — `useManualSync`, `useSyncEnabled`, `formatManualSyncMessage`

## Server Compatibility

The wire protocol (`OutboundEvent`, `ServerEvent`, `PushResponse`, `PullResponse`) is identical to `event-sourced-collection`. Servers built for one work with the other — they're interchangeable.

## Status

The engine, SQLite/PG adapters, transactional `mutate`, conflict detection (`conflictDetection`), a SQLite event-log test backend (`createSqliteEventLogBackend`), and React helpers (`event-sourced-drizzle/react`) are in place. Reads stay on your Drizzle `db`.

## Roadmap

- [x] Wire protocol types (compatible with event-sourced-collection)
- [x] SQLite + PG schema builders with extensible columns
- [x] Internal pipeline (push/pull/replay/retry/dead-letter)
- [x] Lifecycle hooks
- [x] Transport normalization (URL/handlers/raw)
- [x] Logger + structured debug output
- [x] Reference `createSQLiteAdapter` / `createPgAdapter` helpers
- [x] Full transactional mutate (domain write + outbox in one tx)
- [x] React helpers (`useManualSync`, `useSyncEnabled`)
- [x] DB-backed sync backend for tests (push/pull against a second database)
- [x] Conflict detection + row version tracking
