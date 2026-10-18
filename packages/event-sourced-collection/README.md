# event-sourced-collection

Event-sourced local-first database on top of TanStack DB persistence. Every `insert`, `update`, and `delete` is logged to a SQLite event table and synced to your backend when online.

Design decisions, sync pipeline, and open issues: [ARCHITECTURE.md](./ARCHITECTURE.md)  
Config options deep-dive (when to use each knob): [docs/usage.md](./docs/usage.md)  
How compaction folds edits on the device, and a Hono handler that merges them: [docs/compaction.md](./docs/compaction.md)  
How local chains fold, and how a Hono server should merge them: [docs/compaction.md](./docs/compaction.md)  
Compaction on the device and on a Hono server: [docs/compaction.md](./docs/compaction.md)  
Release history: [CHANGELOG.md](./CHANGELOG.md)

## Install

```bash
npm install event-sourced-collection @tanstack/db @tanstack/db-sqlite-persistence-core
```

Plus a platform package:

```bash
# Browser
npm install @tanstack/browser-db-sqlite-persistence

# React Native
npm install @tanstack/react-native-db-sqlite-persistence
```

## Project structure

Keep the local DB in **one folder**. Only `collections.ts` is required; everything else is optional layering.

### Minimal (most apps)

```
src/
└── db/
    └── collections.ts     # types + push/pull + createBrowserEventSourcedDB
                           # exports: db, ensureDb, row types
```

Wire once at app mount:

```
app layout / root
  └─ await ensureDb()      # then use db.collections.* anywhere
```

Components import `db` for reads/writes. Call `db.manualSync()` / `db.sync()` when you want sync — no wrapper file needed.

### Recommended (settings toggle + background sync)

```
src/
├── db/
│   ├── collections.ts     # required — DB factory + types + transport
│   └── settings.ts        # optional — seed settings row, persist syncEnabled
│
├── components/            # or your app shell
│   └── SyncRunner.tsx     # optional — mount-once: ensureDb + poll sync
│
└── routes/…/settings/     # optional — UI toggle calling setSyncEnabled()
```

| File                    | Required? | Role                                                                                              |
| ----------------------- | --------- | ------------------------------------------------------------------------------------------------- |
| `db/collections.ts`     | **Yes**   | Row types, collection registry, sync transport, `createBrowserEventSourcedDB` → `ensureDb` + `db` |
| `db/settings.ts`        | No        | Seeds `"app"` settings row; mirrors `syncEnabled` ↔ `db.setSyncEnabled()`                         |
| `SyncRunner.tsx`        | No        | One effect: `ensureDb()` (or `ensureAppSettings()`), then poll `manualSync` while enabled         |
| Settings route          | No        | Toggle that calls your `setSyncEnabled()` helper                                                  |
| `sync-events.ts`        | No        | Thin `db.sync()` wrappers — skip unless you want a non-React API boundary                         |
| Custom `useSyncEnabled` | No        | Use `useSyncEnabled` from `event-sourced-collection/react` instead                                |

**Do not scatter** sync helpers under `hooks/`, `services/`, and `components/` all at once. If you need a hook, put it next to `SyncRunner` or call the package hooks from the shell.

### What each layer talks to

```
┌─────────────────────────────────────────────────────────┐
│  UI (notes, settings, buttons)                          │
│    import { db } from "@/db/collections"                │
│    db.collections.todos.insert(…)                       │
│    db.manualSync()  /  useManualSync from package       │
└───────────────────────────┬─────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────┐
│  src/db/collections.ts     ← single source of truth     │
│    createBrowserEventSourcedDB({ collections, sync, … })│
│    exports ensureDb, db                                 │
└───────────────────────────┬─────────────────────────────┘
                            │
          ┌─────────────────┴─────────────────┐
          ▼                                   ▼
   OPFS SQLite (local)                 your /api/sync
```

### Startup flow

```
mount → ensureDb()  [+ optional ensureAppSettings()]
         ↓
  db.collections.* ready
         ↓
  reads/writes → outbox
         ↓
  SyncRunner / Sync now → push + pull + replay
```

**Built-in collections** (always present — do not register these ids):

- `db.collections.outbox` — local mutations waiting to upload
- `db.collections.inbox` — server events pulled to this device
- `db.collections.deadletter` — events the server refused permanently, or that ran out of retries
- `db.collections.syncmeta` — single row holding the pull cursor and backend identity
- `db.collections.rowversions` — per-row version index, only written when `conflictDetection` is on

Config knobs (`pullOverlap`, `retry`, …): [docs/usage.md](./docs/usage.md).

## Quick start

Copy the pattern below into `db/collections.ts` (or `data-access-layer/collections.ts` — the folder name does not matter), then wire `ensureDb()` in your app shell.

### 1. Domain types

One type per collection you will register. These are your row shapes in SQLite.

```typescript
type User = {
  id: string;
  name: string;
  email: string;
  createdAt: number;
};

type Todo = {
  id: string;
  userId: string;
  title: string;
  status: "pending" | "complete";
  createdAt: number;
  updatedAt: number;
};

type AppSettings = {
  id: string;
  theme: "light" | "dark";
  language: string;
  syncEnabled: boolean; // persisted preference; mirrored to db.setSyncEnabled()
};
```

### 2. Collection registry + typed DB handle

Keys here must match the `collections` option you pass in step 4. This gives you `db.collections.users`, etc. with full inference.

```typescript
import type { CollectionDef, EventSourcedDB } from "event-sourced-collection";

type AppCollectionDefs = {
  users: CollectionDef<User, string>;
  todos: CollectionDef<Todo, string>;
  settings: CollectionDef<AppSettings, string>;
};

export type AppDb = EventSourcedDB<AppCollectionDefs>;
```

### 3. Sync transport

Implement push/pull against your API. These run during `db.sync()` — upload local outbox rows, download server events.

```typescript
import type { OutboundEvent, PullResponse, PushResponse } from "event-sourced-collection";

const getAccessToken = () => localStorage.getItem("accessToken") ?? "";

async function pushEvents(events: ReadonlyArray<OutboundEvent>): Promise<PushResponse> {
  const response = await fetch("/api/sync/events", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${getAccessToken()}`,
    },
    body: JSON.stringify(events),
  });
  if (!response.ok) throw new Error(`Push failed: ${response.status}`);
  return response.json();
}

async function pullEvents({ since }: { since: number }): Promise<PullResponse> {
  const response = await fetch(`/api/sync/events?since=${since}`, {
    headers: {
      Accept: "application/json",
      Authorization: `Bearer ${getAccessToken()}`,
    },
  });
  if (!response.ok) throw new Error(`Pull failed: ${response.status}`);
  return response.json();
}
```

### 4. Create the database (`collections.ts`)

`collections.ts` is the single source of truth for row types, sync transport, collection registry, indexes, and DB init. Import `db` in components for reads/writes after startup. Call `ensureDb()` (or `ensureAppSettings()`) once at app mount before touching collections.

`createBrowserEventSourcedDB` (from `event-sourced-collection/browser`) wires the SQLite platform, registers collections, and returns a lazy singleton (`ensureDb`) plus a `db` proxy you can import anywhere. **Collections are created here** — there is no separate `createUsersCollection()` call.

The `load` callback is where you import platform packages. Keeping those imports in _your_ app (not inside the library) means you choose `@tanstack/react-db` vs `@tanstack/db`, and SSR bundles stay clean because `load` runs on first `ensureDb()`.

Full reference (matches the example app):

```typescript
import { BasicIndex } from "@tanstack/db";
import { createBrowserEventSourcedDB } from "event-sourced-collection/browser";
import type {
  CollectionDef,
  EventSourcedDB,
  OutboundEvent,
  PullResponse,
  PushResponse,
} from "event-sourced-collection";

// --- Row types (one per collection) ---

export type User = { id: string; name: string; email: string; createdAt: number };
export type Todo = {
  id: string;
  userId: string;
  title: string;
  status: "pending" | "complete";
  createdAt: number;
  updatedAt: number;
};
// Singleton preferences row — use id "app" (see app-settings.ts)
export type AppSettings = {
  id: string;
  theme: "light" | "dark";
  language: string;
  syncEnabled: boolean;
};

type AppCollectionDefs = {
  users: CollectionDef<User, string>;
  todos: CollectionDef<Todo, string>;
  settings: CollectionDef<AppSettings, string>;
};

export type AppDb = EventSourcedDB<AppCollectionDefs>;

// --- Sync transport (runs when sync is enabled) ---

const getAccessToken = () => localStorage.getItem("accessToken") ?? "";

async function pushEvents(events: ReadonlyArray<OutboundEvent>): Promise<PushResponse> {
  const response = await fetch("/api/sync/events", {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${getAccessToken()}` },
    body: JSON.stringify(events),
  });
  if (!response.ok) throw new Error(`Push failed: ${response.status}`);
  return response.json();
}

async function pullEvents({ since }: { since: number }): Promise<PullResponse> {
  const response = await fetch(`/api/sync/events?since=${since}`, {
    headers: { Accept: "application/json", Authorization: `Bearer ${getAccessToken()}` },
  });
  if (!response.ok) throw new Error(`Pull failed: ${response.status}`);
  return response.json();
}

// --- DB init: lazy singleton; db proxy forwards after ensureDb() ---

const { ensureDb, db } = createBrowserEventSourcedDB<AppCollectionDefs>({
  databaseName: "my-app.sqlite",
  debug: import.meta.env.DEV,

  collections: {
    users: {
      getKey: (user: User) => user.id,
      indexes: [{ select: (user: User) => user.id, indexType: BasicIndex, name: "by-id" }],
    },
    todos: {
      getKey: (todo: Todo) => todo.id,
      indexes: [
        { select: (todo: Todo) => todo.id, indexType: BasicIndex, name: "by-id" },
        { select: (todo: Todo) => todo.userId, indexType: BasicIndex, name: "by-user" },
        { select: (todo: Todo) => todo.status, indexType: BasicIndex, name: "by-status" },
        { select: (todo: Todo) => todo.title, indexType: BasicIndex, name: "by-title" },
      ],
    },
    settings: { getKey: (settings: AppSettings) => settings.id },
  },

  syncEnabled: true, // initial default; users can toggle at runtime (see app-settings.ts)
  sync: { pushEvents, pullEvents },

  load: async () => {
    const { createCollection } = await import("@tanstack/react-db");
    const {
      BrowserCollectionCoordinator,
      createBrowserWASQLitePersistence,
      openBrowserWASQLiteOPFSDatabase,
      persistedCollectionOptions,
    } = await import("@tanstack/browser-db-sqlite-persistence");

    return {
      openBrowserWASQLiteOPFSDatabase,
      createBrowserWASQLitePersistence,
      BrowserCollectionCoordinator,
      createCollection,
      persistedCollectionOptions,
    };
  },
});

export { db, ensureDb };
```

The handle is `{ ensureDb, db, close }`:

- `ensureDb()` — runs `load` + setup once (deduped). Throws outside a browser environment.
- `db` — proxy that forwards after `ensureDb()`; throws if used before init.
- `close()` — disposes the engine and SQLite platform; next `ensureDb()` re-initializes.

After setup you get `db.collections.users`, `db.collections.todos`, `db.collections.settings`, plus built-in `outbox` and `inbox` (see [Project structure](#project-structure)).

#### Collection indexes

TanStack DB indexes are **opt-in**. Declare them on each collection in the `collections` registry — the package calls `collection.createIndex(select, { name?, indexType })` for you and keeps them registered across the collection lifecycle (including after SQLite hydration when the collection becomes `ready` again).

**Requirements**

- Import `BasicIndex` from `@tanstack/db` and pass it as `indexType` on every index entry.
- The `select` callback must return the **same field** you filter, join, or `orderBy` on in `useLiveQuery`.
- Use `name` when a collection has more than one index.

**Shape**

```typescript
import { BasicIndex } from "@tanstack/db";
import type { CollectionDef } from "event-sourced-collection";

type SavedMovieRef = { movieId: number; title: string; addedAt: number };

// In your AppCollectionDefs / collections registry:
favorites: CollectionDef<SavedMovieRef, number>;
```

```typescript
collections: {
  todos: {
    getKey: (todo: Todo) => todo.id,
    indexes: [
      { select: (todo: Todo) => todo.id, indexType: BasicIndex, name: "by-id" },
      { select: (todo: Todo) => todo.userId, indexType: BasicIndex, name: "by-user" },
      { select: (todo: Todo) => todo.status, indexType: BasicIndex, name: "by-status" },
    ],
  },

  favorites: {
    getKey: (item: SavedMovieRef) => item.movieId,
    indexes: [{ select: (item) => item.movieId, indexType: BasicIndex, name: "by-movie-id" }],
  },

  settings: { getKey: (settings: AppSettings) => settings.id },
},
```

**Joins** — index the field on the **joined** collection (the right-hand side), not the driving collection:

```typescript
import { eq, useLiveQuery } from "@tanstack/react-db";
import { db } from "./collections";
import { moviesCollection } from "./movies-collection";

// favorites needs an index on movieId because the join is eq(movie.id, favorite.movieId)
const { data } = useLiveQuery((q) =>
  q
    .from({ movie: moviesCollection })
    .leftJoin({ favorite: db.collections.favorites }, ({ movie, favorite }) =>
      eq(movie.id, favorite.movieId),
    )
    .select(({ movie, favorite }) => ({ movie, isFavorite: favorite !== undefined })),
);
```

Without that index, TanStack DB logs a warning and falls back to loading the entire `favorites` collection for each join.

**Filters and sort** — match indexes to your queries:

| Query pattern                       | Index `select`          |
| ----------------------------------- | ----------------------- |
| `eq(todo.userId, userId)`           | `(todo) => todo.userId` |
| `eq(todo.status, "pending")`        | `(todo) => todo.status` |
| `orderBy(({ todo }) => todo.title)` | `(todo) => todo.title`  |

**What you do not need**

- Do not call `collection.createIndex()` yourself after `ensureDb()` when using this package — declare indexes in `collections` instead.
- Do not enable `autoIndex: 'eager'` for collections created here; explicit `indexes` + `BasicIndex` is the supported pattern.

**Debugging** — after `await ensureDb()`, check that indexes exist:

```typescript
db.collections.favorites.getIndexMetadata();
// non-empty array means indexes are registered
```

### 5. App settings (`app-settings.ts`)

Seed a singleton settings row and keep `syncEnabled` in sync with `db.setSyncEnabled()`:

```typescript
import type { AppSettings } from "./collections";
import { db, ensureDb } from "./collections";

export const APP_SETTINGS_ID = "app";

const DEFAULT_APP_SETTINGS: AppSettings = {
  id: APP_SETTINGS_ID,
  theme: "dark",
  language: "en",
  syncEnabled: true,
};

export async function ensureAppSettings(): Promise<AppSettings> {
  const database = await ensureDb();
  const existing = database.collections.settings.get(APP_SETTINGS_ID);

  if (!existing) {
    await database.collections.settings.insert(DEFAULT_APP_SETTINGS).isPersisted.promise;
    database.setSyncEnabled(DEFAULT_APP_SETTINGS.syncEnabled);
    return DEFAULT_APP_SETTINGS;
  }

  const syncEnabled = existing.syncEnabled ?? true;

  if (existing.syncEnabled === undefined) {
    await database.collections.settings.update(APP_SETTINGS_ID, (draft) => {
      draft.syncEnabled = true;
    }).isPersisted.promise;
  }

  database.setSyncEnabled(syncEnabled);
  return { ...existing, syncEnabled };
}

export async function setSyncEnabled(enabled: boolean): Promise<void> {
  const database = await ensureDb();
  database.setSyncEnabled(enabled);

  const existing = database.collections.settings.get(APP_SETTINGS_ID);
  if (!existing) {
    await database.collections.settings.insert({ ...DEFAULT_APP_SETTINGS, syncEnabled: enabled })
      .isPersisted.promise;
    return;
  }

  await database.collections.settings.update(APP_SETTINGS_ID, (draft) => {
    draft.syncEnabled = enabled;
  }).isPersisted.promise;
}
```

### 6. Wire the app shell

Call `ensureAppSettings()` once when the app mounts (it calls `ensureDb()` internally):

```typescript
import { useEffect, useState } from "react";
import { ensureAppSettings } from "./data-access-layer/app-settings";
import { useEventSourcedSync } from "./hooks/common/use-event-sourced-sync";
import { useSyncEnabled } from "./hooks/common/use-sync-enabled";

function AppShell() {
  const [dbReady, setDbReady] = useState(false);

  useEffect(() => {
    void ensureAppSettings().then(() => setDbReady(true));
  }, []);

  const syncEnabled = useSyncEnabled(dbReady);
  useEventSourcedSync(dbReady && syncEnabled);

  if (!dbReady) return <Loading />;
  return <YourRoutes />;
}
```

Optional sync helpers:

```typescript
// sync-events.ts
import { ensureDb } from "./collections";

export async function manualSyncEvents() {
  const database = await ensureDb();
  return database.manualSync();
}

// use-event-sourced-sync.ts — polls manualSync when enabled
export function useEventSourcedSync(enabled: boolean) {
  return useQuery({
    queryKey: ["sync"],
    queryFn: () => manualSyncEvents(),
    enabled,
    refetchInterval: 60_000,
  });
}

// use-sync-enabled.ts — live-query settings.syncEnabled for UI
export function useSyncEnabled(dbReady: boolean) {
  const { data = [] } = useLiveQuery(
    (query) =>
      query
        .from({ setting: db.collections.settings })
        .where(({ setting }) => eq(setting.id, APP_SETTINGS_ID)),
    [dbReady],
  );
  if (!dbReady) return true;
  return data[0]?.syncEnabled ?? true;
}
```

Settings UI toggle (calls `setSyncEnabled` from `app-settings.ts`):

```typescript
import { setSyncEnabled } from "@/data-access-layer/app-settings";
import { useSyncEnabled } from "@/hooks/common/use-sync-enabled";

function SettingsPage() {
  const syncEnabled = useSyncEnabled(true);

  return (
    <Switch
      checked={syncEnabled}
      onCheckedChange={(checked) => void setSyncEnabled(checked)}
    />
  );
}
```

Disable manual **Sync now** when sync is off: `const syncEnabled = useSyncEnabled(true)` and pass it to your button's `disabled` prop.

### 7. Write data

Use collections like normal TanStack DB. Mutations are logged to the event store automatically.

```typescript
import { db } from "./data-access-layer/collections";

const userId = crypto.randomUUID();

db.collections.users.insert({
  id: userId,
  name: "Alice",
  email: "alice@example.com",
  createdAt: Date.now(),
});

db.collections.todos.insert({
  id: crypto.randomUUID(),
  userId,
  title: "Buy groceries",
  status: "pending",
  createdAt: Date.now(),
  updatedAt: Date.now(),
});

db.collections.settings.insert({
  id: "app",
  theme: "dark",
  language: "en",
  syncEnabled: true,
});

db.collections.todos.update("todo-id", (draft) => {
  draft.status = "complete";
  draft.updatedAt = Date.now();
});

db.collections.todos.delete("todo-id");
```

For `settings`, use a fixed id (e.g. `"app"`) as a singleton row. Prefer `ensureAppSettings()` at startup instead of inserting manually:

```typescript
db.collections.settings.update("app", (draft) => {
  draft.theme = "light";
});
```

### 8. Read data

```typescript
import { useLiveQuery } from "@tanstack/react-db";
import { db } from "./data-access-layer/collections";

function TodoList() {
  const { data: todos = [] } = useLiveQuery((q) =>
    q.from({ todo: db.collections.todos }),
  );

  const { data: users = [] } = useLiveQuery((q) =>
    q.from({ user: db.collections.users }),
  );

  const { data: settings = [] } = useLiveQuery((q) =>
    q.from({ setting: db.collections.settings }),
  );

  return (
    <ul>
      {todos.map((todo) => (
        <li key={todo.id}>
          {todo.title} — {users.find((u) => u.id === todo.userId)?.name}
        </li>
      ))}
    </ul>
  );
}
```

### 9. Sync

```typescript
import { ensureDb } from "./data-access-layer/collections";

const db = await ensureDb();
await db.sync();

window.addEventListener("online", () => void db.sync());
```

Or use a periodic hook (see step 6). Respect `syncEnabled` — background sync should stay off when the user opts out.

### 10. Sync from a Web Worker (keep the UI thread free)

Browser SQLite I/O is already off the main thread: `@tanstack/browser-db-sqlite-persistence` opens OPFS inside a dedicated worker. What can still hitch React is your **sync transport** — `JSON.stringify` / `JSON.parse` on large outbox/inbox batches, plus waiting on `fetch` callbacks that resume on the main thread.

The practical pattern: keep the event-sourced DB on the main thread (so `useLiveQuery` stays reactive), and move **only** `push` / `pull` network + JSON work into a Dedicated Worker via a `SyncTransport`.

Copy-paste reference: [`examples/web-worker-sync/`](./examples/web-worker-sync/).

**`sync.worker.ts`** — runs fetch + JSON off the UI thread:

```typescript
/// <reference lib="webworker" />
import type { OutboundEvent, PullResponse, PushResponse } from "event-sourced-collection";

type PushMsg = {
  id: string;
  type: "push";
  url: string;
  headers: Record<string, string>;
  events: ReadonlyArray<OutboundEvent>;
};

type PullMsg = {
  id: string;
  type: "pull";
  url: string;
  headers: Record<string, string>;
  since: number;
};

type Request = PushMsg | PullMsg;
type Success = { id: string; ok: true; result: PushResponse | PullResponse };
type Failure = { id: string; ok: false; error: string };

self.onmessage = async (event: MessageEvent<Request>) => {
  const msg = event.data;

  try {
    if (msg.type === "push") {
      const response = await fetch(msg.url, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...msg.headers },
        body: JSON.stringify(msg.events),
      });
      if (!response.ok) {
        throw new Error(`Push failed with HTTP ${response.status}`);
      }
      const result = (await response.json()) as PushResponse;
      const reply: Success = { id: msg.id, ok: true, result };
      self.postMessage(reply);
      return;
    }

    const pullUrl = new URL(msg.url, self.location.origin);
    pullUrl.searchParams.set("since", String(msg.since));

    const response = await fetch(pullUrl, {
      headers: { Accept: "application/json", ...msg.headers },
    });
    if (!response.ok) {
      throw new Error(`Pull failed with HTTP ${response.status}`);
    }

    const result = (await response.json()) as PullResponse;
    const reply: Success = { id: msg.id, ok: true, result };
    self.postMessage(reply);
  } catch (error) {
    const reply: Failure = {
      id: msg.id,
      ok: false,
      error: error instanceof Error ? error.message : "Sync worker error",
    };
    self.postMessage(reply);
  }
};

export {};
```

**`create-worker-sync-transport.ts`** — bridges `postMessage` into `SyncTransport`:

```typescript
import type {
  OutboundEvent,
  PullResponse,
  PushResponse,
  SyncTransport,
} from "event-sourced-collection";

type WorkerSuccess = { id: string; ok: true; result: PushResponse | PullResponse };
type WorkerFailure = { id: string; ok: false; error: string };

export function createWorkerSyncTransport(options: {
  worker: Worker;
  pushUrl: string;
  pullUrl: string;
  getHeaders?: () => Record<string, string> | Promise<Record<string, string>>;
}): SyncTransport {
  const pending = new Map<
    string,
    {
      resolve: (value: PushResponse | PullResponse) => void;
      reject: (reason: unknown) => void;
    }
  >();

  options.worker.addEventListener(
    "message",
    (event: MessageEvent<WorkerSuccess | WorkerFailure>) => {
      const msg = event.data;
      const entry = pending.get(msg.id);
      if (!entry) return;
      pending.delete(msg.id);
      if (msg.ok) entry.resolve(msg.result);
      else entry.reject(new Error(msg.error));
    },
  );

  function callWorker<T extends PushResponse | PullResponse>(
    payload: Record<string, unknown>,
  ): Promise<T> {
    return new Promise((resolve, reject) => {
      const id = crypto.randomUUID();
      pending.set(id, {
        resolve: resolve as (value: PushResponse | PullResponse) => void,
        reject,
      });
      options.worker.postMessage({ id, ...payload });
    });
  }

  return {
    async push(events: ReadonlyArray<OutboundEvent>) {
      if (events.length === 0) return { confirmed: [] };
      const headers = (await options.getHeaders?.()) ?? {};
      return callWorker<PushResponse>({
        type: "push",
        url: options.pushUrl,
        headers,
        events,
      });
    },
    async pull(since: number) {
      const headers = (await options.getHeaders?.()) ?? {};
      return callWorker<PullResponse>({
        type: "pull",
        url: options.pullUrl,
        headers,
        since,
      });
    },
  };
}
```

**Wire it in `collections.ts`** (Vite / modern bundlers):

```typescript
import { createBrowserEventSourcedDB } from "event-sourced-collection/browser";
import { createWorkerSyncTransport } from "./create-worker-sync-transport";

const syncWorker = new Worker(new URL("./sync.worker.ts", import.meta.url), {
  type: "module",
});

const sync = createWorkerSyncTransport({
  worker: syncWorker,
  pushUrl: "/api/sync/events",
  pullUrl: "/api/sync/events",
  getHeaders: () => ({
    Authorization: `Bearer ${localStorage.getItem("accessToken") ?? ""}`,
  }),
});

const { ensureDb, db } = createBrowserEventSourcedDB({
  databaseName: "my-app.sqlite",
  collections: {/* ... */},
  sync, // worker-backed transport instead of inline pushEvents/pullEvents
  load: async () => {
    /* same as before */
  },
});
```

Call sync the same way as before — `db.sync()`, `db.manualSync()`, or `useManualSync({ sync: () => ensureDb().then((d) => d.manualSync()) })`. The worker only owns the HTTP round-trip; outbox confirmations and inbox replay still apply on the main-thread DB so live queries update immediately.

**What this does / does not move**

| Work                                           | Where it runs                            |
| ---------------------------------------------- | ---------------------------------------- |
| SQLite / OPFS                                  | TanStack's OPFS worker (already)         |
| `fetch` + JSON encode/decode                   | Your Dedicated Worker (this example)     |
| Outbox → confirmed / inbox → `acceptMutations` | Main thread (keeps collections reactive) |

If inbox replay of huge batches still janks the UI, chunk work with `scheduler.yield()` (or similar) around your own UI updates — do **not** open a second OPFS writer from another worker against the same database file.

## Inspecting sync state

### `getSyncStatus()` / `subscribeSyncStatus()`

For the common "am I synced?" question, use the built-in status rather than querying the log yourself:

```typescript
const database = await ensureDb();

database.getSyncStatus();
// {
//   isSyncing: false, isSynced: false, pendingCount: 3, failedCount: 1,
//   deadLetterCount: 0, pullCursor: 128, backendId: "…",
//   lastSyncAt: 1730000000000, lastError: null
// }

const unsubscribe = database.subscribeSyncStatus((status) => {
  setBadge(status.pendingCount);
});
```

The listener fires immediately with the current status, then on every outbox, dead-letter, or sync-metadata change.

### Querying the log directly

`outbox`, `inbox`, and `deadletter` are ordinary queryable collections.

- **outbox** — `sync: false` until the event has been pushed successfully. `sync: true` means the server accepted it (and assigned a `globalSeq`). Rows also carry `syncStatus`, `attemptCount`, `lastAttemptAt`, `nextAttemptAt` (when a retry is scheduled), `lastError`, `lastErrorCode`, and `retryable`, plus `clientId`, `txId` (shared by every event from one transaction), `schemaVersion`, `baseVersion`, and `previous` (row state before the mutation, `null` for inserts).
- **inbox** — `sync: false` until the event has been resolved. `sync: true` means resolved: either applied to the relevant collection, or deliberately skipped. Skipped rows carry `skipped: true` and a human-readable `skipReason`.
- **deadletter** — events that will never be retried automatically. Each row records a `direction` (`outbound` for events this device authored, `inbound` for events pulled from the server), a `reason` (`rejected`, `maxAttemptsExceeded`, `conflict`, `replayFailed`, or `manual`), the `message` and `code`, and the `attemptCount` it died at.

```typescript
import { useLiveQuery } from "@tanstack/react-db";
import { eq } from "@tanstack/db";
import { db } from "./data-access-layer/collections";

function SyncStatus() {
  const { data: unpushed = [] } = useLiveQuery((q) =>
    q.from({ e: db.collections.outbox }).where(({ e }) => eq(e.sync, false)),
  );

  const { data: rejected = [] } = useLiveQuery((q) =>
    q.from({ e: db.collections.deadletter }),
  );

  return (
    <p>
      {unpushed.length} pending upload · {rejected.length} rejected
    </p>
  );
}
```

### Dead-letter queue

Events land here from both directions, so that a single event nothing can be done with never blocks the ones queued behind it.

- **Outbound** (`direction: "outbound"`) — the server rejected it as non-retryable, it exhausted `retry.maxAttempts`, or it lost a conflict check. It leaves the outbox at that point.
- **Inbound** (`direction: "inbound"`, `reason: "replayFailed"`) — applying a pulled event to its collection kept throwing, for instance because it violates a local constraint. The event is retried on `retry.maxAttempts` separate syncs first; only then is it parked and the pull cursor allowed to move past it.

```typescript
await database.retryDeadLetter(); // requeue everything with a fresh retry budget
await database.retryDeadLetter(eventId); // requeue one
await database.discardDeadLetter(eventId); // drop it permanently
```

`retryDeadLetter` routes each event back where it came from: outbound events return to the outbox to be pushed again, inbound events return to the inbox and are replayed immediately.

Surface these in your UI — outbound rows are user work that will otherwise be silently lost, and inbound rows are remote changes this device is missing.

### Pruning the log

The outbox and inbox grow forever unless you compact them. The pull cursor lives in `syncmeta`, not in the inbox, so pruning is safe:

```typescript
// Drop confirmed outbox rows and resolved inbox rows older than a week.
await database.pruneSyncedEvents({ olderThanMs: 7 * 24 * 60 * 60 * 1000 });
```

Only rows with `sync: true` are eligible, so nothing pending is ever discarded. `keepLast` retains the most recent N eligible rows if you want a debugging window. Pruning the log does not touch replayed collection state.

## How it works

| Step                 | What happens                                                                                                     |
| -------------------- | ---------------------------------------------------------------------------------------------------------------- |
| Register collections | `collections: { users, todos, settings }` in `createBrowserEventSourcedDB` (or low-level `createEventSourcedDB`) |
| Access them          | `db.collections.users`, etc.                                                                                     |
| Write data           | `.insert()`, `.update()`, `.delete()`                                                                            |
| Log mutations        | Each write appends a row to `db.collections.outbox` (`sync: false`)                                              |
| Read data            | `useLiveQuery` against those collections                                                                         |
| Sync                 | `await db.sync()` pushes the outbox and pulls into the inbox                                                     |

On push, confirmed outbox rows flip to `sync: true` with their server `globalSeq`. Rejections are either scheduled for retry with exponential backoff or moved to `deadletter`. On pull, new server events are written to `inbox` (`sync: false`), replayed into the target collection via `acceptMutations`, then flipped to `sync: true`. A replay that throws is retried on later syncs and eventually dead-lettered, so it cannot wedge the events behind it. The client's own events come back on pull and are written to `inbox` as already applied — matched by `eventId`, or by `clientId` if the outbox row has already been pruned. Both the pull cursor and this device's `clientId` are persisted in the `syncmeta` row, so the inbox and outbox can be pruned without losing your place or your identity.

Overlapping `sync()` / `manualSync()` calls are serialized internally, so firing sync from a reactive effect on every outbox insert cannot push the same pending events twice. In the browser, a Web Lock additionally elects one tab to sync at a time; the others return `{ deferred: true }` immediately rather than duplicating the work.

Pushes are chunked at `pushBatchSize` (default 100) and each batch's outcome is persisted before the next is sent, so a connection dropping part-way through a large backlog keeps the progress already made. Batches never split a `txId` — the server is asked to commit each transaction atomically, which it cannot do if half of one arrives in a later request. A single transaction larger than `pushBatchSize` is sent whole rather than split.

Step by step:

1. User mutates `db.collections.todos` → row appended to local **outbox** (`sync: false`).
2. `manualSync()` or background hook pushes outbox → `POST /api/sync/events`.
3. Server assigns `globalSeq` → outbox rows flip to `sync: true`.
4. Pull fetches newer events → `GET /api/sync/events?since=<cursor>`.
5. Remote events land in **inbox**, replay into collections, flip to `sync: true`.

## Architecture Deep Dive

This section explains the internals — how the package wraps TanStack DB, how the event-sourcing pipeline works end-to-end, and why it exists.

### Relationship to TanStack DB Persistence

TanStack DB's persistence layer gives you three primitives:

- `createCollection()` — a reactive in-memory collection
- `persistedCollectionOptions()` — wires a collection to SQLite so data survives reloads
- `PersistedCollectionPersistence` — the SQLite persistence instance (OPFS, React Native, Node)

That is **storage**, not **sync**. Out of the box you get durable local state but no event log, no outbox, no push/pull, no conflict detection, and no dead-letter handling.

This package wraps those three primitives by **injecting them** into its factory rather than importing them directly:

```typescript
const db = await createEventSourcedDB({
  persistence: platform.persistence, // SQLiteDriver
  createCollection: modules.createCollection, // from @tanstack/db
  persistedCollectionOptions: modules.persistedCollectionOptions, // from platform package
  collections: { todos: { getKey: (t) => t.id } },
  sync: { push: "/api/events", pull: "/api/events" },
});
```

Platform adapters (`createBrowserEventSourcedDB`, `createReactNativeEventSourcedDB`) supply these automatically via the `load()` callback. The core library never imports platform code, keeping it tree-shakeable and testable.

### What This Adds Over Default TanStack DB Persistence

| Concern                   | TanStack DB default                                | event-sourced-collection                                                                                            |
| ------------------------- | -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| Local persistence         | SQLite (you configure per collection)              | Same, wired automatically for all declared collections                                                              |
| Event log                 | None — mutations applied directly                  | Every mutation captured as a durable outbox event                                                                   |
| Sync                      | Bring your own (Electric, PowerSync, etc.) or none | Built-in push/pull with configurable transport                                                                      |
| Offline support           | Data survives reload, but no sync queue            | Full outbox queue — mutations accumulate offline and push when connectivity returns                                 |
| Conflict detection        | None                                               | Optional `baseVersion` stamping + server-side `CONFLICT` rejection                                                  |
| Dead-letter queue         | None                                               | Events that can't make progress are parked with full diagnostics                                                    |
| Schema evolution          | None                                               | `eventSchemaVersion` + `upcastEvent` migrator for on-the-wire format changes                                        |
| Cross-tab coordination    | None by default                                    | Web Locks prevent concurrent syncs                                                                                  |
| Backend identity tracking | Not handled                                        | Detects backend swaps and can reset cursor + requeue history                                                        |
| Lifecycle hooks           | `onInsert`/`onUpdate`/`onDelete`                   | Full lifecycle: `onReady`, `onSyncStart`, `onSyncComplete`, `onEventPushed`, `onEventApplied`, `onDeadLetter`, etc. |
| Retry semantics           | None                                               | Configurable exponential backoff with transaction-aware batching                                                    |
| Observability             | None                                               | Structured logger + `SyncStatus` subscription (`isSyncing`, `pendingCount`, etc.)                                   |

The short version: TanStack DB persistence gives you "data survives a page reload." This package gives you "data syncs reliably across devices even when the network is flaky."

### The Event-Sourcing Pattern

Every local mutation becomes a **durable, replayable event**. The system never "just writes a row" — it appends an event that describes the mutation, and that event is the source of truth for sync.

```
┌─────────────────────────────────────────────────────────────┐
│  Your app: db.collections.todos.insert(...)                 │
└───────────────────────────┬─────────────────────────────────┘
                            │ onInsert hook fires
                            ▼
┌───────────────────────────────────────────────────────────┐
│  outbox.insert(OutboxEntry) — event persisted in SQLite   │
└───────────────────────────┬───────────────────────────────┘
                            │ sync() called (timer / manual)
                            ▼
┌────────────────────────────────────────────────────────────┐
│  Push: batch by txId → transport.push(events) → server    │
│  ← server returns { confirmed, failed }                   │
│  → mark synced / backoff / dead-letter                    │
└───────────────────────────┬────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────┐
│  Pull: transport.pull(cursor) → ServerEvent[]             │
│  → skip local-origin → insert inbox → replay             │
└───────────────────────────┬────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────┐
│  Replay: upcast → collection.utils.acceptMutations()      │
│  → row version recorded → inbox resolved → cursor advance │
└────────────────────────────────────────────────────────────┘
```

### Reserved Internal Collections

Five internal collections live alongside your user collections in the same SQLite database:

| Collection    | Purpose                                                                                                  |
| ------------- | -------------------------------------------------------------------------------------------------------- |
| `outbox`      | Local mutations waiting to be pushed upstream                                                            |
| `inbox`       | Remote events pulled from the server, awaiting or after replay                                           |
| `deadletter`  | Events that permanently failed (rejected, max retries, conflicts, replay failures)                       |
| `syncmeta`    | Pull cursor position, backend identity, and stable client ID                                             |
| `rowversions` | Per-row version tracking for optimistic conflict detection (only written when `conflictDetection: true`) |

All five use the same `persistedCollectionOptions` → SQLite pipeline as your user collections. They are created internally by the factory; you never register them yourself.

### Mutation Capture (the Write Path)

When you call `.insert()`, `.update()`, or `.delete()` on a user collection, the `persistedCollectionOptions` call was wired with `onInsert`/`onUpdate`/`onDelete` hooks. These hooks:

1. Generate a `uuidv7`-based `eventId` and a `txId` (shared across all mutations in one transaction)
2. Capture the full row payload, the previous state (for updates/deletes), `clientId`, `schemaVersion`, and optionally `baseVersion`
3. Insert an `OutboxEntry` with `sync: false, syncStatus: "pending", attemptCount: 0`
4. Optionally record the new row version in `rowversions` (when `conflictDetection` is on)

The mutation is applied to the local collection immediately (optimistic), and the event is durably persisted in SQLite — even if the app crashes before sync.

### Push Pipeline (Outbox → Server)

The push pipeline (`src/internal/push.ts`) runs during `sync()` or `manualSync()`:

1. **Filter due events** — only events where `sync === false`, the backoff window has passed (`nextAttemptAt <= now`), and it's marked retryable (or never attempted)
2. **Batch by transaction** — events sharing a `txId` are never split across requests. A single transaction larger than `pushBatchSize` is sent whole
3. **Send the batch** — calls `transport.push(events)`
4. **Handle confirmations** — marks each confirmed event `sync: true, syncStatus: "synced"` with the assigned `globalSeq`
5. **Handle failures** — routes each failure through the retry/dead-letter decision (see below)
6. **Persist between batches** — each batch's outcome is saved before the next is sent, so a connection drop doesn't lose progress

### Pull Pipeline (Server → Inbox → Replay)

The pull pipeline (`src/internal/pull.ts`) runs after push:

1. **Read the cursor** from `syncmeta`, rewound by `pullOverlap`
2. **Fetch pages** from the server until `hasMore === false`
3. **Check backend identity** — detects wiped/swapped databases via `backendId`
4. **For each event:**
   - Skip if local-origin (see below)
   - Skip if already resolved in inbox
   - Insert into inbox if new
   - **Replay** into the target collection via `acceptMutations()`
   - Mark inbox row as resolved; advance cursor

### Replay (Applying Remote Events)

The replay engine (`src/internal/replay.ts`) applies pulled events into user collections:

1. **Validates** the target collection exists and isn't reserved
2. **Upcasts** events from older `schemaVersion` via the optional `upcastEvent` function
3. **Calls `collection.utils.acceptMutations()`** — TanStack DB's internal API that applies mutations _without_ triggering the collection's `onInsert`/`onUpdate`/`onDelete` hooks. This is the critical piece: remote events enter the collection without echoing back into the outbox
4. **Records the row version** if `conflictDetection` is enabled
5. **On failure**: increments attempt count; after `maxReplayAttempts`, dead-letters the event so the cursor can advance past it

### How Local Events Don't Echo Back

Your events _are_ pulled back from the server — the server doesn't filter them. The filtering happens **client-side** during pull using a two-pronged check:

```typescript
function isLocalOrigin(event, outbox, clientId): boolean {
  return outbox.has(event.eventId) || event.clientId === clientId;
}
```

**Prong 1: `outbox.has(event.eventId)`** — the event's unique ID is still in the outbox, so it's recognized as local.

**Prong 2: `event.clientId === clientId`** — after the outbox has been pruned and the `eventId` is gone, the `clientId` stamped on the event still matches this device's stable identity.

When either matches, the inbox row is marked as resolved (so the cursor advances) but no replay happens — the data already exists locally from the original optimistic mutation.

### Cursor Overlap (Guarding Against Out-of-Order Sequences)

The pull cursor is a `globalSeq` number — the highest sequence position the client has processed. The problem: databases like Postgres with `BIGSERIAL` allocate sequence numbers _before_ a transaction commits. Two concurrent inserts can take sequences 100 and 101 and commit in reverse order. A client that pulls in that window sees 101, advances past 100, and **misses event 100 forever**.

The `pullOverlap` setting rewinds the cursor before each pull:

```typescript
let cursor = Math.max(0, readPullCursor(syncmeta, inbox) - pullOverlap);
```

With `pullOverlap: 5` and a saved cursor of 101, the client asks for events since 96. Events 97–101 are re-received but are idempotent no-ops (already resolved in the inbox). The cost is a few extra skipped events per pull; the benefit is you never permanently miss a late-committing event.

Prefer fixing this server-side (advisory locks, `xmin` ceiling) and use overlap as belt-and-suspenders. See the [Server contract](#server-contract) section.

### Retry Budget and Dead-Letter Conditions

The retry budget is stored **directly on the outbox/inbox entry itself**, persisted in SQLite. Each `OutboxEntry` carries:

```typescript
{
  attemptCount: number; // how many times we've tried
  lastAttemptAt: number | null;
  nextAttemptAt: number | null; // earliest time this may be retried (backoff)
  lastError: string | null;
  lastErrorCode: string | null;
  retryable: boolean | null;
  syncStatus: "pending" | "synced" | "failed";
}
```

The backoff schedule doubles from `baseDelayMs` (default 1s) each attempt, capped at `maxDelayMs` (default 5min):

```typescript
delay = (baseDelayMs * 2) ^ (attemptCount - 1); // capped at maxDelayMs
```

Because `attemptCount` is persisted in SQLite, the budget survives page reloads, app restarts, and tab switches.

An event is dead-lettered under three conditions:

| Condition                      | Trigger                                                                 | Dead-letter reason                                   |
| ------------------------------ | ----------------------------------------------------------------------- | ---------------------------------------------------- |
| Non-retryable server rejection | Server returns `retryable !== true`                                     | `"rejected"` (or `"conflict"` if code is `CONFLICT`) |
| Retry budget exhausted         | `attemptCount >= maxAttempts` after a retryable failure                 | `"maxAttemptsExceeded"`                              |
| Inbound replay failure         | Remote event's `acceptMutations` keeps throwing for `maxAttempts` syncs | `"replayFailed"`                                     |

Non-retryable rejections are dead-lettered immediately — no retry budget is spent. One rejection = dead letter. This prevents wasting attempts on events the server will never accept.

### Conflict Detection and Row Version Rebasing

When `conflictDetection: true`, the system maintains a per-row version index in the `rowversions` collection. Here's the lifecycle:

**On local mutation:**

1. The mutation hook reads the current row version from `rowversions` (e.g. `"ev-42"`)
2. That version is stamped on the outbox event as `baseVersion: "ev-42"` — telling the server "I wrote against this version; reject me if the row has moved"
3. The local `rowversions` entry is immediately updated to the new event's ID (`"ev-99"`) — an optimistic assumption that this event will succeed

**On server confirmation:** Nothing extra — `"ev-99"` was already recorded locally.

**On conflict rejection:**

1. The server returns `code: "CONFLICT"` (the row was modified by another client)
2. The event is dead-lettered
3. `restoreRowVersionAfterConflict` **rolls back** the row version to the `baseVersion` (`"ev-42"`) that the rejected mutation was authored against

```typescript
// If baseVersion was null (new row), delete the rowversions entry entirely
// Otherwise, rewind to the last known-good version
await recordRowVersion(rowversions, collectionId, key, baseVersion, null);
```

This prevents a single rejected event from poisoning every subsequent write to that row. Without the rollback, future mutations would reference the rejected event's ID as their base — a version the server never accepted — causing cascading conflicts.

**On replay of remote events:** The row version is updated to the replayed event's `eventId`, so future local mutations against that row reference the version the server actually has.

### When Conflict Detection Matters (Multiplayer)

The conflict detection + row version + rebase mechanism is only relevant when **multiple writers can touch the same row concurrently**:

- Multiple users editing shared data (collaborative apps)
- Same user with the app open on multiple devices, both offline, both mutating the same row
- Background processes or admin tools modifying rows that users are also editing

In a single-device, single-user scenario, your outbox events are strictly serialized. No other client races to modify the same row, so the server will never see a stale `baseVersion`. That's why the feature is **off by default** — it adds a write to `rowversions` on every mutation and every replay.

Without conflict detection, last-write-wins silently. With it, the second write is rejected, the client dead-letters it (or surfaces it to the user), and the row version rewinds so the next write is authored against the actual server state.

### Sync Transport Normalization

The sync layer (`src/sync.ts`) normalizes three configuration styles into a unified `{ push, pull }` interface:

1. **URL strings** — `{ push: "/api/events", pull: "/api/events" }` — uses built-in HTTP adapter with automatic header resolution
2. **Handler functions** — `{ pushEvents: fn, pullEvents: fn }` — your own async functions
3. **Raw `SyncTransport`** — `{ push: fn, pull: fn }` — lowest-level, functions-only

You can mix-and-match (e.g. `pushEvents` function + `pullUrl` string). Functions take precedence over URLs when both are set for the same direction. If neither push nor pull is configured, the DB is fully offline (mutations still accumulate in the outbox).

### Platform Adapters

Platform adapters handle the platform-specific wiring so the core stays portable:

**Browser** (`src/platforms/browser-event-sourced-db.ts`):

- Lazy-loads WASM SQLite via the `load()` callback (keeps it off the critical path)
- Sets up OPFS persistence + collection coordinator for multi-tab support
- Installs Web Locks so only one tab syncs at a time
- Returns a `{ db, ensureDb, close }` lazy singleton with a Proxy that throws before initialization

**React Native** (`src/platforms/react-native-event-sourced-db.ts`):

- Same pattern but with React Native SQLite driver
- No Web Locks (single-process environment)

The `load()` callback is yours — you decide which version of `createCollection` (plain `@tanstack/db` vs `@tanstack/react-db`) and which persistence package to import. This keeps SSR bundles clean and lets you control code-splitting.

## Server contract

### `POST /api/sync/events`

Request: array of outbound events (`eventId`, `collectionId`, `type`, `key`, `payload`, `previous`, `txId`, `clientId`, `schemaVersion`, `baseVersion`, `timestamp`).

Requirements on the server:

- **Deduplicate by `eventId`.** Events can be pushed more than once (retry after a timeout, a second tab syncing). A unique constraint plus returning the existing `globalSeq` is enough.
- **Apply each `txId` atomically.** Events sharing a `txId` came from one client transaction and should commit or fail together.
- **Set `retryable` honestly.** `retryable: true` means the client backs off and tries again; anything else moves the event to the dead-letter queue. Use `true` for overload and transient faults, `false` for validation and authorization failures.
- **Honour `baseVersion` if you enable conflict detection.** When it is non-null and does not match the `eventId` of your newest event for that row, reject with code `CONFLICT`.
- **Avoid the `BIGSERIAL` visibility gap** on push (advisory lock) and/or pull (`xmin`). See below and [`examples/postgres-sync-server/`](./examples/postgres-sync-server/).

Copy-paste reference (why each step exists is commented in the file):

- [`examples/postgres-sync-server/handlers.ts`](./examples/postgres-sync-server/handlers.ts)
- [`examples/postgres-sync-server/schema.sql`](./examples/postgres-sync-server/schema.sql)

Response:

```json
{
  "confirmed": [{ "eventId": "...", "globalSeq": 100 }],
  "failed": [
    {
      "eventId": "...",
      "message": "Validation failed",
      "code": "VALIDATION_ERROR",
      "retryable": false
    }
  ]
}
```

### `GET /api/sync/events?since={globalSeq}`

Response:

```json
{
  "events": [
    {
      "globalSeq": 102,
      "eventId": "...",
      "collectionId": "todos",
      "type": "insert",
      "key": "...",
      "payload": {},
      "schemaVersion": 1,
      "timestamp": 0,
      "cursor": "102"
    }
  ],
  "cursor": "102",
  "hasMore": false,
  "backendId": "6f1c…"
}
```

`backendId` is optional but strongly recommended. It is a stable identifier for the event store itself. Without it, wiping or swapping the server restarts `global_seq` at 1 while clients keep asking for events after, say, 500 — so they silently never pull again. With it, the client notices the change and re-pulls from zero. See `backendMismatch` below.

Minimal PostgreSQL schema:

```sql
CREATE TABLE events (
  global_seq        BIGSERIAL PRIMARY KEY,
  event_id          TEXT NOT NULL UNIQUE,
  collection_id     TEXT NOT NULL,
  type              TEXT NOT NULL CHECK (type IN ('insert', 'update', 'delete')),
  key               TEXT NOT NULL,
  payload           JSONB NOT NULL,
  previous          JSONB,
  tx_id             TEXT NOT NULL,
  client_id         TEXT NOT NULL,
  schema_version    INTEGER NOT NULL DEFAULT 1,
  client_timestamp  BIGINT NOT NULL
);

CREATE INDEX idx_events_global_seq ON events (global_seq);

-- Backs the conflict check: newest event for a given row.
CREATE INDEX idx_events_row ON events (collection_id, key, global_seq DESC);

-- Identity of this event store; regenerated whenever the database is recreated.
CREATE TABLE sync_backend (
  id          INTEGER PRIMARY KEY,
  backend_id  TEXT NOT NULL
);
```

### Important: `BIGSERIAL` can skip events (and UUIDv7 does not fix it)

`BIGSERIAL` allocates a value _before_ the transaction commits. Two concurrent
inserts can take 50 and 51 and commit in the opposite order. A client that pulls
in that window sees 51, advances past 50, and **never receives event 50**.

This is about **concurrent transactions on one server**, not about “multiple
devices.” Many phones → one Postgres is still a single sequence authority and is
fine. Multi-device traffic just makes the race more likely.

**Do not switch the pull cursor to UUIDv7 hoping to fix this.** Client
`eventId` is already UUIDv7 (uniqueness / dedup). Server order is a different
job. Minting a sortable UUID before commit has the **same** allocate-before-commit
gap; client-minted UUIDs also suffer clock skew across devices.

**Production handlers (annotated):** see
[`examples/postgres-sync-server/`](./examples/postgres-sync-server/) — advisory
lock on push, `xmin` ceiling on pull, `eventId` dedup, atomic `txId`,
`CONFLICT` / `retryable`, and `backendId`.

Minimal pattern on push:

```sql
BEGIN;
  -- One writer at a time so pullers don't jump over a seq that hasn't committed yet
  SELECT pg_advisory_xact_lock(hashtext('events_seq'));
  INSERT INTO events (...) VALUES (...)
  ON CONFLICT (event_id) DO NOTHING;  -- retries / second tab: keep the first seq
COMMIT;
```

And on pull (useful even if you already lock on write):

```sql
SELECT *
FROM events
WHERE global_seq > $since
  -- Skip numbers that another transaction may still be holding
  AND global_seq < pg_snapshot_xmin(pg_current_snapshot())
ORDER BY global_seq ASC
LIMIT 500;
```

If you cannot change the server, set client `pullOverlap` to re-request the last
N sequence numbers each sync. Replay is idempotent — that narrows the window; it
does not close it. Prefer the server-side fix.

Wire the reference handlers:

```typescript
import { Pool } from "pg";
import { pushEvents, pullEvents } from "./examples/postgres-sync-server/handlers";

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const sync = {
  pushEvents: (events) => pushEvents(pool, events),
  pullEvents: ({ since }) => pullEvents(pool, since),
};
```

## Simulate another device

POST a fake event directly to your sync server to mimic a push from another client. Use a unique `eventId` — it must not already exist in the receiving device's local outbox.

```bash
curl -X POST http://localhost:3000/api/sync/events \
  -H "Content-Type: application/json" \
  -d '[{
    "eventId": "019c0000-0000-7000-8000-remote-demo",
    "collectionId": "todos",
    "type": "insert",
    "key": "remote-todo-demo",
    "payload": {
      "id": "remote-todo-demo",
      "userId": "remote-user",
      "title": "From another device",
      "status": "pending",
      "createdAt": 1730000000000,
      "updatedAt": 1730000000000
    },
    "timestamp": 1730000000000
  }]'
```

Then call `manualSync()` or `db.sync()` on the client. The event appears in inbox and the todo is replayed locally.

Check server state:

```bash
curl "http://localhost:3000/api/sync/events?since=0"
```

## Sync Options

You can run fully offline by omitting `sync`. In that mode local mutations still append to `outbox`, and `sync()` is a no-op.

Use typed functions when your app already has RPC/server-function clients:

```typescript
import type { PullResponse, PushResponse } from "event-sourced-collection";

const sync = {
  pushEvents: async (events): Promise<PushResponse> => {
    return typedRpc.events.push({ events });
  },
  pullEvents: async ({ since }): Promise<PullResponse> => {
    return typedRpc.events.pull({ since });
  },
};

const db = await createEventSourcedDB({ sync /* ... */ });
```

Use URLs when you want the built-in HTTP adapter:

```typescript
const db = await createEventSourcedDB({
  sync: {
    pushUrl: "/api/events",
    pullUrl: "/api/events",
    headers: () => ({ Authorization: `Bearer ${getAccessToken()}` }),
  },
  /* ... */
});
```

You can provide only `pushEvents`/`pushUrl` for upload-only sync or only `pullEvents`/`pullUrl` for download-only sync. If both a function and URL are provided for the same direction, the function is used.

### Reliability options

| Option                 | Default                                      | What it does                                                                                                                                                                                                                                                                                                                                                           |
| ---------------------- | -------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `clientId`             | generated once, then persisted in `syncmeta` | Stamped on every outgoing event, and how this device recognises its own events when they come back on pull. Only pass your own if it is already stable across reloads — an unstable value makes the device treat its own history as remote.                                                                                                                            |
| `unknownEventHandling` | `"skip"`                                     | What to do with a pulled event for a collection this build does not know. `"skip"` records it in the inbox with a `skipReason` and keeps draining later events. `"fail"` stops the pull so it is retried next sync.                                                                                                                                                    |
| `pullOverlap`          | `0`                                          | Rewinds the pull cursor by N sequence numbers each sync. Recovers events that committed out of sequence order on servers using `BIGSERIAL`.                                                                                                                                                                                                                            |
| `retry`                | 8 attempts, 1s base, 5min cap                | Backoff schedule for pushes the server marked `retryable`. Doubles each attempt up to `maxDelayMs`; after `maxAttempts` the event is dead-lettered. `maxAttempts` also bounds how many times a pulled event may fail to replay before it is dead-lettered.                                                                                                             |
| `pushBatchSize`        | `100`                                        | Soft max events per push request. Batches never split a `txId`; a single larger transaction is sent whole. Each batch is persisted before the next is sent.                                                                                                                                                                                                            |
| `backendMismatch`      | `"resetCursor"`                              | What to do when the server reports a different `backendId`. `"resetCursor"` clears the inbox, re-pulls from zero, and requeues every retained outbox event so local history is re-uploaded to the replacement backend (pruned events cannot be recovered). `"fail"` surfaces a `BackendMismatchError`. `"ignore"` carries on, which usually means never pulling again. |
| `eventSchemaVersion`   | `1`                                          | Stamped on every authored event. Bump it when a payload shape changes.                                                                                                                                                                                                                                                                                                 |
| `upcastEvent`          | none                                         | Migrates an event whose `schemaVersion` differs from the current one before it is replayed. Return `null` to skip the event. Without an upcaster, older events are applied as-is and _newer_ ones are refused.                                                                                                                                                         |
| `conflictDetection`    | `false`                                      | Maintains a per-row version index and stamps `baseVersion` on outgoing events so the server can reject stale writes. Costs one extra index write per mutation and per replay.                                                                                                                                                                                          |
| `lock`                 | Web Locks in the browser helper              | Elects a single syncing context. Other contexts get `{ deferred: true }`. Pass `lock: null` to `createBrowserEventSourcedDB` to opt out.                                                                                                                                                                                                                               |
| `hooks`                | none                                         | Lifecycle observation points — see [Lifecycle hooks](#lifecycle-hooks).                                                                                                                                                                                                                                                                                                |

```typescript
const db = await createEventSourcedDB({
  clientId: await getPersistedDeviceId(),
  unknownEventHandling: "skip",
  pullOverlap: 50,
  retry: { maxAttempts: 10, baseDelayMs: 2_000 },
  pushBatchSize: 50,
  backendMismatch: "resetCursor",
  /* ... */
});
```

Prefer `"skip"` unless you would rather stall than diverge: `"fail"` means one
unrecognized event blocks every later event until the client is upgraded.

### Evolving event payloads

Bump `eventSchemaVersion` when a payload shape changes, and provide `upcastEvent` so devices that were offline through the change can still replay old events:

```typescript
const db = await createEventSourcedDB({
  eventSchemaVersion: 2,
  upcastEvent: (event) => {
    if (event.schemaVersion >= 2) return event;

    // v1 stored `completed: 0 | 1`; v2 uses `done: boolean`.
    return {
      ...event,
      payload: { ...event.payload, done: Boolean(event.payload.completed) },
      schemaVersion: 2,
    };
  },
  /* ... */
});
```

An event authored by a _newer_ version than this build supports is refused rather than guessed at, and follows `unknownEventHandling`.

### Lifecycle hooks

`hooks` gives you observation points around the event lifecycle — useful for telemetry, toasts, and debugging. Every hook is optional and fire-and-forget: one that throws is logged and swallowed, so it can never fail a sync.

| Hook                | Fired when                                                        |
| ------------------- | ----------------------------------------------------------------- |
| `onReady`           | Once, after collections preload and startup replay finishes       |
| `onMutation`        | A local mutation is appended to the outbox                        |
| `onSyncStart`       | A `sync()` or `manualSync()` begins (after the lock is acquired)  |
| `onSyncComplete`    | That sync finishes, with the full result                          |
| `onSyncError`       | A phase fails, with `phase: "push" \| "pull" \| "replay"`         |
| `onEventPushed`     | The server confirms an outbound event and assigns its `globalSeq` |
| `onEventApplied`    | A remote event is replayed into a collection                      |
| `onEventSkipped`    | An event is durably recorded but deliberately not applied         |
| `onDeadLetter`      | An event moves to the dead-letter queue                           |
| `onBackendMismatch` | The server reports a different `backendId`                        |

```typescript
const db = await createEventSourcedDB({
  hooks: {
    onDeadLetter: (entry) => {
      toast.error(`Could not save "${entry.collectionId}": ${entry.message}`);
      analytics.track("sync_dead_letter", { reason: entry.reason, code: entry.code });
    },
    onSyncError: ({ phase, error }) => Sentry.captureException(error, { tags: { phase } }),
    onBackendMismatch: ({ expected, received }) =>
      console.warn("sync backend replaced", { expected, received }),
  },
  /* ... */
});
```

Hooks are for observation, not control flow — they cannot cancel or alter an event. Treat the values you are handed as read-only. Note that `onSyncStart` does _not_ fire when the call is deferred because another tab holds the lock.

### Conflict detection

With `conflictDetection: true`, each mutation records the version of the row it was authored against and sends it as `baseVersion`. The server rejects the event with code `CONFLICT` if the row has moved on, and the client parks it in the dead-letter queue with `reason: "conflict"` so you can prompt the user or merge manually.

This is detection, not resolution — there is no automatic rebase. If you do nothing with the dead-letter queue, conflicting edits are dropped rather than silently overwriting.

### Multi-tab locking

`createBrowserEventSourcedDB` installs a Web Locks–backed `lock` by default so only one tab runs push/pull at a time. Other tabs resolve immediately with `{ deferred: true }` (and do not fire `onSyncStart`). Pass `lock: null` to opt out.

This serializes **sync**, not local writes — every tab can still mutate collections; TanStack DB persistence coordinates those. See [ARCHITECTURE.md](./ARCHITECTURE.md) (leader election / `tryRun`) for why the lock does not queue.

### Known limitations

Worth knowing before you ship:

| Topic                       | Behaviour today                                                                                                                                                                                                                                                             | What to do                                                                                                                                            |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| Offline + retries           | Transport failures while offline still increment `attemptCount` and can eventually dead-letter events the server never saw                                                                                                                                                  | Prefer gating background sync on connectivity, or surface deadletter so users can `retryDeadLetter()` after reconnect                                 |
| Dead-letter UI              | Rejected / exhausted events leave the outbox permanently                                                                                                                                                                                                                    | Query `db.collections.deadletter` (or `onDeadLetter`) and offer retry/discard                                                                         |
| Log growth                  | Outbox/inbox grow until you compact                                                                                                                                                                                                                                         | Call `pruneSyncedEvents({ olderThanMs })` on a schedule                                                                                               |
| Conflict index              | With `conflictDetection: true`, `rowversions` is not pruned                                                                                                                                                                                                                 | Expect unbounded growth until a future prune path exists                                                                                              |
| Payload size                | Batching is by event count, not bytes                                                                                                                                                                                                                                       | Keep `pushBatchSize` conservative if payloads are large; oversized single transactions still go in one request                                        |
| Conflicts                   | Detection only — no merge/rebase                                                                                                                                                                                                                                            | Handle `reason: "conflict"` in deadletter yourself                                                                                                    |
| Concurrent edits to one row | Two devices editing the same row between syncs do not converge. A device skips replaying its own events — which is what stops it re-applying its own pruned history — so the later author keeps the earlier value it pulled while everyone else ends on the server's winner | Partition writes by owner where you can, or turn on `conflictDetection` so the second write is rejected with `CONFLICT` instead of silently diverging |
| Realtime                    | No SSE/WebSocket subscribe                                                                                                                                                                                                                                                  | Poll `sync()` / `manualSync()` or drive from `createEffect`                                                                                           |

Deeper rationale and open issues: [ARCHITECTURE.md](./ARCHITECTURE.md).

For existing integrations, the legacy shapes still work:

```typescript
import type { SyncTransport } from "event-sourced-collection";

const transport: SyncTransport = {
  async push(events) {
    /* return confirmed event ids + globalSeq */
  },
  async pull(since) {
    /* return { events, cursor, hasMore } */
  },
};

const db = await createEventSourcedDB({ sync: transport /* ... */ });
```

## Opting out of sync

`syncEnabled` defaults to `true` on `createBrowserEventSourcedDB`. When disabled:

- `sync()` skips push/pull (returns `{ pushed: 0, pulled: 0, skipped: 0, deadLettered: 0, deferred: false, errors: [] }`)
- `manualSync()` still replays pending inbox events locally but does not hit the network
- Background sync hooks should pass `dbReady && syncEnabled` as their `enabled` flag

Toggle at runtime:

```typescript
const database = await ensureDb();
database.setSyncEnabled(false);
```

**Recommended pattern:** persist the preference in your `settings` collection and mirror it on startup — see [App settings](#5-app-settings-app-settingsts) (`ensureAppSettings` + `setSyncEnabled`). That keeps the Settings UI, background sync hook, and engine in sync.

```typescript
await setSyncEnabled(false);
// updates settings row + calls db.setSyncEnabled(false)
```

## React Native

Same helper shape via `createReactNativeEventSourcedDB` (from `event-sourced-collection/react-native`). React Native can't auto-open the database, so `load` returns the `database` driver alongside the platform functions.

```typescript
import { createReactNativeEventSourcedDB } from "event-sourced-collection/react-native";

const { ensureDb, db } = createReactNativeEventSourcedDB<AppCollectionDefs>({
  collections: {
    users: { getKey: (user: User) => user.id },
    todos: { getKey: (todo: Todo) => todo.id },
  },
  sync: { pushEvents, pullEvents },
  load: async () => {
    const { createCollection } = await import("@tanstack/react-native-db");
    const { createReactNativeSQLitePersistence, persistedCollectionOptions } =
      await import("@tanstack/react-native-db-sqlite-persistence");
    const { openDatabase } = await import("react-native-op-sqlite");

    return {
      createReactNativeSQLitePersistence,
      persistedCollectionOptions,
      createCollection,
      database: openDatabase({ name: "my-app.sqlite" }),
    };
  },
});

export { db, ensureDb };
```

Insert, update, delete, `useLiveQuery`, and `sync()` work the same as in the browser.

## Cleanup

Call the handle's `close()` to dispose the engine and release the platform:

```typescript
await close();
```

If you wired things up by hand with the low-level `createEventSourcedDB` + `createBrowserPlatform`, dispose them directly instead:

```typescript
db.dispose();
await platform.close();
```

## API

### `createBrowserEventSourcedDB(config)` / `createReactNativeEventSourcedDB(config)`

The recommended entry points. Import from `event-sourced-collection/browser` or `event-sourced-collection/react-native`.

| Option                                               | Required     | Description                                                                                                                                                                  |
| ---------------------------------------------------- | ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `collections`                                        | Yes          | Collection definitions (`getKey`, optional `schemaVersion`, optional `indexes[]`) — must not use the reserved ids `outbox`, `inbox`, `deadletter`, `syncmeta`, `rowversions` |
| `load`                                               | Yes          | Async callback returning the platform modules + `createCollection` + `persistedCollectionOptions`. Keeps platform/framework imports in your app                              |
| `databaseName`                                       | Browser only | SQLite database file name                                                                                                                                                    |
| `coordinatorDbName`                                  | No (browser) | Multi-tab coordinator name; defaults to `databaseName` without the `.sqlite` suffix                                                                                          |
| `sync`                                               | No           | Offline by default. Same shapes as `createEventSourcedDB`                                                                                                                    |
| `syncEnabled`                                        | No           | Default `true`. When `false`, push/pull are skipped until re-enabled                                                                                                         |
| `schemaVersion`                                      | No           | Default `1` (persistence schema for collections)                                                                                                                             |
| `clientId` / `retry` / `pushBatchSize` / `hooks` / … | No           | Same reliability options as `createEventSourcedDB` — see [Reliability options](#reliability-options)                                                                         |
| `lock`                                               | No           | Defaults to Web Locks. Pass `null` to disable                                                                                                                                |
| `debug`                                              | No           | `boolean` or a custom logger                                                                                                                                                 |

Returns `{ ensureDb, db, close }`:

- `ensureDb()` — runs `load` + setup once (deduped), returns the ready DB. The browser helper throws outside a browser environment.
- `db` — proxy that forwards to the instance after init and throws if used before `ensureDb()`.
- `close()` — disposes the engine (and closes the SQLite platform on browser); the next `ensureDb()` re-initializes.

### `createLazySingleton(factory, options?)`

Dependency-free building block behind the helpers. Wraps an async `factory` into `{ ensure, proxy, reset }`: `ensure()` runs the factory once (deduped, retried after failure), `proxy` forwards to the instance and throws until initialized, `reset()` clears it. Accepts an optional `guard` and `notInitializedMessage`.

### `createEventSourcedDB(config)`

Low-level core for full control or non-standard platforms. The helpers above call this for you.

| Option                                                                                                                                                                                | Required | Description                                                                                                                                                                  |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `persistence`                                                                                                                                                                         | Yes      | TanStack DB persistence config                                                                                                                                               |
| `createCollection`                                                                                                                                                                    | Yes      | TanStack DB `createCollection`                                                                                                                                               |
| `persistedCollectionOptions`                                                                                                                                                          | Yes      | From your platform package                                                                                                                                                   |
| `collections`                                                                                                                                                                         | Yes      | Collection definitions (`getKey`, optional `schemaVersion`, optional `indexes[]`) — must not use the reserved ids `outbox`, `inbox`, `deadletter`, `syncmeta`, `rowversions` |
| `sync`                                                                                                                                                                                | No       | Offline by default. Accepts typed `pushEvents`/`pullEvents`, URL `pushUrl`/`pullUrl`, legacy URL `push`/`pull`, or legacy `SyncTransport`                                    |
| `syncEnabled`                                                                                                                                                                         | No       | Default `true`. When `false`, push/pull are skipped until re-enabled                                                                                                         |
| `schemaVersion`                                                                                                                                                                       | No       | Default `1` (persistence)                                                                                                                                                    |
| `clientId`, `unknownEventHandling`, `pullOverlap`, `retry`, `pushBatchSize`, `backendMismatch`, `eventSchemaVersion`, `upcastEvent`, `conflictDetection`, `lock`, `lockName`, `hooks` | No       | See [Reliability options](#reliability-options) and [Lifecycle hooks](#lifecycle-hooks)                                                                                      |
| `debug`                                                                                                                                                                               | No       | `boolean` or a custom logger                                                                                                                                                 |

Returns an object whose `collections` includes your registered collections plus the built-in `outbox`, `inbox`, `deadletter`, `syncmeta`, and `rowversions`, along with:

| Method                                         | Description                                                                                                                 |
| ---------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `sync()`                                       | Push then pull. Resolves to `{ pushed, pulled, skipped, deadLettered, deferred, errors }`                                   |
| `manualSync()`                                 | Same, plus a replay of any unresolved inbox rows. Adds `replayed` to the result                                             |
| `getSyncEnabled()` / `setSyncEnabled(enabled)` | Runtime toggle for push/pull (default `true` at init via the `syncEnabled` config option)                                   |
| `getSyncStatus()`                              | Current `{ isSyncing, isSynced, pendingCount, failedCount, deadLetterCount, pullCursor, backendId, lastSyncAt, lastError }` |
| `subscribeSyncStatus(listener)`                | Calls `listener` immediately and on every relevant change. Returns an unsubscribe function                                  |
| `retryDeadLetter(eventId?)`                    | Requeues dead-lettered events with a fresh retry budget. Returns the count                                                  |
| `discardDeadLetter(eventId?)`                  | Drops dead-lettered events permanently. Returns the count                                                                   |
| `pruneSyncedEvents(options?)`                  | Compacts confirmed outbox and resolved inbox rows. Returns `{ outbox, inbox }`                                              |
| `dispose()`                                    | Releases subscriptions                                                                                                      |

## Testing

`createMockSyncBackend()` is an in-memory server: it assigns sequence numbers, deduplicates by `eventId`, paginates, and can inject failures. Use it to test your sync wiring without standing up an API.

```typescript
import { createMockSyncBackend } from "event-sourced-collection";

const backend = createMockSyncBackend({
  pageSize: 50,
  backendId: "test-backend",
  rejectPush: (event, attempt) =>
    attempt < 3 ? { message: "overloaded", retryable: true } : undefined,
});

const db = await createEventSourcedDB({ sync: backend /* ... */ });

// Pretend another device pushed something.
backend.seed({ collectionId: "todos", key: "t1", payload: { id: "t1", title: "Remote" } });
await db.sync();

// Simulate an outage, then a wiped server.
backend.failNextPushes(1, "offline");
backend.reset();
backend.setBackendId("test-backend-2");
```

`backend.events`, `backend.pushBatchSizes`, `backend.pushCalls`, and `backend.pullCalls` let you assert on what the client actually did.

### `createBrowserPlatform(deps, config)`

Import from `event-sourced-collection/browser`. Sets up browser SQLite + multi-tab coordinator.

### `createReactNativePlatform(deps, config)`

Import from `event-sourced-collection/react-native`. Sets up React Native SQLite persistence.
