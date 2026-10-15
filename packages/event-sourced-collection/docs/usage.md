# Config options usage guide

This package has a lot of knobs. Most apps only need `collections`, `sync`, and
`load` (browser) / `database` (React Native). The rest exist for edge cases that
are hard to guess from the type alone — schema evolution, concurrent edits,
server resets, and flaky networks.

Hover any option in your editor for a short JSDoc summary. This page goes
deeper: **what it does**, **when to use it**, **when to leave it alone**, and
**copy-paste examples**.

Design internals (push/pull pipeline, lock election, dead-letter reasons): see
[ARCHITECTURE.md](../ARCHITECTURE.md). Quick start: [README.md](../README.md).

---

## Mental model

Every local `insert` / `update` / `delete` appends an **outbox** event. With
`compactOnWrite` (the default), a later update replaces the unsynced chain for
that row with one event instead of appending another partial update. Sync
**pushes** due outbox events, **pulls** server events into the **inbox**, then
**replays** the inbox into your collections. Options below tune that loop —
they do not change how you read/write collections day to day.

```ts
const { ensureDb, db } = createBrowserEventSourcedDB({
  databaseName: "my-app.sqlite",
  collections: {/* … */},
  sync: { pushEvents, pullEvents },
  // everything else is optional and has a safe default
  load: async () => {
    /* dynamic imports */
  },
});
```

---

## `syncEnabled`

**Default:** `true`

Initial gate for push/pull. Call `db.setSyncEnabled(false)` at runtime to pause
sync without tearing down the DB (mutations still queue in the outbox).

**Use when:** you want a Settings toggle (“sync in background”). Persist the
preference in a settings collection and mirror it on startup.

**Skip when:** sync should always run; leave the default.

```ts
createBrowserEventSourcedDB({
  // …
  syncEnabled: true, // initial only
});

// later, from Settings UI:
db.setSyncEnabled(false);
```

---

## `eventSchemaVersion` + `upcastEvent`

**Defaults:** `eventSchemaVersion: 1`, no upcaster

Every outbound event is stamped with `eventSchemaVersion`. On replay, if an
event’s version does not match what your code expects, call `upcastEvent` to
rewrite the payload — or return `null` to skip applying it (still recorded).

**Do use when:** you ship a breaking row-shape change (rename a field, change
enums) and old devices / old server events still exist.

**Don’t bump casually:** leaving it at `1` forever is fine until the first
breaking change. Bumping without an upcaster will skip or fail those events
depending on your handling.

Not the same as `schemaVersion` — that is the **local SQLite** persisted-
collection version, not the wire event version.

```ts
createBrowserEventSourcedDB({
  // …
  eventSchemaVersion: 2,
  upcastEvent: (event) => {
    if (event.collectionId !== "todos") return event;
    if (event.schemaVersion >= 2) return event;

    // v1 stored `done: boolean`; v2 uses `status`
    const payload = { ...event.payload };
    if ("done" in payload) {
      payload.status = payload.done ? "complete" : "pending";
      delete payload.done;
    }
    return { ...event, payload, schemaVersion: 2 };
  },
});
```

Return `null` only when the event is intentionally obsolete:

```ts
upcastEvent: (event) => {
  if (event.collectionId === "legacyNotes") return null; // skip forever
  return event;
},
```

---

## `pullOverlap`

**Default:** `0`

On each sync, pull starts at `max(0, pullCursor - pullOverlap)` instead of the
exact cursor. Replay is idempotent (by `eventId`), so duplicates are cheap.

**Use when:** your server can assign `global_seq` before the row is visible to
readers (classic Postgres `BIGSERIAL` race). A small value like `5` is
belt-and-suspenders even if you also fix the server (`BEGIN IMMEDIATE`, etc.).

**Leave at 0 when:** sequences are assigned under a transaction that makes
commits visible in order (many embedded / libsql setups). Extra overlap only
adds redundant pull traffic.

```ts
pullOverlap: 5,
```

---

## `conflictDetection`

**Default:** `false`

When on, the client maintains a `rowversions` index and stamps `baseVersion` on
outgoing events. A server that checks `baseVersion` can reject stale writes with
code `CONFLICT` (non-retryable → dead-letter). After a CONFLICT, the client
restores `rowversions` to the pre-attempt `baseVersion` so the rejected event id
does not poison later writes.

**Turn on when:** the same row can be edited on two devices before either syncs
and you want last-writer-wins to be explicit (server rejects the loser).

**Leave off when:** single-device edits, or conflicts are rare and “last sync
wins” is acceptable. It costs an extra write on every mutation and every replay.

```ts
conflictDetection: true,
```

Server side (sketch): reject if the stored version ≠ `baseVersion`, respond with
`{ failures: [{ eventId, retryable: false, code: "CONFLICT" }] }`.

**Seeding tip:** `ensureDb()` / `createEventSourcedDB` preloads user collections
before it returns. Seed helpers (e.g. insert a default settings row when missing)
must run **after** that so `get()` sees rows already on disk — otherwise you
re-insert and CONFLICT against the server on every load.

---

## `retry`

**Defaults:** `maxAttempts: 8`, `baseDelayMs: 1000`, `maxDelayMs: 5 minutes`

Applies only to **retryable** push failures. Each attempt doubles the delay
from `baseDelayMs` up to `maxDelayMs`. Non-retryable rejections (and exhausted
attempts) move the event to `deadletter` and remove it from the outbox — it will
**not** be pushed again until you call `retryDeadLetter`.

**Tune when:**

- Networks are flaky → raise `maxAttempts` or `maxDelayMs`.
- You want faster failure surfacing in UI → lower `maxAttempts`.
- Server rate-limits → raise `baseDelayMs`.

**Don’t expect this to fix permanent errors:** schema mismatches and `CONFLICT`
should be `retryable: false` on the server so they dead-letter immediately.

```ts
retry: {
  maxAttempts: 8,
  baseDelayMs: 1_000,
  maxDelayMs: 5 * 60_000,
},
```

Recover later:

```ts
// Outbound: pushes directly to the sync transport (does not requeue the outbox).
// Inbound: requeues into the inbox and replays immediately.
await db.retryDeadLetter(); // all successfully cleared
await db.retryDeadLetter(eventId); // one
await db.discardDeadLetter(eventId); // drop without pushing
```

If the direct push still fails, the event stays in `deadletter` (message /
attemptCount updated) — it is not put back on the automatic sync path.
---

## `pushBatchSize`

**Default:** `100`

Caps how many outbox events go in one push request. Events that share a `txId`
(one TanStack DB transaction) **stay together** even if that exceeds the cap —
a transaction is never split across HTTP calls.

**Raise when:** you often create large bulk imports and want fewer round trips
(watch request body size / gateway limits).

**Lower when:** the server or proxies choke on large payloads, or you want
finer-grained progress / failure isolation.

```ts
pushBatchSize: 100,
```

---

## `backendMismatch`

**Default:** `"resetCursor"`

The server should return a stable `backendId` on pull. If it changes (DB wiped,
restored from blank, pointed at a new store), the client’s pull cursor is
meaningless.

| Policy          | Behavior                                          | When                                                             |
| --------------- | ------------------------------------------------- | ---------------------------------------------------------------- |
| `"resetCursor"` | Clear inbox, pull from 0, requeue retained outbox | **Default.** Safest recovery for demo / single-tenant resets.    |
| `"fail"`        | Error out; leave cursor alone                     | You want an operator to intervene before re-uploading.           |
| `"ignore"`      | Keep old cursor                                   | Almost never — usually means you silently stop receiving events. |

```ts
backendMismatch: "resetCursor",
```

Listen with `hooks.onBackendMismatch` if you need a toast or analytics ping.

**Caveat:** events already removed by `pruneSyncedEvents` cannot be re-uploaded
after a reset. Don’t prune aggressively if you expect backend recreations.

---

## `hooks`

**Default:** none

Optional observers around the lifecycle. They are **fire-and-forget**: a
throwing hook is logged and swallowed so it cannot break sync. Do not mutate
arguments.

Common uses: toasts, logging, metrics, wiring UI to dead-letters.

```ts
const hooks: EventSourcedHooks = {
  onReady: ({ clientId, pullCursor }) => {
    console.info("[sync] ready", { clientId, pullCursor });
  },
  onSyncComplete: ({ trigger, result }) => {
    if (result.deadLettered > 0) {
      toast.warning(`${result.deadLettered} event(s) need attention`);
    }
  },
  onDeadLetter: (entry) => {
    console.warn("[sync] dead letter", entry.eventId, entry.reason);
  },
  onBackendMismatch: ({ expected, received, policy }) => {
    console.warn("[sync] backend changed", { expected, received, policy });
  },
  onSyncError: ({ phase, error }) => {
    console.error(`[sync] ${phase} failed`, error);
  },
};

createBrowserEventSourcedDB({
  // …
  hooks,
});
```

Full list: `onReady`, `onMutation`, `onSyncStart`, `onSyncComplete`,
`onSyncError`, `onEventPushed`, `onEventApplied`, `onEventSkipped`,
`onDeadLetter`, `onBackendMismatch`.

---

## `clientId`

**Default:** generated once and stored in `syncmeta`

Stamped on every outbound event. Used to attribute writes and to skip
re-applying this device’s own events when they come back on pull.

**Pass your own only if** it is stable across reloads (e.g. `localStorage`). A
new random id every boot makes the client treat its own history as remote.

```ts
function getPersistedClientId(): string {
  const key = "my-app-client-id";
  let id = localStorage.getItem(key);
  if (!id) {
    id = crypto.randomUUID();
    localStorage.setItem(key, id);
  }
  return id;
}

createBrowserEventSourcedDB({
  // …
  clientId: getPersistedClientId(),
});
```

---

## `unknownEventHandling`

**Default:** `"skip"`

If the server sends an event for a collection this client does not register
(older app build, newer server), `"skip"` keeps sync moving; `"fail"` halts the
pull so the event retries next time (useful while you roll out a new
collection).

```ts
unknownEventHandling: "skip",
```

---

## `schemaVersion` (persistence) vs indexes on collections

`schemaVersion` on the DB config (or per `CollectionDef`) is the **local**
persisted-collection version passed through to TanStack persistence — not the
wire event version.

```ts
collections: {
  todos: {
    getKey: (t) => t.id,
    schemaVersion: 1, // optional override of the DB default
    indexes: [
      { select: (t) => t.userId, name: "by-user" },
    ],
  },
},
```

---

## Browser-only: `lock` / `databaseName`

`createBrowserEventSourcedDB` defaults to a **Web Locks** sync lock so only one
tab pushes/pulls at a time (`lockName` defaults to `databaseName`).

```ts
// default — one tab syncs
createBrowserEventSourcedDB({ databaseName: "my-app.sqlite" /* … */ });

// every tab syncs independently (usually worse)
createBrowserEventSourcedDB({
  databaseName: "my-app.sqlite",
  lock: null,
  // …
});
```

---

## Suggested starting config

Sensible defaults for a multi-device app with a Postgres- or SQLite-backed API:

```ts
createBrowserEventSourcedDB({
  databaseName: "my-app.sqlite",
  collections: { users, todos, settings },
  sync: { pushEvents, pullEvents },
  syncEnabled: true,
  eventSchemaVersion: 1,
  pullOverlap: 5, // cheap insurance; drop to 0 if you own sequence assignment
  conflictDetection: true, // if the server checks baseVersion
  retry: { maxAttempts: 8, baseDelayMs: 1_000, maxDelayMs: 5 * 60_000 },
  pushBatchSize: 100,
  backendMismatch: "resetCursor",
  hooks: {
    onSyncError: ({ phase, error }) => console.error(phase, error),
    onDeadLetter: (e) => console.warn("dead letter", e.eventId, e.reason),
  },
  load: async () => {
    /* dynamic imports of createCollection + browser SQLite modules */
  },
});
```

Omit anything you are happy with at its default — the type is intentionally
optional-heavy so you only opt into the messy ones when you need them.
