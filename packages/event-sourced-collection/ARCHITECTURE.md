# Architecture & Design Decisions

Technical reference for contributors and anyone evaluating this library's tradeoffs.
Usage and API: [README.md](./README.md).

---

## Core Philosophy

This library answers one question: **How do you sync local-first data without downloading entire tables?**

The answer: log every mutation as an event. Sync the event log. Replay events on both sides.

This is event sourcing applied to client-side state, scoped to a single user's data.

---

## System Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│  Client (Browser / React Native / Expo / Node)                        │
│                                                                        │
│  ┌───────────────────────────────┐  ┌───────────────────────────────┐ │
│  │  outbox (persisted collection)│  │  inbox (persisted collection) │ │
│  │  local mutations to push      │  │  server events pulled down    │ │
│  │                               │  │                               │ │
│  │  eventId | type | key | sync  │  │  eventId | globalSeq | sync   │ │
│  │  ──────────────────────────── │  │  ──────────────────────────── │ │
│  │   e1     | ins  | t1  | false │  │   e9     |   50      | true   │ │
│  │   e2     | upd  | t1  | true  │  │   e10    |   51      | false  │ │
│  └───────────────────────────────┘  └───────────────────────────────┘ │
│           ▲            │                               │               │
│           │     exhausted retries                acceptMutations       │
│           │     or hard rejection                replays inbox into    │
│           │            ▼                         state collections     │
│    onInsert/    ┌──────────────┐                       │               │
│    onUpdate/    │  deadletter  │                       │               │
│    onDelete     │  terminal    │                       │               │
│    hooks        └──────────────┘                       │               │
│           │                                            ▼               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                │
│  │  users       │  │  todos       │  │  settings    │                │
│  │  (persisted) │  │  (persisted) │  │  (persisted) │                │
│  │  collection  │  │  collection  │  │  collection  │                │
│  └──────────────┘  └──────────────┘  └──────────────┘                │
│                                                                        │
│  syncmeta: { pullCursor, backendId, lastSyncAt, lastError }           │
│  rowversions: last event per row (only when conflictDetection is on)  │
└──────────────────────────────────────────────────────────────────────┘
      │  push (batched, backoff)      │  pull (since syncmeta.pullCursor)
      ▼                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Server                                                                │
│                                                                        │
│  events table (BIGSERIAL global_seq, event_id UNIQUE, payload JSONB)  │
│  sync_backend table (backend_id — changes when the store is recreated)│
│                                                                        │
│  POST /api/events → assigns global_seq, deduplicates by event_id,     │
│                     rejects stale writes when base_version is sent    │
│  GET  /api/events?since=N → events after N sorted ASC, plus backendId │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Source map

```
src/
├── create-event-sourced-db.ts   # orchestration: wire collections, sync, status, APIs
├── types.ts                     # public wire + config types
├── sync.ts                      # transport normalization (handlers / URLs / SyncTransport)
├── mock-sync-backend.ts         # in-memory server for tests
├── internal/
│   ├── constants.ts             # reserved ids, defaults, CONFLICT code
│   ├── types.ts                 # replay / meta types shared by internals
│   ├── hooks.ts                 # fire-and-forget lifecycle emitter
│   ├── serial-queue.ts          # in-process sync serialization
│   ├── sync-meta.ts             # syncmeta row: cursor, backendId, lastSync*
│   ├── push.ts                  # due set, backoff, batching, dead-letter
│   ├── pull.ts                  # pagination, origin skip, backend identity
│   └── replay.ts                # upcast, acceptMutations, rowversions
└── platforms/
    ├── web-locks.ts             # browser SyncLock (ifAvailable)
    ├── browser-event-sourced-db.ts
    └── react-native-event-sourced-db.ts
```

`create-event-sourced-db.ts` should stay thin. Push, pull, and replay each have
enough edge cases (backoff, tx-aware batching, backend reset, upcast asymmetry)
that they were split out so a change in one path does not force a re-read of the
whole engine.

Platform packages only inject persistence + an optional lock. The core never
imports Web Locks or SQLite drivers directly — that keeps Node tests and RN
bundles free of browser APIs.

### Reserved collections

| Id            | Role                                                          |
| ------------- | ------------------------------------------------------------- |
| `outbox`      | Local mutations; push due set                                 |
| `inbox`       | Pulled events; replay / skip markers                          |
| `deadletter`  | Terminal push failures and unapplicable pulled events         |
| `syncmeta`    | Singleton: cursor, backendId, clientId, lastSyncAt, lastError |
| `rowversions` | Last applied event id per row (conflict detection only)       |

User collection ids must not collide with these.

---

## Sync pipeline (one `sync()` call)

```
sync() / manualSync()
  └─ serial queue (this context)
       └─ lock.tryRun (cross-tab, optional)
            └─ onSyncStart
                 ├─ pushOutbox
                 │    due = !sync && (not failed OR retryable & nextAttemptAt ≤ now)
                 │    batch by txId (never split a transaction)
                 │    for each batch:
                 │      stamp attemptCount / lastAttemptAt
                 │      transport.push
                 │      confirmed → sync=true, onEventPushed
                 │      failed retryable → nextAttemptAt = now + backoff
                 │      failed hard / maxAttempts → deadletter, onDeadLetter
                 │      transport throw → backoff whole batch, stop with counts kept
                 └─ pullInbox
                      since = max(0, syncmeta.pullCursor - pullOverlap)
                      for each page:
                        reconcile backendId
                          → resetCursor also requeues the synced outbox
                        skip local origin (outbox.has || clientId match)
                        insert inbox row → replayEvent → resolve
                        replay threw → bump inbox attemptCount
                          → under budget: halt, retry next sync
                          → budget spent: inbound deadletter, cursor advances
                        halt leaves cursor unmoved
                      writePullCursor after each successful page
                 ├─ pushOutbox again, only if a backend reset requeued events
                 └─ writeSyncOutcome(lastSyncAt, lastError)
            └─ onSyncComplete
```

`manualSync()` then additionally drains unresolved inbox rows (same replay
path). Deferred lock acquisition returns `{ deferred: true }` without firing
`onSyncStart` — another tab is already doing the work.

---

## Design Decisions

### 1. Separate Outbox and Inbox (Not One Mixed Table)

**Decision:** Outgoing local mutations live in an `outbox` collection; pulled server events live in an `inbox` collection. Both are ordinary persisted TanStack DB collections, exposed as `db.collections.outbox` and `db.collections.inbox`. There is no separate raw event table and no separate cursor table.

**Why:**

| Factor              | One Mixed Table                                           | Outbox + Inbox                                                |
| ------------------- | --------------------------------------------------------- | ------------------------------------------------------------- |
| Mental model        | A single row could be "ours, pending" or "theirs, synced" | Direction is explicit: outbox = ours, inbox = theirs          |
| Per-row `sync` flag | Overloaded — means "pushed" or "applied" depending on row | Unambiguous — outbox `sync` = pushed, inbox `sync` = replayed |
| Visualization       | Needs raw SQL access to inspect                           | Just `useLiveQuery(db.collections.outbox)`                    |
| Cursor              | Dedicated `esdb_cursor` row to keep in step               | A single `syncmeta` row, seeded from inbox state on upgrade   |
| Storage layer       | Hand-rolled SQLite table + indexes                        | Reuses TanStack DB persistence (reactive, multi-tab aware)    |

Three more reserved collections support the sync loop: `deadletter` (events that
will never be retried, in either direction), `syncmeta` (one row holding the pull
cursor, the backend identity, and this device's own identity), and `rowversions`
(per-row version index, only written when `conflictDetection` is enabled).

**Per-row sync semantics:**

- **outbox** — `sync: false` means the mutation has not been pushed yet. `sync: true` means the server accepted it (no errors) and assigned a `globalSeq`.
- **outbox `syncStatus`** — `pending` means eligible to send, `synced` means confirmed, and `failed` means the server rejected it but marked it retryable; `nextAttemptAt` records when the backoff window opens. A rejection that is _not_ retryable never lingers as `failed` — it moves straight to `deadletter`.
- **inbox** — `sync: false` means the event was pulled but not yet resolved. `sync: true` means resolved: applied on top of the local data, originated locally and already applied before push, or deliberately skipped. Skipped rows carry `skipped: true` and a `skipReason`, and still allow the cursor to advance.
- **deadletter** — terminal. Rows carry a `direction` (`outbound` / `inbound`) and a `reason` (`rejected`, `maxAttemptsExceeded`, `conflict`, `replayFailed`, `manual`), and only move via `retryDeadLetter()` or `discardDeadLetter()`. `retryDeadLetter()` returns each event to the queue it came from: the outbox for outbound, the inbox for inbound.

**FK ordering** is still preserved: the inbox replays server events sorted by `globalSeq`, so parents always precede children.

**Why the cursor moved out of the inbox:** deriving it from `max(synced globalSeq)` meant pruning the inbox silently rewound the client. Storing it in `syncmeta` decouples log retention from sync position; the derived value is still read as a floor so existing databases upgrade without re-pulling.

**Constraint:** Because these are persisted collections, durability and multi-tab coordination are inherited from TanStack DB's persistence layer rather than implemented here.

---

### 2. Server Assigns Order (Not Client)

**Decision:** The server assigns `global_seq` via `BIGSERIAL`. The client never determines canonical order.

**Why:**

- Foreign keys require causal ordering (parent before child)
- The network delivers events in arbitrary order
- Multiple devices produce events independently
- Only a central authority can guarantee a consistent total order

**Tradeoff:** Requires a server. This library does not support peer-to-peer sync.

**Constraint:** If you need P2P, you need CRDTs or vector clocks — a fundamentally different architecture that adds significant complexity for marginal benefit in single-user scenarios.

---

### 3. Hooks Intercept Normal Collection API (No Special Dispatch)

**Decision:** Users call `collection.insert()` / `update()` / `delete()` normally. Injected `onInsert` / `onUpdate` / `onDelete` hooks log events transparently.

**Why:**

- Zero learning curve — same API as TanStack DB without this library
- `useLiveQuery` works unchanged
- No "dual write" where users must remember to call both `collection.insert()` and `events.log()`
- Impossible to forget to log an event

**Tradeoff:** You cannot log custom domain events (e.g., `"user:promoted"` as a distinct event type). Every event is either `insert`, `update`, or `delete`.

**Constraint:** If you need rich domain events, you'd extend this library's hook to derive a domain event type from the mutation data, or add a `dispatch()` API alongside the collection API.

---

### 4. Replay Uses `acceptMutations` (No Re-Logging Guard Needed)

**Decision:** Server events are replayed into state collections via the persistence layer's `acceptMutations` util, which bypasses the `onInsert` / `onUpdate` / `onDelete` write handlers entirely.

**Why:**

The mutation hooks (which append to the outbox) only fire on user-initiated writes. `acceptMutations` applies and persists rows without invoking those handlers, so replaying a pulled event never re-appends it to the outbox. This removes the need for the old global `isSyncing` boolean and its associated race window — there is no longer a guard that could drop a concurrent user mutation.

**Idempotent recovery:** Replay is keyed by the row's primary key, so re-applying the same event is a no-op upsert. On startup, any inbox row still marked `sync: false` (e.g. the process crashed between writing the inbox row and applying it) is simply replayed again. The inbox `sync` flag is the durable record of "applied or not".

**Tradeoff:** A user mutation made during an in-flight pull is appended to the outbox immediately and will be pushed on the next sync — there is no longer any window where it is silently skipped.

---

### 5. State Collections Are Source of Truth Locally

**Decision:** The local persisted collections (state) are the source of truth for the UI. The event log is an audit trail + sync mechanism. We don't rebuild state from events on startup.

**Why:**

- Instant hydration — state is already materialized in SQLite
- No replay cost on app start (could be seconds for large event logs)
- TanStack DB's persistence layer already handles this correctly
- The event log is only used for sync, not for state reconstruction

**Tradeoff:** If the event log and state diverge (bug, partial failure), the state wins locally. The server's event log is the ultimate arbiter — next sync will correct any drift.

**Constraint:** This means you cannot "replay from event 0" to rebuild state client-side. If you need that (time-travel debugging, undo/redo), extend the library to add a `rebuildFromEvents()` method.

---

### 6. Platform Dependencies Are Injected (Not Imported)

**Decision:** The library never imports `@tanstack/browser-db-sqlite-persistence` or `@tanstack/react-native-db-sqlite-persistence` directly. Users pass them in via a `load()` callback on the platform helpers, or as explicit parameters to the low-level `createEventSourcedDB` API.

**Why:**

- One npm package works on all platforms
- No conditional imports or build-time platform detection
- No React Native metro bundler issues with browser-only code
- The library has zero bundled platform or framework dependencies
- Users choose `@tanstack/react-db` vs `@tanstack/db` vs `@tanstack/react-native-db` in their own `load()` — the package never decides

**Recommended setup (browser):**

```typescript
import { createBrowserEventSourcedDB } from "event-sourced-collection/browser";

const { ensureDb, db } = createBrowserEventSourcedDB({
  databaseName: "my-app.sqlite",
  collections: { todos: { getKey: (t) => t.id } },
  sync: { pushEvents, pullEvents },
  load: async () => {
    const { createCollection } = await import("@tanstack/react-db");
    const platform = await import("@tanstack/browser-db-sqlite-persistence");
    return { ...platform, createCollection };
  },
});
```

`createBrowserEventSourcedDB` / `createReactNativeEventSourcedDB` orchestrate platform setup, call `createEventSourcedDB`, and return `{ ensureDb, db, close }` backed by `createLazySingleton` (deduped init + proxy). The `load` callback keeps dynamic imports in userland so SSR bundles stay clean.

**Low-level alternative:** Pass `createCollection`, `persistedCollectionOptions`, and `persistence` directly to `createEventSourcedDB` + `createBrowserPlatform` when you need full control.

**Tradeoff:** Users write a small `load()` function. That is intentional — it is the price of zero coupling.

**Constraint:** If even `load()` feels verbose, the helpers already collapse ~80 lines of manual singleton/proxy/platform wiring into one call.

---

### 7. Handler-First Sync With HTTP Fallback

**Decision:** Sync accepts typed `pushEvents` and `pullEvents` functions first, plus `pushUrl` and `pullUrl` as HTTP convenience fallbacks. The legacy `SyncTransport` shape is still accepted.

**Why:**

- HTTP is universally available (browser, RN, Node)
- RESTful endpoints are the easiest to implement server-side
- Type-safe RPC/server-function clients should not lose type safety by routing through raw URLs
- Function handlers let users call existing clients, queues, workers, or local persistence without the library knowing the transport

**Tradeoff:** HTTP is request/response — no real-time push from server. The client must poll or call `sync()` explicitly unless user-provided handlers integrate a realtime trigger.

**Future improvement:** Add a `subscribe()` option that uses SSE or WebSocket for real-time server push, falling back to polling.

---

### 8. No CRDT, No Vector Clocks

**Decision:** Conflicts are resolved by server ordering. The server's `global_seq` is law.

**Why:**

- Target scope: single user, possibly multi-device
- Cross-user conflicts are extremely rare (data is scoped to userId)
- Same-user conflicts on multi-device are resolved by "last writer wins" (server decides sequence)
- CRDTs add: metadata overhead, merge complexity, tombstone management, convergence testing
- None of that is needed when one server is the authority

**Tradeoff:** Two simultaneous offline edits to the same field → server picks one order. No automatic merge. The "losing" edit is still in the event log (audit trail), but the state reflects the server's chosen order.

**Constraint:** If you need real-time collaborative editing (Google Docs style), this is the wrong architecture. Use Yjs, Automerge, or similar.

---

### 9. `BIGSERIAL` for Server Ordering (Not UUIDv7)

**Decision:** Server uses PostgreSQL's `BIGSERIAL` for `global_seq`. Client
`eventId` remains UUIDv7 for uniqueness and dedup — those are different jobs.

**Why:**

| Factor                               | BIGSERIAL (`global_seq`)      | UUIDv7                                                      |
| ------------------------------------ | ----------------------------- | ----------------------------------------------------------- |
| Storage per event                    | 8 bytes                       | 16 bytes                                                    |
| Index / cursor                       | Dense integer compare         | Lexicographic string                                        |
| Who assigns order                    | One server (authority)        | Whoever mints the UUID                                      |
| Multi-device → one API → one DB      | Correct                       | Unnecessary for order                                       |
| Multiple independent write primaries | Needs a distributed sequencer | Helps generate unique sortable IDs without a central SERIAL |

**“Distributed writers” means multiple databases minting order, not multiple
phones.** Several devices pushing into one Postgres is the intended design.

**Critical caveat — sequence values are allocated before commit.** Two concurrent
inserts can take `global_seq` 50 and 51, and 51 can commit first. A client that
pulls during that window sees 51, advances its cursor to 51, and **never sees
50**. Multi-device traffic increases concurrency and thus this race; it does not
require switching to UUIDv7. Server-minted UUIDv7 before commit has the same gap.

How to handle it (pick at least one server-side approach):

| Approach                                        | Where       | Notes                                                      |
| ----------------------------------------------- | ----------- | ---------------------------------------------------------- |
| `pg_advisory_xact_lock` around insert           | Server push | One writer assigns+commits at a time; simplest correct fix |
| Serve only `global_seq < pg_snapshot_xmin(...)` | Server pull | Never returns in-flight sequences                          |
| `pullOverlap`                                   | Client      | Fallback; narrows the window, does not close it            |

Annotated handlers (plain comments, not a checklist):
[`examples/postgres-sync-server/`](./examples/postgres-sync-server/).

Sketch:

```ts
// PUSH — one DB transaction per client txId group
await client.query("BEGIN");
// Keep assign+commit ordered so pullers don't skip a late-committing row
await client.query("SELECT pg_advisory_xact_lock($1)", [EVENTS_SEQ_LOCK]);
// Same eventId again → return the existing global_seq
// baseVersion mismatch → CONFLICT, retryable: false
// Sibling failure → ROLLBACK the whole group
await client.query("COMMIT");

// PULL
// backendId change → client resets cursor after a DB wipe
// xmin ceiling → don't serve sequences still in flight
WHERE global_seq > $since
  AND global_seq < pg_snapshot_xmin(pg_current_snapshot())
```

**Constraint:** Only if you run multiple independent write primaries would you
replace central `BIGSERIAL` with a distributed id scheme — and you would still
need a total-order story (or CRDTs), not “UUIDv7 as pull cursor” alone.

---

### 10. Delete Events Store the Full Object (Not Just the Key)

**Decision:** When a row is deleted, the event payload contains the entire deleted object, not just the key.

**Why:**

- Server can process the delete without looking up what was deleted
- Enables "undo delete" on the server side
- Audit trail shows what was deleted, not just that something was deleted
- Replay can restore the object if the delete is later reversed

**Tradeoff:** Slightly larger event payloads for deletes.

Update and delete events additionally carry `previous`, the row state before the
mutation. That makes every event invertible, which is the prerequisite for
rollback, undo, and any future rebase support. Inserts have `previous: null`.

An update's `payload` is the whole row after the change, not the keys that
changed. `createMutationHook` stores TanStack's `modified` draft. A draft edit
that touches only `title` still records `color` and every other field, and
`Object.assign` of a partial onto the draft does the same. Returning a new
partial object from the callback, or `delete`ing a key on the draft, writes no
update at all. Confirmed 15 Oct 2026 with
`pnpm exec tsx scripts/collection-test-scripts/probe-update-payloads.ts`.

The one way to store a partial is to insert an outbox row by hand. Compaction
then treats that payload as the whole latest row and drops every field it does
not contain. The live collection is not updated by that insert, so the row and
its outbox disagree.

`replaceUnpushedOutboxEvents` deletes the events the folded row already
accounts for: every unpushed insert and update for that row between the oldest
event in the chain and the latest, then inserts the single replacement.

---

### 11. Unknown Events Are Recorded, Not Retried Forever

**Decision:** A pulled event targeting a collection this client does not know
about is written to the inbox, marked resolved with a `skipReason`, and the
cursor moves past it. Configurable via `unknownEventHandling: "skip" | "fail"`,
defaulting to `"skip"`.

**Why:**

Halting on an unrecognized event means the cursor never advances, so every
subsequent sync re-fetches the same blocked event and **no later event ever
lands**. That turns a single forward-compatibility mismatch into total sync
failure — which is the normal state of affairs during any staged rollout where
an old client meets events from a new one.

Skipping keeps the pipeline draining. The event is still durably recorded, so
nothing is lost and it stays visible for debugging.

**Tradeoff:** A skipped event is not automatically re-applied after the client
learns the collection. Query the inbox for `skipped: true` rows and replay them
if you need that.

**Constraint:** Use `"fail"` when you would rather stall than diverge — the
event stays unresolved and is retried on every sync.

---

### 12. Conflicts Are Detected, Not Resolved (Opt-In)

**Decision:** With `conflictDetection: true`, each mutation records the version
of the row it was authored against (`baseVersion`, the `eventId` of the last
event applied to that row) and sends it with the event. The server rejects the
push with code `CONFLICT` when the row has moved on. Off by default.

**Why not always on:** it costs a write to the `rowversions` index on every
mutation _and_ every replay. Single-device apps get nothing for that.

**What it does not do:** there is no rebase. A losing event goes to the
dead-letter queue with `reason: "conflict"` and stops there. Resolution is your
application's job — show the user both versions, re-apply on top of the new
state, or discard.

**Without it,** the semantics are not "last writer wins" but **"stale writer
wins"**: a pending local write pushed _after_ a conflicting server event was
pulled lands at a higher `global_seq` and overwrites it, with neither side able
to detect that it happened. For single-user single-device this never comes up;
for multi-device it is real, silent data loss on concurrently edited rows.

Full parent links plus automatic rebase, as LiveStore does, remains out of
scope.

---

### 13. Failed Pushes Retry With Backoff, Then Dead-Letter

**Decision:** A rejection the server marks `retryable: true` is rescheduled with
exponential backoff (`retry.baseDelayMs` doubling to `retry.maxDelayMs`).
Anything else — or an event that exhausts `retry.maxAttempts` — is moved out of
the outbox into `deadletter`.

**Why the queue:** the outbox is ordered by `localSeq` and processed in order. A
permanently rejected event left in place is re-examined on every sync forever
and, worse, invites the reader to treat the head of the queue as "stuck". Moving
it out keeps the outbox meaning exactly one thing: work that will still be
attempted.

**Why not retry forever:** an event the server will never accept (schema
mismatch, revoked permission, validation failure) is not a transient fault.
Retrying it indefinitely burns battery and hides the failure from the user.

**Tradeoff:** dead-lettered events are user work that will be lost unless
something surfaces them. `retryDeadLetter()` and `discardDeadLetter()` exist, but
a UI that never reads `db.collections.deadletter` is silently dropping data.

**History:** earlier, `syncStatus === "failed"` permanently filtered events out
of the due set, so `retryable` / `attemptCount` metadata was write-only. The
dead-letter path is what makes that metadata meaningful.

**Backoff is deliberately jitter-free** so retry scheduling is reproducible in
tests. Add jitter in your push handler if many clients share one server.

---

### 13b. Inbound Events Dead-Letter Too

**Decision:** A pulled event whose `acceptMutations` call throws is not
propagated as a pull error. The failure is recorded on the inbox row
(`attemptCount`, `lastError`) and the page halts so the event is retried on the
next sync. Once it has failed `retry.maxAttempts` times it is copied to
`deadletter` with `direction: "inbound"` and `reason: "replayFailed"`, its inbox
row is resolved as skipped, and the cursor advances past it.

**Why:** replay used to rethrow. Because the cursor only advances past resolved
events, one event the local schema could not accept — a constraint violation, a
row referencing something this build does not have — stopped that client pulling
_anything_ ever again. Every later event queued behind it silently, and the only
symptom was a repeating error in `lastError`. Outbound events had a full
retry-then-park pipeline; inbound events had none, despite being the direction
the client has no control over.

**Why retry before parking:** replay failures are often ordering artefacts that
resolve once an earlier event lands, so giving up on the first throw would park
recoverable events. Halting the page preserves `globalSeq` ordering while the
budget lasts.

**Tradeoff:** advancing past a parked event means the client is knowingly
inconsistent with the server for that row. That is strictly better than being
consistent-but-frozen for every row, but it does mean `deadletter` must be
surfaced for inbound rows too, not just outbound ones. `retryDeadLetter()` sends
them back to the inbox and replays immediately rather than pushing them, which
would otherwise re-send another device's event as if this one had authored it.

---

### 13c. Client Identity Is Persisted

**Decision:** `clientId` defaults to a generated id written into the `syncmeta`
row on first run and reused on every subsequent one, rather than a fresh value
per process.

**Why:** `isLocalOrigin` decides whether a pulled event is this device's own echo
by checking `outbox.has(eventId) || event.clientId === clientId`. The `clientId`
half exists precisely so that origin detection keeps working after the outbox has
been pruned — but a per-process id makes that half useless the moment the page
reloads. A client that had pruned its outbox and then rewound its cursor (via
`pullOverlap`, or via a `resetCursor` backend mismatch) would re-apply its _own_
history as if it were remote. Replaying an old insert is harmless; replaying an
old delete silently destroys whatever now lives at that key.

**Tradeoff:** the identity is scoped to the database file, so copying a SQLite
file to a second device gives both the same identity and each will treat the
other's events as its own echoes. Pass an explicit `clientId` if you clone
databases.

---

### 14. Pushes Are Batched, With Progress Persisted Per Batch

**Decision:** The outbox is sent in chunks of `pushBatchSize` (default 100), and
each batch's confirmations are persisted before the next request goes out.

**Why:** a device offline for a week comes back with thousands of pending
events. One unbounded POST either times out or exceeds a body limit, and on
failure _nothing_ is durable — the next attempt does exactly the same thing and
fails the same way. Batching makes the work resumable.

A transport-level failure stops the loop and is reported alongside the counts
from batches that already succeeded, rather than thrown, so partial progress is
visible to the caller.

**Batches never split a `txId`.** The server contract asks for each transaction
to commit atomically, which is impossible if half its events arrive in a later
request — and if that later request never succeeds, the server has permanently
applied a partial transaction. Batching therefore packs whole transaction groups
and lets a batch exceed `pushBatchSize` rather than split one.

---

### 15. Backend Identity Guards Against a Reset Server

**Decision:** The pull response may carry a `backendId`. The client stores the
first one it sees and compares on every subsequent sync. On mismatch the default
policy `resetCursor` clears the inbox, re-pulls from zero, _and_ marks every
retained outbox event pending again so local history is re-uploaded.

**Why:** the cursor is just an integer. Wiping or swapping the server restarts
`global_seq` at 1 while the client keeps asking for events after 500. The server
truthfully returns nothing, forever, with no error anywhere. Before this, that
failure mode was undetectable from the client.

Re-pulling from zero is safe because replay is an idempotent upsert.

Requeuing the outbox matters just as much as resetting the cursor. Rows the
client already flipped to `sync: true` describe data the replacement backend has
never seen; without the requeue the client keeps them locally, believes they are
safely synced, and never uploads them again — so a restored-from-backup server
quietly loses every write made since the backup. Push is idempotent by `eventId`,
so re-uploading to a _restored_ backend is a no-op rather than a duplicate.

The reset is only discovered during pull, after this sync's push has run, so the
requeued events would otherwise sit unsent until the next sync. `pushPull` runs a
second push pass when a reset requeued anything, making one `sync()` enough to
recover. `baseVersion` is cleared on requeue because it names event ids from the
old backend's history.

**Tradeoff:** events already removed by `pruneSyncedEvents` cannot be re-uploaded
— pruning trades recoverability for space, and this is where that bill arrives.
Servers that do not send `backendId` keep the old behaviour: the check is skipped
entirely rather than guessed at.

---

### 16. Events Carry a Schema Version

**Decision:** Every authored event is stamped with `eventSchemaVersion`. On
replay, an event whose version differs is passed through the optional
`upcastEvent` hook.

**Why:** payload shapes change. A device that was offline across a deploy
replays events written by the old shape into code expecting the new one. Without
a version there is nothing to branch on and the mismatch surfaces as corrupt
data rather than an error.

**Asymmetric default:** with no upcaster, _older_ events are applied as-is with
a warning (usually additive changes, usually fine), but _newer_ events are
refused via `unknownEventHandling`. This build cannot know what a future field
means, and guessing is worse than skipping.

**Upgrade note:** outbox rows written before `schemaVersion` / `baseVersion`
existed can still sit in SQLite. `toOutboundEvent` and dead-letter construction
default `schemaVersion` to `1` and `baseVersion` to `null` so a push after
upgrade does not send `undefined` on the wire.

---

### 17. Leader Election Is Injected, Not Imported

**Decision:** `createEventSourcedDB` accepts an optional `lock` implementing
`tryRun(name, fn)`. The browser helper defaults to a Web Locks implementation;
the core imports nothing platform-specific.

**Why `tryRun` and not a queue:** if five tabs each queue a sync behind the
lock, you get five sequential syncs where one would do. `tryRun` returns
`{ acquired: false }` immediately when another context holds the lock, and the
caller gets `{ deferred: true }` back from `sync()`.

**Tradeoff:** this only elects a leader per sync call, not for the process
lifetime as LiveStore's shared worker does. Writes still happen in every tab —
it is TanStack DB's persistence layer, not this library, that coordinates those.

---

### 18. Lifecycle Hooks Observe, They Do Not Intercept

**Decision:** `hooks` exposes fire-and-forget callbacks (`onMutation`,
`onSyncStart`, `onDeadLetter`, and so on). A hook that throws is logged and
swallowed. Hooks cannot cancel, delay, or rewrite an event.

**Why:** the alternative — awaited, failable, mutating middleware — makes every
hook part of the sync loop's correctness. One slow analytics call would stall
sync; one thrown error would abort a push mid-batch. Observation is the common
need; the rare cases that genuinely want to transform data already have
`upcastEvent` (replay) and a custom transport (push/pull).

**Tradeoff:** you cannot veto a mutation from a hook. Validate before calling
`insert`/`update`/`delete` instead.

---

## Status and pruning

`getSyncStatus()` scans the outbox for pending/failed counts and reads
`syncmeta`. It is O(n) in outbox size and runs on every outbox / deadletter /
syncmeta change when subscribers exist — fine for modest backlogs, worth
caching if you keep tens of thousands of pending rows.

`pruneSyncedEvents` only deletes `sync: true` rows. It writes the cursor first
so pruning cannot rewind pull position. It does **not** prune `rowversions` or
`deadletter` (see open issues).

---

## Pros

- Zero learning curve for TanStack DB users
- Offline-first by default — works without network
- Partial sync — never downloads entire tables
- Foreign key safety — server controls insertion order
- Audit trail — complete history of every mutation
- Multi-device — events from all devices converge
- Platform agnostic — browser, React Native, Expo, Node
- Type-safe — full inference from collection definitions
- Small — ~4KB, minimal dependencies
- No build plugins or code generation

## Cons

- Requires a server (not P2P)
- No real-time push (polling or explicit sync)

<!-- TODO: resolve the rebase stuff -->

- No rollback or rebase — conflicts are detected and parked, never merged (decision #12)
- Conflict detection is opt-in and costs an index write per mutation and per replay
- Dead-lettered events are lost unless your UI surfaces them, in both directions (decisions #13, #13b)
- Two devices editing the same row between syncs do not converge (see Open issues)
- Leader election is per sync call, not a long-lived leader owning all writes (decision #17)
- Log pruning is manual — call `pruneSyncedEvents()` yourself on a schedule

<!-- TODO: resolve this -->

- Offline transport failures still consume retry budget (see Open issues)

<!-- TODO: resolve this whe should be able to replay from 0 -->

- Cannot replay from event 0 to rebuild state (state is materialized separately)
- Update events store full modified object (not a minimal diff)

<!-- TODO: resolve this and implment the custom functionality -->

- Materialization is a plain upsert, so there are no custom materializers or derived read models

---

## When NOT to Use This

| Scenario                                      | Better Alternative             |
| --------------------------------------------- | ------------------------------ |
| Real-time multiplayer / collaborative editing | CRDTs (Yjs, Automerge)         |
| Server-authoritative with no offline          | Direct API calls + React Query |
| Simple cache layer                            | TanStack Query                 |
| Multi-tenant with cross-user data             | Electric SQL, PowerSync        |
| Streaming large datasets                      | TanStack DB with Electric sync |

---

## When to Use This

- Single-user app with multi-device sync
- Offline-first mobile app (React Native)
- Todo apps, note-taking, personal finance, habit trackers
- Any app where the user owns their data and edits offline
- Apps that need an audit trail of changes
- Apps where you control the backend and want simple sync

---

## Adjacent TanStack Libraries & How They Compose

TanStack DB 0.6 shipped several features that directly improve this library's capabilities. Here's how each one fits.

### `createEffect` — Reactive Sync Trigger

**What it is:** A database-trigger-like API that fires `onEnter` / `onUpdate` / `onExit` callbacks when rows enter/leave a live query result. Runs incrementally on deltas, not full result sets.

**How it helps us:**

| Without createEffect                      | With createEffect                                          |
| ----------------------------------------- | ---------------------------------------------------------- |
| User must call `db.sync()` manually       | Auto-syncs when pending events exist                       |
| Polling interval for connectivity         | Reactive — fires the instant an event is logged            |
| No way to react to server-applied changes | `onEnter` fires when a new server event materializes a row |

**Integration pattern:**

```typescript
createEffect({
  query: (q) => q.from({ e: db.collections.outbox }).where(({ e }) => eq(e.sync, false)),
  skipInitial: false,
  onEnter: async (_event, ctx) => {
    await syncWithRetry({ signal: ctx.signal });
  },
});
```

This replaces `setInterval(() => db.sync(), 30000)` with a reactive approach that fires immediately when data is dirty.

**Trade-off:** Adds a dependency on `createEffect` from `@tanstack/db`. Currently optional — users who prefer manual sync can ignore it.

---

### `@tanstack/offline-transactions` — Production Sync Reliability

**What it is:** An `OfflineExecutor` that orchestrates:

- Persistent outbox (IndexedDB/localStorage)
- Leader election (WebLocks/BroadcastChannel)
- Retry with exponential backoff
- Connectivity detection
- Idempotency keys for at-least-once delivery
- Graceful degradation when storage is unavailable

**How it helps us:**

Our library's `sync()` function is a simple push+pull. In production, you need:

- Only one tab performing sync (leader election)
- Automatic retry when push fails (network blip)
- Backoff so you don't DDoS your own server
- Connectivity awareness (don't try to sync when offline)

`@tanstack/offline-transactions` provides all of this.

**Integration pattern:**

```typescript
import { createOfflineTransaction } from "@tanstack/offline-transactions";

const offlineSync = createOfflineTransaction({
  mutationFn: async () => {
    const result = await db.sync();
    if (result.errors.length > 0) throw result.errors[0];
  },
  retryConfig: { maxRetries: 5, backoffMs: 1000 },
});
```

**Trade-off:** Optional dependency. Our library works without it (manual sync), but production apps should use it.

---

### Virtual Props (`$synced`, `$origin`) — Sync State Visibility

**What they are:**

- `$synced: boolean` — whether the row is confirmed by sync or still optimistic/local
- `$origin: 'local' | 'remote'` — whether the last confirmed change came from this client or upstream

**How they help us:**

| Problem                                 | Solution with Virtual Props                                  |
| --------------------------------------- | ------------------------------------------------------------ |
| "Which todos haven't synced yet?"       | `where(({ todo }) => eq(todo.$synced, false))`               |
| "Show a spinner on unsynced items"      | Render based on `todo.$synced` in the UI                     |
| "Skip re-logging server-applied events" | Already handled — `acceptMutations` bypasses the write hooks |

**Note:** This library no longer needs a re-logging guard. Server replay goes through `acceptMutations`, which never invokes the outbox-logging hooks (see Design Decision #4). Virtual props remain useful for surfacing optimistic vs confirmed state in the UI.

---

### `includes` — Hierarchical Queries

**What it is:** Nested subqueries that project normalized data into the hierarchical shape of your UI, without N+1 queries. Each included field is a child collection with independent reactivity.

**How it helps us:**

Event-sourced state collections are flat (users, todos, settings). But UIs are hierarchical (a project has issues, each issue has comments). With `includes`, users query across their event-sourced collections with full hierarchical projection:

```typescript
const { data: users } = useLiveQuery((q) =>
  q.from({ u: db.collections.users }).select(({ u }) => ({
    id: u.id,
    name: u.name,
    todos: q
      .from({ t: db.collections.todos })
      .where(({ t }) => eq(t.userId, u.id))
      .select(({ t }) => ({ id: t.id, title: t.title })),
  })),
);
```

This would be extremely painful to build from scratch (incremental nested query evaluation). TanStack DB handles it in one query graph.

---

### `queryOnce` — One-Shot Reads

**What it is:** Execute a query once and get the result as a promise, without subscribing to updates.

**How it helps us:**

Useful internally for sync operations:

- "Get all pending events" — one-shot, no subscription needed
- "Get current cursor state" — read once at sync start
- Useful for migration scripts, exports, debugging

```typescript
const pending = await queryOnce((q) =>
  q
    .from({ e: db.collections.outbox })
    .where(({ e }) => eq(e.sync, false))
    .orderBy(({ e }) => e.localSeq, "asc"),
);
```

**When the in-memory loop stays.** A `queryOnce` call builds a result set. That is the right read when a predicate drops most rows (due outbox, pending inbox, one row's unsynced chain). It is the wrong read when the code already walks every row for a max, a count, or a list of keys. Those loops do not allocate a filtered array. Replacing them was measured on 15 Oct 2026 with `pnpm exec tsx scripts/probe-remaining-scans.ts` (5,500 outbox rows, 2,000 inbox rows, 1,500 todos, `localSeq` and `globalSeq` indexed):

| Function                          | Loop                      | `queryOnce`                                                                                                                      |
| --------------------------------- | ------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `nextLocalSeq`                    | 4.1 ms                    | 22.6 ms. `orderBy` + `limit(1)` still returns one full outbox row. Without the index, TanStack loads the whole collection first. |
| `getSyncStatus`                   | 5.0 ms for both counts    | 86 ms + 13 ms for two `count()` queries. Counts matched.                                                                         |
| `readPullCursor`                  | 1.5 ms                    | 19.9 ms. The max matched. The persisted cursor is still the floor; inbox rows only raise it.                                     |
| Delete-all in `resetLocalReplica` | key iterator, no row copy | 5,500 full rows, about 2.8 MB                                                                                                    |
| `compact()`                       | outbox keys only          | A live row with no outbox events has nothing to fold, so the collection key set is not walked.                                   |

Do not convert these five because a caller can `await`. The query is the extra copy.

---

### Summary: What We Get for Free vs What We Build

| Layer                                         | Source                         | Lines of Code |
| --------------------------------------------- | ------------------------------ | ------------- |
| SQLite persistence (8 platforms)              | TanStack DB                    | ~2,800        |
| Reactive query engine (differential dataflow) | TanStack DB                    | ~8,000        |
| Multi-tab coordination                        | TanStack DB                    | ~1,200        |
| Optimistic mutations + rollback               | TanStack DB                    | ~2,000        |
| Hierarchical includes                         | TanStack DB                    | ~1,500        |
| Reactive effects (createEffect)               | TanStack DB                    | ~600          |
| Offline retry + leader election               | @tanstack/offline-transactions | ~800          |
| **Event log + sync protocol + hook wiring**   | **This library**               | **~300**      |

We build 300 lines. We get 17,000+ lines of battle-tested infrastructure for free.

---

## Open issues (ordered by production risk)

These are known gaps, not forgotten TODOs. They are listed so contributors do
not "fix" them without the surrounding context.

### 0. Concurrent edits to one row do not converge

Two devices that edit the same row between syncs end up disagreeing, and neither
notices. Say `a` and `b` both edit row `t1`; `a`'s event lands at `globalSeq` 2
and `b`'s at 3, so the server's answer is unambiguously `b`. `a` pulls both,
skips its own event 2, applies 3, and is correct. `b` pulls both, applies `a`'s
event 2, then reaches its _own_ event 3 and skips it as local origin — so `b`
finishes holding `a`'s value while everyone else holds `b`'s.

The origin skip is not incidental: it is what stops a client re-applying its own
pruned history (decision #13c). Dropping it would trade this bug for a worse one.

**Direction:** replace the origin check with a per-row applied-sequence
watermark. Record the highest `globalSeq` applied to each `(collectionId, key)`,
updating it both on replay and on push confirmation, and apply an event only when
its `globalSeq` exceeds it. That makes replay order-independent and idempotent,
fixes this case, and subsumes the origin check. The cost is a watermark write per
mutation and per replay — which is what `rowversions` already does under
`conflictDetection`, currently opt-in precisely because of that cost. Turning it
on unconditionally is the decision this needs.

**Meanwhile:** `conflictDetection: true` turns the silent divergence into a
visible `CONFLICT` dead-letter, which is the safer failure. Partitioning writes by
owner avoids it entirely. Pinned by the "does not yet converge when two devices
edit the same row concurrently" test.

### 1. Connectivity vs retry budget

Sync runs whether or not the device is online. Offline transport failures still
increment `attemptCount` and schedule backoff. A long offline window can exhaust
`maxAttempts` and dead-letter events the server never saw.

**Direction:** only charge the retry budget when a server response was received
(or gate push on an injectable `isOnline` / `navigator.onLine`).

### 2. Outbox durability vs state durability

`persistedCollectionOptions` wraps our mutation hook: it awaits the hook (the
outbox append) and _then_ calls `persistAndConfirmCollectionMutations` to write
collection state. So the previously documented hazard — state persisting with no
event — does not occur: if the outbox append rejects, state is never written and
TanStack rolls the optimistic mutation back. That direction is pinned by the
"rolls the row back when the outbox append fails" test.

The remaining exposure is the mirror image. The outbox append has already
committed by the time the state write runs, so a state write that fails leaves an
event with no local row. The mutation rejects and the optimistic value is rolled
back, but the event survives, gets pushed, and materialises on every _other_
device. The authoring client then skips it on pull as its own echo and never
materialises it — the one device that originated the row is the only one without
it.

The two writes want to be one transaction. LiveStore co-commits eventlog and
state; the hook model cannot, since the two collections are persisted
independently. Closing it properly means either a shared transaction across the
outbox and the target collection, or reconciling the outbox against collection
state at startup.

### 3. `rowversions` growth

With `conflictDetection: true`, one index row per touched key forever, including
deletes. `pruneSyncedEvents` ignores it.

### 4. Byte-size batching

`pushBatchSize` counts events, not bytes. Large payloads (or one huge
transaction) can still blow body limits. LiveStore-style ~900KB caps would
complement tx-aware batching.

### 5. Per-call leader only

`localSeq` allocation has a TOCTOU window across tabs; `eventId` (uuidv7) breaks
ties so order stays deterministic. Cross-tab write leadership is still TanStack's
job, not ours.

### 6. No `AbortSignal`

A sync cannot be cancelled. bfcache / background tabs can hold the Web Lock
until the browser times it out.

### 7. Unbounded pull pages

Non-advancing cursors stop the loop; a server that always advances with
`hasMore: true` does not. A `maxPullPages` safety valve is missing.

### 8. Status scan cost

Full outbox scan on every mutation when a sync-status subscriber is attached.

### Scope limits (not bugs)

No rebase, no realtime subscribe, manual pruning only, plain upsert
materialization, full-object updates (not diffs), no cold-start rebuild from
event 0.

### Example server notes

The demo API does per-event conflict/dedup queries (fine for demos) and has no
auth or user scoping — do not treat it as a production sync backend.

---

## Testing notes

- Prefer `createMockSyncBackend()` over hand-rolled transports: it assigns
  `globalSeq`, dedupes by `eventId`, paginates, and can inject outages /
  rejections / backend resets.
- Assert `pushBatchSizes` when testing batching; assert deadletter reasons for
  conflict vs rejection vs max attempts.
- Hook tests should prove a throwing hook does **not** fail insert/sync.
- Repo vitest via `vite-plus` may lack native bindings in some environments;
  a plain vitest config with `unstubGlobals: true` is enough for this package's
  unit tests.

---

## Future Directions

Highest-impact production gaps are detailed under [Open issues](#open-issues-ordered-by-production-risk). Product directions after those:

1. **Real-time subscribe** — SSE/WebSocket for instant server push, replacing polling
2. **Automatic rebase** — roll back divergent local events, apply upstream, re-apply on top. Events are invertible (`previous` is recorded), so the raw material exists
3. **Long-lived leader** — a shared worker owning all writes, rather than per-call election
4. **Automatic pruning** — a retention policy rather than a manual `pruneSyncedEvents()` call
5. **Selective sync** — sync only specific collections or subsets
6. **Undo/redo** — leverage the invertible event log for time-travel
7. **Compression** — delta encoding for update events (store diff, not full object)
8. **Multi-user awareness** — optional user_id scoping on the event table
9. **Rebuild from events** — optional cold-start replay for disaster recovery
