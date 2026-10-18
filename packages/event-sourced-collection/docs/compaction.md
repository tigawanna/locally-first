# Compaction

A row that is edited many times should not keep a matching pile of updates. The device folds its own unsynced chain before push. The server folds again, because **several devices share one Postgres log**.

`payload` is always the **whole row**, not a patch. Replaying an insert and then the last update for that row is enough to rebuild it. That is why the server can drop the updates in the middle.

## Postgres schema

Store enough to find one row's events, tell a compacted snapshot from an ordinary edit, and know which server event the device started from.

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
  base_version      TEXT,
  compaction        BOOLEAN NOT NULL DEFAULT FALSE,
  compacted_at      BIGINT,
  client_timestamp  BIGINT NOT NULL,
  server_timestamp  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_events_global_seq ON events (global_seq);
CREATE INDEX idx_events_row ON events (collection_id, key, global_seq);
```

`idx_events_row` is the compaction index: one collection, one key, ordered by `global_seq`. JSONB is for `payload` and `previous`. You do not need JSON operators to pick the winning event. You need them if you merge fields (see below).

| Column                 | Role                                                                                       |
| ---------------------- | ------------------------------------------------------------------------------------------ |
| `collection_id`, `key` | One logical row. Compaction never crosses these.                                           |
| `type`                 | Insert stays. Updates in the middle of a span can go. A delete ends the span.              |
| `payload`              | Full row after this event.                                                                 |
| `previous`             | Full row before this event. Used to see which fields this event actually changed.          |
| `base_version`         | `event_id` this device had already applied. Events after that id are ones it had not seen. |
| `compaction`           | True when this payload already replaced a local chain on the device.                       |
| `compacted_at`         | Unix ms when that fold happened.                                                           |

Leave the insert row in place once it has a `global_seq`. Devices that already pulled it will not see a mutated insert: pull only returns `global_seq > cursor`.

## On the device

`compactOnWrite` defaults to `true`. An update looks at unsynced outbox events for the same collection and key, oldest first. If that chain is only an insert and updates, they become **one** event:

- The chain still contains the insert: the replacement is an `insert` of the latest row.
- The insert was already synced: the replacement is an `update` of the latest row. `previous` is the row before the first unsynced edit. `baseVersion` is that oldest event's server id.

The replacement has `compaction: true` and `compactedAt`. Already-synced events stay; they are the server's problem.

A delete ends one span of that row. The next insert of the same key starts another. `compactOnWrite` only folds unsynced insert and updates after the latest delete.

Set `compactOnWrite: false` to keep every edit. Call `db.compact()` later. A fold does not run while a push of that row is in flight.

### What the device sends

Insert, then color, then two titles, none synced. Outbox holds one insert:

```json
{
  "eventId": "a-fold-1",
  "collectionId": "todos",
  "type": "insert",
  "key": "task-1",
  "payload": { "id": "task-1", "title": "Task 2", "color": "red" },
  "previous": null,
  "baseVersion": null,
  "clientId": "phone-A",
  "compaction": true,
  "compactedAt": 1760000000000
}
```

Insert already synced, then two title edits. Outbox holds one update:

```json
{
  "eventId": "a-fold-2",
  "collectionId": "todos",
  "type": "update",
  "key": "task-1",
  "payload": { "id": "task-1", "title": "Task 2", "color": "red" },
  "previous": { "id": "task-1", "title": "Task", "color": "red" },
  "baseVersion": "evt-insert-1",
  "clientId": "phone-A",
  "compaction": true,
  "compactedAt": 1760000000100
}
```

## One row in Postgres is a span

Filter `WHERE collection_id = $1 AND key = $2 ORDER BY global_seq`. Split that list on `type = 'delete'`. Compaction runs inside one span only.

```json
[
  { "globalSeq": 1, "type": "insert", "payload": { "title": "Task", "color": "blue" } },
  { "globalSeq": 2, "type": "update", "payload": { "title": "Task", "color": "red" } },
  { "globalSeq": 3, "type": "update", "payload": { "title": "Task 2", "color": "red" } },
  { "globalSeq": 4, "type": "delete", "payload": { "title": "Task 2", "color": "red" } },
  { "globalSeq": 5, "type": "insert", "payload": { "title": "Task", "color": "blue" } },
  { "globalSeq": 6, "type": "update", "payload": { "title": "New", "color": "blue" } }
]
```

Two spans: seq 1–4 (closed by delete) and seq 5–6 (still live). Do not fold seq 6 into seq 1. Do not drop the delete.

## Approach A — last stored update wins

Several devices, one Postgres. After a push (or on a job), for each `(collection_id, key)` span:

1. Keep the insert.
2. Keep the update with the highest `global_seq`.
3. Delete every other update in that span.
4. Leave deletes alone.

```sql
WITH span AS (
  SELECT event_id, type, global_seq,
         first_value(event_id) FILTER (WHERE type = 'insert')
           OVER (ORDER BY global_seq) AS insert_id,
         last_value(event_id) FILTER (WHERE type = 'update')
           OVER (ORDER BY global_seq
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_update_id
  FROM events
  WHERE collection_id = $1 AND key = $2
    AND global_seq > $last_delete_seq
    AND global_seq < $next_delete_seq
)
DELETE FROM events
WHERE event_id IN (
  SELECT event_id FROM span
  WHERE type = 'update'
    AND event_id IS DISTINCT FROM last_update_id
);
```

Same device, three events. The last payload already contains every earlier field:

```json
[
  {
    "globalSeq": 1,
    "type": "insert",
    "clientId": "A",
    "payload": { "title": "Task", "color": "blue" }
  },
  {
    "globalSeq": 2,
    "type": "update",
    "clientId": "A",
    "payload": { "title": "Task", "color": "red" }
  },
  {
    "globalSeq": 3,
    "type": "update",
    "clientId": "A",
    "payload": { "title": "Task 2", "color": "red" }
  }
]
```

After collapse, seq 1 and seq 3 remain. Replay is `{ "title": "Task 2", "color": "red" }`. Same as replaying all three.

Two devices, last snapshot is stale. Phone B set the color. Phone A renamed from the original insert and never pulled the color:

```json
[
  {
    "globalSeq": 1,
    "type": "insert",
    "clientId": "A",
    "eventId": "evt-1",
    "payload": { "title": "Task", "color": "blue" }
  },
  {
    "globalSeq": 2,
    "type": "update",
    "clientId": "B",
    "eventId": "evt-2",
    "payload": { "title": "Task", "color": "red" }
  },
  {
    "globalSeq": 3,
    "type": "update",
    "clientId": "A",
    "eventId": "evt-3",
    "compaction": true,
    "baseVersion": "evt-1",
    "payload": { "title": "Task 2", "color": "blue" }
  }
]
```

Keeping seq 3 drops `"color": "red"`. Seq 3 arrived last, but it was folded from seq 1. That is the whole cost of this approach: last stored update wins the **entire object**, including fields that writer did not change.

A delete in the span. "Latest update" is not the latest event:

```json
[
  { "globalSeq": 1, "type": "insert", "payload": { "title": "Task", "color": "blue" } },
  { "globalSeq": 2, "type": "update", "payload": { "title": "Task", "color": "red" } },
  { "globalSeq": 3, "type": "update", "payload": { "title": "Task 2", "color": "red" } },
  { "globalSeq": 4, "type": "delete", "payload": { "title": "Task 2", "color": "red" } }
]
```

Dropping seq 2 and seq 3 and keeping the insert puts the task back. Collapse only updates after the insert and before the next delete. The delete stays.

### Caveats for A

- Devices that already pulled seq 2 keep it locally. They still receive seq 3 on the next pull, and seq 3 overwrites the row. Do not mutate seq 1 in place.
- After you delete seq 2, a later push may send `baseVersion: "evt-2"`. Do not `CONFLICT` just because that id is gone. Last-write-wins already discarded it.
- `compaction: true` is not required for this approach. Any latest update is a full row. The flag is useful as a hint to run the job, not as a different merge.

## Approach B — overlay fields the device had not seen

Same schema. Different rule when `compaction` is true: the payload is a proposal, not a wipe.

1. Load the span for that collection and key.
2. Find the event whose `event_id = base_version`.
3. For every event after that id, copy onto the compacted payload each field that changed (`payload.field IS DISTINCT FROM previous.field`).
4. Insert one new update with that merged payload. Delete the updates the fold covered.

Phone A compacted from seq 1. Phone B's color change is after that base, so it is copied on:

```json
[
  {
    "globalSeq": 1,
    "type": "insert",
    "eventId": "evt-1",
    "payload": { "title": "Task", "color": "blue" }
  },
  {
    "globalSeq": 2,
    "type": "update",
    "eventId": "evt-2",
    "previous": { "title": "Task", "color": "blue" },
    "payload": { "title": "Task", "color": "red" }
  }
]
```

Incoming compacted event:

```json
{
  "eventId": "evt-3",
  "type": "update",
  "baseVersion": "evt-1",
  "compaction": true,
  "payload": { "title": "Task 2", "color": "blue" },
  "previous": { "title": "Task", "color": "blue" }
}
```

Seq 2 changed `color` (`blue` → `red`). Overlay onto A's payload:

```json
{ "title": "Task 2", "color": "red" }
```

Store that as a new event (seq 4). Drop seq 2 and do not keep evt-3 as-is. Pullers that already have seq 2 still get seq 4, which is the merged row.

Both devices changed the same field. Last overlay wins that field:

```json
{
  "fromA": { "title": "Task 2", "color": "blue" },
  "fromB": { "title": "Task", "color": "red" },
  "sameField": "neither title nor color is merged per-author; the later event's changed fields overwrite"
}
```

If B also set `title` after A's base, the overlay replaces A's title too. This is still not CRDT conflict UI. It only keeps **fields the other writer actually changed**.

JSONB helpers for "this event changed this key":

```sql
SELECT e.event_id, changed.key, changed.value
FROM events e
CROSS JOIN LATERAL jsonb_each(e.payload) AS changed(key, value)
WHERE e.collection_id = $1 AND e.key = $2
  AND e.global_seq > (
    SELECT global_seq FROM events WHERE event_id = $base_version
  )
  AND (e.previous -> changed.key) IS DISTINCT FROM changed.value;
```

### Caveats for B

- Needs `previous` on stored updates. Without it you cannot tell a field the writer set from a field they merely copied in a full-row snapshot.
- Needs `base_version`. If it is null, treat every stored event in the span as unseen, or fall back to approach A.
- Still last-write-wins **per field**, not a user-facing merge.

## What to pick

|                                      | Approach A                                   | Approach B                                 |
| ------------------------------------ | -------------------------------------------- | ------------------------------------------ |
| Postgres work                        | `ORDER BY global_seq`, delete middle updates | Same, plus `jsonb_each` vs `previous`      |
| Multi-device                         | Newest full row wins                         | Newest **changed fields** win              |
| Lost data in the color/title example | B's color                                    | Nothing, unless both edited the same field |
| Mental model                         | Last write wins                              | Last write wins per field                  |

Use A if a row is one blob and you accept that the last pusher overwrites it. Use B if two phones often edit different fields of the same row. Neither is a conflict UI.

Ordinary events with `compaction = false` still append. For A you can compact them on a schedule. For B you only merge when the flag is set; otherwise keep the usual `base_version` conflict check.

## Hono sketch (approach B, same merge as the SQL)

```ts
import { Hono } from "hono";
import type { OutboundEvent, PushResponse } from "event-sourced-collection";

type StoredEvent = OutboundEvent & { globalSeq: number };

const app = new Hono();

app.post("/api/sync/events", async (c) => {
  const batch = await c.req.json<OutboundEvent[]>();
  const confirmed: PushResponse["confirmed"] = [];

  for (const event of batch) {
    if (event.compaction) {
      confirmed.push(await acceptCompacted(event));
      continue;
    }
    confirmed.push(await append(event));
  }

  return c.json({ confirmed } satisfies PushResponse);
});

function overlayChangedFields(
  compacted: Record<string, unknown>,
  newer: StoredEvent,
): Record<string, unknown> {
  const previous = newer.previous ?? {};
  const next = { ...compacted };
  for (const [field, value] of Object.entries(newer.payload)) {
    if (previous[field] !== value) next[field] = value;
  }
  return next;
}
```

`append` inserts into `events` and returns `{ eventId, globalSeq }`. `acceptCompacted` runs inside the same transaction that takes `pg_advisory_xact_lock` on the log: load the span, overlay events after `base_version`, delete covered updates, insert one new compacted row.

`pruneSyncedEvents` on the client is a different operation. It deletes events the server has already confirmed. Server compaction rewrites the log other devices will pull.
