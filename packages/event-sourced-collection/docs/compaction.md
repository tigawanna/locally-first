# Compaction

A row that is edited many times should not push a matching pile of partial updates. This package folds that chain on the device, then marks the event so a server can fold it again against anything the device had not pulled yet.

## On the device

`compactOnWrite` defaults to `true`.

An `update` looks at unsynced outbox events for the same collection and key, oldest first. If that chain is only an insert and updates, they are deleted and replaced by **one** event:

- The chain still contains the insert: the replacement is an `insert` of the latest row. The server has never stored this row, so a create is the right event.
- The insert was already synced: the replacement is an `update` of the latest row. `previous` is the row as it was before the first unsynced edit.

The replacement has `compaction: true` and `compactedAt` set to when the chain was folded. Already-synced events stay in the outbox; they are the server's problem to merge.

A delete ends one span of that row's outbox history: the events from its insert through that delete. The code calls one span a life. The next insert of the same key starts another. `compactOnWrite` only folds the unsynced insert and updates that come after the latest delete. The delete, and every event before it, stays until `compact()`.

Set `compactOnWrite: false` to keep every edit as its own event. Call `db.compact()` to fold them later. `compact()` visits outbox keys only. For a row that still exists, it folds the contiguous unsynced insert and update tail into one event. Events the server has already accepted stay where they are, so a later `compact()` does not upload them again. The replacement keeps the `localSeq` and `txId` of the chain it came from, and the next `sync()` pushes it.

For a span that ends in a delete, `compact()` does not fold it into the next insert:

- The server never saw that span: the insert, the updates, and the delete are dropped.
- The server did see it: synced events stay, and the unsynced tail becomes one delete of the last synced row.

`pruneSyncedEvents` is a different operation. It deletes events the server has already confirmed. `compact` rewrites an unsynced chain into one event the next sync will push, and leaves the synced prefix in place.

A fold does not run while a push of that row is in flight. The request already left with the old payload, so the device appends the newer edit behind it instead of deleting the row the server is about to confirm. `compact()` takes the same sync lock as `sync()`, and waits rather than rewriting rows another tab is pushing.

## On the server

When a pushed event has `compaction: true`, treat `compactedAt` as "this device folded its local chain at this time." Do not delete every older update for that row.

The device may have compacted from a stale view. A newer update can already be in the log because another device pushed it, or because this device's push landed before its pull. Dropping those newer events leaves the row wrong.

Merge instead:

1. Find every stored event for the same collection and key.
2. Fold this compacted payload together with any event that is newer than what the device had based the compaction on.
3. Write one new compacted event for that row, with a fresh `compactedAt`, and drop the events that fold covered — including this one.

That second compaction is what lets the row catch up. The client's event is a proposal of its latest local row, not a instruction to discard the log.
