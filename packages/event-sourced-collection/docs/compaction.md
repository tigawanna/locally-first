# Compaction

A row that is edited many times should not push a matching pile of partial updates. This package folds that chain on the device, then marks the event so a server can fold it again against anything the device had not pulled yet.

## On the device

`compactOnWrite` defaults to `true`.

An `update` looks at unsynced outbox events for the same collection and key, oldest first. If that chain is only an insert and updates, they are deleted and replaced by **one** event:

- The chain still contains the insert: the replacement is an `insert` of the latest row. The server has never stored this row, so a create is the right event.
- The insert was already synced: the replacement is an `update` of the latest row. `previous` is the row as it was before the first unsynced edit.

The replacement has `compaction: true` and `compactedAt` set to when the chain was folded. Already-synced events stay in the outbox; they are the server's problem to merge.

Set `compactOnWrite: false` to keep every edit as its own event. Call `db.compact()` to fold them later. `compact()` walks each synced collection, and for each current row id loads only that row's outbox events. Events older than the latest `compactedAt` stay as they are. That compaction and anything newer become one event, including events that have already synced. The replacement is unpushed, so the next `sync()` sends it.

`pruneSyncedEvents` is a different operation. It deletes events the server has already confirmed. `compact` rewrites events that have not been pushed.

## On the server

When a pushed event has `compaction: true`, treat `compactedAt` as "this device folded its local chain at this time." Do not delete every older update for that row.

The device may have compacted from a stale view. A newer update can already be in the log because another device pushed it, or because this device's push landed before its pull. Dropping those newer events leaves the row wrong.

Merge instead:

1. Find every stored event for the same collection and key.
2. Fold this compacted payload together with any event that is newer than what the device had based the compaction on.
3. Write one new compacted event for that row, with a fresh `compactedAt`, and drop the events that fold covered — including this one.

That second compaction is what lets the row catch up. The client's event is a proposal of its latest local row, not a instruction to discard the log.
