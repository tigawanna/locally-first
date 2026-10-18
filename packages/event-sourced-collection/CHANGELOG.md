# Changelog

All notable changes to `event-sourced-collection` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## [0.0.15] - 2026-10-16

Switch to this if you edit the same rows often, or you want fewer round trips.

### Added

- `compactOnWrite` (default `true`). An update replaces the unsynced outbox chain
  for that row with one `compaction` event carrying `compactedAt`, instead of
  appending another partial update. The folded event keeps its place in the
  queue and its transaction, and `compaction` / `compactedAt` are sent on push.
  Set `compactOnWrite: false` only if the server must see every intermediate
  edit. Server guidance is in `docs/compaction.md`: merge the compacted event
  with newer upstream updates, do not drop them.
- `db.compact()`. Folds unsynced insert/update tails that were written with
  compaction off, or that are still sitting as a chain. Already-synced events
  stay, so a later compact does not upload them again.
- `syncBatch` (`minEvents` and/or `delayMinutes`). Holds the push until enough
  unpushed rows are waiting or the delay has elapsed. Pull still runs. A held
  push is reported as `SyncResult.batchHold` (`SyncBatchLimitError` fields) and
  is not thrown. With `compactOnWrite`, `minEvents` counts outbox rows, so
  repeated edits of one row count once.

```ts
const db = createEventSourcedDB({
  // compactOnWrite: true is the default
  syncBatch: { minEvents: 20, delayMinutes: 10 },
  // ...
});

await db.compact();
```

### Changed

- `resetLocalReplica()` clears the stored backend id, so a client configured
  with `backendMismatch: "fail"` can adopt a rebuilt backend after a local reset.

## [0.0.14] - 2026-10-06

### Added

- `db.resetLocalReplica()`. Drops local rows and sync state for a fresh device
  replica. `localOnly` collections are kept. Refuses while unsynced changes are
  still in the outbox and throws `UnsyncedChangesError` unless you pass
  `{ discardPending: true }`.

## [0.0.13] - 2026-10-06

### Added

- `localOnly: true` on a collection definition. The collection stays on the
  device and does not write outbox events or participate in push/pull.

## [0.0.12] - 2026-10-06

### Added

- `db.getPushLimits()` / `db.setPushLimits()`. Read and change `maxPushEvents`
  and `maxPushBytes` after the database is created.

## [0.0.11] - 2026-10-06

### Added

- `maxPushBytes` and `syncPreset` on database creation, so a push batch stops
  on byte size as well as event count.
- `recordLocalEchoes` (also listed under 0.0.10). Set `false` to advance the
  pull cursor past this device's own events without writing resolved inbox
  echo rows.

## [0.0.10] - 2026-09-08

### Added

- `recordLocalEchoes` option (default `true`). Set `false` to advance the pull
  cursor past this device's own events without writing resolved inbox echo rows.

## [0.0.9] - 2026-09-08

### Added

- `useSyncStatus` / `useEnsureDb` on `event-sourced-collection/react`
- `useManualSync({ db })` so `syncing` follows `subscribeSyncStatus` (load sync + manual)

## [0.0.7] - 2026-08-09

Reliability-focused release: dead-lettering, backend identity, retries, conflict detection, multi-tab locking, React helpers, and published examples.

### Added

- Dead-letter collection for permanently rejected or exhausted outbound/inbound events
- `backendId` / `backendMismatch` handling (`resetCursor` | `fail` | `ignore`) and `BackendMismatchError`
- Retry config with exponential backoff; batched push that preserves earlier batch progress on failure
- `getSyncStatus()` / `subscribeSyncStatus()` for pending/failed/synced observability
- `pruneSyncedEvents()` to compact confirmed outbox and resolved inbox rows
- Optional conflict detection via `rowversions` + `baseVersion`
- Lifecycle `hooks` (`onMutation`, `onSyncStart`, `onEventPushed`, `onBackendMismatch`, `onDeadLetter`, …)
- Event schema evolution: `eventSchemaVersion`, `upcastEvent`, `unknownEventHandling`
- Multi-tab sync lock via Web Locks (`createWebLocksSyncLock`, `supportsWebLocks`); `lock: null` to opt out
- `createMockSyncBackend()` for in-memory push/pull tests (pagination, rejections, backend reset)
- `event-sourced-collection/react` entry: `useManualSync`, `useSyncEnabled`, `formatManualSyncMessage`
- Published examples: `examples/web-worker-sync/` (Dedicated Worker transport) and `examples/postgres-sync-server/`
- Repository and bugs metadata on the package

### Changed

- Sync transport surface expanded (`SyncLock`, `SyncStatus`, `RetryConfig`, prune/hook types, …)
- Package `files` now includes `examples/`

## [0.0.6] - 2026-06-24

### Changed

- Documentation and project-structure guidance for browser apps (`collections.ts`, settings, sync wrappers)

## [0.0.5] - 2026-06-24

### Added

- `CollectionIndexDef` and typed collection index registration on defs

## [0.0.2] - 2026-06-24

### Added

- `createLazySingleton()` for deferred DB init (`ensure` / `proxy` / `reset`)
- `ARCHITECTURE.md` shipped with the package

## [0.0.1] - 2026-06-23

### Added

- Initial release: `createEventSourcedDB`, browser and React Native helpers
- Outbox / inbox sync over `SyncTransport` or `createHttpTransport`
- `createEventSourcedLogger`, `generateEventId` (UUIDv7)
- Platform entry points: `event-sourced-collection/browser`, `event-sourced-collection/react-native`

[0.0.15]: https://github.com/tigawanna/locally-first/releases/tag/v0.0.15
[0.0.14]: https://www.npmjs.com/package/event-sourced-collection/v/0.0.14
[0.0.13]: https://www.npmjs.com/package/event-sourced-collection/v/0.0.13
[0.0.12]: https://www.npmjs.com/package/event-sourced-collection/v/0.0.12
[0.0.11]: https://www.npmjs.com/package/event-sourced-collection/v/0.0.11
[0.0.10]: https://www.npmjs.com/package/event-sourced-collection/v/0.0.10
[0.0.7]: https://www.npmjs.com/package/event-sourced-collection/v/0.0.7
[0.0.6]: https://www.npmjs.com/package/event-sourced-collection/v/0.0.6
[0.0.5]: https://www.npmjs.com/package/event-sourced-collection/v/0.0.5
[0.0.2]: https://www.npmjs.com/package/event-sourced-collection/v/0.0.2
[0.0.1]: https://www.npmjs.com/package/event-sourced-collection/v/0.0.1
