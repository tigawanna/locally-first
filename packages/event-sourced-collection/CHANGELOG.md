# Changelog

All notable changes to `event-sourced-collection` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.0.7]: https://www.npmjs.com/package/event-sourced-collection/v/0.0.7
[0.0.6]: https://www.npmjs.com/package/event-sourced-collection/v/0.0.6
[0.0.5]: https://www.npmjs.com/package/event-sourced-collection/v/0.0.5
[0.0.2]: https://www.npmjs.com/package/event-sourced-collection/v/0.0.2
[0.0.1]: https://www.npmjs.com/package/event-sourced-collection/v/0.0.1
