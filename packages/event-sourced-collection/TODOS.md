# Todos

Next work in `event-sourced-collection`, in order. Background for both is in `ARCHITECTURE.md` under Open issues.

## 1. Concurrent edits to one row do not converge

**Status:** Not implemented

Two devices that edit the same row between syncs finish on different values. The server orders the events, but the device whose event landed later skips its own echo on pull and keeps the other device's older row. `compactOnWrite` does not fix this: each device only folds its own chain.

Pinned by the test "does not yet converge when two devices edit the same row concurrently" in `src/__tests__/integration/sync-convergence.test.ts`.

**Implement:** a per-row applied-sequence watermark. Record the highest `globalSeq` applied to each `(collectionId, key)`, on replay and on push confirmation, and apply an event only when its `globalSeq` is higher. That replaces the origin skip. `rowversions` already does this write when `conflictDetection` is on; the decision is whether the watermark is always on.

Until then, `conflictDetection: true` turns the silent split into a `CONFLICT` dead-letter.

## 2. Offline failures still spend the retry budget

**Status:** Not implemented

Sync runs whether or not the device is online. A transport failure increments `attemptCount` and schedules backoff. A long offline window can exhaust `maxAttempts` and dead-letter events the server never saw.

**Implement:** charge the retry budget only when a server response was received, or gate push on an injectable `isOnline` / `navigator.onLine`.
