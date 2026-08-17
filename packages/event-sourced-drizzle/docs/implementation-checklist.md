# Event Sourced Drizzle - Implementation Checklist

Based on TanStack DB patterns, this document outlines features to implement.

## Phase 1: Core Foundation

### 1.1 Schema Validation

**Status:** Not Implemented

**TanStack DB Pattern:**

- Uses `@standard-schema` for type-safe validation
- Validates on insert/update before mutation
- Returns typed output matching schema
- Throws `SchemaValidationError` with issues array

**Implementation:**

```typescript
// types.ts additions
import type { StandardSchemaV1 } from "@standard-schema/spec";

export type EventSourcedDrizzleSchema = StandardSchemaV1;

export interface EventSourcedCollectionConfig<T> {
  schema?: EventSourcedDrizzleSchema<T>;
  // ... existing fields
}

// In mutate functions:
if (config.schema) {
  const result = config.schema["~standard"].validate(payload);
  if (result.issues) {
    throw new SchemaValidationError(result.issues);
  }
}
```

**Priority:** Low - Domain validation happens at API layer

---

### 1.2 Virtual Properties for Rows

**Status:** DONE

**Implemented in:** `src/core/virtual-props.ts`

**What was built:**

- `VirtualRowProps<TKey>` type with `$synced`, `$origin`, `$key`, `$collectionId`, `$syncStatus`, `$pendingCount`
- `WithVirtualProps<T, TKey>` utility type for enriched rows
- `OutboxStateProvider` interface for querying outbox state
- `createVirtualPropsEnricher()` factory that batch-queries outbox and enriches rows
- `stripVirtualProps()` helper to remove virtual props before passing to APIs
- `hasVirtualProps()` type guard

**Priority:** Low - Can add as computed properties on returned rows

---

### 1.3 Enhanced Error Types

**Status:** DONE

**Implemented in:** `src/core/errors.ts`

**What was built:**

- `EventSourcedDrizzleError` — root error class
- `SchemaValidationError` — event/row validation failures
- `UnknownCollectionError` — mutation targets unknown collection
- `DuplicateKeyError` — insert targets existing key
- `KeyNotFoundError` — update/delete targets missing key
- `EventConflictError` — server rejects due to baseVersion mismatch
- `SyncConflictError` — non-recoverable sync conflict
- `RetryExhaustedError` — max retries exceeded, dead-lettered
- `ReplayHaltedError` — inbox replay halted on unrecoverable event
- `EventNotFoundError` — event not found in outbox/inbox
- `TransactionStateError` — transaction in unexpected state
- `NoActiveTransactionError` — mutation outside required context
- `BackendMismatchError` — backend identity changed

**Priority:** Medium - Improves debugging and DX

---

## Phase 2: Advanced Features

### 2.1 Optimistic State Management

**Status:** DONE

**Implemented in:** `src/core/optimistic-state.ts`

**What was built:**

- `OptimisticStateTracker` class with:
  - `track()` / `confirm()` / `remove()` / `updateStatus()` — mutation lifecycle
  - `loadFromOutbox()` — bulk-load on startup
  - `isPending()` / `getPendingKeys()` / `getEntriesForKey()` — queries
  - `summary()` — aggregate view (pendingCount, syncingCount, failedCount, per-collection)
  - `subscribe()` — reactive listener for UI updates
  - `clear()` — reset on dispose
- Integrated into engine: standalone mutates track after commit, ambient transactions track after transact commit
- Exposed as `engine.optimistic` on the returned handle

**Priority:** Medium - Enables responsive UI with pending state

---

### 2.2 Transaction Nesting with Ambient Transactions

**Status:** DONE

**Implemented in:** `src/core/transaction.ts`

**What was built:**

- `TransactionImpl` class with state tracking and entry accumulation
- Module-level transaction stack (`pushTransaction` / `popTransaction`)
- `getActiveTransaction()` — public API to check ambient context
- `getActiveTransactionImpl()` — internal for mutate integration
- `createTransact()` factory bound to adapter/hooks/optimistic
- All mutate methods check for ambient transaction:
  - If present: defer domain op + outbox row to transaction entries
  - If absent: commit immediately in own DB transaction
- `engine.transact()` returns `{ txId, run }` for grouping mutations
- All entries in a transaction share the same `txId` and commit atomically

**Priority:** Medium - Enables batched mutations from multiple sources

---

### 2.3 Comprehensive Hook System

**Status:** Basic hooks implemented

**TanStack DB Pattern:**

- Event emitter with typed events
- Status change events
- Index events
- Subscribers change events

**Enhance Event Sourced Drizzle:**

```typescript
export type EventSourcedHooks = {
  // Existing hooks...

  // New hooks
  onEventValidated?: (context: { event: OutboxRow }) => void;
  onEventOutboxed?: (context: { event: OutboxRow }) => void;
  onOutboxBatchStart?: (context: { count: number }) => void;
  onOutboxBatchComplete?: (context: { pushed: number }) => void;
  onInboxBatchStart?: (context: { count: number }) => void;
  onInboxEventApplying?: (context: { event: InboxRow }) => void;
  onInboxEventApplied?: (context: { event: InboxRow }) => void;
  onRetryScheduled?: (context: { eventId: string; attempt: number }) => void;
  onRetryExhausted?: (context: { event: DeadLetterRow }) => void;
  onSchemaMigration?: (context: { fromVersion: number; toVersion: number }) => void;
};
```

**Priority:** Low - Nice to have for observability

---

## Phase 3: Data Management

### 3.1 Index Management

**Status:** Not Implemented (Drizzle handles natively)

**Priority:** Low - Drizzle handles this

---

### 3.2 Collection Metadata Storage

**Status:** Minimal - Uses sync-meta table

**Priority:** Low - Can use sync-meta table

---

### 3.3 Bulk Operations

**Status:** Not Implemented

**Note:** With ambient transactions now in place, bulk operations can be achieved by wrapping multiple inserts in `engine.transact().run(...)`. A dedicated `bulkInsert` convenience method could be added later.

**Priority:** Low - Can use transact() as workaround

---

## Phase 4: Sync Enhancements

### 4.1 Truncate/Reset Support

**Status:** Not Implemented

**Priority:** Low - Rarely needed

---

### 4.2 Load Subset / Pagination

**Status:** Not Implemented

**Priority:** Low - Handled by pull implementation

---

### 4.3 Conflict Detection & Resolution

**Status:** Not Implemented

**Note:** Error types for conflicts are now in place (`EventConflictError`, `SyncConflictError`). Actual resolution strategies (last-write-wins, server-wins, custom merge) still need implementation.

**Priority:** Medium - Important for multi-device sync

---

## Phase 5: Shared Protocol (Backend Compatibility)

### 5.1 Canonical Protocol Types

**Status:** DONE

**Implemented in:** `src/core/protocol.ts`

**What was built:**

- All wire protocol types extracted to a single canonical file:
  - `OutboundEvent`, `ServerEvent`, `PushConfirmation`, `PushFailure`, `PushResponse`, `PullResponse`
  - `PushEventsFn`, `PullEventsFn`, `SyncTransport`, `SyncUrlConfig`, `SyncHandlersConfig`, `NormalizedSyncTransport`
- `src/core/sync.ts` now re-exports from `protocol.ts` (backward compatible)
- `Protocol*` aliases exported from index for explicit imports
- Fully documented with JSDoc explaining backend compatibility requirements

**Priority:** Medium - Prevents type drift between engines

---

## Priority Summary

| Feature                 | Priority | Effort | Impact         | Status          |
| ----------------------- | -------- | ------ | -------------- | --------------- |
| Schema Validation       | Low      | Small  | DX             | Not Implemented |
| Virtual Properties      | Low      | Small  | DX             | **DONE**        |
| Error Types             | Medium   | Medium | DX, Debugging  | **DONE**        |
| Protocol Types          | Medium   | Small  | Backend Compat | **DONE**        |
| Optimistic State        | Medium   | Medium | UX             | **DONE**        |
| Ambient Transactions    | Medium   | Medium | DX             | **DONE**        |
| Hook System Enhancement | Low      | Small  | Observability  | Not Implemented |
| Truncate Support        | Low      | Small  | Edge Cases     | Not Implemented |
| Conflict Resolution     | Medium   | Medium | Data Integrity | Not Implemented |

---

## Recommended Implementation Order (Updated)

1. ~~**Enhanced Error Types**~~ — DONE
2. **Schema Validation** — Optional feature, low effort
3. ~~**Ambient Transactions**~~ — DONE
4. ~~**Virtual Properties**~~ — DONE
5. ~~**Optimistic State**~~ — DONE
6. **Conflict Resolution** — Next critical feature for production

---

## New Files Created

| File                           | Purpose                                             |
| ------------------------------ | --------------------------------------------------- |
| `src/core/errors.ts`           | Domain-specific error types (13 error classes)      |
| `src/core/protocol.ts`         | Canonical wire protocol types shared with backend   |
| `src/core/virtual-props.ts`    | Virtual property enrichment for domain rows         |
| `src/core/transaction.ts`      | Ambient transaction system with stack-based context |
| `src/core/optimistic-state.ts` | In-memory tracker for pending sync mutations        |

## Modified Files

| File                                       | Changes                                                 |
| ------------------------------------------ | ------------------------------------------------------- |
| `src/index.ts`                             | Added exports for all new modules                       |
| `src/core/sync.ts`                         | Refactored to re-export types from protocol.ts          |
| `src/core/types.ts`                        | Added `transact` and `optimistic` to engine type        |
| `src/core/create-event-sourced-drizzle.ts` | Integrated ambient tx + optimistic tracking into mutate |
