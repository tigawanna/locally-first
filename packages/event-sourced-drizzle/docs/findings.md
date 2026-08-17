# Event Sourced Drizzle - Findings from TanStack DB Introspection

This document outlines patterns and implementations from TanStack DB that can be leveraged or adapted for Event Sourced Drizzle.

## 1. Transaction System

TanStack DB has a sophisticated transaction system that handles grouping multiple operations, automatic merging of mutations, and ambient transaction support.

### Key Files

- `packages/db/src/transactions.ts`

### Patterns Identified

**Transaction Creation:**

```typescript
// Ambient transaction support
export function getActiveTransaction(): Transaction | undefined {
  if (transactionStack.length > 0) {
    return transactionStack.slice(-1)[0];
  }
  return undefined;
}

// Create transaction with mutation function
const tx = createTransaction({
  mutationFn: async ({ transaction }) => {
    await api.saveChanges(transaction.mutations);
  },
});

tx.mutate(() => {
  collection.insert({ id: "1", text: "Buy milk" });
  collection.update("2", (draft) => {
    draft.completed = true;
  });
});
```

**Mutation Merging Logic:**
The system merges mutations for the same key according to rules:

- `insert + update` → `insert` (merge changes, keep empty original)
- `insert + delete` → `null` (cancel both)
- `update + delete` → `delete` (delete dominates)
- `update + update` → `update` (union changes)
- `delete + delete` → `delete`
- `insert + insert` → `insert` (replace with latest)

**Transaction States:**

- `pending` - Created but mutate() not yet called
- `persisting` - mutate() called, mutationFn executing
- `completed` - Successfully finished
- `failed` - Rolled back due to error

### Relevance to Event Sourced Drizzle

Currently uses Drizzle's `adapter.transaction()` directly. Could benefit from:

- Ambient transaction detection for nested operations
- Mutation merging for batching
- Deferred promise pattern (`isPersisted.promise`)

---

## 2. Collection Manager Architecture

TanStack DB uses a composition pattern with multiple manager classes.

### Key Files

- `packages/db/src/collection/index.ts`
- `packages/db/src/collection/mutations.ts`
- `packages/db/src/collection/state.ts`
- `packages/db/src/collection/changes.ts`
- `packages/db/src/collection/sync.ts`
- `packages/db/src/collection/events.ts`

### Manager Classes

1. **CollectionMutationsManager** - Handles insert/update/delete logic
2. **CollectionStateManager** - Core data storage with optimistic state
3. **CollectionChangesManager** - Change tracking and event emission
4. **CollectionLifecycleManager** - Status tracking (idle → loading → ready → error)
5. **CollectionSyncManager** - Sync begin/write/commit/loadSubset
6. **CollectionIndexesManager** - Index creation and maintenance
7. **CollectionEventsManager** - Event emission for status/index changes

### Pattern: Manager Dependency Injection

```typescript
class CollectionMutationsManager {
  private lifecycle!: CollectionLifecycleManager;
  private state!: CollectionStateManager;
  private collection!: CollectionImpl;

  setDeps(deps: {
    lifecycle: CollectionLifecycleManager;
    state: CollectionStateManager;
    collection: CollectionImpl;
  }) {
    this.lifecycle = deps.lifecycle;
    this.state = deps.state;
    this.collection = deps.collection;
  }
}
```

### Relevance to Event Sourced Drizzle

Current implementation is flat. Could adopt manager pattern for:

- Better separation of concerns
- Testability through dependency injection
- Easier feature addition

---

## 3. Schema Validation

TanStack DB integrates with `@standard-schema` for type-safe validation.

### Key Files

- `packages/db/src/collection/mutations.ts`
- `packages/db/src/types.ts`

### Pattern

```typescript
import type { StandardSchemaV1 } from '@standard-schema/spec'

public validateData(
  data: unknown,
  type: `insert` | `update`,
  key?: TKey
): TOutput | never {
  if (!this.config.schema) return data as TOutput

  const result = this.config.schema[`~standard`].validate(data)

  if (`issues` in result && result.issues) {
    throw new SchemaValidationError(type, result.issues)
  }

  return result.value as TOutput
}
```

### Relevance to Event Sourced Drizzle

Not currently implemented. Could add:

- Schema validation on mutate
- Input type inference from schema
- Better error messages for validation failures

---

## 4. Change Tracking (Proxy-based)

TanStack DB uses proxies to track nested changes in objects.

### Key Files

- `packages/db/src/proxy.ts`

### Pattern: Create Change Proxy

```typescript
export function createChangeProxy<T extends Record<string, any>>(
  target: T,
  parent?: { tracker: ChangeTracker; prop: string | symbol },
): {
  proxy: T;
  getChanges: () => Record<string, symbol, any>;
};
```

**Features:**

- Deep cloning preserves Date, RegExp, TypedArray, Map, Set, Temporal
- Handles array methods (push, pop, filter, map, etc.)
- Handles Map/Set iteration with proxied elements
- Tracks revert-to-original behavior

### Relevance to Event Sourced Drizzle

Could be adapted for:

- Update draft handling (currently just passes patch object)
- Better change diffing
- Nested change tracking in update payloads

---

## 5. Virtual Properties

TanStack DB computes virtual properties ($synced, $origin, $key, $collectionId) for every row.

### Key Files

- `packages/db/src/virtual-props.ts`
- `packages/db/src/collection/state.ts`

### Pattern

```typescript
interface VirtualRowProps<TKey> {
  $synced: boolean        // No pending local changes
  $origin: VirtualOrigin  // 'local' | 'remote'
  $key: TKey
  $collectionId: string
}

public enrichWithVirtualProps(
  row: TOutput,
  key: TKey
): WithVirtualProps<TOutput, TKey> {
  return {
    ...row,
    $synced: this.isRowSynced(key),
    $origin: this.getRowOrigin(key),
    $key: key,
    $collectionId: this.collection.id,
  }
}
```

### Relevance to Event Sourced Drizzle

Could add virtual props computed from sync state:

- `$synced` - Has event been pushed/acked
- `$localSeq` - Local sequence number
- `$pendingSync` - Still in outbox

---

## 6. Optimistic State Management

TanStack DB maintains separate optimistic and synced state.

### Key Files

- `packages/db/src/collection/state.ts`

### Pattern

```typescript
// Synced state (confirmed by backend)
public syncedData: SortedMap<TKey, TOutput>

// Optimistic state (pending confirmation)
public optimisticUpserts = new Map<TKey, TOutput>()
public optimisticDeletes = new Set<TKey>()

// Pending (from completed transactions, not yet applied)
public pendingOptimisticUpserts = new Map<TKey, TOutput>()
public pendingOptimisticDeletes = new Set<TKey>()

public recomputeOptimisticState(triggeredByUserAction: boolean): void {
  // Build optimistic state from transactions
}
```

### Relevance to Event Sourced Drizzle

Could adopt for:

- Showing pending sync status in UI
- Optimistic local updates before events are acknowledged
- Better handling of concurrent sync and local mutations

---

## 7. Comprehensive Error Handling

TanStack DB has granular error types for different failure modes.

### Key Files

- `packages/db/src/errors.ts`

### Error Categories

- **Schema errors:** SchemaValidationError, InvalidSchemaError
- **Transaction errors:** TransactionNotPendingMutateError, MissingMutationFunctionError
- **Operation errors:** DuplicateKeyError, UpdateKeyNotFoundError, MissingInsertHandlerError
- **Collection errors:** CollectionInErrorStateError, InvalidCollectionStatusTransitionError

### Pattern

```typescript
export class SchemaValidationError extends TanStackDBError {
  type: `insert` | `update`;
  issues: ReadonlyArray<{
    message: string;
    path?: ReadonlyArray<string | number | symbol>;
  }>;
}
```

### Relevance to Event Sourced Drizzle

Currently uses minimal error types. Could add:

- EventValidationError for failed upcast events
- SyncConflictError for conflict detection
- RetryableError for transient failures

---

## 8. Sync Transaction API

TanStack DB provides a structured API for sync implementations.

### Key Files

- `packages/db/src/collection/sync.ts`

### Pattern

```typescript
config.sync.sync({
  begin: (options?: { immediate?: boolean }) => {
    // Start a sync transaction
  },
  write: (message) => {
    // Add a change message (insert/update/delete)
  },
  commit: () => {
    // Commit the sync transaction
  },
  truncate: () => {
    // Clear all data and restart
  },
  metadata: {
    row: { get, set, delete },
    collection: { get, set, delete, list }
  }
})
```

### Relevance to Event Sourced Drizzle

Current sync implementation is monolithic. Could refactor to:

- Separate pull handling into structured API
- Add truncate for reset scenarios
- Support for sync metadata storage

---

## 9. Live Query Integration

TanStack DB has deep integration with live queries.

### Key Files

- `packages/db/src/live-query-observer.ts`
- `packages/db/src/collection/changes.ts`

### Pattern

```typescript
public subscribeChanges(
  callback: (changes: Array<ChangeMessage>) => void,
  options: SubscribeChangesOptions
): CollectionSubscription {
  this.addSubscriber()
  // ... setup subscription
  return subscription
}
```

### Relevance to Event Sourced Drizzle

Could add:

- Live subscription to collection changes
- Automatic sync triggering on subscription
- Change batching for UI optimization

---

## 10. Index System

TanStack DB supports creating indexes on collections.

### Key Files

- `packages/db/src/indexes/`
- `packages/db/src/indexes/base-index.ts`
- `packages/db/src/indexes/basic-index.ts`

### Pattern

```typescript
collection.createIndex((row) => row.age, { indexType: BasicIndex });
```

### Relevance to Event Sourced Drizzle

Drizzle handles indexes at SQL level. Pattern less relevant but could:

- Document index patterns for domain tables
- Use Drizzle indexes for efficient queries

---

## 11. Shared Backend Compatibility (Critical)

Both sync engines (Event Sourced Collection and Event Sourced Drizzle) **must** talk to the same backend. The backend lives in `apps/example/src/server/sync/remote.ts` and currently imports types from `event-sourced-collection`. The wire protocol is already nearly identical between the two packages.

### Wire Protocol (Already Shared)

**Push (Client → Server):**

```typescript
// OutboundEvent — identical shape in both packages
type OutboundEvent = {
  eventId: string;
  collectionId: string;
  type: "insert" | "update" | "delete";
  key: string | number;
  payload: Record<string, unknown>;
  previous: Record<string, unknown> | null;
  txId: string;
  clientId: string;
  schemaVersion: number;
  baseVersion: string | null;
  timestamp: number;
};

// PushResponse — same in both
type PushResponse = {
  confirmed: ReadonlyArray<{ eventId: string; globalSeq: number }>;
  failed?: ReadonlyArray<{ eventId: string; message: string; code?: string; retryable?: boolean }>;
};
```

**Pull (Server → Client):**

```typescript
// ServerEvent — same shape in both packages
type ServerEvent = {
  globalSeq: number;
  eventId: string;
  collectionId: string;
  type: "insert" | "update" | "delete";
  key: string | number;
  payload: Record<string, unknown>;
  previous?: Record<string, unknown> | null;
  clientId?: string;
  schemaVersion?: number;
  timestamp: number;
  cursor: string;
  backendId?: string;
};

// PullResponse — same in both
type PullResponse = {
  events: ReadonlyArray<ServerEvent>;
  cursor: string;
  hasMore: boolean;
  backendId?: string;
};
```

### Current Status

| Aspect                               | Collection | Drizzle                  | Match?                                |
| ------------------------------------ | ---------- | ------------------------ | ------------------------------------- |
| OutboundEvent shape                  | ✅         | ✅                       | ✅ Identical                          |
| PushResponse shape                   | ✅         | ✅                       | ✅ Identical                          |
| ServerEvent shape                    | ✅         | ✅                       | ✅ Identical                          |
| PullResponse shape                   | ✅         | ✅                       | ✅ Identical                          |
| PushEventsFn signature               | ✅         | ✅                       | ✅ Identical                          |
| PullEventsFn signature               | ✅         | ✅                       | ✅ Identical                          |
| HTTP transport (createHttpTransport) | ✅         | ✅ (createSyncTransport) | ✅ Same logic                         |
| SyncHandlersConfig                   | ✅         | ✅                       | ✅ Identical                          |
| SyncUrlConfig                        | ✅         | ✅                       | ✅ Identical                          |
| backendId handling                   | ✅         | ✅                       | ✅ Both support BackendMismatchPolicy |

### Backend Implementation (apps/example/src/server/sync/remote.ts)

The backend currently:

- Imports types from `event-sourced-collection`
- Stores events in a `sync_events` table with auto-incrementing `globalSeq`
- Groups events by `txId` and commits/rolls back atomically
- Deduplicates by `eventId` (idempotent push)
- Detects conflicts via `baseVersion` (optional)
- Supports `backendId` for database-swap detection
- Returns events ordered by `globalSeq` for pull

### What Must Stay Aligned

1. **Wire types must remain identical.** Both packages define the same types independently. Consider extracting a shared `event-sourced-protocol` package or at minimum ensuring the types are copy-pasted and kept in sync.

2. **Event semantics.** Both engines produce events with the same meaning:
   - `insert` = new row
   - `update` = partial patch to existing row
   - `delete` = remove row by key

3. **txId grouping.** The server commits all events with the same `txId` atomically. Both clients must produce consistent txId semantics.

4. **eventId uniqueness.** Both clients generate UUIDs for eventId. The server uses this for deduplication.

5. **schemaVersion + upcast.** Both clients support `upcastEvent` for handling events from older schema versions. The server stores schemaVersion per event.

6. **clientId.** Both clients resolve a stable client identity. The server uses this to skip echoed events during pull.

### Recommendations

| Action                        | Priority     | Description                                                              |
| ----------------------------- | ------------ | ------------------------------------------------------------------------ |
| Extract shared protocol types | Medium       | Create `event-sourced-protocol` package or shared types file             |
| Backend type imports          | Low          | Backend should import from protocol package, not from one engine         |
| Integration tests             | High         | Verify both clients can push/pull from same backend                      |
| Backend agnostic of client    | Already done | Server treats events as opaque — doesn't care which engine produced them |

### Key Insight

The backend is **already client-agnostic**. It doesn't know or care whether an event came from Event Sourced Collection (TanStack DB based) or Event Sourced Drizzle. Both clients can sync with the same server simultaneously — a user could even run both engines against the same backend and they'd correctly share state.

The only risk is **type drift** — if one package changes a wire type (adds a field, renames something) without updating the other, the backend contract breaks. A shared types package or a strict protocol spec would prevent this.

---

## Summary of High-Priority Adoptions

| Pattern                                 | Effort | Value  | Recommendation              |
| --------------------------------------- | ------ | ------ | --------------------------- |
| Transaction system with ambient support | Medium | High   | Adopt for nested operations |
| Schema validation with standard-schema  | Low    | Medium | Add validation hooks        |
| Proxy-based change tracking             | High   | Medium | Consider for update API     |
| Virtual properties                      | Low    | High   | Add $synced, $pending flags |
| Comprehensive errors                    | Low    | Medium | Add domain-specific errors  |
| Manager architecture                    | High   | Medium | Keep current flat structure |
| Optimistic state                        | Medium | High   | Track pending sync status   |

---

## Comparison: Current vs TanStack DB

### Current Event Sourced Drizzle

- Flat structure in `create-event-sourced-drizzle.ts`
- Direct Drizzle transaction usage
- Simple mutate API (insert/update/delete)
- Hook-based events (onReady, onMutation, onSyncComplete)
- No schema validation
- No virtual properties
- No optimistic state separation
- Minimal error types

### TanStack DB

- Manager-based architecture
- Custom transaction system wrapping storage
- Rich mutation API with transaction support
- Event-based collection changes
- Standard-schema integration
- Virtual props ($synced, $origin, $key, $collectionId)
- Separate synced/optimistic state
- 70+ error types for specific failure modes

### Conclusion

Event Sourced Drizzle should focus on adopting:

1. Schema validation for domain writes
2. Virtual properties for sync state visibility
3. Enhanced error types
4. Potentially transaction system refactor for ambient transaction support
