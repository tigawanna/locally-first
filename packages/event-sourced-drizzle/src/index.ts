export { createEventSourcedDrizzle } from "./core/create-event-sourced-drizzle";
export { generateEventId } from "./utils/uuid";
export { createEventSourcedLogger } from "./utils/logger";
export { createSyncTransport, SyncPushError, SyncPullError } from "./core/sync";
export { BackendMismatchError } from "./internal/pull";

export {
  EventSourcedDrizzleError,
  SchemaValidationError,
  UnknownCollectionError,
  DuplicateKeyError,
  KeyNotFoundError,
  EventConflictError,
  SyncConflictError,
  RetryExhaustedError,
  ReplayHaltedError,
  EventNotFoundError,
  TransactionStateError,
  NoActiveTransactionError,
  BackendMismatchError as BackendIdentityMismatchError,
} from "./core/errors";

export {
  createVirtualPropsEnricher,
  stripVirtualProps,
  hasVirtualProps,
} from "./core/virtual-props";

export { getActiveTransaction } from "./core/transaction";

export { createOptimisticStateTracker, OptimisticStateTracker } from "./core/optimistic-state";

export { createSqliteEventLogBackend } from "./testing/sqlite-event-log-backend";
export type {
  SqliteEventLogBackend,
  SqliteEventLogBackendOptions,
} from "./testing/sqlite-event-log-backend";

export type {
  CollectionDef,
  CollectionMap,
  EventSourcedDrizzle,
  EventSourcedDrizzleConfig,
  InferInsert,
  InferSelect,
  MutateApi,
  TableLike,
  RetryConfig,
  SyncStatus,
  PruneOptions,
  PruneResult,
  MutationType,
  OutboxSyncStatus,
  OutboxRow,
  InboxRow,
  DeadLetterRow,
  DeadLetterReason,
  DrizzleAdapter,
  UpcastEventFn,
  EventSourcedHooks,
  BackendMismatchPolicy,
  ManualSyncResult,
  SyncResult,
  EventSourcedLogger,
} from "./core/types";

export type {
  OutboundEvent,
  ServerEvent,
  PushConfirmation,
  PushEventsFn,
  PushFailure,
  PushResponse,
  PullEventsFn,
  PullResponse,
  SyncHandlersConfig,
  SyncTransport,
  SyncUrlConfig,
  NormalizedSyncTransport,
} from "./core/sync";

// Canonical protocol types — same as above, available via explicit import path.
export type {
  MutationType as ProtocolMutationType,
  OutboundEvent as ProtocolOutboundEvent,
  ServerEvent as ProtocolServerEvent,
  PushConfirmation as ProtocolPushConfirmation,
  PushFailure as ProtocolPushFailure,
  PushResponse as ProtocolPushResponse,
  PullResponse as ProtocolPullResponse,
  PushEventsFn as ProtocolPushEventsFn,
  PullEventsFn as ProtocolPullEventsFn,
  SyncTransport as ProtocolSyncTransport,
} from "./core/protocol";

export type {
  VirtualOrigin,
  VirtualRowProps,
  WithVirtualProps,
  OutboxStateProvider,
  VirtualPropsEnricherConfig,
  EnrichFn,
} from "./core/virtual-props";

export type {
  Transaction,
  TransactionState,
  TransactFn,
  TransactOptions,
} from "./core/transaction";

export type {
  OptimisticEntry,
  OptimisticSummary,
  OptimisticListener,
} from "./core/optimistic-state";
