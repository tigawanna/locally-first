export { createEventSourcedDB, UnsyncedChangesError } from "./core/create-event-sourced-db";
export { createEventSourcedDBHandle, resolveModules } from "./core/create-event-sourced-db-handle";
export { createLazySingleton } from "./core/lazy-singleton";
export { createHttpTransport, SyncPushError, SyncPullError } from "./core/sync";
export { SYNC_PUSH_PRESETS, resolvePushLimits } from "./core/sync-presets";
export { createMockSyncBackend } from "./testing/mock-sync-backend";
export { createWebLocksSyncLock, supportsWebLocks } from "./platforms/web-locks";
export { generateEventId } from "./utils/uuid";
export { createEventSourcedLogger } from "./utils/logger";

export type {
  EventSourcedDBHandle,
  EventSourcedDBHandleSetup,
  ModulesInput,
} from "./core/create-event-sourced-db-handle";
export type { LazySingleton, LazySingletonOptions } from "./core/lazy-singleton";
export { BackendMismatchError } from "./internal/pull";
export type {
  MockRejectFn,
  MockRejection,
  MockSyncBackend,
  MockSyncBackendOptions,
} from "./testing/mock-sync-backend";

export type {
  BackendMismatchPolicy,
  CollectionDef,
  CollectionIndexConstructor,
  CollectionIndexDef,
  CollectionMap,
  DeadLetterDirection,
  DeadLetterEntry,
  DeadLetterReason,
  EventSourcedDB,
  EventSourcedDBConfig,
  EventSourcedOptions,
  EventSourcedSharedOptions,
  EventSourcedHooks,
  InboxEntry,
  EventSourcedLogger,
  InferKey,
  InferState,
  ManualSyncResult,
  MutationType,
  OutboundEvent,
  OutboxEntry,
  OutboxSyncStatus,
  PersistedCollectionPersistence,
  PruneOptions,
  PruneResult,
  ResetLocalReplicaOptions,
  ResetLocalReplicaResult,
  PullEventsFn,
  PullResponse,
  PushConfirmation,
  PushEventsFn,
  PushFailure,
  PushResponse,
  ReservedCollections,
  RetryConfig,
  RowVersionEntry,
  ServerEvent,
  SyncHandlersConfig,
  SyncLock,
  SyncMetaEntry,
  SQLiteDriver,
  SyncPhase,
  SyncPushLimits,
  SyncPushPreset,
  SyncResult,
  SyncStatus,
  SyncTransport,
  SyncTrigger,
  SyncUrlConfig,
  UnknownEventHandling,
  UpcastableEvent,
  UpcastEventFn,
} from "./core/types";

export type {
  CreateCollectionFn,
  InjectedCreateCollection,
  InjectedModuleFn,
  PersistedCollectionOptionsFn,
} from "./core/persisted-collection";
