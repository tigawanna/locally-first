import type { Collection } from "@tanstack/db";

import type { EmitHook } from "../../internal/hooks";
import type {
  AcceptMutationsCollection,
  ReplayContext,
  ResolvedRetryConfig,
} from "../../internal/types";
import type { EventSourcedLogger } from "../../utils/logger";
import type {
  BackendMismatchPolicy,
  DeadLetterEntry,
  InboxEntry,
  OutboxEntry,
  RowVersionEntry,
  SyncLock,
  SyncMetaEntry,
  SyncBatchOptions,
  SyncPushLimits,
  SyncStatus,
} from "../types";
import type { NormalizedSyncTransport } from "../sync";

export type SyncRuntime = {
  outbox: Collection<OutboxEntry, string>;
  inbox: Collection<InboxEntry, string>;
  deadletter: Collection<DeadLetterEntry, string>;
  syncmeta: Collection<SyncMetaEntry, string>;
  rowversions: Collection<RowVersionEntry, string>;
  transport: NormalizedSyncTransport | null;
  clientId: { value: string };
  replayContext: ReplayContext;
  replayTargets: Record<string, AcceptMutationsCollection>;
  syncedCollections: ReadonlyArray<{
    collectionId: string;
    collection: Collection<Record<string, unknown>, string | number>;
  }>;
  emit: EmitHook;
  log: EventSourcedLogger;
  retry: ResolvedRetryConfig;
  pullOverlap: number;
  recordLocalEchoes: boolean;
  backendMismatch: BackendMismatchPolicy;
  conflictDetection: boolean;
  lock: SyncLock | null | undefined;
  lockName: string;
  runExclusive: <T>(fn: () => Promise<T>) => Promise<T>;
  getSyncEnabled: () => boolean;
  getPushLimits: () => SyncPushLimits;
  syncBatch: SyncBatchOptions | undefined;
  isSyncing: () => boolean;
  setSyncing: (value: boolean) => void;
  notifyStatus: () => void;
  statusListeners: Set<(status: SyncStatus) => void>;
};
