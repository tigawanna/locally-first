import type { Collection } from "@tanstack/db";

import type {
  DeadLetterEntry,
  InboxEntry,
  MutationType,
  OutboxEntry,
  RowVersionEntry,
  SyncMetaEntry,
  UnknownEventHandling,
  UpcastEventFn,
} from "../core/types";
import type { EventSourcedLogger } from "../utils/logger";
import type { EmitHook } from "./hooks";

export type ReplayMutation = {
  mutationId: string;
  type: MutationType;
  key: string | number;
  modified: Record<string, unknown>;
  original: Record<string, unknown>;
  changes: Record<string, unknown>;
  collection: AcceptMutationsCollection;
};

export type AcceptMutationsCollection = {
  id?: string;
  utils: {
    acceptMutations?: (transaction: { mutations: Array<ReplayMutation> }) => Promise<void> | void;
  };
};

export type MutationHookParams = {
  transaction: {
    mutations: ReadonlyArray<{
      mutationId: string;
      key: string | number;
      modified: Record<string, unknown>;
      original: Record<string, unknown>;
    }>;
  };
};

export type MetaCollections = {
  outbox: Collection<OutboxEntry, string>;
  inbox: Collection<InboxEntry, string>;
  deadletter: Collection<DeadLetterEntry, string>;
  syncmeta: Collection<SyncMetaEntry, string>;
  rowversions: Collection<RowVersionEntry, string>;
};

export type ReplayOutcome =
  | { status: "applied" }
  | { status: "skipped"; reason: string }
  /** Cannot be resolved yet; leave the cursor put and try again next sync. */
  | { status: "halted"; reason: string }
  /** Applying the mutation threw. The caller decides whether to retry or park it. */
  | { status: "failed"; error: Error };

export type ReplayContext = {
  targets: Record<string, AcceptMutationsCollection>;
  /** Collections whose inbound events are skipped (see `CollectionDef.localOnly`). */
  localOnlyIds: ReadonlySet<string>;
  rowversions: Collection<RowVersionEntry, string>;
  deadletter: Collection<DeadLetterEntry, string>;
  unknownEventHandling: UnknownEventHandling;
  eventSchemaVersion: number;
  upcastEvent?: UpcastEventFn;
  conflictDetection: boolean;
  /** Failed replays of one inbound event before it is dead-lettered. */
  maxReplayAttempts: number;
  emit: EmitHook;
  log: EventSourcedLogger;
};

/** The shape `replayEvent` needs, satisfied by both inbox rows and server events. */
export type ReplayableEvent = {
  eventId: string;
  collectionId: string;
  type: MutationType;
  key: string | number;
  payload: Record<string, unknown>;
  previous?: Record<string, unknown> | null;
  schemaVersion?: number;
  globalSeq?: number | null;
};

export type ResolvedRetryConfig = {
  maxAttempts: number;
  baseDelayMs: number;
  maxDelayMs: number;
};
