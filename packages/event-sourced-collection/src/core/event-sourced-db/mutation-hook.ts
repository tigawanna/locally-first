import type { Collection } from "@tanstack/db";

import {
  allocateLocalSeq,
  pendingForKey,
  replacePending,
  squashedEntry,
} from "../../internal/compact-outbox";
import type { EmitHook } from "../../internal/hooks";
import type { MutationHookParams } from "../../internal/types";
import { readRowVersion, recordRowVersion } from "../../internal/replay";
import type { EventSourcedLogger } from "../../utils/logger";
import { generateEventId } from "../../utils/uuid";
import type { MutationType, OutboxEntry, RowVersionEntry } from "../types";
import { outboxEntry } from "./outbox-entry";

type SeqCounter = { value: number };

export type ClientIdRef = { value: string };

export type MutationContext = {
  outbox: Collection<OutboxEntry, string>;
  rowversions: Collection<RowVersionEntry, string>;
  collectionId: string;
  type: MutationType;
  seq: SeqCounter;
  clientId: ClientIdRef;
  eventSchemaVersion: number;
  conflictDetection: boolean;
  compactOnWrite: boolean;
  emit: EmitHook;
  log: EventSourcedLogger;
};

/** Handler for `localOnly` collections: persist locally, author no event. */
export async function noOutboxHook(): Promise<Record<string, unknown>> {
  return {};
}

/** Collection `onInsert` / `onUpdate` / `onDelete` hook that writes one outbox event per mutation. */
export function createMutationHook(context: MutationContext) {
  const {
    outbox,
    rowversions,
    collectionId,
    type,
    seq,
    clientId,
    eventSchemaVersion,
    conflictDetection,
    compactOnWrite,
    emit,
    log,
  } = context;

  return async (params: MutationHookParams): Promise<Record<string, unknown>> => {
    const txId = generateEventId();
    const timestamp = Date.now();

    for (const mutation of params.transaction.mutations) {
      const payload = type === "delete" ? mutation.original : mutation.modified;
      const eventId = generateEventId();
      const pending =
        compactOnWrite && type === "update"
          ? await pendingForKey(outbox, collectionId, mutation.key)
          : [];
      const squash =
        pending.length > 0 &&
        pending.every((entry) => entry.type === "insert" || entry.type === "update");
      const squashed = squash ? squashedEntry(pending, mutation.modified, timestamp) : null;

      const entry: OutboxEntry = squashed
        ? outboxEntry(
            {
              collectionId,
              key: mutation.key,
              clientId: clientId.value,
              schemaVersion: eventSchemaVersion,
            },
            squashed,
            { outbox, seq, eventId, txId, timestamp },
          )
        : {
            eventId,
            collectionId,
            type,
            key: mutation.key,
            payload,
            previous: type === "insert" ? null : mutation.original,
            txId,
            clientId: clientId.value,
            schemaVersion: eventSchemaVersion,
            baseVersion: conflictDetection
              ? readRowVersion(rowversions, collectionId, mutation.key)
              : null,
            timestamp,
            localSeq: allocateLocalSeq(outbox, seq),
            globalSeq: null,
            sync: false,
            syncStatus: "pending",
            attemptCount: 0,
            lastAttemptAt: null,
            nextAttemptAt: null,
            lastError: null,
            lastErrorCode: null,
            retryable: null,
          };

      if (squashed) await replacePending(outbox, pending, entry);
      else await outbox.insert(entry).isPersisted.promise;

      if (conflictDetection) {
        await recordRowVersion(rowversions, collectionId, mutation.key, eventId, null);
      }

      log.debug("outbox entry created", {
        eventId,
        collectionId,
        type,
        key: entry.key,
        localSeq: entry.localSeq,
        txId,
      });

      emit("onMutation", entry);
    }

    return {};
  };
}
