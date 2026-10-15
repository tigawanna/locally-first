import type { Collection } from "@tanstack/db";

import {
  allocateLocalSeq,
  eventsAfterLastDelete,
  foldedOutboxBody,
  replaceUnpushedOutboxEvents,
  unpushedOutboxEventsForKey,
} from "../../internal/compact-outbox";
import type { EmitHook } from "../../internal/hooks";
import type { MutationHookParams } from "../../internal/types";
import { readRowVersion, recordRowVersion } from "../../internal/replay";
import type { EventSourcedLogger } from "../../utils/logger";
import { generateEventId } from "../../utils/uuid";
import type { MutationType, OutboxEntry, RowVersionEntry } from "../types";
import { buildUnsyncedOutboxEntry } from "./outbox-entry";

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

    // Each item is one row change from the TanStack transaction: its key, the row before, and the row after.
    for (const rowChange of params.transaction.mutations) {
      const eventId = generateEventId();

      // Outbox events for this row that have not been pushed yet. Empty unless this update may fold them.
      let unpushedEvents: OutboxEntry[] = [];
      if (compactOnWrite && type === "update") {
        const unpushedForRow = await unpushedOutboxEventsForKey(
          outbox,
          collectionId,
          rowChange.key,
        );
        unpushedEvents = eventsAfterLastDelete(unpushedForRow);
      }

      // Those unpushed events can be folded only when every one is an insert or an update.
      const unpushedEventsCanFold =
        unpushedEvents.length > 0 &&
        unpushedEvents.every((event) => event.type === "insert" || event.type === "update");

      let outboxRow: OutboxEntry;
      // Fold that insert/update chain into one event and drop the rows it replaces.
      if (unpushedEventsCanFold) {
        outboxRow = buildUnsyncedOutboxEntry({
          identity: {
            collectionId,
            key: rowChange.key,
            clientId: clientId.value,
            schemaVersion: eventSchemaVersion,
          },
          body: foldedOutboxBody({
            unpushedEvents,
            latest: rowChange.modified,
            now: timestamp,
          }),
          ids: { outbox, seq, eventId, txId, timestamp },
        });
        await replaceUnpushedOutboxEvents(outbox, unpushedEvents, outboxRow);
      } else {
        // Otherwise write one pending event: deletes store the old row, inserts have no previous.
        let payload = rowChange.modified;
        let previous: Record<string, unknown> | null = rowChange.original;
        if (type === "delete") payload = rowChange.original;
        if (type === "insert") previous = null;

        let baseVersion: string | null = null;
        if (conflictDetection) {
          baseVersion = readRowVersion(rowversions, collectionId, rowChange.key);
        }

        outboxRow = {
          eventId,
          collectionId,
          type,
          key: rowChange.key,
          payload,
          previous,
          txId,
          clientId: clientId.value,
          schemaVersion: eventSchemaVersion,
          baseVersion,
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
        await outbox.insert(outboxRow).isPersisted.promise;
      }

      // Point conflict detection at this event so the next local write knows its base.
      if (conflictDetection) {
        await recordRowVersion(rowversions, collectionId, rowChange.key, eventId, null);
      }

      log.debug("outbox entry created", {
        eventId,
        collectionId,
        type,
        key: outboxRow.key,
        localSeq: outboxRow.localSeq,
        txId,
      });

      emit("onMutation", outboxRow);
    }

    return {};
  };
}
