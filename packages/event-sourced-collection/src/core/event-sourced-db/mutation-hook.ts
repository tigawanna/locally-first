import type { Collection } from "@tanstack/db";

import {
  allocateLocalSeq,
  anchorFoldedEvent,
  eventsAfterLastDelete,
  foldableTail,
  foldedOutboxBody,
  replaceUnpushedOutboxEvents,
  unpushedOutboxEventsForKey,
} from "../../internal/compact-outbox";
import type { EmitHook } from "../../internal/hooks";
import type { OutboxWriteGate } from "../../internal/outbox-gate";
import type { MutationHookParams } from "../../internal/types";
import { readRowVersion, recordRowVersion } from "../../internal/replay";
import type { EventSourcedLogger } from "../../utils/logger";
import { generateEventId } from "../../utils/uuid";
import type { MutationType, OutboxEntry, RowVersionEntry } from "../types";
import { buildUnsyncedOutboxEntry } from "./outbox-entry";
import type { ClientIdRef, SeqCounter } from "./refs";

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
  outboxGate: OutboxWriteGate;
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
    outboxGate,
    emit,
    log,
  } = context;

  return (params: MutationHookParams): Promise<Record<string, unknown>> =>
    outboxGate.run(async () => {
      const txId = generateEventId();
      const timestamp = Date.now();
      // A transaction that writes several rows has to stay one `txId`. Folding
      // one of those rows back into an older chain would pull it out of this
      // transaction, so only a single-row edit keeps the chain's old identity.
      const preservePlace = params.transaction.mutations.length === 1;

      // Each item is one row change from the TanStack transaction: its key, the row before, and the row after.
      for (const rowChange of params.transaction.mutations) {
        const eventId = generateEventId();

        // Unsynced insert/update suffix for this row. Empty when a push of this
        // row is in flight, or when the chain is not a foldable insert/update run.
        let foldTail: OutboxEntry[] = [];
        if (compactOnWrite && type === "update") {
          const unpushedForRow = await unpushedOutboxEventsForKey(
            outbox,
            collectionId,
            rowChange.key,
          );
          const tail = foldableTail(eventsAfterLastDelete(unpushedForRow), (id) =>
            outboxGate.isReserved(id),
          );
          if (
            tail.length > 0 &&
            tail.every((event) => event.type === "insert" || event.type === "update")
          ) {
            foldTail = tail;
          }
        }

        let outboxRow: OutboxEntry;
        // Fold that insert/update chain into one event and drop the rows it replaces.
        if (foldTail.length > 0) {
          const anchor = anchorFoldedEvent(foldTail, outbox);
          outboxRow = buildUnsyncedOutboxEntry({
            identity: {
              collectionId,
              key: rowChange.key,
              clientId: clientId.value,
              schemaVersion: eventSchemaVersion,
            },
            body: foldedOutboxBody({
              unpushedEvents: foldTail,
              latest: rowChange.modified,
              now: timestamp,
            }),
            ids: preservePlace
              ? {
                  outbox,
                  seq,
                  eventId: anchor.eventId,
                  txId: anchor.txId,
                  timestamp: anchor.timestamp,
                  localSeq: anchor.localSeq,
                }
              : { outbox, seq, eventId, txId, timestamp },
          });
          await replaceUnpushedOutboxEvents(outbox, foldTail, outboxRow);
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
          await recordRowVersion(rowversions, collectionId, rowChange.key, outboxRow.eventId, null);
        }

        log.debug("outbox entry created", {
          eventId: outboxRow.eventId,
          collectionId,
          type,
          key: outboxRow.key,
          localSeq: outboxRow.localSeq,
          txId: outboxRow.txId,
        });

        emit("onMutation", outboxRow);
      }

      return {};
    });
}
