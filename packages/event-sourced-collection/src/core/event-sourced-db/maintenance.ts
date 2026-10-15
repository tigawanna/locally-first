import {
  syncedInboxBefore,
  syncedOutboxBefore,
  unsyncedOutboxEntries,
} from "../../internal/collection-queries";
import { SYNCMETA_KEY } from "../../internal/constants";
import { toOutboundEvent } from "../../internal/push";
import { recordRowVersion, replayInbox } from "../../internal/replay";
import { ensureSyncMeta, readPullCursor, writePullCursor } from "../../internal/sync-meta";
import { generateEventId } from "../../utils/uuid";
import { normalizePushResponse } from "../sync";
import type {
  DeadLetterEntry,
  OutboundEvent,
  PruneOptions,
  PruneResult,
  ResetLocalReplicaOptions,
  ResetLocalReplicaResult,
} from "../types";
import { requeueInboundDeadLetter, selectDeadLetters } from "./dead-letter";
import { UnsyncedChangesError } from "./errors";
import type { SyncRuntime } from "./runtime";
import { trimKeepLast } from "./trim";

/** Dead-letter retry, pruning, and local-replica reset, all serialized on the sync runtime. */
export function createMaintenance(
  deps: SyncRuntime,
  withLock: <T>(fn: () => Promise<T>, deferred: T) => Promise<T>,
) {
  const {
    outbox,
    inbox,
    deadletter,
    rowversions,
    syncmeta,
    transport,
    clientId,
    replayContext,
    replayTargets,
    syncedCollections,
    emit,
    log,
    conflictDetection,
    runExclusive,
    notifyStatus,
  } = deps;

  /** Pushes outbound dead letters, or requeues inbound ones and replays them. Omit `eventId` to retry all. */
  function retryDeadLetter(eventId?: string): Promise<number> {
    return runExclusive(async () => {
      const targets = selectDeadLetters(deadletter, eventId);
      const outbound: DeadLetterEntry[] = [];
      let cleared = 0;
      let inboundRequeued = 0;

      for (const entry of targets) {
        if ((entry.direction ?? "outbound") === "inbound") {
          await requeueInboundDeadLetter(inbox, entry);
          await deadletter.delete(entry.eventId).isPersisted.promise;
          inboundRequeued++;
          cleared++;
        } else {
          outbound.push(entry);
        }
      }

      if (inboundRequeued > 0) {
        await replayInbox(inbox, replayContext);
      }

      if (outbound.length > 0) {
        if (!transport?.push) {
          throw new Error(
            "Cannot retry outbound dead-letter events: no push transport is configured.",
          );
        }

        const events: OutboundEvent[] = outbound.map((entry) =>
          toOutboundEvent({
            eventId: entry.eventId,
            collectionId: entry.collectionId,
            type: entry.type,
            key: entry.key,
            payload: entry.payload,
            previous: entry.previous,
            txId: entry.txId ?? entry.eventId,
            clientId: entry.clientId ?? clientId.value,
            schemaVersion: entry.schemaVersion,
            baseVersion: entry.baseVersion ?? null,
            timestamp: entry.timestamp,
            localSeq: entry.localSeq ?? 0,
            globalSeq: null,
            sync: false,
            syncStatus: "pending",
            attemptCount: entry.attemptCount,
            lastAttemptAt: null,
            nextAttemptAt: null,
            lastError: null,
            lastErrorCode: null,
            retryable: null,
          }),
        );

        const response = normalizePushResponse(await transport.push(events));
        const confirmed = new Set(response.confirmed.map((item) => item.eventId));
        const failedById = new Map((response.failed ?? []).map((item) => [item.eventId, item]));
        const now = Date.now();

        for (const confirmation of response.confirmed) {
          const entry = outbound.find((row) => row.eventId === confirmation.eventId);
          if (!entry) continue;

          if (deadletter.has(entry.eventId)) {
            await deadletter.delete(entry.eventId).isPersisted.promise;
          }

          if (conflictDetection) {
            await recordRowVersion(
              rowversions,
              entry.collectionId,
              entry.key,
              entry.eventId,
              confirmation.globalSeq,
            );
          }

          emit("onEventPushed", {
            eventId: confirmation.eventId,
            globalSeq: confirmation.globalSeq,
          });
          cleared++;
        }

        for (const entry of outbound) {
          if (confirmed.has(entry.eventId)) continue;

          const failure = failedById.get(entry.eventId);
          const message = failure?.message ?? "push failed during dead-letter retry";
          const code = failure?.code ?? null;

          if (deadletter.has(entry.eventId)) {
            await deadletter.update(entry.eventId, (draft) => {
              draft.attemptCount = (draft.attemptCount ?? 0) + 1;
              draft.message = message;
              draft.code = code;
              draft.failedAt = now;
            }).isPersisted.promise;
          }
        }

        log.info("outbound dead-letter events pushed", {
          attempted: outbound.length,
          confirmed: response.confirmed.length,
          failed: response.failed?.length ?? 0,
        });
      } else {
        log.info("dead-letter events retried", {
          count: targets.length,
          inbound: inboundRequeued,
        });
      }

      return cleared;
    });
  }

  /** Deletes dead-letter rows without retrying them. Omit `eventId` to discard all. */
  function discardDeadLetter(eventId?: string): Promise<number> {
    return runExclusive(async () => {
      const targets = selectDeadLetters(deadletter, eventId);

      for (const entry of targets) {
        await deadletter.delete(entry.eventId).isPersisted.promise;
      }

      log.info("dead-letter events discarded", { count: targets.length });

      return targets.length;
    });
  }

  /** Deletes already-synced outbox and inbox rows older than the cutoff, keeping the newest `keepLast`. */
  function pruneSyncedEvents(options: PruneOptions = {}): Promise<PruneResult> {
    return runExclusive(async () => {
      const olderThanMs = Math.max(0, options.olderThanMs ?? 0);
      const keepLast = Math.max(0, options.keepLast ?? 0);
      const cutoff = Date.now() - olderThanMs;

      // The cursor is derived partly from resolved inbox rows, so persist it
      // before those rows are removed.
      await writePullCursor(syncmeta, readPullCursor(syncmeta, inbox));

      const outboxTargets = trimKeepLast(await syncedOutboxBefore(outbox, cutoff), keepLast);
      const inboxTargets = trimKeepLast(await syncedInboxBefore(inbox, cutoff), keepLast);

      for (const entry of outboxTargets) {
        await outbox.delete(entry.eventId).isPersisted.promise;
      }

      for (const entry of inboxTargets) {
        await inbox.delete(entry.eventId).isPersisted.promise;
      }

      log.info("pruned synced events", {
        outbox: outboxTargets.length,
        inbox: inboxTargets.length,
      });

      return { outbox: outboxTargets.length, inbox: inboxTargets.length };
    });
  }

  /**
   * Wipes this device's replica: the local copy of synced collections, plus
   * the outbox, inbox, dead letter, row versions, and pull cursor. The server
   * is left alone. A new client id is issued so a later pull replays this
   * device's own history. Refuses while the outbox still has unpushed changes
   * unless `discardPending` is set.
   */
  function resetLocalReplica(
    options: ResetLocalReplicaOptions = {},
  ): Promise<ResetLocalReplicaResult> {
    const deferred: ResetLocalReplicaResult = {
      deferred: true,
      removedRows: 0,
      discardedPending: 0,
      clientId: clientId.value,
    };

    return runExclusive(() =>
      withLock(async () => {
        const pending = await unsyncedOutboxEntries(outbox);
        if (pending.length > 0 && options.discardPending !== true) {
          throw new UnsyncedChangesError(pending.length);
        }

        // acceptMutations is the replay path: it writes the collection and its
        // persistence without calling onDelete, so no outbox event is authored.
        let removedRows = 0;
        for (const { collectionId, collection } of syncedCollections) {
          const target = replayTargets[collectionId];
          const rows = [...collection.state.entries()];
          if (rows.length === 0 || !target?.utils.acceptMutations) continue;

          await target.utils.acceptMutations({
            mutations: rows.map(([key, row]) => ({
              mutationId: generateEventId(),
              type: "delete" as const,
              key,
              modified: row,
              original: row,
              changes: row,
              collection: target,
            })),
          });
          removedRows += rows.length;
        }

        for (const meta of [outbox, inbox, deadletter] as const) {
          for (const eventId of meta.state.keys()) {
            await meta.delete(eventId).isPersisted.promise;
          }
        }
        for (const id of rowversions.state.keys()) {
          await rowversions.delete(id).isPersisted.promise;
        }

        // Pulled events are skipped when their clientId is ours. A fresh id
        // makes this device's own history replay like anyone else's.
        clientId.value = generateEventId();
        await ensureSyncMeta(syncmeta);
        await syncmeta.update(SYNCMETA_KEY, (draft) => {
          draft.clientId = clientId.value;
          draft.pullCursor = 0;
          draft.lastError = null;
          // Re-recorded on the next pull, so a reset is how a "fail" client
          // accepts a rebuilt backend.
          draft.backendId = null;
        }).isPersisted.promise;

        log.warn("local replica reset", {
          removedRows,
          discardedPending: pending.length,
          clientId: clientId.value,
        });
        notifyStatus();

        return {
          deferred: false,
          removedRows,
          discardedPending: pending.length,
          clientId: clientId.value,
        };
      }, deferred),
    );
  }
  return { retryDeadLetter, discardDeadLetter, pruneSyncedEvents, resetLocalReplica };
}
