import { pullInbox } from "../../internal/pull";
import { pushOutbox } from "../../internal/push";
import { replayInbox } from "../../internal/replay";
import { readPullCursor, readSyncMeta, writeSyncOutcome } from "../../internal/sync-meta";
import type { ManualSyncResult, SyncResult, SyncStatus, SyncTrigger } from "../types";
import { emptyResult, SyncBatchLimitError, toError } from "./errors";
import { decideSyncBatch } from "./sync-batch";
import type { SyncRuntime } from "./runtime";

/** Push, pull, status, and the cross-context lock around one sync. */
export function createSyncCycle(deps: SyncRuntime) {
  const {
    outbox,
    inbox,
    deadletter,
    rowversions,
    syncmeta,
    transport,
    clientId,
    replayContext,
    emit,
    log,
    retry,
    pullOverlap,
    recordLocalEchoes,
    backendMismatch,
    lock,
    lockName,
    runExclusive,
    outboxGate,
    getSyncEnabled,
    getPushLimits,
    syncBatch,
    isSyncing,
    setSyncing,
    notifyStatus,
    statusListeners,
  } = deps;

  /** Logs a sync failure, emits `onSyncError`, and returns it as an `Error`. */
  function recordError(phase: "push" | "pull" | "replay", label: string, err: unknown): Error {
    const error = toError(err);
    log.error(`${label} ${phase} failed`, { message: error.message });
    emit("onSyncError", { phase, error });
    return error;
  }

  /** `syncBatch` verdict for the current outbox. Pull is never held by this. */
  function readBatchVerdict() {
    let pendingCount = 0;
    let oldestPendingAt: number | null = null;
    for (const entry of outbox.state.values()) {
      if (entry.sync) continue;
      pendingCount++;
      if (oldestPendingAt === null || entry.timestamp < oldestPendingAt) {
        oldestPendingAt = entry.timestamp;
      }
    }
    return decideSyncBatch({
      batch: syncBatch,
      pendingCount,
      lastSyncAt: readSyncMeta(syncmeta).lastSyncAt,
      oldestPendingAt,
      now: Date.now(),
    });
  }

  /** Pushes the outbox, pulls the inbox, and pushes again if a backend reset requeued history. */
  async function pushPull(label: string): Promise<SyncResult> {
    await outbox.preload();
    await inbox.preload();

    const errors: Error[] = [];
    let pushed = 0;
    let pulled = 0;
    let skipped = 0;
    let deadLettered = 0;
    let pushAttempted = false;
    let pushFailed = false;
    let batchHold: SyncBatchLimitError | null = null;

    async function runPush(): Promise<void> {
      if (!transport?.push) {
        log.debug(`${label} push skipped: no push transport configured`);
        return;
      }

      pushAttempted = true;
      try {
        const outcome = await pushOutbox({
          outbox,
          deadletter,
          rowversions,
          push: transport.push,
          batchSize: getPushLimits().pushBatchSize,
          maxBytes: getPushLimits().maxPushBytes,
          retry,
          now: Date.now(),
          emit,
          log,
          outboxGate,
        });
        pushed += outcome.pushed;
        deadLettered += outcome.deadLettered;
        if (outcome.error) {
          pushFailed = true;
          errors.push(recordError("push", label, outcome.error));
        }
      } catch (err) {
        pushFailed = true;
        errors.push(recordError("push", label, err));
      }
    }

    const verdict = readBatchVerdict();
    if (verdict.allowed) {
      await runPush();
    } else {
      batchHold = new SyncBatchLimitError(verdict);
      log.info("push held by syncBatch", {
        pendingCount: verdict.pendingCount,
        minEvents: verdict.minEvents,
        delayRemainingMs: verdict.delayRemainingMs,
      });
    }

    let requeued = 0;

    try {
      if (transport?.pull) {
        const outcome = await pullInbox({
          outbox,
          inbox,
          syncmeta,
          pull: transport.pull,
          clientId: clientId.value,
          pullOverlap,
          recordLocalEchoes,
          backendMismatch,
          context: replayContext,
        });
        pulled = outcome.pulled;
        skipped = outcome.skipped;
        deadLettered += outcome.deadLettered;
        requeued = outcome.requeued;
      } else {
        log.debug(`${label} pull skipped: no pull transport configured`);
      }
    } catch (err) {
      errors.push(recordError("pull", label, err));
    }

    // A backend reset is only discovered during pull, by which point this
    // sync's push has already run. Upload the requeued history now so a single
    // sync is enough to recover rather than leaving the gap open until the next.
    if (requeued > 0) {
      const again = readBatchVerdict();
      if (again.allowed) {
        log.info(`${label} re-pushing history after backend reset`, { requeued });
        await runPush();
      } else {
        batchHold ??= new SyncBatchLimitError(again);
        log.info("re-push held by syncBatch", { requeued });
      }
    }

    // A failed or held push must not move lastSyncAt, or delayMinutes starts over.
    const stampLastSync = pushAttempted && !pushFailed;
    if (stampLastSync || errors.length > 0) {
      await writeSyncOutcome(
        syncmeta,
        stampLastSync ? Date.now() : null,
        errors[0]?.message ?? null,
      );
    }

    return { pushed, pulled, skipped, deadLettered, deferred: false, batchHold, errors };
  }

  /** Runs `fn` under the cross-context lock when one is configured. */
  async function withLock<T>(fn: () => Promise<T>, deferred: T): Promise<T> {
    if (!lock) return fn();

    const outcome = await lock.tryRun(lockName, fn);

    if (!outcome.acquired) {
      log.debug("sync deferred: lock held by another context", { lockName });
      return deferred;
    }

    return outcome.result;
  }

  /** Marks the runtime syncing, emits start and complete, then clears the flag. */
  async function tracked<T extends SyncResult>(
    trigger: SyncTrigger,
    fn: () => Promise<T>,
  ): Promise<T> {
    setSyncing(true);
    notifyStatus();
    emit("onSyncStart", { trigger });

    try {
      const result = await fn();
      emit("onSyncComplete", { trigger, result });
      return result;
    } finally {
      setSyncing(false);
      notifyStatus();
    }
  }

  /** Pushes then pulls when sync and a transport are both configured. */
  function sync(): Promise<SyncResult> {
    return runExclusive(async () => {
      if (!getSyncEnabled()) {
        log.debug("sync skipped: sync disabled");
        return emptyResult();
      }

      if (!transport) {
        log.warn("sync skipped: no transport configured");
        return emptyResult();
      }

      await outbox.preload();

      return withLock(
        () =>
          tracked("sync", async () => {
            log.info("sync started");
            const result = await pushPull("sync");
            log.info("sync finished", {
              pushed: result.pushed,
              pulled: result.pulled,
              skipped: result.skipped,
              deadLettered: result.deadLettered,
              errorCount: result.errors.length,
            });
            return result;
          }),
        emptyResult({ deferred: true }),
      );
    });
  }

  /** Same as `sync`, then replays the inbox even when automatic sync is off. */
  function manualSync(): Promise<ManualSyncResult> {
    return runExclusive(async () => {
      await outbox.preload();

      return withLock<ManualSyncResult>(
        () =>
          tracked("manualSync", async () => {
            log.info("manual sync started");

            await outbox.preload();
            await inbox.preload();

            let result: SyncResult = emptyResult();

            if (getSyncEnabled() && transport) {
              result = await pushPull("manual sync");
            } else if (!getSyncEnabled()) {
              log.debug("manual sync push/pull skipped: sync disabled");
            } else {
              log.warn("manual sync: no transport configured, skipping push/pull");
            }

            const errors = [...result.errors];
            let replayed = 0;

            try {
              const outcome = await replayInbox(inbox, replayContext);
              replayed = outcome.applied;
              result = { ...result, skipped: result.skipped + outcome.skipped };
            } catch (err) {
              errors.push(recordError("replay", "manual sync", err));
            }

            log.info("manual sync finished", {
              pushed: result.pushed,
              pulled: result.pulled,
              skipped: result.skipped,
              deadLettered: result.deadLettered,
              replayed,
              errorCount: errors.length,
            });

            return { ...result, replayed, errors };
          }),
        { ...emptyResult({ deferred: true }), replayed: 0 },
      );
    });
  }

  /** Counts pending and failed outbox rows and reads the stored cursor and last error. */
  function getSyncStatus(): SyncStatus {
    let pendingCount = 0;
    let failedCount = 0;

    for (const entry of outbox.state.values()) {
      if (entry.sync) continue;
      pendingCount++;
      if (entry.syncStatus === "failed") failedCount++;
    }

    const stored = readSyncMeta(syncmeta);

    return {
      isSyncing: isSyncing(),
      isSynced: pendingCount === 0,
      pendingCount,
      failedCount,
      deadLetterCount: deadletter.state.size,
      pullCursor: readPullCursor(syncmeta, inbox),
      backendId: stored.backendId,
      lastSyncAt: stored.lastSyncAt,
      lastError: stored.lastError,
    };
  }

  /** Calls `listener` with the current status now and again whenever it changes. */
  function subscribeSyncStatus(listener: (status: SyncStatus) => void): () => void {
    statusListeners.add(listener);
    listener(getSyncStatus());
    return () => statusListeners.delete(listener);
  }

  return { sync, manualSync, getSyncStatus, subscribeSyncStatus, withLock };
}
