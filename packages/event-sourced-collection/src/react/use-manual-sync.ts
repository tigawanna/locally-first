import { useCallback, useEffect, useState } from "react";

import type { ManualSyncResult } from "../core/types";
import { formatManualSyncMessage } from "./format-manual-sync-message";
import type { DbWithSettings } from "./settings-collection";
import type { SyncStatusSource } from "./use-sync-status";
import { useSyncEnabled } from "./use-sync-enabled";

export type UseManualSyncOptions = {
  /** When false, skips reading settings and treats sync as enabled. */
  enabled?: boolean;
  settingsId: string;
  ensureDb: () => Promise<DbWithSettings>;
  /** Typically `() => db.manualSync()` or `() => ensureDb().then((d) => d.manualSync())`. */
  sync: () => Promise<ManualSyncResult>;
  disabledMessage?: string;
  /**
   * Live DB handle. When set, `syncing` follows {@link SyncStatusSource.subscribeSyncStatus}
   * so the button tracks load sync, background sync, and this `runSync` call.
   *
   * When omitted, `syncing` is only true while `runSync` is awaiting (legacy).
   */
  db?: SyncStatusSource;
};

export type UseManualSyncReturn = {
  syncEnabled: boolean;
  /** True while any sync is in flight when `db` was passed; otherwise only during `runSync`. */
  syncing: boolean;
  syncMessage: string | null;
  /**
   * Runs the sync callback, updates `syncMessage`, and returns the raw result
   * on success (including results that contain `errors`).
   * Returns `undefined` when sync is disabled or the callback throws.
   */
  runSync: () => Promise<ManualSyncResult | undefined>;
};

const DEFAULT_DISABLED_MESSAGE = "Sync is disabled in Settings.";

/**
 * Settings-gated manual sync UI helper.
 *
 * Prefer passing `db` so `syncing` shares the same global tracker as
 * {@link import("./use-sync-status").useSyncStatus} — then a load-time
 * `db.sync()` also disables the button / shows a spinner.
 */
export function useManualSync({
  enabled = true,
  settingsId,
  ensureDb,
  sync,
  disabledMessage = DEFAULT_DISABLED_MESSAGE,
  db,
}: UseManualSyncOptions): UseManualSyncReturn {
  const syncEnabled = useSyncEnabled({ enabled, settingsId, ensureDb });
  const [legacySyncing, setLegacySyncing] = useState(false);
  const [liveSyncing, setLiveSyncing] = useState(() => db?.getSyncStatus().isSyncing ?? false);
  const [syncMessage, setSyncMessage] = useState<string | null>(null);

  useEffect(() => {
    if (!db) {
      setLiveSyncing(false);
      return;
    }
    return db.subscribeSyncStatus((status) => {
      setLiveSyncing(status.isSyncing);
    });
  }, [db]);

  const syncing = db ? liveSyncing : legacySyncing;

  const runSync = useCallback(async (): Promise<ManualSyncResult | undefined> => {
    if (!syncEnabled) {
      setSyncMessage(disabledMessage);
      return undefined;
    }

    if (!db) setLegacySyncing(true);
    setSyncMessage(null);

    try {
      const result = await sync();
      setSyncMessage(formatManualSyncMessage(result));
      return result;
    } catch (error) {
      setSyncMessage(error instanceof Error ? error.message : "Sync failed");
      return undefined;
    } finally {
      if (!db) setLegacySyncing(false);
    }
  }, [db, disabledMessage, sync, syncEnabled]);

  return {
    syncEnabled,
    syncing,
    syncMessage,
    runSync,
  };
}
