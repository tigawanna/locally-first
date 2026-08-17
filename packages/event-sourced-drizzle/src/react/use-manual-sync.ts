import { useCallback, useState } from "react";

import type { ManualSyncResult } from "../core/types";
import { formatManualSyncMessage } from "./format-manual-sync-message";
import { useSyncEnabled, type SyncEnabledEngine } from "./use-sync-enabled";

export type ManualSyncEngine = SyncEnabledEngine & {
  manualSync: () => Promise<ManualSyncResult>;
};

export type UseManualSyncOptions = {
  engine: ManualSyncEngine;
  disabledMessage?: string;
};

export type UseManualSyncReturn = {
  syncEnabled: boolean;
  syncing: boolean;
  syncMessage: string | null;
  runSync: () => Promise<ManualSyncResult | undefined>;
};

const DEFAULT_DISABLED_MESSAGE = "Sync is disabled.";

/**
 * Manual sync UI state for an {@link EventSourcedDrizzle} engine.
 */
export function useManualSync({
  engine,
  disabledMessage = DEFAULT_DISABLED_MESSAGE,
}: UseManualSyncOptions): UseManualSyncReturn {
  const syncEnabled = useSyncEnabled(engine);
  const [syncing, setSyncing] = useState(false);
  const [syncMessage, setSyncMessage] = useState<string | null>(null);

  const runSync = useCallback(async (): Promise<ManualSyncResult | undefined> => {
    if (!syncEnabled) {
      setSyncMessage(disabledMessage);
      return undefined;
    }

    setSyncing(true);
    setSyncMessage(null);

    try {
      const result = await engine.manualSync();
      setSyncMessage(formatManualSyncMessage(result));
      return result;
    } catch (error) {
      setSyncMessage(error instanceof Error ? error.message : "Sync failed");
      return undefined;
    } finally {
      setSyncing(false);
    }
  }, [disabledMessage, engine, syncEnabled]);

  return { syncEnabled, syncing, syncMessage, runSync };
}
