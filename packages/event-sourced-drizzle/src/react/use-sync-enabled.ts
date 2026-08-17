import { useEffect, useState } from "react";

export type SyncEnabledEngine = {
  getSyncEnabled: () => boolean;
  subscribeSyncEnabled: (listener: (enabled: boolean) => void) => () => void;
};

/**
 * Tracks `engine.getSyncEnabled()` and re-renders when `setSyncEnabled` runs.
 */
export function useSyncEnabled(engine: SyncEnabledEngine): boolean {
  const [enabled, setEnabled] = useState(() => engine.getSyncEnabled());

  useEffect(() => {
    setEnabled(engine.getSyncEnabled());
    return engine.subscribeSyncEnabled(setEnabled);
  }, [engine]);

  return enabled;
}
