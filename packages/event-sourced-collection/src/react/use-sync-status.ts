import { useEffect, useState } from "react";

import type { SyncStatus } from "../core/types";

/**
 * Minimal DB surface needed for sync status. Accepts the full
 * {@link import("../core/types").EventSourcedDB} or a test double.
 */
export type SyncStatusSource = {
  getSyncStatus: () => SyncStatus;
  subscribeSyncStatus: (listener: (status: SyncStatus) => void) => () => void;
};

/**
 * Live {@link SyncStatus} for spinners, badges, and pending counts.
 *
 * `getSyncStatus()` alone is a snapshot — it will go stale in React. Always
 * subscribe (this hook, or `db.subscribeSyncStatus` directly).
 *
 * @example
 * ```tsx
 * import { useSyncStatus } from "event-sourced-collection/react"
 *
 * function SyncBadge({ db }: { db: SyncStatusSource }) {
 *   const { isSyncing, pendingCount } = useSyncStatus(db)
 *   if (isSyncing) return <Spinner />
 *   return <span>{pendingCount} pending</span>
 * }
 * ```
 */
export function useSyncStatus(db: SyncStatusSource): SyncStatus {
  const [status, setStatus] = useState(() => db.getSyncStatus());

  useEffect(() => db.subscribeSyncStatus(setStatus), [db]);

  return status;
}
