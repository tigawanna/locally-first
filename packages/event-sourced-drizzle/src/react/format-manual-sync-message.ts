import type { ManualSyncResult } from "../core/types";

export function formatManualSyncMessage(result: ManualSyncResult): string {
  if (result.errors.length > 0) {
    return `Sync failed: ${result.errors[0]?.message ?? "Unknown error"}`;
  }

  if (result.deferred) {
    return "Another tab is already syncing.";
  }

  const parts = [
    `Pushed ${result.pushed}, pulled ${result.pulled}, replayed ${result.replayed} inbox event(s).`,
  ];

  if (result.skipped > 0) {
    parts.push(`Skipped ${result.skipped} unrecognized event(s).`);
  }

  if (result.deadLettered > 0) {
    parts.push(`Moved ${result.deadLettered} rejected event(s) to the dead-letter queue.`);
  }

  return parts.join(" ");
}
