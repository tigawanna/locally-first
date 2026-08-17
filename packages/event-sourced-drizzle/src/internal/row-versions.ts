import { rowVersionMetaKey } from "./constants";
import type { DrizzleAdapter } from "./types";

export async function readRowVersion(
  adapter: DrizzleAdapter,
  collectionId: string,
  key: string,
): Promise<string | null> {
  const value = await adapter.readMeta(rowVersionMetaKey(collectionId, key));
  return value ? value : null;
}

export async function writeRowVersion(
  adapter: DrizzleAdapter,
  collectionId: string,
  key: string,
  eventId: string,
): Promise<void> {
  await adapter.writeMeta(rowVersionMetaKey(collectionId, key), eventId);
}

export async function restoreRowVersion(
  adapter: DrizzleAdapter,
  collectionId: string,
  key: string,
  baseVersion: string | null,
): Promise<void> {
  await adapter.writeMeta(rowVersionMetaKey(collectionId, key), baseVersion ?? "");
}

export function pendingRowVersion(
  tx: { entries: Array<{ outboxRow: { collectionId: string; key: string; eventId: string } }> },
  collectionId: string,
  key: string,
): string | null {
  for (let i = tx.entries.length - 1; i >= 0; i--) {
    const row = tx.entries[i]!.outboxRow;
    if (row.collectionId === collectionId && row.key === key) return row.eventId;
  }
  return null;
}
