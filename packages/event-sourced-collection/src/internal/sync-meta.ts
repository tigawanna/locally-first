import type { Collection } from "@tanstack/db";

import type { InboxEntry, SyncMetaEntry } from "../core/types";
import { SYNCMETA_KEY } from "./constants";

const EMPTY: SyncMetaEntry = {
  id: SYNCMETA_KEY,
  backendId: null,
  clientId: null,
  pullCursor: 0,
  lastSyncAt: null,
  lastError: null,
};

export function readSyncMeta(syncmeta: Collection<SyncMetaEntry, string>): SyncMetaEntry {
  return syncmeta.get(SYNCMETA_KEY) ?? EMPTY;
}

export async function ensureSyncMeta(syncmeta: Collection<SyncMetaEntry, string>): Promise<void> {
  if (syncmeta.has(SYNCMETA_KEY)) return;
  await syncmeta.insert({ ...EMPTY }).isPersisted.promise;
}

async function patch(
  syncmeta: Collection<SyncMetaEntry, string>,
  apply: (draft: SyncMetaEntry) => void,
): Promise<void> {
  await ensureSyncMeta(syncmeta);
  await syncmeta.update(SYNCMETA_KEY, apply).isPersisted.promise;
}

export function readBackendId(syncmeta: Collection<SyncMetaEntry, string>): string | null {
  return readSyncMeta(syncmeta).backendId;
}

/**
 * Resolves this device's identity, persisting it on first run so it survives
 * reloads. Without that, a client stops recognising its own pulled-back events
 * as soon as the outbox is pruned and will replay its own history as if it were
 * someone else's.
 *
 * Preference: explicit `configured` id, then the stored `syncmeta.clientId`,
 * then `generate()`.
 *
 * @example First open generates and stores an id; the next open reuses it
 * ```ts
 * import { resolveClientId } from "./sync-meta"
 *
 * const clientId = await resolveClientId(
 *   db.collections.syncmeta,
 *   undefined,
 *   () => crypto.randomUUID(),
 * )
 * // later, after close + reopen on the same file:
 * const again = await resolveClientId(db.collections.syncmeta, undefined, () => {
 *   throw new Error("should not generate a second id")
 * })
 * // again === clientId
 * ```
 */
export async function resolveClientId(
  syncmeta: Collection<SyncMetaEntry, string>,
  configured: string | undefined,
  generate: () => string,
): Promise<string> {
  const stored = readSyncMeta(syncmeta).clientId ?? null;
  const clientId = configured ?? stored ?? generate();

  if (stored !== clientId) {
    await patch(syncmeta, (draft) => {
      draft.clientId = clientId;
    });
  }

  return clientId;
}

/**
 * The cursor lives in `syncmeta` so the inbox can be pruned freely. Older
 * databases predate that row, so fall back to the highest resolved inbox
 * sequence and let the first successful pull persist it.
 *
 * @example
 * ```ts
 * import { readPullCursor } from "./sync-meta"
 *
 * const since = readPullCursor(db.collections.syncmeta, db.collections.inbox)
 * const page = await transport.pull(since)
 * ```
 */
export function readPullCursor(
  syncmeta: Collection<SyncMetaEntry, string>,
  inbox: Collection<InboxEntry, string>,
): number {
  let cursor = readSyncMeta(syncmeta).pullCursor;

  for (const entry of inbox.state.values()) {
    if (entry.sync && entry.globalSeq > cursor) cursor = entry.globalSeq;
  }

  return cursor;
}

export async function writePullCursor(
  syncmeta: Collection<SyncMetaEntry, string>,
  cursor: number,
): Promise<void> {
  await patch(syncmeta, (draft) => {
    draft.pullCursor = cursor;
  });
}

export async function writeBackendId(
  syncmeta: Collection<SyncMetaEntry, string>,
  backendId: string,
): Promise<void> {
  await patch(syncmeta, (draft) => {
    draft.backendId = backendId;
  });
}

/**
 * Records the outcome of a sync. `lastSyncAt` is left untouched when it is
 * `null`, so a failed or held push does not restart the `syncBatch` delay.
 */
export async function writeSyncOutcome(
  syncmeta: Collection<SyncMetaEntry, string>,
  lastSyncAt: number | null,
  lastError: string | null,
): Promise<void> {
  await patch(syncmeta, (draft) => {
    if (lastSyncAt !== null) draft.lastSyncAt = lastSyncAt;
    draft.lastError = lastError;
  });
}
