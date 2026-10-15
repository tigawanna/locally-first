import type { Collection } from "@tanstack/db";

import type { DeadLetterEntry, InboxEntry } from "../types";

/** One dead-letter row, or every row in sequence order when `eventId` is omitted. */
export function selectDeadLetters(
  deadletter: Collection<DeadLetterEntry, string>,
  eventId?: string,
): DeadLetterEntry[] {
  if (eventId === undefined) {
    return [...deadletter.state.values()].sort(
      (a, b) => (a.localSeq ?? a.globalSeq ?? 0) - (b.localSeq ?? b.globalSeq ?? 0),
    );
  }

  const entry = deadletter.get(eventId);
  return entry ? [entry] : [];
}

/** Puts a parked inbound event back in the inbox so replay can pick it up again. */
export async function requeueInboundDeadLetter(
  inbox: Collection<InboxEntry, string>,
  entry: DeadLetterEntry,
): Promise<void> {
  if (inbox.has(entry.eventId)) {
    await inbox.update(entry.eventId, (draft) => {
      draft.sync = false;
      draft.skipped = false;
      draft.skipReason = null;
      draft.attemptCount = 0;
      draft.lastError = null;
    }).isPersisted.promise;
    return;
  }

  await inbox.insert({
    eventId: entry.eventId,
    globalSeq: entry.globalSeq ?? 0,
    collectionId: entry.collectionId,
    type: entry.type,
    key: entry.key,
    payload: entry.payload,
    previous: entry.previous,
    clientId: entry.clientId,
    schemaVersion: entry.schemaVersion,
    timestamp: entry.timestamp,
    sync: false,
    skipped: false,
    skipReason: null,
    attemptCount: 0,
    lastError: null,
  }).isPersisted.promise;
}
