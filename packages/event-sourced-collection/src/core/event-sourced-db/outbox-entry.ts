import type { Collection } from "@tanstack/db";

import { allocateLocalSeq } from "../../internal/compact-outbox";
import type { OutboxEntry } from "../types";

type SeqCounter = { value: number };

/** Fills a pending outbox row from identity, body, and freshly allocated ids. */
export function outboxEntry(
  identity: Pick<OutboxEntry, "collectionId" | "key" | "clientId" | "schemaVersion">,
  body: Pick<OutboxEntry, "type" | "payload" | "previous" | "baseVersion"> &
    Partial<Pick<OutboxEntry, "compaction" | "compactedAt">>,
  ids: {
    outbox: Collection<OutboxEntry, string>;
    seq: SeqCounter;
    eventId: string;
    txId: string;
    timestamp: number;
  },
): OutboxEntry {
  return {
    eventId: ids.eventId,
    collectionId: identity.collectionId,
    type: body.type,
    key: identity.key,
    payload: body.payload,
    previous: body.previous,
    txId: ids.txId,
    clientId: identity.clientId,
    schemaVersion: identity.schemaVersion,
    baseVersion: body.baseVersion,
    timestamp: ids.timestamp,
    localSeq: allocateLocalSeq(ids.outbox, ids.seq),
    globalSeq: null,
    sync: false,
    syncStatus: "pending",
    attemptCount: 0,
    lastAttemptAt: null,
    nextAttemptAt: null,
    lastError: null,
    lastErrorCode: null,
    retryable: null,
    compaction: body.compaction,
    compactedAt: body.compactedAt,
  };
}
