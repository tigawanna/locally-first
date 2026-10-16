import type { Collection } from "@tanstack/db";

import { allocateLocalSeq } from "../../internal/compact-outbox";
import type { OutboxEntry } from "../types";
import type { SeqCounter } from "./refs";

type OutboxIdentity = Pick<OutboxEntry, "collectionId" | "key" | "clientId" | "schemaVersion">;
type OutboxBody = Pick<OutboxEntry, "type" | "payload" | "previous" | "baseVersion"> &
  Partial<Pick<OutboxEntry, "compaction" | "compactedAt">>;

/**
 * Builds one outbox row that has not been synced yet.
 * Copies which collection and row this is, plus the event body, then stamps
 * a new event id, transaction id, timestamp, and the next local sequence.
 */
export function buildUnsyncedOutboxEntry(input: {
  identity: OutboxIdentity;
  body: OutboxBody;
  ids: {
    outbox: Collection<OutboxEntry, string>;
    seq: SeqCounter;
    eventId: string;
    txId: string;
    timestamp: number;
    /** Kept when a fold should stay where the chain already was. */
    localSeq?: number;
  };
}): OutboxEntry {
  const { identity, body, ids } = input;
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
    localSeq: ids.localSeq ?? allocateLocalSeq(ids.outbox, ids.seq),
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
