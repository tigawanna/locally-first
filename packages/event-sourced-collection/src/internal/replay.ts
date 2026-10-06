import type { Collection } from "@tanstack/db";

import type { DeadLetterEntry, InboxEntry, RowVersionEntry, UpcastableEvent } from "../core/types";
import { DEFAULT_EVENT_SCHEMA_VERSION, RESERVED_IDS } from "./constants";
import type { ReplayableEvent, ReplayContext, ReplayOutcome } from "./types";

export type ReplaySummary = { applied: number; skipped: number; deadLettered: number };

export function rowVersionId(collectionId: string, key: string | number): string {
  return `${collectionId}::${String(key)}`;
}

/**
 * Records the version a row is now at. Used to stamp `baseVersion` on future
 * local mutations so the server can detect writes authored against stale state.
 */
export async function recordRowVersion(
  rowversions: Collection<RowVersionEntry, string>,
  collectionId: string,
  key: string | number,
  version: string,
  globalSeq: number | null,
): Promise<void> {
  const id = rowVersionId(collectionId, key);

  if (rowversions.has(id)) {
    await rowversions.update(id, (draft) => {
      draft.version = version;
      draft.globalSeq = globalSeq;
    }).isPersisted.promise;
    return;
  }

  await rowversions.insert({ id, collectionId, key, version, globalSeq }).isPersisted.promise;
}

export function readRowVersion(
  rowversions: Collection<RowVersionEntry, string>,
  collectionId: string,
  key: string | number,
): string | null {
  return rowversions.get(rowVersionId(collectionId, key))?.version ?? null;
}

/**
 * After a CONFLICT dead-letter, undo the optimistic `recordRowVersion` that
 * stamped the rejected event id as head. Restores `baseVersion`, or deletes
 * the index row when the mutation was authored against an unknown row.
 */
export async function restoreRowVersionAfterConflict(
  rowversions: Collection<RowVersionEntry, string>,
  collectionId: string,
  key: string | number,
  baseVersion: string | null,
): Promise<void> {
  const id = rowVersionId(collectionId, key);

  if (baseVersion === null) {
    if (rowversions.has(id)) {
      await rowversions.delete(id).isPersisted.promise;
    }
    return;
  }

  await recordRowVersion(rowversions, collectionId, key, baseVersion, null);
}

export async function replayEvent(
  context: ReplayContext,
  event: ReplayableEvent,
): Promise<ReplayOutcome> {
  const { targets, log } = context;
  const { eventId, collectionId } = event;

  // A reserved target can never become valid, so retrying it would wedge the
  // pipeline forever. Unknown collections can become valid after a client
  // upgrade, so those honour unknownEventHandling.
  if (RESERVED_IDS.has(collectionId)) {
    const reason = `reserved collection "${collectionId}"`;
    log.warn("replay skipped: reserved collection", {
      eventId,
      collectionId,
      type: event.type,
      key: event.key,
    });
    context.emit("onEventSkipped", { eventId, collectionId, reason });
    return { status: "skipped", reason };
  }

  if (context.localOnlyIds.has(collectionId)) {
    const reason = `local-only collection "${collectionId}"`;
    log.debug("replay skipped: local-only collection", { eventId, collectionId });
    context.emit("onEventSkipped", { eventId, collectionId, reason });
    return { status: "skipped", reason };
  }

  const migrated = upcast(context, event);
  if (migrated.status !== "ok") return migrated.outcome;

  const target = targets[collectionId];
  if (!target) {
    return unresolvable(context, event, `unknown collection "${collectionId}"`, {
      knownCollections: Object.keys(targets).filter((id) => !RESERVED_IDS.has(id)),
    });
  }

  if (!target.utils.acceptMutations) {
    return unresolvable(context, event, `collection "${collectionId}" is missing acceptMutations`, {
      targetId: target.id,
    });
  }

  const { type, key, payload } = migrated.event;

  log.debug("replay applying mutation", { eventId, collectionId, type, key });

  try {
    await target.utils.acceptMutations({
      mutations: [
        {
          mutationId: eventId,
          type,
          key,
          modified: payload,
          original: payload,
          changes: payload,
          collection: target,
        },
      ],
    });
  } catch (err) {
    const error = err instanceof Error ? err : new Error(String(err));
    log.error("replay mutation failed", {
      eventId,
      collectionId,
      type,
      key,
      message: error.message,
    });
    // Reported rather than thrown: one unapplicable event must not be able to
    // wedge the pull pipeline behind it forever.
    return { status: "failed", error };
  }

  if (context.conflictDetection) {
    await recordRowVersion(
      context.rowversions,
      collectionId,
      key,
      eventId,
      event.globalSeq ?? null,
    );
  }

  log.info("replay mutation accepted", { eventId, collectionId, type, key });
  context.emit("onEventApplied", { eventId, collectionId, type, key });

  return { status: "applied" };
}

type UpcastResult =
  | { status: "ok"; event: UpcastableEvent }
  | { status: "blocked"; outcome: ReplayOutcome };

/**
 * Brings an event authored under a different schema version into the current
 * shape. Without an upcaster, older events are applied as-is (best effort) but
 * newer ones are refused, since this build cannot know what the fields mean.
 */
function upcast(context: ReplayContext, event: ReplayableEvent): UpcastResult {
  const version = event.schemaVersion ?? DEFAULT_EVENT_SCHEMA_VERSION;

  const current: UpcastableEvent = {
    eventId: event.eventId,
    collectionId: event.collectionId,
    type: event.type,
    key: event.key,
    payload: event.payload,
    previous: event.previous ?? null,
    schemaVersion: version,
  };

  if (version === context.eventSchemaVersion) {
    return { status: "ok", event: current };
  }

  if (context.upcastEvent) {
    const migrated = context.upcastEvent(current);

    if (!migrated) {
      const reason = `upcaster dropped schema version ${version}`;
      context.log.warn("replay skipped: upcaster dropped event", {
        eventId: event.eventId,
        collectionId: event.collectionId,
        schemaVersion: version,
      });
      context.emit("onEventSkipped", {
        eventId: event.eventId,
        collectionId: event.collectionId,
        reason,
      });
      return { status: "blocked", outcome: { status: "skipped", reason } };
    }

    return { status: "ok", event: migrated };
  }

  if (version > context.eventSchemaVersion) {
    return {
      status: "blocked",
      outcome: unresolvable(
        context,
        event,
        `event schema version ${version} is newer than supported version ${context.eventSchemaVersion}`,
        {},
      ),
    };
  }

  context.log.warn("replaying older event schema version without an upcaster", {
    eventId: event.eventId,
    collectionId: event.collectionId,
    schemaVersion: version,
    currentVersion: context.eventSchemaVersion,
  });

  return { status: "ok", event: current };
}

function unresolvable(
  context: ReplayContext,
  event: ReplayableEvent,
  reason: string,
  details: Record<string, unknown>,
): ReplayOutcome {
  const halt = context.unknownEventHandling === "fail";

  context.log.warn(halt ? "replay halted" : "replay skipped", {
    eventId: event.eventId,
    collectionId: event.collectionId,
    type: event.type,
    key: event.key,
    reason,
    ...details,
  });

  if (halt) return { status: "halted", reason };

  context.emit("onEventSkipped", {
    eventId: event.eventId,
    collectionId: event.collectionId,
    reason,
  });

  return { status: "skipped", reason };
}

/**
 * What to do after a replay threw: come back to it next sync, or give up and
 * park it in the dead-letter queue.
 */
export type ReplayFailureResolution = "retry" | "deadLettered";

/**
 * Records a failed replay against its inbox row and, once the attempt budget is
 * spent, moves the event to the dead-letter queue and resolves the inbox row so
 * the cursor can advance past it. Otherwise a single event the local schema
 * cannot accept blocks every event behind it, permanently.
 */
export async function handleReplayFailure(
  context: ReplayContext,
  inbox: Collection<InboxEntry, string>,
  event: ReplayableEvent,
  error: Error,
  now: number,
): Promise<ReplayFailureResolution> {
  const { deadletter, maxReplayAttempts, log } = context;
  const existing = inbox.get(event.eventId);
  const attemptCount = (existing?.attemptCount ?? 0) + 1;

  if (existing) {
    await inbox.update(event.eventId, (draft) => {
      draft.attemptCount = attemptCount;
      draft.lastError = error.message;
    }).isPersisted.promise;
  }

  if (attemptCount < maxReplayAttempts) {
    log.warn("replay failed; retrying on a later sync", {
      eventId: event.eventId,
      collectionId: event.collectionId,
      attemptCount,
      maxReplayAttempts,
      message: error.message,
    });

    return "retry";
  }

  const reason = `replay failed after ${attemptCount} attempts: ${error.message}`;

  if (!deadletter.has(event.eventId)) {
    const record: DeadLetterEntry = {
      eventId: event.eventId,
      collectionId: event.collectionId,
      type: event.type,
      key: event.key,
      payload: event.payload,
      previous: event.previous ?? null,
      txId: null,
      clientId: existing?.clientId ?? null,
      schemaVersion: event.schemaVersion ?? DEFAULT_EVENT_SCHEMA_VERSION,
      timestamp: existing?.timestamp ?? now,
      localSeq: null,
      globalSeq: event.globalSeq ?? null,
      direction: "inbound",
      reason: "replayFailed",
      message: error.message,
      code: null,
      attemptCount,
      failedAt: now,
    };

    await deadletter.insert(record).isPersisted.promise;
    context.emit("onDeadLetter", record);
  }

  if (existing) {
    await resolveInboxEntry(inbox, event.eventId, { status: "skipped", reason });
  }

  log.warn("inbound event dead-lettered", {
    eventId: event.eventId,
    collectionId: event.collectionId,
    attemptCount,
    message: error.message,
  });

  return "deadLettered";
}

/**
 * Marks an inbox row as resolved — either applied or deliberately skipped.
 * Skipped rows still count as resolved so the pull cursor can move past them.
 */
export async function resolveInboxEntry(
  inbox: Collection<InboxEntry, string>,
  eventId: string,
  outcome: Extract<ReplayOutcome, { status: "applied" | "skipped" }>,
): Promise<void> {
  await inbox.update(eventId, (draft) => {
    draft.sync = true;
    draft.skipped = outcome.status === "skipped";
    draft.skipReason = outcome.status === "skipped" ? outcome.reason : null;
  }).isPersisted.promise;
}

export async function replayInbox(
  inbox: Collection<InboxEntry, string>,
  context: ReplayContext,
): Promise<ReplaySummary> {
  const { log } = context;

  const pending = [...inbox.state.values()]
    .filter((entry) => !entry.sync)
    .sort((a, b) => a.globalSeq - b.globalSeq);

  log.info("pending inbox replay started", { pendingCount: pending.length });

  let applied = 0;
  let skipped = 0;
  let deadLettered = 0;

  for (const entry of pending) {
    const outcome = await replayEvent(context, entry);

    if (outcome.status === "halted") break;

    if (outcome.status === "failed") {
      const resolution = await handleReplayFailure(
        context,
        inbox,
        entry,
        outcome.error,
        Date.now(),
      );

      // Still retryable: stop here so events stay in order until it resolves.
      if (resolution === "retry") break;

      deadLettered++;
      continue;
    }

    await resolveInboxEntry(inbox, entry.eventId, outcome);

    if (outcome.status === "skipped") {
      skipped++;
      continue;
    }

    applied++;

    log.info("pending inbox replay applied", {
      eventId: entry.eventId,
      globalSeq: entry.globalSeq,
      collectionId: entry.collectionId,
    });
  }

  log.info("pending inbox replay finished", { applied, skipped, deadLettered });

  return { applied, skipped, deadLettered };
}
