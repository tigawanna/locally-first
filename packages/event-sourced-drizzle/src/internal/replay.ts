import { DEFAULT_EVENT_SCHEMA_VERSION } from "./constants";
import { writeRowVersion } from "./row-versions";
import type {
  DeadLetterRow,
  DrizzleAdapter,
  InboxRow,
  MutationType,
  ReplayContext,
  ReplayOutcome,
  UpcastableEvent,
} from "./types";

export type ReplaySummary = { applied: number; skipped: number; deadLettered: number };

type ReplayableEvent = {
  eventId: string;
  globalSeq: number;
  collectionId: string;
  type: MutationType;
  key: string;
  payload: Record<string, unknown>;
  previous: Record<string, unknown> | null;
  schemaVersion: number;
  clientId: string | null;
  timestamp: number;
};

/**
 * Replays all unresolved inbox rows in globalSeq order.
 */
export async function replayInbox(
  adapter: DrizzleAdapter,
  context: ReplayContext,
): Promise<ReplaySummary> {
  const unresolved = await adapter.queryUnresolvedInbox();
  let applied = 0;
  let skipped = 0;
  let deadLettered = 0;

  for (const row of unresolved) {
    const outcome = await replayEvent(context, row);

    if (outcome.status === "halted") break;

    if (outcome.status === "failed") {
      const resolution = await handleReplayFailure(context, row, outcome.error, Date.now());
      if (resolution === "retry") break;
      deadLettered++;
      continue;
    }

    if (outcome.status === "skipped") {
      await adapter.updateInbox(row.eventId, {
        sync: true,
        skipped: true,
        skipReason: outcome.reason,
      });
      skipped++;
      continue;
    }

    // applied
    await adapter.updateInbox(row.eventId, { sync: true });
    applied++;
  }

  return { applied, skipped, deadLettered };
}

export async function replayEvent(
  context: ReplayContext,
  event: ReplayableEvent,
): Promise<ReplayOutcome> {
  const { collections, adapter, log } = context;
  const { eventId, collectionId } = event;

  const target = collections[collectionId];
  if (!target) {
    return unresolvable(context, event, `unknown collection "${collectionId}"`);
  }

  const migrated = upcast(context, event);
  if (migrated.status !== "ok") return migrated.outcome;

  const { type, key, payload } = migrated.event;

  log.debug("replay applying mutation", { eventId, collectionId, type, key });

  try {
    switch (type) {
      case "insert":
        await adapter.domainInsert(collectionId, {
          ...payload,
          [getKeyField(target, payload)]: key,
        });
        break;
      case "update":
        await adapter.domainUpdate(collectionId, key, payload);
        break;
      case "delete":
        await adapter.domainDelete(collectionId, key);
        break;
    }
  } catch (err) {
    const error = err instanceof Error ? err : new Error(String(err));
    log.error("replay mutation failed", {
      eventId,
      collectionId,
      type,
      key,
      message: error.message,
    });
    return { status: "failed", error };
  }

  if (context.conflictDetection) {
    await writeRowVersion(context.adapter, collectionId, key, eventId);
  }

  log.info("replay mutation accepted", { eventId, collectionId, type, key });
  context.emit("onEventApplied", { eventId, collectionId, type, key });

  return { status: "applied" };
}

function getKeyField(
  _target: { getKey: (row: never) => string | number },
  _payload: Record<string, unknown>,
): string {
  // The key field is embedded in the payload already.
  // This is a placeholder — domain insert receives the full payload with key included.
  return "";
}

type UpcastResult =
  | { status: "ok"; event: UpcastableEvent }
  | { status: "blocked"; outcome: ReplayOutcome };

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
    if (migrated === null) {
      const reason = `upcast returned null for schemaVersion ${version}`;
      log_skip(context, event, reason);
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
      ),
    };
  }

  // Older version without upcaster: apply as-is.
  context.log.warn("replaying older event schema version without an upcaster", {
    eventId: event.eventId,
    collectionId: event.collectionId,
    schemaVersion: version,
    currentVersion: context.eventSchemaVersion,
  });

  return { status: "ok", event: current };
}

function log_skip(context: ReplayContext, event: ReplayableEvent, reason: string): void {
  context.log.warn("replay skipped", {
    eventId: event.eventId,
    collectionId: event.collectionId,
    reason,
  });
  context.emit("onEventSkipped", {
    eventId: event.eventId,
    collectionId: event.collectionId,
    reason,
  });
}

function unresolvable(
  context: ReplayContext,
  event: ReplayableEvent,
  reason: string,
): ReplayOutcome {
  const halt = context.unknownEventHandling === "fail";

  context.log.warn(halt ? "replay halted" : "replay skipped", {
    eventId: event.eventId,
    collectionId: event.collectionId,
    type: event.type,
    key: event.key,
    reason,
  });

  if (halt) return { status: "halted", reason };

  context.emit("onEventSkipped", {
    eventId: event.eventId,
    collectionId: event.collectionId,
    reason,
  });

  return { status: "skipped", reason };
}

export type ReplayFailureResolution = "retry" | "deadLettered";

export async function handleReplayFailure(
  context: ReplayContext,
  event: InboxRow,
  error: Error,
  now: number,
): Promise<ReplayFailureResolution> {
  const { adapter, maxReplayAttempts, log, emit } = context;
  const attemptCount = (event.attemptCount ?? 0) + 1;

  await adapter.updateInbox(event.eventId, {
    attemptCount,
    lastError: error.message,
  });

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

  const record: DeadLetterRow = {
    eventId: event.eventId,
    collectionId: event.collectionId,
    type: event.type,
    key: event.key,
    payload: event.payload,
    previous: event.previous ?? null,
    txId: null,
    clientId: event.clientId,
    schemaVersion: event.schemaVersion,
    timestamp: event.timestamp,
    localSeq: null,
    globalSeq: event.globalSeq,
    direction: "inbound",
    reason: "replayFailed",
    message: error.message,
    code: null,
    attemptCount,
    failedAt: now,
  };

  await adapter.insertDeadLetter(record);
  await adapter.updateInbox(event.eventId, {
    sync: true,
    skipped: true,
    skipReason: "replayFailed",
  });

  log.warn("replay event dead-lettered", {
    eventId: event.eventId,
    collectionId: event.collectionId,
    attemptCount,
    message: error.message,
  });

  emit("onDeadLetter", record);

  return "deadLettered";
}
