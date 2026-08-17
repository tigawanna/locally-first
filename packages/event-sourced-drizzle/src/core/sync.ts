// Wire protocol types — canonical definitions live in protocol.ts.
// Re-exported here for backward compatibility.
export type {
  OutboundEvent,
  ServerEvent,
  PushConfirmation,
  PushFailure,
  PushResponse,
  PullResponse,
  PushEventsFn,
  PullEventsFn,
  SyncTransport,
  SyncUrlConfig,
  SyncHandlersConfig,
  NormalizedSyncTransport,
} from "./protocol";

import type {
  NormalizedSyncTransport,
  PullResponse,
  PushEventsFn,
  PushResponse,
  SyncHandlersConfig,
  SyncTransport,
  SyncUrlConfig,
} from "./protocol";

// --- Transport normalization ---

type HeaderConfig = SyncHandlersConfig["headers"];

async function resolveHeaders(headers: HeaderConfig): Promise<Record<string, string>> {
  if (!headers) return {};
  if (typeof headers === "function") return headers();
  return headers;
}

function appendSince(url: string, since: number): string {
  const separator = url.includes("?") ? "&" : "?";
  return `${url}${separator}since=${encodeURIComponent(String(since))}`;
}

function createHttpPush(url: string, headers: HeaderConfig): PushEventsFn {
  return async (events) => {
    if (events.length === 0) return { confirmed: [] };
    const resolvedHeaders = await resolveHeaders(headers);
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", ...resolvedHeaders },
      body: JSON.stringify(events),
    });
    if (!response.ok) throw new SyncPushError(response.status, await response.text());
    return response.json() as Promise<PushResponse>;
  };
}

function createHttpPull(
  url: string,
  headers: HeaderConfig,
): (since: number) => Promise<PullResponse> {
  return async (since) => {
    const resolvedHeaders = await resolveHeaders(headers);
    const pullUrl = appendSince(url, since);
    const response = await fetch(pullUrl, {
      headers: { Accept: "application/json", ...resolvedHeaders },
    });
    if (!response.ok) throw new SyncPullError(response.status, await response.text());
    return response.json() as Promise<PullResponse>;
  };
}

function isTransport(
  value: SyncHandlersConfig | SyncUrlConfig | SyncTransport,
): value is SyncTransport {
  return (
    "push" in value &&
    typeof value.push === "function" &&
    "pull" in value &&
    typeof value.pull === "function"
  );
}

/**
 * Normalizes `sync` config into push/pull functions. Accepts REST URLs,
 * custom handler functions, or an already-built {@link SyncTransport}.
 * Pass the result (or the same config) as `sync` on the engine.
 *
 * @example REST endpoints
 * ```ts
 * import { createEventSourcedDrizzle, createSyncTransport } from "event-sourced-drizzle"
 *
 * const sync = createSyncTransport({
 *   pushUrl: "/api/sync/events",
 *   pullUrl: "/api/sync/events",
 *   headers: () => ({ Authorization: `Bearer ${token}` }),
 * })
 *
 * const engine = await createEventSourcedDrizzle({ adapter, collections, sync })
 * ```
 *
 * @example Custom handlers
 * ```ts
 * import { createSyncTransport } from "event-sourced-drizzle"
 *
 * const sync = createSyncTransport({
 *   pushEvents: async (events) => postEvents(events),
 *   pullEvents: async ({ since }) => getEvents(since),
 * })
 * ```
 */
export function createSyncTransport(
  config?: SyncHandlersConfig | SyncUrlConfig | SyncTransport,
): NormalizedSyncTransport | null {
  if (!config) return null;

  if (isTransport(config)) {
    return { push: config.push, pull: config.pull };
  }

  const pushUrl =
    "pushUrl" in config
      ? config.pushUrl
      : "push" in config && typeof config.push === "string"
        ? config.push
        : undefined;
  const pullUrl =
    "pullUrl" in config
      ? config.pullUrl
      : "pull" in config && typeof config.pull === "string"
        ? config.pull
        : undefined;

  const push =
    "pushEvents" in config && config.pushEvents
      ? config.pushEvents
      : pushUrl
        ? createHttpPush(pushUrl, config.headers)
        : undefined;

  const pullEvents = "pullEvents" in config ? config.pullEvents : undefined;
  const pull = pullEvents
    ? (since: number) => pullEvents({ since })
    : pullUrl
      ? createHttpPull(pullUrl, config.headers)
      : undefined;

  if (!push && !pull) return null;
  return { push, pull };
}

/**
 * Thrown when the HTTP push endpoint returns a non-OK status.
 *
 * @example
 * ```ts
 * import { SyncPushError } from "event-sourced-drizzle"
 *
 * const result = await engine.sync()
 * const pushError = result.errors.find((error) => error instanceof SyncPushError)
 * if (pushError) {
 *   console.error(pushError.status, pushError.body)
 * }
 * ```
 */
export class SyncPushError extends Error {
  constructor(
    public readonly status: number,
    public readonly body: string,
  ) {
    super(`Event push failed: HTTP ${status}`);
    this.name = "SyncPushError";
  }
}

/**
 * Thrown when the HTTP pull endpoint returns a non-OK status.
 *
 * @example
 * ```ts
 * import { SyncPullError } from "event-sourced-drizzle"
 *
 * const result = await engine.sync()
 * const pullError = result.errors.find((error) => error instanceof SyncPullError)
 * if (pullError) {
 *   console.error(pullError.status, pullError.body)
 * }
 * ```
 */
export class SyncPullError extends Error {
  constructor(
    public readonly status: number,
    public readonly body: string,
  ) {
    super(`Event pull failed: HTTP ${status}`);
    this.name = "SyncPullError";
  }
}
