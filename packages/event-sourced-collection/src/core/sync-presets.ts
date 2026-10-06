import {
  DEFAULT_PUSH_BATCH_SIZE,
  MAX_PUSH_BATCH_SIZE,
  MIN_PUSH_BATCH_SIZE,
} from "../internal/constants";
import type { SyncPushLimits, SyncPushPreset } from "./types";

/**
 * Push limits per hosting platform. Byte caps leave headroom under the
 * platform's request body limit (Vercel functions: 4.5 MB; AWS Lambda and
 * Netlify: 6 MB) for headers and JSON size estimation drift.
 *
 * @example
 * ```ts
 * createBrowserEventSourcedDB({ ..., syncPreset: "vercel" })
 * ```
 */
export const SYNC_PUSH_PRESETS: Readonly<Record<SyncPushPreset, SyncPushLimits>> = {
  default: { pushBatchSize: DEFAULT_PUSH_BATCH_SIZE, maxPushBytes: null },
  serverless: { pushBatchSize: 25, maxPushBytes: 4_000_000 },
  vercel: { pushBatchSize: 10, maxPushBytes: 3_500_000 },
};

export function resolvePushLimits(config: {
  syncPreset?: SyncPushPreset;
  pushBatchSize?: number;
  maxPushBytes?: number;
}): SyncPushLimits {
  const preset = SYNC_PUSH_PRESETS[config.syncPreset ?? "default"];
  const size = Math.floor(config.pushBatchSize ?? preset.pushBatchSize);
  const bytes = config.maxPushBytes ?? preset.maxPushBytes;

  return {
    pushBatchSize: Math.min(MAX_PUSH_BATCH_SIZE, Math.max(MIN_PUSH_BATCH_SIZE, size)),
    maxPushBytes: bytes === null ? null : Math.max(1, Math.floor(bytes)),
  };
}
