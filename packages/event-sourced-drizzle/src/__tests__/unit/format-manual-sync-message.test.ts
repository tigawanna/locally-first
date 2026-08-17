import { describe, expect, it } from "vitest";

import { formatManualSyncMessage } from "../../react/format-manual-sync-message";
import type { ManualSyncResult } from "../../core/types";

function result(overrides: Partial<ManualSyncResult> = {}): ManualSyncResult {
  return {
    pushed: 0,
    pulled: 0,
    replayed: 0,
    skipped: 0,
    deadLettered: 0,
    deferred: false,
    errors: [],
    ...overrides,
  };
}

describe("formatManualSyncMessage", () => {
  it("formats a successful sync result", () => {
    expect(formatManualSyncMessage(result({ pushed: 2, pulled: 3, replayed: 1 }))).toBe(
      "Pushed 2, pulled 3, replayed 1 inbox event(s).",
    );
  });

  it("reports skipped events when some could not be applied", () => {
    expect(formatManualSyncMessage(result({ pulled: 1, skipped: 2 }))).toBe(
      "Pushed 0, pulled 1, replayed 0 inbox event(s). Skipped 2 unrecognized event(s).",
    );
  });

  it("reports dead-lettered events", () => {
    expect(formatManualSyncMessage(result({ deadLettered: 3 }))).toBe(
      "Pushed 0, pulled 0, replayed 0 inbox event(s). Moved 3 rejected event(s) to the dead-letter queue.",
    );
  });

  it("reports when another tab holds the sync lock", () => {
    expect(formatManualSyncMessage(result({ deferred: true }))).toBe(
      "Another tab is already syncing.",
    );
  });

  it("formats the first error when present", () => {
    expect(formatManualSyncMessage(result({ errors: [new Error("network down")] }))).toBe(
      "Sync failed: network down",
    );
  });
});
