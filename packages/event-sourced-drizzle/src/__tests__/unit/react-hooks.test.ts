/** @vitest-environment jsdom */
import { act, renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { useManualSync } from "../../react/use-manual-sync";
import { useSyncEnabled } from "../../react/use-sync-enabled";
import type { ManualSyncResult } from "../../core/types";

function emptyManual(): ManualSyncResult {
  return {
    pushed: 1,
    pulled: 0,
    replayed: 0,
    skipped: 0,
    deadLettered: 0,
    deferred: false,
    errors: [],
  };
}

function createFakeEngine(initialEnabled = true) {
  let enabled = initialEnabled;
  const listeners = new Set<(value: boolean) => void>();

  return {
    getSyncEnabled: () => enabled,
    setSyncEnabled: (value: boolean) => {
      enabled = value;
      for (const listener of listeners) listener(value);
    },
    subscribeSyncEnabled: (listener: (value: boolean) => void) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    manualSync: async () => emptyManual(),
  };
}

describe("useSyncEnabled", () => {
  it("tracks engine.setSyncEnabled", () => {
    const engine = createFakeEngine(true);
    const { result } = renderHook(() => useSyncEnabled(engine));
    expect(result.current).toBe(true);

    act(() => {
      engine.setSyncEnabled(false);
    });
    expect(result.current).toBe(false);
  });
});

describe("useManualSync", () => {
  it("runs manualSync and sets a message", async () => {
    const engine = createFakeEngine(true);
    const { result } = renderHook(() => useManualSync({ engine }));

    await act(async () => {
      await result.current.runSync();
    });

    expect(result.current.syncMessage).toContain("Pushed 1");
    expect(result.current.syncing).toBe(false);
  });

  it("does not sync when disabled", async () => {
    const engine = createFakeEngine(false);
    const { result } = renderHook(() => useManualSync({ engine }));

    await act(async () => {
      const value = await result.current.runSync();
      expect(value).toBeUndefined();
    });

    expect(result.current.syncMessage).toBe("Sync is disabled.");
  });
});
