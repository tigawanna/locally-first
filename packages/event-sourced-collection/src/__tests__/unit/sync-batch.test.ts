import { describe, expect, it } from "vite-plus/test";

import { decideSyncBatch } from "../../core/event-sourced-db/sync-batch";

const minute = 60_000;

describe("decideSyncBatch", () => {
  const base = { oldestPendingAt: 0, lastSyncAt: null as number | null };

  it("allows a sync when no batch limits are set", () => {
    expect(decideSyncBatch({ ...base, batch: undefined, pendingCount: 3, now: 1 }).allowed).toBe(
      true,
    );
  });

  it("starts the delay at the oldest unpushed event when nothing has synced yet", () => {
    const batch = { delayMinutes: 10, minEvents: 20 };
    expect(decideSyncBatch({ ...base, batch, pendingCount: 1, now: minute }).allowed).toBe(false);
    expect(decideSyncBatch({ ...base, batch, pendingCount: 1, now: 10 * minute }).allowed).toBe(
      true,
    );
  });

  it("opens when either the event count or the delay is reached", () => {
    const batch = { minEvents: 20, delayMinutes: 10 };
    const lastSyncAt = 0;

    expect(
      decideSyncBatch({ ...base, batch, pendingCount: 20, lastSyncAt, now: minute }).allowed,
    ).toBe(true);
    expect(
      decideSyncBatch({ ...base, batch, pendingCount: 1, lastSyncAt, now: 10 * minute }).allowed,
    ).toBe(true);
    expect(
      decideSyncBatch({ ...base, batch, pendingCount: 5, lastSyncAt, now: minute }).allowed,
    ).toBe(false);
  });

  it("still pulls when nothing is waiting to push", () => {
    expect(
      decideSyncBatch({
        ...base,
        batch: { minEvents: 20, delayMinutes: 10 },
        pendingCount: 0,
        lastSyncAt: 0,
        now: minute,
      }).allowed,
    ).toBe(true);
  });
});
