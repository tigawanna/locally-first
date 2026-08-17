import { describe, expect, it } from "vitest";
import { createOptimisticStateTracker } from "../../core/optimistic-state";
import type { OptimisticEntry } from "../../core/optimistic-state";

function makeEntry(overrides: Partial<OptimisticEntry> = {}): OptimisticEntry {
  return {
    eventId: "e1",
    collectionId: "todos",
    type: "insert",
    key: "k1",
    syncStatus: "pending",
    timestamp: Date.now(),
    attemptCount: 0,
    ...overrides,
  };
}

describe("OptimisticStateTracker", () => {
  it("starts empty", () => {
    const tracker = createOptimisticStateTracker();
    expect(tracker.size).toBe(0);
    expect(tracker.isPending("todos", "k1")).toBe(false);
  });

  it("tracks a mutation", () => {
    const tracker = createOptimisticStateTracker();
    tracker.track(makeEntry());

    expect(tracker.size).toBe(1);
    expect(tracker.isPending("todos", "k1")).toBe(true);
    expect(tracker.isPending("todos", "k2")).toBe(false);
  });

  it("confirms a mutation and removes it", () => {
    const tracker = createOptimisticStateTracker();
    tracker.track(makeEntry());
    tracker.confirm("e1");

    expect(tracker.size).toBe(0);
    expect(tracker.isPending("todos", "k1")).toBe(false);
  });

  it("removes a mutation", () => {
    const tracker = createOptimisticStateTracker();
    tracker.track(makeEntry());
    tracker.remove("e1");

    expect(tracker.size).toBe(0);
    expect(tracker.isPending("todos", "k1")).toBe(false);
  });

  it("updates sync status", () => {
    const tracker = createOptimisticStateTracker();
    tracker.track(makeEntry());
    tracker.updateStatus("e1", "failed", 3);

    const entries = tracker.getEntriesForKey("todos", "k1");
    expect(entries[0]!.syncStatus).toBe("failed");
    expect(entries[0]!.attemptCount).toBe(3);
  });

  it("returns pending keys for a collection", () => {
    const tracker = createOptimisticStateTracker();
    tracker.track(makeEntry({ eventId: "e1", key: "k1" }));
    tracker.track(makeEntry({ eventId: "e2", key: "k2" }));
    tracker.track(makeEntry({ eventId: "e3", key: "k1" })); // second event for k1

    const keys = tracker.getPendingKeys("todos");
    expect(keys).toEqual(new Set(["k1", "k2"]));
  });

  it("returns empty set for unknown collection", () => {
    const tracker = createOptimisticStateTracker();
    expect(tracker.getPendingKeys("unknown")).toEqual(new Set());
  });

  it("provides a summary", () => {
    const tracker = createOptimisticStateTracker();
    tracker.track(makeEntry({ eventId: "e1", key: "k1", syncStatus: "pending", attemptCount: 0 }));
    tracker.track(makeEntry({ eventId: "e2", key: "k2", syncStatus: "failed", attemptCount: 3 }));
    tracker.track(makeEntry({ eventId: "e3", key: "k3", syncStatus: "pending", attemptCount: 1 }));

    const summary = tracker.summary();
    expect(summary.pendingCount).toBe(3);
    expect(summary.failedCount).toBe(1);
    expect(summary.syncingCount).toBe(1); // e3 has attemptCount > 0
    expect(summary.collections["todos"]!.count).toBe(3);
  });

  it("notifies subscribers on track", () => {
    const tracker = createOptimisticStateTracker();
    let notified = 0;
    tracker.subscribe(() => notified++);

    tracker.track(makeEntry());
    expect(notified).toBe(1);
  });

  it("notifies subscribers on confirm", () => {
    const tracker = createOptimisticStateTracker();
    tracker.track(makeEntry());

    let notified = 0;
    tracker.subscribe(() => notified++);
    tracker.confirm("e1");
    expect(notified).toBe(1);
  });

  it("unsubscribes correctly", () => {
    const tracker = createOptimisticStateTracker();
    let notified = 0;
    const unsub = tracker.subscribe(() => notified++);

    tracker.track(makeEntry());
    expect(notified).toBe(1);

    unsub();
    tracker.track(makeEntry({ eventId: "e2" }));
    expect(notified).toBe(1); // no additional notification
  });

  it("bulk loads from outbox", () => {
    const tracker = createOptimisticStateTracker();
    tracker.loadFromOutbox([
      makeEntry({ eventId: "e1", key: "k1" }),
      makeEntry({ eventId: "e2", key: "k2" }),
    ]);

    expect(tracker.size).toBe(2);
    expect(tracker.isPending("todos", "k1")).toBe(true);
    expect(tracker.isPending("todos", "k2")).toBe(true);
  });

  it("clears all state", () => {
    const tracker = createOptimisticStateTracker();
    tracker.track(makeEntry());
    tracker.clear();

    expect(tracker.size).toBe(0);
    expect(tracker.isPending("todos", "k1")).toBe(false);
  });

  it("handles multiple events for the same key", () => {
    const tracker = createOptimisticStateTracker();
    tracker.track(makeEntry({ eventId: "e1", key: "k1" }));
    tracker.track(makeEntry({ eventId: "e2", key: "k1" }));

    expect(tracker.isPending("todos", "k1")).toBe(true);
    expect(tracker.getEntriesForKey("todos", "k1")).toHaveLength(2);

    // Confirm one — still pending because second event remains
    tracker.confirm("e1");
    expect(tracker.isPending("todos", "k1")).toBe(true);
    expect(tracker.getEntriesForKey("todos", "k1")).toHaveLength(1);

    // Confirm second
    tracker.confirm("e2");
    expect(tracker.isPending("todos", "k1")).toBe(false);
  });
});
