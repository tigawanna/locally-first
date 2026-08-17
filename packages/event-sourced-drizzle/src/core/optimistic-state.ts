/**
 * Optimistic state tracker for Event Sourced Drizzle.
 *
 * Since Drizzle reads from SQL directly (no in-memory store), this module
 * provides an in-memory overlay that tracks which rows have pending local
 * mutations. Consumers can use this to:
 *
 * - Show pending indicators in the UI
 * - Know which rows are awaiting server confirmation
 * - Detect conflicts between local and remote state
 *
 * The tracker is updated automatically by the engine on mutate and sync.
 *
 * @example
 * ```ts
 * const tracker = engine.optimistic
 *
 * // Check if a specific row has pending changes
 * tracker.isPending("todos", "todo-1") // true if outbox has unsynced events
 *
 * // Get all pending keys for a collection
 * tracker.getPendingKeys("todos") // Set { "todo-1", "todo-3" }
 *
 * // Subscribe to optimistic state changes
 * tracker.subscribe(() => {
 *   console.log("Optimistic state changed", tracker.summary())
 * })
 * ```
 */

import type { MutationType, OutboxSyncStatus } from "../internal/types";

// --- Types ---

/** Represents a single pending optimistic mutation. */
export type OptimisticEntry = {
  eventId: string;
  collectionId: string;
  type: MutationType;
  key: string;
  syncStatus: OutboxSyncStatus;
  timestamp: number;
  attemptCount: number;
};

/** Summary of optimistic state across all collections. */
export type OptimisticSummary = {
  /** Total number of pending (unsynced) mutations. */
  pendingCount: number;
  /** Number of mutations currently being synced. */
  syncingCount: number;
  /** Number of mutations that have failed at least once. */
  failedCount: number;
  /** Per-collection breakdown of pending keys. */
  collections: Record<string, { pendingKeys: Set<string>; count: number }>;
};

export type OptimisticListener = () => void;

// --- Tracker ---

/**
 * In-memory tracker for optimistic (pending sync) mutations.
 * Provides a fast lookup layer over the outbox state.
 * The engine exposes this as `engine.optimistic`.
 *
 * @example
 * ```ts
 * const tracker = engine.optimistic
 *
 * tracker.isPending("todos", "todo-1")
 * tracker.getPendingKeys("todos")
 *
 * const unsubscribe = tracker.subscribe(() => {
 *   console.log(tracker.summary())
 * })
 * ```
 */
export class OptimisticStateTracker {
  /** eventId → entry */
  private entries = new Map<string, OptimisticEntry>();
  /** collectionId → Set<key> for fast isPending checks */
  private pendingByCollection = new Map<string, Map<string, Set<string>>>();
  private listeners = new Set<OptimisticListener>();

  // --- Mutations ---

  /**
   * Record a new optimistic mutation (called by mutate API after outbox insert).
   */
  track(entry: OptimisticEntry): void {
    this.entries.set(entry.eventId, entry);
    this.addToIndex(entry.collectionId, entry.key, entry.eventId);
    this.notify();
  }

  /**
   * Mark a mutation as synced and remove from optimistic state.
   * Called when push confirms the event.
   */
  confirm(eventId: string): void {
    const entry = this.entries.get(eventId);
    if (!entry) return;

    this.entries.delete(eventId);
    this.removeFromIndex(entry.collectionId, entry.key, eventId);
    this.notify();
  }

  /**
   * Update the sync status of a pending mutation (e.g. on retry).
   */
  updateStatus(eventId: string, syncStatus: OutboxSyncStatus, attemptCount?: number): void {
    const entry = this.entries.get(eventId);
    if (!entry) return;

    entry.syncStatus = syncStatus;
    if (attemptCount !== undefined) {
      entry.attemptCount = attemptCount;
    }
    this.notify();
  }

  /**
   * Remove a mutation from tracking (e.g. dead-lettered or manually cleared).
   */
  remove(eventId: string): void {
    const entry = this.entries.get(eventId);
    if (!entry) return;

    this.entries.delete(eventId);
    this.removeFromIndex(entry.collectionId, entry.key, eventId);
    this.notify();
  }

  /**
   * Bulk-load initial state from outbox on engine startup.
   */
  loadFromOutbox(entries: ReadonlyArray<OptimisticEntry>): void {
    for (const entry of entries) {
      this.entries.set(entry.eventId, entry);
      this.addToIndex(entry.collectionId, entry.key, entry.eventId);
    }
    if (entries.length > 0) this.notify();
  }

  /**
   * Clear all tracked state (e.g. on dispose or reset).
   */
  clear(): void {
    this.entries.clear();
    this.pendingByCollection.clear();
    this.notify();
  }

  // --- Queries ---

  /**
   * Check whether a specific row has any pending unsynced mutations.
   */
  isPending(collectionId: string, key: string | number): boolean {
    const keys = this.pendingByCollection.get(collectionId);
    if (!keys) return false;
    const eventIds = keys.get(String(key));
    return !!eventIds && eventIds.size > 0;
  }

  /**
   * Get all keys with pending mutations for a collection.
   */
  getPendingKeys(collectionId: string): Set<string> {
    const keys = this.pendingByCollection.get(collectionId);
    if (!keys) return new Set();

    const result = new Set<string>();
    for (const [key, eventIds] of keys) {
      if (eventIds.size > 0) result.add(key);
    }
    return result;
  }

  /**
   * Get all pending entries for a specific key.
   */
  getEntriesForKey(collectionId: string, key: string | number): OptimisticEntry[] {
    const keys = this.pendingByCollection.get(collectionId);
    if (!keys) return [];

    const eventIds = keys.get(String(key));
    if (!eventIds) return [];

    const result: OptimisticEntry[] = [];
    for (const eventId of eventIds) {
      const entry = this.entries.get(eventId);
      if (entry) result.push(entry);
    }
    return result;
  }

  /**
   * Get a summary of the current optimistic state.
   */
  summary(): OptimisticSummary {
    let pendingCount = 0;
    let syncingCount = 0;
    let failedCount = 0;
    const collections: Record<string, { pendingKeys: Set<string>; count: number }> = {};

    for (const entry of this.entries.values()) {
      pendingCount++;
      if (entry.syncStatus === "failed") failedCount++;
      // "pending" with attemptCount > 0 means it's been attempted — treat as "syncing"
      if (entry.syncStatus === "pending" && entry.attemptCount > 0) syncingCount++;

      if (!collections[entry.collectionId]) {
        collections[entry.collectionId] = { pendingKeys: new Set(), count: 0 };
      }
      collections[entry.collectionId]!.pendingKeys.add(entry.key);
      collections[entry.collectionId]!.count++;
    }

    return { pendingCount, syncingCount, failedCount, collections };
  }

  /** Total number of pending mutations. */
  get size(): number {
    return this.entries.size;
  }

  // --- Subscriptions ---

  /**
   * Subscribe to optimistic state changes. Returns an unsubscribe function.
   */
  subscribe(listener: OptimisticListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  // --- Internal ---

  private addToIndex(collectionId: string, key: string, eventId: string): void {
    let keys = this.pendingByCollection.get(collectionId);
    if (!keys) {
      keys = new Map();
      this.pendingByCollection.set(collectionId, keys);
    }
    let eventIds = keys.get(key);
    if (!eventIds) {
      eventIds = new Set();
      keys.set(key, eventIds);
    }
    eventIds.add(eventId);
  }

  private removeFromIndex(collectionId: string, key: string, eventId: string): void {
    const keys = this.pendingByCollection.get(collectionId);
    if (!keys) return;
    const eventIds = keys.get(key);
    if (!eventIds) return;
    eventIds.delete(eventId);
    if (eventIds.size === 0) keys.delete(key);
    if (keys.size === 0) this.pendingByCollection.delete(collectionId);
  }

  private notify(): void {
    for (const listener of this.listeners) {
      try {
        listener();
      } catch {
        // Swallow listener errors — fire and forget.
      }
    }
  }
}

/**
 * Creates a new {@link OptimisticStateTracker}. Prefer `engine.optimistic`
 * unless you are wiring a standalone overlay.
 *
 * @example
 * ```ts
 * import { createOptimisticStateTracker } from "event-sourced-drizzle"
 *
 * const tracker = createOptimisticStateTracker()
 * tracker.track({
 *   eventId: "evt-1",
 *   collectionId: "todos",
 *   type: "insert",
 *   key: "todo-1",
 *   syncStatus: "pending",
 *   timestamp: Date.now(),
 *   attemptCount: 0,
 * })
 * tracker.isPending("todos", "todo-1") // true
 * ```
 */
export function createOptimisticStateTracker(): OptimisticStateTracker {
  return new OptimisticStateTracker();
}
