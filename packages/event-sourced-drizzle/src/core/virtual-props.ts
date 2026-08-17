/**
 * Virtual properties enrichment for domain rows.
 *
 * TanStack DB adds virtual properties ($synced, $origin, $key, $collectionId)
 * to every row in memory. In Event Sourced Drizzle, rows come from SQL queries
 * and are not held in memory by the engine. This module provides utilities to
 * enrich rows with sync state metadata by checking the outbox.
 *
 * Usage:
 * ```ts
 * import { createVirtualPropsEnricher } from "event-sourced-drizzle"
 *
 * const enrich = createVirtualPropsEnricher(engine)
 * const rows = await db.select().from(todos)
 * const enriched = await enrich("todos", rows)
 * // enriched[0].$synced === true | false
 * ```
 */

import type { OutboxSyncStatus } from "../internal/types";

// --- Types ---

/** The origin of the most recent mutation for this row. */
export type VirtualOrigin = "local" | "remote";

/**
 * Virtual properties computed from sync state.
 * These are added to domain rows by the enricher and are not persisted.
 */
export type VirtualRowProps<TKey extends string | number = string | number> = {
  /** Whether this row has been fully synced (no pending outbox events). */
  $synced: boolean;
  /** Whether the most recent mutation originated locally or from a remote client. */
  $origin: VirtualOrigin;
  /** The primary key of this row (for convenience). */
  $key: TKey;
  /** The collection this row belongs to. */
  $collectionId: string;
  /** Current sync status of the most recent local mutation, if any. */
  $syncStatus: OutboxSyncStatus | null;
  /** Number of pending outbox events for this key. */
  $pendingCount: number;
};

/**
 * A domain row enriched with virtual properties.
 */
export type WithVirtualProps<T extends object, TKey extends string | number = string | number> = T &
  VirtualRowProps<TKey>;

// --- Outbox State Provider ---

/**
 * Minimal interface for querying outbox state needed by the enricher.
 * Adapters or the engine itself can satisfy this.
 */
export type OutboxStateProvider = {
  /**
   * Returns pending outbox entries for a given collection + set of keys.
   * Grouped by key for efficient enrichment.
   */
  queryPendingByKeys: (
    collectionId: string,
    keys: ReadonlyArray<string | number>,
  ) => Promise<
    Map<string, { syncStatus: OutboxSyncStatus; count: number; latestOrigin: VirtualOrigin }>
  >;
};

// --- Enricher ---

/**
 * Configuration for creating a virtual properties enricher.
 */
export type VirtualPropsEnricherConfig = {
  /** Source of outbox state. */
  outboxState: OutboxStateProvider;
};

/**
 * A function that enriches a batch of domain rows with virtual properties.
 */
export type EnrichFn = <T extends object, TKey extends string | number = string>(
  collectionId: string,
  rows: ReadonlyArray<T>,
  getKey: (row: T) => TKey,
) => Promise<Array<WithVirtualProps<T, TKey>>>;

/**
 * Creates an enricher function that adds virtual properties to domain rows.
 *
 * The enricher queries the outbox to determine sync status for each row,
 * then attaches computed `$synced`, `$origin`, `$key`, `$collectionId`,
 * `$syncStatus`, and `$pendingCount` properties.
 *
 * @example
 * ```ts
 * import { createVirtualPropsEnricher } from "event-sourced-drizzle"
 *
 * const enrich = createVirtualPropsEnricher({ outboxState: adapter })
 * const rows = await db.select().from(todos)
 * const enriched = await enrich("todos", rows, (row) => row.id)
 * console.log(enriched[0].$synced, enriched[0].$pendingCount)
 * ```
 */
export function createVirtualPropsEnricher(config: VirtualPropsEnricherConfig): EnrichFn {
  const { outboxState } = config;

  return async function enrich<T extends object, TKey extends string | number = string>(
    collectionId: string,
    rows: ReadonlyArray<T>,
    getKey: (row: T) => TKey,
  ): Promise<Array<WithVirtualProps<T, TKey>>> {
    if (rows.length === 0) return [];

    // Extract keys for batch lookup
    const keys = rows.map(getKey);

    // Query outbox for pending state
    const pendingMap = await outboxState.queryPendingByKeys(collectionId, keys);

    // Enrich each row
    return rows.map((row, i) => {
      const key = keys[i]!;
      const pending = pendingMap.get(String(key));

      const virtualProps: VirtualRowProps<TKey> = {
        $synced: !pending || pending.count === 0,
        $origin: pending ? pending.latestOrigin : "remote",
        $key: key,
        $collectionId: collectionId,
        $syncStatus: pending?.syncStatus ?? null,
        $pendingCount: pending?.count ?? 0,
      };

      return { ...row, ...virtualProps } as WithVirtualProps<T, TKey>;
    });
  };
}

/**
 * Strips virtual properties from an enriched row.
 * Useful when passing rows to APIs that don't expect the extra fields.
 *
 * @example
 * ```ts
 * import { stripVirtualProps } from "event-sourced-drizzle"
 *
 * const [todo] = await enrich("todos", rows, (row) => row.id)
 * const payload = stripVirtualProps(todo)
 * await fetch("/api/todos", { method: "POST", body: JSON.stringify(payload) })
 * ```
 */
export function stripVirtualProps<T extends object>(row: WithVirtualProps<T, any>): T {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const { $synced, $origin, $key, $collectionId, $syncStatus, $pendingCount, ...rest } =
    row as Record<string, unknown>;
  return rest as T;
}

/**
 * Type guard for rows that already have virtual properties attached.
 *
 * @example
 * ```ts
 * import { hasVirtualProps } from "event-sourced-drizzle"
 *
 * if (hasVirtualProps(row)) {
 *   console.log(row.$synced, row.$pendingCount)
 * }
 * ```
 */
export function hasVirtualProps<T extends object>(
  row: T,
): row is WithVirtualProps<T, string | number> {
  return "$synced" in row && "$origin" in row && "$key" in row && "$collectionId" in row;
}
