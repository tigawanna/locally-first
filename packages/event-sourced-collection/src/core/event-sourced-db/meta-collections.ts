import type { Collection } from "@tanstack/db";

import type { EventSourcedLogger } from "../../utils/logger";
import type { EventSourcedDBConfig } from "../types";

type CollectionDefConstraint = {
  getKey: (state: never) => string | number;
  schemaVersion?: number;
  localOnly?: boolean;
  indexes?: ReadonlyArray<{
    select: (row: never) => unknown;
    name?: string;
    indexType?: new (id: number, expression: never, name?: string, options?: never) => unknown;
  }>;
};

export type MetaCollectionFactory = Pick<
  EventSourcedDBConfig<Record<string, CollectionDefConstraint>>,
  "createCollection" | "persistedCollectionOptions" | "persistence"
>;

type IndexableCollection = {
  createIndex: (
    indexCallback: (row: Record<string, unknown>) => unknown,
    config?: {
      name?: string;
      indexType?: new (id: number, expression: never, name?: string, options?: never) => unknown;
    },
  ) => unknown;
  getIndexMetadata?: () => ReadonlyArray<unknown>;
  on?: (event: "status:ready", callback: () => void) => () => void;
};

/**
 * Builds a meta collection: a built-in collection the library owns (outbox,
 * inbox, dead letter, sync meta, row versions), not a user domain collection.
 * It is created and persisted the same way as a user collection.
 */
export function createMetaCollection<TEntry extends object>(
  config: MetaCollectionFactory,
  id: string,
  getKey: (entry: TEntry) => string,
  schemaVersion: number,
): Collection<TEntry, string> {
  const options = config.persistedCollectionOptions<TEntry, string>({
    id,
    getKey,
    persistence: config.persistence,
    schemaVersion,
  });

  return config.createCollection(options);
}

/** Registers a collection's configured indexes, and registers them again once the collection reports ready. */
export function applyCollectionIndexes(
  collection: Collection<Record<string, unknown>, string | number>,
  collectionId: string,
  indexes: CollectionDefConstraint["indexes"],
  log: EventSourcedLogger,
): void {
  if (!indexes?.length) return;

  const indexable = collection as IndexableCollection;

  const register = (): void => {
    if ((indexable.getIndexMetadata?.().length ?? 0) > 0) return;

    for (const indexDef of indexes) {
      indexable.createIndex(indexDef.select as (row: Record<string, unknown>) => unknown, {
        name: indexDef.name,
        indexType: indexDef.indexType,
      });

      log.debug("registered collection index", { collectionId, name: indexDef.name });
    }
  };

  register();

  indexable.on?.("status:ready", register);
}
