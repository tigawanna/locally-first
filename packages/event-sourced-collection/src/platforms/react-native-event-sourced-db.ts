import {
  createEventSourcedDBHandle,
  resolveModules,
  type ModulesInput,
} from "../core/create-event-sourced-db-handle";
import type {
  CreateCollectionFn,
  InjectedCreateCollection,
  InjectedModuleFn,
  PersistedCollectionOptionsFn,
} from "../core/persisted-collection";
import type { EventSourcedSharedOptions, PersistedCollectionPersistence } from "../core/types";
import { createReactNativePlatform } from "./react-native";

type CollectionDefConstraint = {
  getKey: (state: never) => string | number;
};

export type ReactNativeEventSourcedModules = {
  createReactNativeSQLitePersistence: (options: {
    database: never;
  }) => PersistedCollectionPersistence;
  /** Already-opened op-sqlite (or compatible) database. */
  database: unknown;
  createCollection: InjectedCreateCollection;
  persistedCollectionOptions: InjectedModuleFn;
};

/**
 * React Native entry-point config. Shared sync/lifecycle fields come from
 * {@link EventSourcedSharedOptions} (hover those properties for docs).
 * Deep guide: `docs/usage.md`.
 */
export type ReactNativeEventSourcedDBConfig<TDefs extends Record<string, CollectionDefConstraint>> =
  EventSourcedSharedOptions & {
    collections: TDefs;
    /**
     * React Native persistence modules from
     * `@tanstack/react-native-db-sqlite-persistence` plus `createCollection`.
     * Pass the imported bindings directly, or a function to delay native SQLite.
     */
    modules: ModulesInput<ReactNativeEventSourcedModules>;
  };

export type { EventSourcedDBHandle as ReactNativeEventSourcedDBHandle } from "../core/create-event-sourced-db-handle";

/**
 * React Native–flavoured event-sourced DB, returned as a lazy singleton.
 *
 * @param config - See {@link ReactNativeEventSourcedDBConfig} for every option.
 *
 * @example
 * ```ts
 * import { createReactNativeEventSourcedDB } from "event-sourced-collection/react-native"
 *
 * const { ensureDb, db } = createReactNativeEventSourcedDB({
 *   collections: { todos: { getKey: (todo: { id: string }) => todo.id } },
 *   modules: async () => {
 *     const { createCollection } = await import("@tanstack/react-native-db")
 *     const { createReactNativeSQLitePersistence, persistedCollectionOptions } =
 *       await import("@tanstack/react-native-db-sqlite-persistence")
 *     const { openDatabase } = await import("react-native-op-sqlite")
 *     return {
 *       createCollection,
 *       createReactNativeSQLitePersistence,
 *       persistedCollectionOptions,
 *       database: openDatabase({ name: "app.sqlite" }),
 *     }
 *   },
 * })
 *
 * await ensureDb()
 * ```
 */
export function createReactNativeEventSourcedDB<
  const TDefs extends Record<string, CollectionDefConstraint>,
>(config: ReactNativeEventSourcedDBConfig<TDefs>) {
  return createEventSourcedDBHandle<TDefs>({
    setup: async () => {
      const modules = await resolveModules(config.modules);
      const platform = createReactNativePlatform(
        { createReactNativeSQLitePersistence: modules.createReactNativeSQLitePersistence },
        { database: modules.database },
      );

      return {
        persistence: platform.persistence,
        createCollection: modules.createCollection as CreateCollectionFn,
        persistedCollectionOptions:
          modules.persistedCollectionOptions as PersistedCollectionOptionsFn,
        collections: config.collections,
        sync: config.sync,
        syncEnabled: config.syncEnabled,
        schemaVersion: config.schemaVersion,
        debug: config.debug,
        clientId: config.clientId,
        unknownEventHandling: config.unknownEventHandling,
        pullOverlap: config.pullOverlap,
        eventSchemaVersion: config.eventSchemaVersion,
        upcastEvent: config.upcastEvent,
        retry: config.retry,
        pushBatchSize: config.pushBatchSize,
        maxPushBytes: config.maxPushBytes,
        syncPreset: config.syncPreset,
        recordLocalEchoes: config.recordLocalEchoes,
        backendMismatch: config.backendMismatch,
        conflictDetection: config.conflictDetection,
        hooks: config.hooks,
        lock: config.lock,
        lockName: config.lockName,
      };
    },
  });
}
