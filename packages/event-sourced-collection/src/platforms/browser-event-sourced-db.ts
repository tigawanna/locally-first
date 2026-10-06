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
import type { EventSourcedSharedOptions, SyncLock } from "../core/types";
import { createBrowserPlatform } from "./browser";
import type { BrowserPlatformDeps } from "./browser-types";
import { createWebLocksSyncLock } from "./web-locks";

type CollectionDefConstraint = {
  getKey: (state: never) => string | number;
};

export type BrowserEventSourcedModules = BrowserPlatformDeps & {
  createCollection: InjectedCreateCollection;
  persistedCollectionOptions: InjectedModuleFn;
};

/**
 * Browser entry-point config. Shared sync/lifecycle fields come from
 * {@link EventSourcedSharedOptions} (hover those properties for docs).
 * Deep guide: `docs/usage.md`.
 */
export type BrowserEventSourcedDBConfig<TDefs extends Record<string, CollectionDefConstraint>> =
  Omit<EventSourcedSharedOptions, "lock" | "lockName"> & {
    /**
     * User collection registry. Object keys become `db.collections.<key>` and
     * the wire `collectionId`. Must not collide with reserved names.
     */
    collections: TDefs;
    /**
     * OPFS SQLite database file name (e.g. `"my-app.sqlite"`). Also used as the
     * default sync lock namespace so only one tab syncs for this DB at a time.
     */
    databaseName: string;
    /**
     * Name for the cross-tab collection coordinator database. Defaults to
     * `databaseName` with a trailing `.sqlite` stripped.
     */
    coordinatorDbName?: string;
    /**
     * Browser OPFS persistence modules from
     * `@tanstack/browser-db-sqlite-persistence` plus `createCollection`.
     * Pass the imported bindings directly, or a function to keep WASM off the
     * critical path.
     */
    modules: ModulesInput<BrowserEventSourcedModules>;
    /**
     * Defaults to a Web Locks–backed lock so only one tab syncs at a time.
     * Pass `null` to opt out and let every tab sync independently.
     */
    lock?: SyncLock | null;
  };

export type { EventSourcedDBHandle as BrowserEventSourcedDBHandle } from "../core/create-event-sourced-db-handle";

/**
 * Browser-flavoured event-sourced DB: OPFS SQLite + optional Web Locks sync
 * election, returned as a lazy singleton.
 *
 * @param config - See {@link BrowserEventSourcedDBConfig} for every option.
 *
 * @example
 * ```ts
 * import { createBrowserEventSourcedDB } from "event-sourced-collection/browser"
 * import type { CollectionDef, EventSourcedDB } from "event-sourced-collection"
 *
 * type Todo = { id: string; title: string }
 * type Defs = { todos: CollectionDef<Todo, string> }
 *
 * const { ensureDb, db } = createBrowserEventSourcedDB<Defs>({
 *   databaseName: "app.sqlite",
 *   collections: { todos: { getKey: (todo) => todo.id } },
 *   sync: { push: "/api/sync/events", pull: "/api/sync/events" },
 *   modules: async () => {
 *     const { createCollection } = await import("@tanstack/db")
 *     const {
 *       BrowserCollectionCoordinator,
 *       createBrowserWASQLitePersistence,
 *       openBrowserWASQLiteOPFSDatabase,
 *       persistedCollectionOptions,
 *     } = await import("@tanstack/browser-db-sqlite-persistence")
 *     return {
 *       createCollection,
 *       BrowserCollectionCoordinator,
 *       createBrowserWASQLitePersistence,
 *       openBrowserWASQLiteOPFSDatabase,
 *       persistedCollectionOptions,
 *     }
 *   },
 * })
 *
 * export async function start() {
 *   await ensureDb()
 * }
 * export { db }
 * ```
 */
export function createBrowserEventSourcedDB<
  const TDefs extends Record<string, CollectionDefConstraint>,
>(config: BrowserEventSourcedDBConfig<TDefs>) {
  return createEventSourcedDBHandle<TDefs>({
    guard: assertBrowserEnvironment,
    setup: async () => {
      const modules = await resolveModules(config.modules);

      const platform = await createBrowserPlatform(
        {
          openBrowserWASQLiteOPFSDatabase: modules.openBrowserWASQLiteOPFSDatabase,
          createBrowserWASQLitePersistence: modules.createBrowserWASQLitePersistence,
          BrowserCollectionCoordinator: modules.BrowserCollectionCoordinator,
        },
        {
          databaseName: config.databaseName,
          coordinatorDbName: config.coordinatorDbName,
        },
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
        lock: config.lock === null ? undefined : (config.lock ?? createWebLocksSyncLock()),
        lockName: config.databaseName,
        close: platform.close,
      };
    },
  });
}

function assertBrowserEnvironment(): void {
  if (typeof window === "undefined") {
    throw new Error("Event-sourced DB (browser) is only available in a browser environment.");
  }
}
