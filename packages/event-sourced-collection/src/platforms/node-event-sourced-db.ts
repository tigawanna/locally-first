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
import { createNodePlatform } from "./node";

type CollectionDefConstraint = {
  getKey: (state: never) => string | number;
};

export type NodeEventSourcedModules = {
  createNodeSQLitePersistence: (options: { database: never }) => PersistedCollectionPersistence;
  /** Already-opened better-sqlite3 (or compatible) database. */
  database: unknown;
  createCollection: InjectedCreateCollection;
  persistedCollectionOptions: InjectedModuleFn;
};

export type NodeEventSourcedDBConfig<TDefs extends Record<string, CollectionDefConstraint>> =
  EventSourcedSharedOptions & {
    collections: TDefs;
    /**
     * Node persistence modules. Pass the imported bindings directly, or a
     * function if you still want to delay loading `better-sqlite3`.
     */
    modules: ModulesInput<NodeEventSourcedModules>;
  };

/**
 * Node-flavoured event-sourced DB: better-sqlite3 persistence, returned as a
 * lazy singleton.
 *
 * @example
 * ```ts
 * import { createCollection } from "@tanstack/db"
 * import {
 *   createNodeSQLitePersistence,
 *   persistedCollectionOptions,
 * } from "@tanstack/node-db-sqlite-persistence"
 * import Database from "better-sqlite3"
 * import { createNodeEventSourcedDB } from "event-sourced-collection/node"
 *
 * type Todo = { id: string; title: string }
 *
 * const sqlite = new Database("app.sqlite")
 * const { ensureDb, db, close } = createNodeEventSourcedDB({
 *   collections: { todos: { getKey: (todo: Todo) => todo.id } },
 *   sync: { push: "http://localhost:3000/api/sync/events", pull: "http://localhost:3000/api/sync/events" },
 *   modules: {
 *     database: sqlite,
 *     createNodeSQLitePersistence,
 *     createCollection,
 *     persistedCollectionOptions,
 *   },
 * })
 *
 * await ensureDb()
 * await db.collections.todos.insert({ id: "t1", title: "Hi" }).isPersisted.promise
 * await db.sync()
 * await close()
 * ```
 */
export function createNodeEventSourcedDB<
  const TDefs extends Record<string, CollectionDefConstraint>,
>(config: NodeEventSourcedDBConfig<TDefs>) {
  return createEventSourcedDBHandle<TDefs>({
    setup: async () => {
      const modules = await resolveModules(config.modules);
      const platform = createNodePlatform(
        { createNodeSQLitePersistence: modules.createNodeSQLitePersistence },
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
        close: () => {
          const database = modules.database as { close?: () => void };
          database.close?.();
        },
      };
    },
  });
}
