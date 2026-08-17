/**
 * Ambient transaction support for Event Sourced Drizzle.
 *
 * Allows grouping multiple mutations into a single atomic unit where:
 * - All domain writes happen in one DB transaction
 * - All outbox entries share the same txId
 * - The server commits/rolls back the entire group atomically
 *
 * Inspired by TanStack DB's transaction system which uses a stack-based
 * ambient transaction pattern.
 *
 * @example
 * ```ts
 * import { createTransaction, getActiveTransaction } from "event-sourced-drizzle"
 *
 * // Group multiple mutations atomically
 * await engine.transact(async (tx) => {
 *   await engine.mutate.insert("todos", { id: "1", text: "Buy milk" })
 *   await engine.mutate.insert("todos", { id: "2", text: "Walk dog" })
 *   await engine.mutate.update("projects", "proj-1", { todoCount: 2 })
 * })
 * // All 3 mutations share a txId, committed in one DB transaction.
 * ```
 */

import type { DrizzleAdapter, OutboxRow } from "../internal/types";
import type { EmitHook } from "../internal/hooks";
import type { EventSourcedLogger } from "../utils/logger";
import type { OptimisticStateTracker } from "./optimistic-state";
import { generateEventId } from "../utils/uuid";

// --- Transaction State ---

export type TransactionState = "pending" | "committing" | "committed" | "failed";

export type TransactionEntry = {
  outboxRow: OutboxRow;
  domainOp: () => Promise<void>;
};

/**
 * Represents an ambient transaction context.
 * Mutations executed while this transaction is active will join it
 * rather than creating their own isolated transactions.
 */
export interface Transaction {
  /** Unique transaction ID shared by all mutations in this group. */
  readonly id: string;
  /** Current state of the transaction. */
  readonly state: TransactionState;
  /** Number of mutations accumulated so far. */
  readonly size: number;
}

// Internal transaction implementation
class TransactionImpl implements Transaction {
  public state: TransactionState = "pending";
  public entries: TransactionEntry[] = [];

  constructor(public readonly id: string) {}

  get size(): number {
    return this.entries.length;
  }

  addEntry(entry: TransactionEntry): void {
    this.entries.push(entry);
  }
}

// --- Transaction Stack ---

/**
 * Module-level transaction stack. The top of the stack is the "active"
 * ambient transaction that mutations will join.
 */
let transactionStack: TransactionImpl[] = [];

/**
 * Returns the currently active ambient transaction, or `undefined` if none.
 * Mutations called inside `engine.transact().run(...)` join this transaction.
 *
 * @example
 * ```ts
 * import { getActiveTransaction } from "event-sourced-drizzle"
 *
 * const { run } = engine.transact()
 * await run(async () => {
 *   const tx = getActiveTransaction()
 *   console.log(tx?.txId, tx?.state)
 *   await engine.mutate.insert("todos", { id: "1", title: "Buy milk", done: false })
 * })
 * ```
 */
export function getActiveTransaction(): Transaction | undefined {
  return transactionStack.length > 0 ? transactionStack[transactionStack.length - 1] : undefined;
}

/** @internal — used by the mutate API to add entries to the active transaction. */
export function getActiveTransactionImpl(): TransactionImpl | undefined {
  return transactionStack.length > 0 ? transactionStack[transactionStack.length - 1] : undefined;
}

function pushTransaction(tx: TransactionImpl): void {
  transactionStack.push(tx);
}

function popTransaction(tx: TransactionImpl): void {
  const idx = transactionStack.indexOf(tx);
  if (idx !== -1) {
    transactionStack.splice(idx, 1);
  }
}

// --- Public API ---

export type TransactOptions = {
  /** Custom transaction ID. Auto-generated if not provided. */
  txId?: string;
};

export type TransactFn = (options?: TransactOptions) => {
  txId: string;
  /**
   * Execute a callback within this transaction context.
   * All mutations called inside `run` join this transaction.
   */
  run: <T>(callback: () => Promise<T>) => Promise<T>;
};

/**
 * Creates the `transact` function bound to an engine's adapter and hooks.
 * This is called internally by `createEventSourcedDrizzle`.
 *
 * @internal
 */
export function createTransact(deps: {
  adapter: DrizzleAdapter;
  emit: EmitHook;
  log: EventSourcedLogger;
  optimistic?: OptimisticStateTracker;
}): TransactFn {
  const { adapter, emit, log, optimistic } = deps;

  return function transact(options?: TransactOptions) {
    const txId = options?.txId ?? generateEventId();

    return {
      txId,
      run: async <T>(callback: () => Promise<T>): Promise<T> => {
        const tx = new TransactionImpl(txId);
        pushTransaction(tx);

        try {
          // Execute the user callback — mutations called inside will detect
          // the active transaction and add entries to `tx.entries` instead of
          // committing immediately.
          const result = await callback();

          // Now commit all accumulated entries in a single DB transaction.
          if (tx.entries.length > 0) {
            tx.state = "committing";

            await adapter.transaction(async () => {
              for (const entry of tx.entries) {
                await entry.domainOp();
                await adapter.insertOutbox(entry.outboxRow);
              }
            });

            tx.state = "committed";

            // Emit mutation hooks for each entry after successful commit.
            for (const entry of tx.entries) {
              emit("onMutation", entry.outboxRow);
              optimistic?.track({
                eventId: entry.outboxRow.eventId,
                collectionId: entry.outboxRow.collectionId,
                type: entry.outboxRow.type,
                key: entry.outboxRow.key,
                syncStatus: "pending",
                timestamp: entry.outboxRow.timestamp,
                attemptCount: 0,
              });
            }

            log.debug("transact committed", { txId, mutations: tx.entries.length });
          } else {
            tx.state = "committed";
            log.debug("transact committed (empty)", { txId });
          }

          return result;
        } catch (err) {
          tx.state = "failed";
          log.error("transact failed", {
            txId,
            message: err instanceof Error ? err.message : String(err),
          });
          throw err;
        } finally {
          popTransaction(tx);
        }
      },
    };
  };
}
