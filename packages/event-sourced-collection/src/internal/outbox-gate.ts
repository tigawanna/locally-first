import { createSerialQueue } from "./serial-queue";

/**
 * Serializes outbox rewrites (folds, compaction) with the moments a push reads
 * or applies those rows. The network call itself stays outside the queue.
 * Event ids reserved for an in-flight push must not be deleted or rewritten:
 * the request already left with their old payload.
 */
export type OutboxWriteGate = {
  run<T>(fn: () => Promise<T>): Promise<T>;
  reserve(eventIds: readonly string[]): void;
  release(eventIds: readonly string[]): void;
  isReserved(eventId: string): boolean;
};

export function createOutboxWriteGate(): OutboxWriteGate {
  const run = createSerialQueue();
  const reserved = new Set<string>();

  return {
    run,
    reserve(eventIds) {
      for (const eventId of eventIds) reserved.add(eventId);
    },
    release(eventIds) {
      for (const eventId of eventIds) reserved.delete(eventId);
    },
    isReserved(eventId) {
      return reserved.has(eventId);
    },
  };
}
