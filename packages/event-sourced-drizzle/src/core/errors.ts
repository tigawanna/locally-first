/**
 * Root error class for all Event Sourced Drizzle errors.
 * Consumers can catch this to handle any engine error uniformly.
 */
export class EventSourcedDrizzleError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "EventSourcedDrizzleError";
  }
}

// --- Schema / Validation Errors ---

/**
 * Thrown when an event or row fails schema validation.
 */
export class SchemaValidationError extends EventSourcedDrizzleError {
  public readonly operation: "insert" | "update";
  public readonly issues: ReadonlyArray<{
    message: string;
    path?: ReadonlyArray<string | number | symbol>;
  }>;

  constructor(
    operation: "insert" | "update",
    issues: ReadonlyArray<{ message: string; path?: ReadonlyArray<string | number | symbol> }>,
  ) {
    const summary = issues
      .map((i) => `  - ${i.message} (path: ${i.path?.join(".") ?? "root"})`)
      .join("\n");
    super(`${operation} validation failed:\n${summary}`);
    this.name = "SchemaValidationError";
    this.operation = operation;
    this.issues = issues;
  }
}

// --- Collection Errors ---

/**
 * Thrown when a mutation targets a collection that doesn't exist in the registry.
 */
export class UnknownCollectionError extends EventSourcedDrizzleError {
  public readonly collectionId: string;

  constructor(collectionId: string) {
    super(`Unknown collection: "${collectionId}"`);
    this.name = "UnknownCollectionError";
    this.collectionId = collectionId;
  }
}

/**
 * Thrown when an insert targets a key that already exists.
 */
export class DuplicateKeyError extends EventSourcedDrizzleError {
  public readonly collectionId: string;
  public readonly key: string | number;

  constructor(collectionId: string, key: string | number) {
    super(`Cannot insert into "${collectionId}": key "${key}" already exists`);
    this.name = "DuplicateKeyError";
    this.collectionId = collectionId;
    this.key = key;
  }
}

/**
 * Thrown when an update or delete targets a key that doesn't exist.
 */
export class KeyNotFoundError extends EventSourcedDrizzleError {
  public readonly collectionId: string;
  public readonly key: string | number;
  public readonly operation: "update" | "delete";

  constructor(collectionId: string, key: string | number, operation: "update" | "delete") {
    super(`Cannot ${operation} in "${collectionId}": key "${key}" not found`);
    this.name = "KeyNotFoundError";
    this.collectionId = collectionId;
    this.key = key;
    this.operation = operation;
  }
}

// --- Sync / Push Errors ---

/**
 * Thrown when the server rejects an event due to a version conflict (baseVersion mismatch).
 */
export class EventConflictError extends EventSourcedDrizzleError {
  public readonly eventId: string;
  public readonly collectionId: string;
  public readonly key: string;
  public readonly serverMessage: string;

  constructor(eventId: string, collectionId: string, key: string, serverMessage: string) {
    super(`Conflict on "${collectionId}/${key}": ${serverMessage}`);
    this.name = "EventConflictError";
    this.eventId = eventId;
    this.collectionId = collectionId;
    this.key = key;
    this.serverMessage = serverMessage;
  }
}

/**
 * Thrown when a sync push/pull cycle encounters a non-recoverable conflict.
 */
export class SyncConflictError extends EventSourcedDrizzleError {
  public readonly phase: "push" | "pull";
  public readonly eventIds: string[];

  constructor(phase: "push" | "pull", eventIds: string[], message: string) {
    super(`Sync ${phase} conflict: ${message}`);
    this.name = "SyncConflictError";
    this.phase = phase;
    this.eventIds = eventIds;
  }
}

/**
 * Thrown when an outbox event exhausts its retry budget and is dead-lettered.
 */
export class RetryExhaustedError extends EventSourcedDrizzleError {
  public readonly eventId: string;
  public readonly attemptCount: number;
  public readonly lastError: string | null;

  constructor(eventId: string, attemptCount: number, lastError: string | null) {
    super(
      `Event "${eventId}" exhausted ${attemptCount} retries` + (lastError ? `: ${lastError}` : ""),
    );
    this.name = "RetryExhaustedError";
    this.eventId = eventId;
    this.attemptCount = attemptCount;
    this.lastError = lastError;
  }
}

/**
 * Thrown when inbox replay is halted due to an unrecoverable event.
 */
export class ReplayHaltedError extends EventSourcedDrizzleError {
  public readonly eventId: string;
  public readonly reason: string;

  constructor(eventId: string, reason: string) {
    super(`Replay halted at event "${eventId}": ${reason}`);
    this.name = "ReplayHaltedError";
    this.eventId = eventId;
    this.reason = reason;
  }
}

/**
 * Thrown when looking up an event that doesn't exist in outbox or inbox.
 */
export class EventNotFoundError extends EventSourcedDrizzleError {
  public readonly eventId: string;
  public readonly location: "outbox" | "inbox";

  constructor(eventId: string, location: "outbox" | "inbox") {
    super(`Event "${eventId}" not found in ${location}`);
    this.name = "EventNotFoundError";
    this.eventId = eventId;
    this.location = location;
  }
}

// --- Transaction Errors ---

/**
 * Thrown when a transaction operation is attempted on a transaction that is
 * no longer in the expected state (e.g. mutating a committed transaction).
 */
export class TransactionStateError extends EventSourcedDrizzleError {
  public readonly expected: string;
  public readonly actual: string;

  constructor(expected: string, actual: string) {
    super(`Transaction expected state "${expected}" but is "${actual}"`);
    this.name = "TransactionStateError";
    this.expected = expected;
    this.actual = actual;
  }
}

/**
 * Thrown when a mutation is attempted outside of a valid transaction context
 * when one is required.
 */
export class NoActiveTransactionError extends EventSourcedDrizzleError {
  constructor() {
    super("No active transaction — mutations require a transaction context");
    this.name = "NoActiveTransactionError";
  }
}

// --- Backend Errors ---

/**
 * Thrown when the backend reports a different identity than the one this client
 * last synced with, and the configured policy is "fail".
 */
export class BackendMismatchError extends EventSourcedDrizzleError {
  public readonly expected: string | null;
  public readonly received: string;

  constructor(expected: string | null, received: string) {
    super(
      `Backend identity mismatch: expected "${expected ?? "(none)"}", received "${received}". ` +
        `The server database may have been reset or swapped.`,
    );
    this.name = "BackendMismatchError";
    this.expected = expected;
    this.received = received;
  }
}
