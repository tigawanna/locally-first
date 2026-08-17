import { describe, expect, it } from "vitest";
import {
  EventSourcedDrizzleError,
  SchemaValidationError,
  UnknownCollectionError,
  DuplicateKeyError,
  KeyNotFoundError,
  EventConflictError,
  RetryExhaustedError,
  ReplayHaltedError,
  TransactionStateError,
  BackendMismatchError,
} from "../../core/errors";

describe("error types", () => {
  it("EventSourcedDrizzleError is instanceof Error", () => {
    const err = new EventSourcedDrizzleError("test");
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe("EventSourcedDrizzleError");
  });

  it("SchemaValidationError includes operation and issues", () => {
    const err = new SchemaValidationError("insert", [{ message: "required", path: ["title"] }]);
    expect(err).toBeInstanceOf(EventSourcedDrizzleError);
    expect(err.operation).toBe("insert");
    expect(err.issues).toHaveLength(1);
    expect(err.message).toContain("insert validation failed");
  });

  it("UnknownCollectionError includes collectionId", () => {
    const err = new UnknownCollectionError("widgets");
    expect(err.collectionId).toBe("widgets");
    expect(err.message).toContain("widgets");
  });

  it("DuplicateKeyError includes collectionId and key", () => {
    const err = new DuplicateKeyError("todos", "t1");
    expect(err.collectionId).toBe("todos");
    expect(err.key).toBe("t1");
  });

  it("KeyNotFoundError includes operation", () => {
    const err = new KeyNotFoundError("todos", "t1", "update");
    expect(err.operation).toBe("update");
    expect(err.message).toContain("update");
  });

  it("EventConflictError includes all context", () => {
    const err = new EventConflictError("e1", "todos", "t1", "row changed");
    expect(err.eventId).toBe("e1");
    expect(err.collectionId).toBe("todos");
    expect(err.key).toBe("t1");
    expect(err.serverMessage).toBe("row changed");
  });

  it("RetryExhaustedError includes attempt count", () => {
    const err = new RetryExhaustedError("e1", 8, "timeout");
    expect(err.eventId).toBe("e1");
    expect(err.attemptCount).toBe(8);
    expect(err.lastError).toBe("timeout");
    expect(err.message).toContain("8 retries");
  });

  it("ReplayHaltedError includes reason", () => {
    const err = new ReplayHaltedError("e1", "unknown collection");
    expect(err.eventId).toBe("e1");
    expect(err.reason).toBe("unknown collection");
  });

  it("TransactionStateError includes expected and actual", () => {
    const err = new TransactionStateError("pending", "committed");
    expect(err.expected).toBe("pending");
    expect(err.actual).toBe("committed");
  });

  it("BackendMismatchError includes expected and received", () => {
    const err = new BackendMismatchError("abc", "xyz");
    expect(err.expected).toBe("abc");
    expect(err.received).toBe("xyz");
    expect(err.message).toContain("abc");
    expect(err.message).toContain("xyz");
  });
});
