import { describe, expect, it } from "vitest";
import { createHookEmitter } from "../../internal/hooks";
import { createEventSourcedLogger } from "../../utils/logger";

describe("createHookEmitter", () => {
  const log = createEventSourcedLogger(false);

  it("calls the registered hook with the argument", () => {
    const calls: unknown[] = [];
    const emit = createHookEmitter({ onReady: (ctx) => calls.push(ctx) }, log);

    emit("onReady", { clientId: "c1", pullCursor: 0 });
    expect(calls).toEqual([{ clientId: "c1", pullCursor: 0 }]);
  });

  it("does nothing when no hook is registered", () => {
    const emit = createHookEmitter({}, log);
    // Should not throw
    emit("onReady", { clientId: "c1", pullCursor: 0 });
  });

  it("does nothing when hooks config is undefined", () => {
    const emit = createHookEmitter(undefined, log);
    emit("onReady", { clientId: "c1", pullCursor: 0 });
  });

  it("swallows errors thrown by hooks", () => {
    const emit = createHookEmitter(
      {
        onReady: () => {
          throw new Error("hook error");
        },
      },
      log,
    );

    // Should not throw
    emit("onReady", { clientId: "c1", pullCursor: 0 });
  });

  it("calls onMutation hook", () => {
    const mutations: unknown[] = [];
    const emit = createHookEmitter({ onMutation: (entry) => mutations.push(entry) }, log);

    const fakeOutbox = { eventId: "e1", collectionId: "todos", type: "insert" as const };
    emit("onMutation", fakeOutbox);
    expect(mutations).toHaveLength(1);
    expect(mutations[0]).toBe(fakeOutbox);
  });
});
