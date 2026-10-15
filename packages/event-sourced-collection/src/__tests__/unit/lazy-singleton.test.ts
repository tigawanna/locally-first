import { describe, expect, it } from "vite-plus/test";

import { createLazySingleton } from "../../core/lazy-singleton";

type Box = { value: number; greet: () => string };

function makeBox(value: number): Box {
  return { value, greet: () => `value:${value}` };
}

describe("createLazySingleton", () => {
  it("runs the factory once across concurrent ensure() calls", async () => {
    let factoryCalls = 0;
    const singleton = createLazySingleton(async () => {
      factoryCalls += 1;
      return makeBox(1);
    });

    const [a, b] = await Promise.all([singleton.ensure(), singleton.ensure()]);

    expect(factoryCalls).toBe(1);
    expect(a).toBe(b);
  });

  it("returns the cached instance on subsequent ensure() calls", async () => {
    let factoryCalls = 0;
    const singleton = createLazySingleton(async () => {
      factoryCalls += 1;
      return makeBox(2);
    });

    const first = await singleton.ensure();
    const second = await singleton.ensure();

    expect(factoryCalls).toBe(1);
    expect(second).toBe(first);
  });

  it("throws when the proxy is used before initialization", () => {
    const singleton = createLazySingleton(async () => makeBox(3), {
      notInitializedMessage: "not ready",
    });

    expect(() => singleton.proxy.value).toThrow("not ready");
  });

  it("forwards property access and method calls through the proxy after init", async () => {
    const singleton = createLazySingleton(async () => makeBox(4));

    await singleton.ensure();

    expect(singleton.proxy.value).toBe(4);
    expect(singleton.proxy.greet()).toBe("value:4");
    expect("value" in singleton.proxy).toBe(true);
  });

  it("invokes the guard on every ensure() call", async () => {
    let guardCalls = 0;
    const singleton = createLazySingleton(async () => makeBox(5), {
      guard: () => {
        guardCalls += 1;
      },
    });

    await singleton.ensure();
    await singleton.ensure();

    expect(guardCalls).toBe(2);
  });

  it("propagates guard failures without running the factory", async () => {
    let factoryCalls = 0;
    const singleton = createLazySingleton(
      async () => {
        factoryCalls += 1;
        return makeBox(6);
      },
      {
        guard: () => {
          throw new Error("blocked");
        },
      },
    );

    await expect(singleton.ensure()).rejects.toThrow("blocked");
    expect(factoryCalls).toBe(0);
  });

  it("allows retrying after a failed factory run", async () => {
    let factoryCalls = 0;
    const singleton = createLazySingleton(async () => {
      factoryCalls += 1;
      if (factoryCalls === 1) throw new Error("boom");
      return makeBox(7);
    });

    await expect(singleton.ensure()).rejects.toThrow("boom");
    const instance = await singleton.ensure();

    expect(factoryCalls).toBe(2);
    expect(instance.value).toBe(7);
  });

  it("re-initializes after reset()", async () => {
    let factoryCalls = 0;
    const singleton = createLazySingleton(async () => {
      factoryCalls += 1;
      return makeBox(factoryCalls === 1 ? 8 : 9);
    });

    const first = await singleton.ensure();
    singleton.reset();
    const second = await singleton.ensure();

    expect(factoryCalls).toBe(2);
    expect(first.value).toBe(8);
    expect(second.value).toBe(9);
    expect(() => singleton.proxy.value).not.toThrow();
  });
});
