import { describe, expect, it } from "vitest";
import { createEventSourcedLogger } from "../../utils/logger";

describe("createEventSourcedLogger", () => {
  it("returns noop logger when config is false", () => {
    const log = createEventSourcedLogger(false);
    // Should not throw and do nothing
    log.debug("test");
    log.info("test");
    log.warn("test");
    log.error("test");
  });

  it("returns noop logger when config is undefined", () => {
    const log = createEventSourcedLogger(undefined);
    log.debug("test");
    log.info("test");
  });

  it("returns console logger when config is true", () => {
    const log = createEventSourcedLogger(true);
    expect(log.debug).toBeTypeOf("function");
    expect(log.info).toBeTypeOf("function");
    expect(log.warn).toBeTypeOf("function");
    expect(log.error).toBeTypeOf("function");
  });

  it("returns custom logger when config is a logger object", () => {
    const calls: string[] = [];
    const custom = {
      debug: (msg: string) => calls.push(`debug:${msg}`),
      info: (msg: string) => calls.push(`info:${msg}`),
      warn: (msg: string) => calls.push(`warn:${msg}`),
      error: (msg: string) => calls.push(`error:${msg}`),
    };
    const log = createEventSourcedLogger(custom);

    log.info("hello");
    expect(calls).toContain("info:hello");
    expect(log).toBe(custom);
  });
});
