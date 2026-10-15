import { describe, expect, it } from "vite-plus/test";

import { backoffDelay } from "../../internal/push";

describe("backoffDelay", () => {
  const retry = { maxAttempts: 8, baseDelayMs: 100, maxDelayMs: 1_000 };

  it("doubles each attempt", () => {
    expect(backoffDelay(1, retry)).toBe(100);
    expect(backoffDelay(2, retry)).toBe(200);
    expect(backoffDelay(3, retry)).toBe(400);
    expect(backoffDelay(4, retry)).toBe(800);
  });

  it("never exceeds maxDelayMs", () => {
    expect(backoffDelay(5, retry)).toBe(1_000);
    expect(backoffDelay(50, retry)).toBe(1_000);
  });

  it("treats a zero or negative attempt as the first one", () => {
    expect(backoffDelay(0, retry)).toBe(100);
    expect(backoffDelay(-3, retry)).toBe(100);
  });
});
