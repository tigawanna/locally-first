import { describe, expect, it } from "vitest";
import { generateEventId } from "../../utils/uuid";

describe("generateEventId", () => {
  it("returns a string", () => {
    const id = generateEventId();
    expect(typeof id).toBe("string");
  });

  it("returns unique values", () => {
    const ids = new Set(Array.from({ length: 100 }, () => generateEventId()));
    expect(ids.size).toBe(100);
  });

  it("produces UUIDv7 format (36 chars with dashes)", () => {
    const id = generateEventId();
    expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  });

  it("generates monotonically increasing IDs", () => {
    const ids = Array.from({ length: 10 }, () => generateEventId());
    const sorted = [...ids].sort();
    expect(ids).toEqual(sorted);
  });
});
