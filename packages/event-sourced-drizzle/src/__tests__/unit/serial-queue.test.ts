import { describe, expect, it } from "vitest";
import { createSerialQueue } from "../../internal/serial-queue";

describe("createSerialQueue", () => {
  it("runs a single task and returns its result", async () => {
    const run = createSerialQueue();
    const result = await run(async () => 42);
    expect(result).toBe(42);
  });

  it("serializes concurrent tasks", async () => {
    const run = createSerialQueue();
    const order: number[] = [];

    const p1 = run(async () => {
      await new Promise((r) => setTimeout(r, 30));
      order.push(1);
    });
    const p2 = run(async () => {
      order.push(2);
    });
    const p3 = run(async () => {
      order.push(3);
    });

    await Promise.all([p1, p2, p3]);
    expect(order).toEqual([1, 2, 3]);
  });

  it("continues after a task throws", async () => {
    const run = createSerialQueue();

    const p1 = run(async () => {
      throw new Error("boom");
    });

    await expect(p1).rejects.toThrow("boom");

    // Next task should still run
    const result = await run(async () => "ok");
    expect(result).toBe("ok");
  });

  it("does not run tasks in parallel", async () => {
    const run = createSerialQueue();
    let running = 0;
    let maxConcurrent = 0;

    async function task() {
      running++;
      maxConcurrent = Math.max(maxConcurrent, running);
      await new Promise((r) => setTimeout(r, 10));
      running--;
    }

    await Promise.all([run(task), run(task), run(task)]);
    expect(maxConcurrent).toBe(1);
  });
});
