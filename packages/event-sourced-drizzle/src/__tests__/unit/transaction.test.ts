import { describe, expect, it } from "vite-plus/test";
import { getActiveTransaction, getActiveTransactionImpl } from "../../core/transaction";

describe("transaction stack", () => {
  it("returns undefined when no transaction is active", () => {
    expect(getActiveTransaction()).toBeUndefined();
    expect(getActiveTransactionImpl()).toBeUndefined();
  });
});
