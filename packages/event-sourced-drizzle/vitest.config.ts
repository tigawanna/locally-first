import { defineConfig } from "vite-plus";

export default defineConfig({
  test: {
    environment: "node",
    include: ["src/__tests__/**/*.test.ts"],
    exclude: ["**/node_modules/**", "**/dist/**"],
    globals: false,
    restoreMocks: true,
    unstubGlobals: true,
    testTimeout: 15000,
  },
});
