import { fileURLToPath, URL } from "node:url";
import { defineConfig } from "vite-plus";

export default defineConfig({
  resolve: {
    alias: {
      "@": fileURLToPath(new URL("./src", import.meta.url)),
      "event-sourced-collection": fileURLToPath(
        new URL("../../packages/event-sourced-collection/dist/index.mjs", import.meta.url),
      ),
      "event-sourced-collection/browser": fileURLToPath(
        new URL("../../packages/event-sourced-collection/dist/browser.mjs", import.meta.url),
      ),
    },
  },
  test: {
    environment: "node",
    exclude: ["**/node_modules/**", "**/dist/**", "**/.output/**"],
    globals: false,
    restoreMocks: true,
    // The app has no tests of its own yet; the suite lives in the packages.
    passWithNoTests: true,
  },
});
