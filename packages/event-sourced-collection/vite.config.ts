import { defineConfig } from "vite-plus";

export default defineConfig({
  test: {
    environment: "node",
    include: ["src/__tests__/**/*.test.ts"],
    exclude: ["**/node_modules/**", "**/dist/**"],
    globals: false,
    restoreMocks: true,
    unstubGlobals: true,
  },
  pack: {
    entry: ["src/index.ts", "src/browser.ts", "src/node.ts", "src/react.ts", "src/react-native.ts"],
    format: ["esm"],
    dts: true,
    clean: true,
    outDir: "dist",
    sourcemap: true,
    tsconfig: "./tsconfig.json",
    exports: true,
    deps: {
      neverBundle: [
        "@tanstack/db",
        "@tanstack/db-sqlite-persistence-core",
        "@tanstack/browser-db-sqlite-persistence",
        "@tanstack/react-native-db-sqlite-persistence",
        "@tanstack/expo-db-sqlite-persistence",
        "@tanstack/node-db-sqlite-persistence",
        "react",
        "uuidv7",
      ],
    },
  },
});
