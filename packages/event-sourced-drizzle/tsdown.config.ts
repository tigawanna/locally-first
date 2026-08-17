import { defineConfig } from "tsdown";

export default defineConfig({
  entry: ["src/index.ts", "src/sqlite.ts", "src/pg.ts", "src/react.ts"],
  format: ["esm"],
  dts: true,
  clean: true,
  outDir: "dist",
  sourcemap: true,
  tsconfig: "./tsconfig.json",
  exports: true,
  deps: {
    neverBundle: ["drizzle-orm", "uuidv7", "react"],
  },
});
