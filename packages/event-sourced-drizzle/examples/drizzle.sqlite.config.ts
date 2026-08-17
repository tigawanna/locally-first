import { defineConfig } from "drizzle-kit";

export default defineConfig({
  dialect: "sqlite",
  schema: "./examples/sqlite.ts",
  out: "./.playground/drizzle-sqlite",
  dbCredentials: {
    url: "./.playground/app.sqlite",
  },
});
