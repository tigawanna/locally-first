import { defineConfig } from "drizzle-kit";

export default defineConfig({
  dialect: "postgresql",
  schema: "./examples/pglite.ts",
  out: "./.playground/drizzle-pg",
  dbCredentials: {
    url: "postgresql://localhost:5432/playground",
  },
});
