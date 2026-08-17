# Examples

Runnable SQLite and PGlite apps. Writes go through `engine.mutate`; reads use your Drizzle `db`.

| File                                                    | Purpose                                |
| ------------------------------------------------------- | -------------------------------------- |
| `sqlite.ts`                                             | better-sqlite3 + `createSQLiteAdapter` |
| `pglite.ts`                                             | PGlite + `createPgAdapter`             |
| `drizzle.sqlite.config.ts` / `drizzle.pglite.config.ts` | drizzle-kit generate configs           |

```bash
pnpm --filter event-sourced-drizzle example:generate:sqlite
pnpm --filter event-sourced-drizzle example:sqlite

pnpm --filter event-sourced-drizzle example:generate:pglite
pnpm --filter event-sourced-drizzle example:pglite
```

**Do not** `db.insert` / `db.update` / `db.delete` on synced domain tables. Those bypass the outbox, so the change never syncs. Use `engine.mutate.insert` / `update` / `delete`.

**Do** query with Drizzle as usual: `db.select().from(todos)`, joins, filters, aggregates.

These examples call `migrate()` after `drizzle-kit generate`. In your app, use the same drizzle-kit (or Vite) pipeline you already have — this package does not run migrations for you. In the browser, generate SQL with drizzle-kit and apply it at runtime (for example [proj-airi/drizzle-orm-browser](https://github.com/proj-airi/drizzle-orm-browser)).
