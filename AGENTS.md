<!--VITE PLUS START-->

# Using Vite+, the Unified Toolchain for the Web

This project is using Vite+, a unified toolchain built on top of Vite, Rolldown, Vitest, tsdown, Oxlint, Oxfmt, and Vite Task. Vite+ wraps runtime management, package management, and frontend tooling in a single global CLI called `vp`. Vite+ is distinct from Vite, and it invokes Vite through `vp dev` and `vp build`. Run `vp help` to print a list of commands and `vp <command> --help` for information about a specific command.

Docs are local at `node_modules/vite-plus/docs` or online at https://viteplus.dev/guide/.

## Built-in Commands vs Scripts

`vp <name>` runs a built-in command. `vp run <name>` runs a `package.json` script or a `vite.config.ts` task. Scripts cannot overwrite built-ins, so `vp dev` and `vp run dev` may do different things. Check `package.json` and `vite.config.ts` first, and run `vp run <name>` when the project defines a script or task with that name.

## Tool Versions

Run `vp toolchain` to show versions and relationships in the active Vite+
release. Add a tool name to select part of the graph. For example, run
`vp toolchain vite`. Use `--global` to ignore the local `vite-plus` package. Use
`vp why <package>` to show the package-manager dependency graph.

## Review Checklist

- [ ] Run `vp install` after pulling remote changes and before getting started.
- [ ] Run `vp check` and `vp test` to format, lint, type check and test changes.
- [ ] Check if there are `vite.config.ts` tasks or `package.json` scripts necessary for validation, run via `vp run <script>`.
- [ ] If setup, runtime, or package-manager behavior looks wrong, run `vp env doctor` and include its output when asking for help.

<!--VITE PLUS END-->

## Names

Use descriptive variable and function names. The name should say what the value is or what the function does, so a reader does not have to open the implementation to tell `pendingOutboxEntries` from `syncedOutboxEntries`.

## Prefer readable branches

A ternary is fine when both sides are values: `status === "synced" ? syncedLabel : pendingLabel`.

Stop there. Once a branch is another conditional, or it calls a function that does real work, write it as `if` statements. Declare the result with `let` and assign it inside each block. Readability comes before a shorter expression.

```ts
let nextEntry = existingEntry;
if (operation === "delete") {
  nextEntry = tombstone(existingEntry);
} else if (operation === "update") {
  nextEntry = mergeUpdate(existingEntry, payload);
} else {
  nextEntry = insertEntry(payload);
}
```

## Check assumptions with a short script

Before locking an implementation or adding tests, run a small Node script against the real code when the behavior is uncertain. Throwaway scripts live in `packages/event-sourced-collection/scripts/collection-test-scripts/` (for example `check-compaction.ts`). Run one with `pnpm exec tsx` from that package:

```bash
pnpm exec tsx scripts/collection-test-scripts/check-compaction.ts
```

Use the output to confirm the assumption, then implement and add tests.
