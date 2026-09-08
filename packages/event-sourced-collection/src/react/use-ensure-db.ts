import { useEffect, useState } from "react";

export type UseEnsureDbOptions<TDb> = {
  /** Lazy singleton opener from `createBrowserEventSourcedDB` / platform helpers. */
  ensureDb: () => Promise<TDb>;
  /**
   * Re-run init when these change (e.g. `[isAuthenticated]`).
   * `ensureDb` itself stays idempotent; use this to re-apply app gates.
   */
  deps?: ReadonlyArray<unknown>;
  /**
   * After OPFS/SQLite is ready. Use for settings gates, `setSyncEnabled`,
   * fire-and-forget `db.sync()`, e2e handles, etc. Errors here fail the hook.
   */
  onReady?: (db: TDb) => void | Promise<void>;
};

export type UseEnsureDbResult =
  | { ready: false; error: null }
  | { ready: false; error: Error }
  | { ready: true; error: null };

/**
 * Browser-safe `ensureDb()` for React: runs in `useEffect`, exposes ready/error.
 *
 * Does not know about auth or settings — pass those through `onReady` / `deps`.
 *
 * @example
 * ```tsx
 * const { ready, error } = useEnsureDb({
 *   ensureDb,
 *   deps: [isAuthenticated],
 *   onReady: (db) => {
 *     db.setSyncEnabled(isAuthenticated && settings.syncEnabled)
 *     if (db.getSyncEnabled()) void db.sync()
 *   },
 * })
 * ```
 */
export function useEnsureDb<TDb>({
  ensureDb,
  deps = [],
  onReady,
}: UseEnsureDbOptions<TDb>): UseEnsureDbResult {
  const [ready, setReady] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    let cancelled = false;

    ensureDb()
      .then(async (db) => {
        if (cancelled) return;
        if (onReady) await onReady(db);
        if (cancelled) return;
        setReady(true);
        setError(null);
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        setReady(false);
        setError(err instanceof Error ? err : new Error(String(err)));
      });

    return () => {
      cancelled = true;
    };
    // Caller owns stability of ensureDb / onReady; deps drive re-init.
    // eslint-disable-next-line react-hooks/exhaustive-deps -- intentional deps array API
  }, deps);

  if (error) return { ready: false, error };
  if (!ready) return { ready: false, error: null };
  return { ready: true, error: null };
}
