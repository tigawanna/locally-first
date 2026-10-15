/** Drops the newest `keepLast` items from an ascending-sorted removal list. */
export function trimKeepLast<T>(items: ReadonlyArray<T>, keepLast: number): T[] {
  if (keepLast <= 0) return [...items];
  return items.slice(0, Math.max(0, items.length - keepLast));
}
