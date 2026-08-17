import { uuidv7 } from "uuidv7";

/**
 * Time-ordered event id (UUIDv7). Used as `eventId` on outbox rows and as the
 * default device `clientId` when you do not pass one.
 *
 * @example
 * ```ts
 * import { generateEventId } from "event-sourced-drizzle"
 *
 * const eventId = generateEventId()
 * const clientId = generateEventId()
 * ```
 */
export function generateEventId(): string {
  return uuidv7();
}
