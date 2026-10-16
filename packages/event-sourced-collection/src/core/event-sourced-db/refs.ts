/** Next `localSeq` to hand out. Shared by every collection on one database. */
export type SeqCounter = { value: number };

/** Persisted client id. Replaced when the local replica is reset. */
export type ClientIdRef = { value: string };
