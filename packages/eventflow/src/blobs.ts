import type { Engine } from "./engine.js";
import { NonRetriableError } from "./errors.js";
import { hash } from "./ids.js";
import type { WireEvent } from "./types.js";

// A large event's data is kept in the table, in parts, and what's passed on has a reference
// to it: a run's payload, a held run's message, a callback and the table's own items are
// each limited to less than an event may be.

/** The largest data that's carried with its event. */
export const INLINE_BYTES = 100 * 1024;

/** It was in the table, and has expired: reading it again won't help. */
export class GoneError extends NonRetriableError {}

/** The event with its data kept apart, if that's large. The same event makes the same reference. */
export async function slim(engine: Engine, event: WireEvent, ttlMs = blobTtl(engine)): Promise<WireEvent> {
  if (event.blob) return event;
  const body = JSON.stringify(event.data) ?? "null";
  const bytes = Buffer.byteLength(body);
  if (bytes <= INLINE_BYTES) return event;
  // Named by what's in it, for its tenant, so one that's written again is the same.
  const key = `X#${hash(JSON.stringify([event.tenant ?? null, event.id, hash(body, 52)]), 52)}`;
  await engine.store.putBlob(key, body, ttlMs);
  return { ...event, data: null, blob: { key, bytes } };
}

/** The event with its data. */
export async function fat(engine: Engine, event: WireEvent): Promise<WireEvent> {
  if (!event?.blob) return event;
  const { blob, ...rest } = event;
  const body = await engine.store.getBlob(blob.key);
  if (body === undefined) throw new GoneError(`The data of the event "${event.id}" is gone.`);
  return { ...rest, data: JSON.parse(body) };
}

/** A result that's kept apart if it's large, as an invoke's. */
export async function slimValue(engine: Engine, name: string, value: unknown): Promise<{ value: unknown } | { blob: string }> {
  const body = JSON.stringify(value) ?? "null";
  if (Buffer.byteLength(body) <= INLINE_BYTES) return { value };
  const key = `X#${hash(JSON.stringify([name, hash(body, 52)]), 52)}`;
  await engine.store.putBlob(key, body, blobTtl(engine));
  return { blob: key };
}

export async function fatValue(engine: Engine, kept: { value?: unknown; blob?: string }): Promise<unknown> {
  if (!kept.blob) return kept.value;
  const body = await engine.store.getBlob(kept.blob);
  if (body === undefined) throw new GoneError("The result is gone.");
  return JSON.parse(body);
}

/** As long as a run that has it, and as it can be looked at, retried and replayed. */
function blobTtl(engine: Engine) {
  return ((engine.resource.runTimeoutSeconds ?? 31_622_400) + 106 * 86_400) * 1000;
}
