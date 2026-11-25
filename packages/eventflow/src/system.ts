import { nameMatches } from "./definition.js";
import type { Engine } from "./engine.js";
import { keyed } from "./sign.js";
import { SYSTEM_EVENTS, type WireEvent } from "./types.js";
import type { Registered } from "./worker.js";

// The events about runs that ended, as Inngest sends them: "inngest/function.finished" for
// each, and "inngest/function.failed" or "inngest/function.cancelled". They're sent only
// where a function has a trigger for one, or `systemEvents` asks for them, so they cost
// nothing otherwise.

export interface Ended {
  fn: string;
  runId: string;
  /** The event the run started with, and for a batch, all of them. */
  event: WireEvent;
  events?: WireEvent[];
  outcome: "completed" | "failed" | "cancelled";
  result?: unknown;
  error?: { name: string; message: string };
}

/** The largest result an event carries. A larger one is left out. */
const MAX_RESULT_BYTES = 64 * 1024;

export function wantedSystemEvents(registry: Map<string, Registered>, engine: Engine, ended: Pick<Ended, "event" | "outcome">) {
  // What a run about a run did isn't told, or a function that fails would be started again
  // by its own failure, forever.
  if ((SYSTEM_EVENTS as readonly string[]).includes(ended.event.name)) return [];
  const names = [
    "inngest/function.finished",
    ...(ended.outcome === "failed" ? ["inngest/function.failed"] : ended.outcome === "cancelled" ? ["inngest/function.cancelled"] : []),
  ];
  if (engine.systemEvents) return names;
  const triggers = [...registry.values()].flatMap((r) => r.def.triggers.flatMap((t) => (t.kind === "event" ? [t.name] : [])));
  return names.filter((n) => triggers.some((t) => nameMatches(t, n)));
}

export async function sendSystemEvents(registry: Map<string, Registered>, engine: Engine, ended: Ended) {
  for (const name of wantedSystemEvents(registry, engine, ended)) {
    const large = ended.result !== undefined && Buffer.byteLength(JSON.stringify(ended.result)) > MAX_RESULT_BYTES;
    const full = {
      function_id: ended.fn,
      run_id: ended.runId,
      event: ended.event,
      ...(ended.events && ended.events.length > 1 ? { events: ended.events.map((e) => ({ id: e.id, name: e.name, ts: e.ts })) } : {}),
      ...(ended.error ? { error: ended.error } : {}),
      ...(ended.result !== undefined && !large ? { result: ended.result } : {}),
      _inngest: { status: ended.outcome === "completed" ? "Completed" : ended.outcome === "failed" ? "Failed" : "Cancelled" },
    };
    // The run's own event can be as large as an event may be: then it's named, not carried.
    const data =
      Buffer.byteLength(JSON.stringify(full)) > 200 * 1024
        ? { ...full, event: { id: ended.event.id, name: ended.event.name, ts: ended.event.ts }, result: undefined }
        : full;
    await engine.sendSystem({
      // The same each time it's sent, so one that's sent again starts nothing new.
      // It's keyed, so nobody else can work it out.
      id: keyed(JSON.stringify([name, ended.runId])),
      name,
      data,
      ts: Date.now(),
      ...(ended.event.tenant ? { tenant: ended.event.tenant } : {}),
    });
  }
}
