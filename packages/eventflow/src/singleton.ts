import type { Definition } from "./definition.js";
import type { Engine } from "./engine.js";
import { key } from "./ids.js";
import type { Lock } from "./store.js";
import type { WireEvent } from "./types.js";

/**
 * How long a run that has a singleton, and isn't found, is taken for one that's still to be
 * started: longer than the queue takes to deliver its message again.
 */
export const LOCK_GRACE_MS = 45 * 60_000;

export function lockTtl(engine: Engine) {
  return (engine.resource.runTimeoutSeconds ?? 31_622_400) * 1000 + 2 * 86_400_000;
}

export function singletonKey(def: Definition, event: WireEvent) {
  if (!def.singleton) return undefined;
  const k = def.singleton.key?.({ event });
  return k === false ? undefined : key("S", def.id, event.tenant ?? null, k ?? null);
}

/** What taking a singleton needs to know of runs, and to stop one. */
export interface LockRuns {
  find(run: string): Promise<{ arn: string } | undefined>;
  running(arn: string): Promise<boolean>;
  stop(run: string, arn: string): Promise<unknown>;
}

/**
 * Take a singleton for a run. A run that has it and has ended, as when it was stopped, or
 * its worker was, loses it. So does one that's running, with mode "cancel". Which runs it
 * was taken from is kept with it until they're stopped, so one isn't left running however
 * this stops.
 */
export async function takeLock(engine: Engine, runs: LockRuns, item: string, id: string, mode: "skip" | "cancel", order?: string) {
  for (let attempt = 0; attempt < 10; attempt++) {
    const held = await engine.store.lockTake(item, { run: id, until: Date.now() + LOCK_GRACE_MS, ...(order ? { order } : {}) }, lockTtl(engine));
    if (!held) {
      await settleLock(engine, runs, item, id);
      return true;
    }
    const arn = held.arn ?? (await runs.find(held.run))?.arn;
    const running = arn ? await runs.running(arn) : held.until > Date.now();
    if (running && mode === "skip") return false;
    // A later event's run replaced this one already, as when this message came again late.
    if (mode === "cancel" && order && held.order && held.order > order) return false;
    // Too many that it can't be sure of: those first.
    if ((held.replaces?.length ?? 0) >= 50) await settleLock(engine, runs, item, held.run);
    const mine: Lock = {
      run: id,
      until: Date.now() + LOCK_GRACE_MS,
      replaces: [...(held.replaces ?? []), { run: held.run, ...(arn ? { arn } : {}) }],
      ...(order ? { order } : {}),
    };
    if (!(await engine.store.lockMove(item, held.run, mine, lockTtl(engine)))) continue;
    await settleLock(engine, runs, item, id);
    return true;
  }
  throw new Error("A singleton is busy.");
}

/** Stop the runs a singleton was taken from. One that hasn't started finds that it lost it. */
export async function settleLock(engine: Engine, runs: LockRuns, item: string, id: string) {
  const lock = await engine.store.lockRead(item);
  if (lock?.run !== id || !lock.replaces?.length) return lock;
  for (const r of lock.replaces) {
    const arn = r.arn ?? (await runs.find(r.run))?.arn;
    if (arn) await runs.stop(r.run, arn);
  }
  await engine.store.lockSettled(item, id);
  return lock;
}

/** Where an event comes among a singleton's: by when it was received, then its ID. */
export function lockOrder(event: { id: string }, received: number) {
  return `${String(received).padStart(15, "0")}:${event.id}`;
}
