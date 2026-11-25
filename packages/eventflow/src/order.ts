import * as durable from "@aws/durable-execution-sdk-js";
import type { Grace, Invocation } from "./invocation.js";

// The order in which a function is given its steps' results.
//
// On a replay every step that's done returns at once. A function whose path depends on which
// finished first, like `Promise.race([pay, timeout])` or two branches that share a step ID,
// would go another way than it did, and do another thing under the same name. So the order is
// saved, and the function is given the results one at a time, in that order, each time.
//
// A result's place is saved in the same write as the result, which costs nothing. What ends
// with no code of ours, like a sleep, has its place saved in a step of its own. Either way
// it's saved before the function has the result, so nothing comes of it that a replay
// wouldn't come to.

export const ORDER = "sst:order";
// The test runner replaces the timers, and this isn't a wait of the function's.
const later = globalThis.setTimeout;
const immediate = globalThis.setImmediate;

/** How long a step that's next in order has to be called, where others wait for it. */
const MISSING_MS = 5_000;

type Place = [scope: string, id: string, seq: number];

/** A result with its place, as it's saved. */
export function unwrap(text: string | undefined | null): { value: any; place?: Place } {
  if (text === undefined || text === null || text === "") return { value: undefined };
  const value = JSON.parse(text);
  const place = value !== null && typeof value === "object" ? value[ORDER] : undefined;
  return Array.isArray(place) ? { value: value.v, place: place as Place } : { value };
}

/** A failed step's place, as it's saved with its error. */
export function placeOfError(data: unknown): Place | undefined {
  if (typeof data !== "string" || !data.startsWith(`{"${ORDER}"`)) return undefined;
  return unwrap(data).place;
}

/** The step that a result's place is saved in, where it's in none of its own. */
export const isMarker = (name: unknown) => typeof name === "string" && name.startsWith(`${ORDER}:`);

/** The places that are saved, by the attempt they're of. */
export function placesOf(ctx: durable.DurableContext): Map<string, Map<string, number>> {
  const out = new Map<string, Map<string, number>>();
  for (const op of Object.values(((ctx as any)._executionContext?._stepData ?? {}) as Record<string, any>)) {
    const text = op?.StepDetails?.Result ?? op?.ContextDetails?.Result;
    let place = placeOfError(op?.StepDetails?.Error?.ErrorData);
    // Without reading every result: a place comes first in what's saved.
    if (!place && typeof text === "string" && text.startsWith(`{"${ORDER}":[`)) place = unwrap(text).place;
    if (!place) continue;
    const scope = out.get(place[0]) ?? new Map<string, number>();
    out.set(place[0], scope.set(place[1], place[2]));
  }
  return out;
}

/** A step's last error, with its place. */
export class Failed extends durable.DurableOperationError {
  errorType = "StepError";
  constructor(
    readonly error: unknown,
    private readonly data: string,
  ) {
    super((error as any)?.message ?? "Unknown error");
  }
  toErrorObject(): any {
    const { error } = this;
    const base =
      error instanceof durable.DurableOperationError
        ? error.toErrorObject()
        : typeof (error as any)?.message === "string"
          ? { ErrorType: (error as any).name, ErrorMessage: (error as any).message }
          : { ErrorMessage: "Unknown error" };
    return { ...base, ErrorData: this.data, StackTrace: undefined };
  }
}

export class Order {
  /** The function is going on from its start, or from a result, and nothing else. */
  fresh = false;
  /** The time it has for that, in this invocation. */
  grace?: Grace;
  private last: number;
  private readonly places = new Map<string, number>();
  private readonly entities = new Map<string, string>();
  /** The places of which the results aren't given yet, where they're to come first. */
  private readonly expected = new Map<number, string>();
  private readonly ready = new Map<number, (grace: Grace) => void>();
  private readonly called = new Set<string>();
  private readonly waiting = new Map<string, () => void>();
  private pumping = false;
  private closed = false;
  private readonly closing: Promise<void>;
  /** The attempt has ended: nothing more is given, or waited for. */
  close!: () => void;

  /**
   * `earlier` are the steps of earlier attempts, in their order. Those up to `strict` are
   * waited for; the function may not come to the rest.
   */
  constructor(
    private readonly inv: Invocation,
    private readonly scope: string,
    saved: Map<string, number> | undefined,
    earlier: string[],
    strict: number,
    private readonly mark: (name: string, place: () => Place) => Promise<unknown>,
  ) {
    this.closing = new Promise((resolve) => {
      this.close = () => {
        this.closed = true;
        resolve();
      };
    });
    earlier.forEach((id, i) => {
      this.places.set(id, i + 1);
      if (i < strict) this.expected.set(i + 1, id);
    });
    this.last = earlier.length;
    for (const [id, seq] of saved ?? []) {
      this.places.set(id, seq);
      this.expected.set(seq, id);
      this.last = Math.max(this.last, seq);
    }
  }

  seqOf(id: string) {
    return this.places.get(id);
  }

  /** Run what the function does first. */
  begin<T>(fn: () => T): T {
    this.fresh = true;
    immediate(() => (this.fresh = false));
    return fn();
  }

  // A result's place: after those that have one. One that has its own keeps it, as where a
  // large result is made again from what's in it.
  private take(id: string, entity?: string): number {
    if (entity !== undefined) this.entities.set(id, entity);
    const known = this.places.get(id);
    if (known !== undefined) return known;
    const seq = ++this.last;
    this.places.set(id, seq);
    this.expected.set(seq, id);
    return seq;
  }

  private wrap(id: string, value: unknown, entity?: string) {
    return JSON.stringify({ [ORDER]: [this.scope, id, this.take(id, entity)], v: value });
  }

  /** How a step's result is saved: with its place. */
  serdes(id: string): any {
    return {
      serialize: async (value: unknown, c: { entityId: string }) => this.wrap(id, value, c.entityId),
      deserialize: async (text: string | undefined) => unwrap(text).value,
    };
  }

  /** What's saved in place of a result that's too large for it. */
  summary(id: string) {
    return () => this.wrap(id, undefined);
  }

  /** A step's last error, with its place. */
  failed(id: string, error: unknown) {
    return new Failed(error, this.wrap(id, undefined));
  }

  /** A step's result, for the function: it's given in its order. */
  give<T>(id: string, promise: Promise<T>, onGiven: (grace: Grace) => void): Promise<T> {
    this.called.add(id);
    this.waiting.get(id)?.();
    const given = new Promise<T>((resolve, reject) => {
      const settled = (give: () => void) =>
        void this.settle(id, (grace) => {
          onGiven(grace);
          give();
        });
      promise.then(
        (v) => settled(() => resolve(v)),
        (e) => settled(() => reject(e)),
      );
    });
    // One that fails before the function awaits it isn't an error of the process.
    given.catch(() => {});
    return given;
  }

  private async settle(id: string, give: (grace: Grace) => void) {
    if (this.closed) return;
    const work = async () => {
      if (this.places.get(id) === undefined) await this.mark(`${ORDER}:${this.scope}:${id}`, () => [this.scope, id, this.take(id)]);
      const entity = this.entities.get(id);
      if (entity !== undefined) await this.inv.written(entity);
      const seq = this.places.get(id)!;
      this.expected.set(seq, id);
      await new Promise<void>((resolve) => {
        this.ready.set(seq, (grace) => {
          give(grace);
          resolve();
        });
        void this.pump();
      });
    };
    await this.inv.hold(() => Promise.race([work(), this.closing])).catch(() => {
      // Its place couldn't be saved, so the function isn't given it: the invocation ends.
    });
  }

  private async pump() {
    if (this.pumping) return;
    this.pumping = true;
    while (this.ready.size && !this.closed) {
      let seq = Infinity;
      for (const s of this.expected.keys()) if (s < seq) seq = s;
      const id = this.expected.get(seq)!;
      const give = this.ready.get(seq);
      if (!give) {
        // It's on its way: it goes on here as it's there.
        if (this.called.has(id)) break;
        if (!(await this.calls(id))) {
          console.warn(
            JSON.stringify({ level: "warn", message: `The function went on without the step "${id}", which it called before. Was it changed, or does it depend on more than its steps?` }),
          );
          this.expected.delete(seq);
        }
        continue;
      }
      this.expected.delete(seq);
      this.ready.delete(seq);
      this.fresh = true;
      this.grace = this.inv.given();
      give(this.grace);
      // All that comes of it at once, before the next.
      await new Promise<void>((resolve) => immediate(() => resolve()));
      this.fresh = false;
      this.grace = undefined;
    }
    this.pumping = false;
  }

  // Whether a step is called, in a while.
  private calls(id: string): Promise<boolean> {
    return new Promise((resolve) => {
      const timer = later(() => {
        this.waiting.delete(id);
        resolve(false);
      }, MISSING_MS);
      this.waiting.set(id, () => {
        clearTimeout(timer);
        this.waiting.delete(id);
        resolve(true);
      });
    });
  }
}
