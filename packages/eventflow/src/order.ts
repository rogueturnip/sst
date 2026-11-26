import * as durable from "@aws/durable-execution-sdk-js";
import { hash } from "./ids.js";
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
const clear = globalThis.clearTimeout;
const immediate = globalThis.setImmediate;

/** How long a step that's next in order has to be called, where others wait for it. */
export const missing = { ms: 30_000 };

const OWN = 0, MARK = 1, CUT = 2;
/** The attempt of the function it's of, by which one that ended before can be told. */
export type Place = [scope: string, id: string, seq: number, attempt: number, kind: number, led?: string, base?: string];

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

/** The places that are saved, by what they're of. */
export function placesOf(ctx: durable.DurableContext): (scope: string, earlier?: string[], rest?: string[]) => Places {
  const all = new Map<string, Place[]>();
  for (const op of Object.values(((ctx as any)._executionContext?._stepData ?? {}) as Record<string, any>)) {
    const text = op?.StepDetails?.Result ?? op?.ContextDetails?.Result;
    let place = placeOfError(op?.StepDetails?.Error?.ErrorData);
    // Without reading every result: a place comes first in what's saved.
    if (!place && typeof text === "string" && text.startsWith(`{"${ORDER}":[`)) place = unwrap(text).place;
    if (place) all.set(place[0], [...(all.get(place[0]) ?? []), place]);
  }
  return (scope, earlier, rest) => new Places(scope, all.get(scope) ?? [], earlier, rest);
}

/**
 * The places of a function's results, in all its attempts. A later attempt goes on from the
 * same steps, so it has them in the same order.
 */
export class Places {
  /** The attempt that's running. */
  attempt = 0;
  /** Told as a result gets its place. */
  onTake?: (id: string, seq: number, attempt: number) => void;
  private last: number;
  private readonly own = new Map<string, { seq: number; attempt: number }>();
  private readonly marks = new Map<string, { seq: number; attempt: number }>();
  private readonly cuts: { seq: number; attempt: number }[] = [];
  private readonly entities = new Map<string, string>();
  /** The attempt that began each step, in this invocation. */
  readonly callers = new Map<string, number>();
  /** The result each was called on, as the function came to it: "" for its start. */
  readonly leds = new Map<string, string>();
  /** The ID each was called with, of which it's a use. */
  readonly bases = new Map<string, string>();

  /** The order of the results of a retry's run that have no place: they're given in it. */
  readonly rank: Map<string, number>;

  /** `earlier` are the results of the run it's a retry of, in their order, and `rest` those after the first step it runs again. */
  constructor(
    readonly scope: string,
    saved: Place[],
    earlier: string[] = [],
    rest: string[] = [],
  ) {
    this.rank = new Map(rest.map((id, i) => [id, i]));
    earlier.forEach((id, i) => this.own.set(id, { seq: i + 1, attempt: -1 }));
    this.last = earlier.length;
    for (const place of saved) this.add(place);
  }

  private add([, id, seq, attempt, kind]: Place) {
    this.last = Math.max(this.last, seq);
    if (kind === CUT) this.cuts.push({ seq, attempt });
    else if (kind === OWN) this.own.set(id, { seq, attempt });
    else if ((this.marks.get(id)?.attempt ?? -1) <= attempt) this.marks.set(id, { seq, attempt });
  }

  /** A result's place, unless the attempt went on without it. */
  at(id: string) {
    const place = this.marks.get(id) ?? this.own.get(id);
    if (!place) return undefined;
    return this.cuts.some((c) => c.attempt > place.attempt && place.seq >= c.seq) ? undefined : place;
  }

  /** Whether a step ended before, in an attempt that had another path. */
  lost(id: string) {
    return (this.marks.has(id) || this.own.has(id)) && !this.at(id);
  }

  *all(): Iterable<[id: string, place: { seq: number; attempt: number }]> {
    for (const id of new Set([...this.own.keys(), ...this.marks.keys()])) {
      const place = this.at(id);
      if (place) yield [id, place];
    }
  }

  entity(id: string) {
    return this.entities.get(id);
  }

  // A result's place: after those that have one. One that has its own keeps it, as where a
  // large result is made again from what's in it.
  private wrap(id: string, value: unknown, entity?: string) {
    if (entity !== undefined) this.entities.set(id, entity);
    let own = this.own.get(id);
    if (!own) {
      this.own.set(id, (own = { seq: ++this.last, attempt: this.callers.get(id) ?? this.attempt }));
      this.onTake?.(id, own.seq, own.attempt);
    }
    return JSON.stringify({ [ORDER]: [this.scope, id, own.seq, own.attempt, OWN, this.leds.get(id), this.bases.get(id)], v: value });
  }

  /** A place for a result that has none, or one that the attempt goes on without. */
  mark(id: string, attempt: number): Place {
    const place: Place = [this.scope, id, ++this.last, attempt, MARK, this.leds.get(id), this.bases.get(id)];
    this.add(place);
    this.onTake?.(id, place[2], attempt);
    return place;
  }

  /** Go on without what earlier attempts had, from a place on. */
  cut(seq: number, attempt: number): Place {
    return [this.scope, "", seq, attempt, CUT];
  }
  isCut(place: Place) {
    this.add(place);
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

/** The order of one attempt. */
export class Order {
  /** The function is going on from its start, or from a result, and nothing else. */
  fresh = false;
  /** The step whose result it's going on from. */
  current?: string;
  /** The steps whose results it has had. */
  readonly given = new Set<string>();
  /** The time it has for that, in this invocation. */
  grace?: Grace;
  /** The places of which the results aren't given yet. */
  private readonly expected = new Map<number, { id: string; attempt: number }>();
  private readonly ready = new Map<number, { give: (grace: Grace) => void; moved: () => void }>();
  private readonly called = new Set<string>();
  private pumping = false;
  private stalled?: { seq: number; timer: any };
  private closed = false;
  private readonly closing: Promise<void>;
  /** The attempt has ended: nothing more is given, or waited for. */
  close!: () => void;

  private readonly attempt: number;

  constructor(
    private readonly inv: Invocation,
    private readonly places: Places,
    private readonly save: (name: string, place: () => unknown) => Promise<unknown>,
    private readonly remaining: () => number,
    private readonly refuse: (id: string) => void,
  ) {
    this.closing = new Promise((resolve) => {
      this.close = () => {
        this.closed = true;
        this.unstall();
        resolve();
      };
    });
    this.attempt = places.attempt;
    for (const [id, place] of places.all()) this.expected.set(place.seq, { id, attempt: place.attempt });
    places.onTake = (id, seq, attempt) => this.expected.set(seq, { id, attempt });
  }

  /** Run what the function does first. */
  begin<T>(fn: () => T): T {
    this.fresh = true;
    immediate(() => (this.fresh = false));
    return fn();
  }

  /** A step's result, for the function: it's given in its order. */
  give<T>(id: string, promise: Promise<T>, onGiven: (grace: Grace) => void): Promise<T> {
    this.called.add(id);
    if (this.stalled && this.expected.get(this.stalled.seq)?.id === id) this.unstall();
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
    const { places } = this;
    const work = async () => {
      for (let given = false; !given && !this.closed; ) {
        // Of those that are there at once, the first in the order they had.
        const rank = places.rank.get(id);
        if (!places.at(id) && rank !== undefined)
          while (!this.closed) {
            const before = [...this.called].find((o) => o !== id && (places.rank.get(o) ?? Infinity) < rank && !places.at(o));
            if (!before) break;
            await new Promise<void>((resolve) => immediate(() => resolve()));
          }
        if (!places.at(id)) await this.save(this.name(id), () => places.mark(id, this.attempt));
        const entity = places.entity(id);
        if (entity !== undefined) await this.inv.written(entity);
        const place = places.at(id);
        if (!place) continue;
        this.expected.set(place.seq, { id, attempt: place.attempt });
        given = await new Promise<boolean>((resolve) => {
          this.ready.set(place.seq, {
            give: (grace) => {
              give(grace);
              resolve(true);
            },
            moved: () => resolve(false),
          });
          void this.pump();
        });
      }
    };
    await this.inv.hold(() => Promise.race([work(), this.closing])).catch(() => {
      // Its place couldn't be saved, so the function isn't given it: the invocation ends.
    });
  }

  private name(id: string) {
    const name = `${ORDER}:${this.places.scope}:${this.attempt}:`;
    return name + (id.length > 100 ? `#${hash(id, 26)}` : id);
  }

  private head() {
    let seq = Infinity;
    for (const s of this.expected.keys()) if (s < seq) seq = s;
    return seq;
  }

  private async pump() {
    if (this.pumping) return;
    this.pumping = true;
    while (this.ready.size && !this.closed) {
      const seq = this.head();
      const next = this.ready.get(seq);
      if (!next) {
        // It's on its way, or the function comes to it: it goes on here as it's there.
        this.stall(seq);
        break;
      }
      this.unstall();
      const giving = this.expected.get(seq)?.id;
      this.expected.delete(seq);
      this.ready.delete(seq);
      this.fresh = true;
      this.current = giving;
      if (giving !== undefined) this.given.add(giving);
      this.grace = this.inv.given();
      next.give(this.grace);
      // All that comes of it at once, before the next.
      await new Promise<void>((resolve) => immediate(() => resolve()));
      this.fresh = false;
      this.current = undefined;
      this.grace = undefined;
    }
    this.pumping = false;
  }

  private unstall() {
    if (this.stalled) clear(this.stalled.timer);
    this.stalled = undefined;
  }

  // The next in order isn't called, and others wait for it.
  private stall(seq: number) {
    if (this.stalled?.seq === seq) return;
    this.unstall();
    if (this.called.has(this.expected.get(seq)!.id)) return;
    const ms = Math.min(missing.ms, Math.max(1000, this.remaining() / 2));
    this.stalled = { seq, timer: later(() => void this.gone(seq), ms) };
  }

  private async gone(seq: number) {
    this.stalled = undefined;
    const { id, attempt } = this.expected.get(seq)!;
    // This attempt called it: were it gone on without, what came first may be another.
    if (attempt === this.attempt) return this.refuse(id);
    // An earlier one did, or the run it's a retry of. It's gone on without, and without all
    // that came after, from here on.
    this.pumping = true;
    try {
      await this.save(this.name("!"), () => {
        return this.places.cut(seq, this.attempt);
      });
      this.places.isCut(this.places.cut(seq, this.attempt));
      console.warn(
        JSON.stringify({ level: "warn", message: `The function went on without the step "${id}", which it called before. Was it changed, or does it depend on more than its steps?` }),
      );
      for (const [s, e] of [...this.expected])
        if (!this.places.at(e.id) || this.places.at(e.id)!.seq !== s) {
          this.expected.delete(s);
          const waiting = this.ready.get(s);
          this.ready.delete(s);
          waiting?.moved();
        }
    } catch {
      // It couldn't be saved: the invocation ends.
    } finally {
      this.pumping = false;
    }
    void this.pump();
  }
}
