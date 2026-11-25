import type * as durable from "@aws/durable-execution-sdk-js";

// The durable SDK ends an invocation 20 ms after the last operation that runs, if others
// wait. It doesn't know of work between operations: what EventFlow reads, and what a
// function does between two steps. Ended in the middle, that work would go on only when
// something else woke the run, and until then, in a Lambda that's frozen, or that runs
// another's. So an invocation is held while EventFlow works, and for a moment after a step
// gave a function its result, as another step may come of it.

/** How long a function has, after a step, to come to the next one, where others wait. */
const GRACE_MS = 500;

interface Manager {
  shouldTerminate(): unknown;
  checkAndTerminate(): void;
}

export class Invocation {
  private holds = 0;
  private until = 0;

  private constructor(
    private readonly manager: Manager | undefined,
    private readonly termination: { isTerminated: boolean } | undefined,
  ) {
    if (!manager) return;
    const { shouldTerminate } = manager;
    manager.shouldTerminate = () => (this.holds > 0 || Date.now() < this.until ? undefined : shouldTerminate.call(manager));
  }

  private static readonly known = new WeakMap<object, Invocation>();
  static of(ctx: durable.DurableContext): Invocation {
    const c = ctx as any;
    const manager = c.checkpoint;
    const termination = c._executionContext?.terminationManager;
    if (typeof manager?.shouldTerminate !== "function" || typeof manager?.checkAndTerminate !== "function" || typeof termination?.isTerminated !== "boolean")
      throw new Error("EventFlow needs the version of @aws/durable-execution-sdk-js that it's built for. Don't override it.");
    let known = Invocation.known.get(manager);
    if (!known) Invocation.known.set(manager, (known = new Invocation(manager, termination)));
    return known;
  }

  /** The invocation has ended, and what still runs of it must start nothing. */
  get over() {
    return this.termination?.isTerminated === true;
  }

  /** Keep the invocation while this runs. */
  async hold<T>(fn: () => Promise<T>): Promise<T> {
    this.holds++;
    try {
      return await fn();
    } finally {
      this.holds--;
      if (!this.holds) this.manager?.checkAndTerminate();
    }
  }

  /** A step gave the function its result: what comes of it may be another step. */
  given() {
    this.until = Date.now() + GRACE_MS;
    setTimeout(() => this.manager?.checkAndTerminate(), GRACE_MS + 5).unref?.();
  }

  /** The next step came. */
  came() {
    this.until = 0;
  }
}
