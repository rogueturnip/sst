import type * as durable from "@aws/durable-execution-sdk-js";

// The durable SDK ends an invocation 20 ms after the last operation that runs, if others
// wait. It doesn't know of work between operations: what EventFlow reads, and what a
// function does between two steps. Ended in the middle, that work would go on only when
// something else woke the run, and until then, in a Lambda that's frozen, or that runs
// another's. So an invocation is held while EventFlow works, and for a moment after a step
// gave a function its result, as another step may come of it.

/** How long a function has, after a step, to come to the next one, where others wait. */
const GRACE_MS = 500;

/** A moment that a function has after one of its steps. */
export interface Grace {
  until: number;
}

interface Manager {
  checkpoint(id: string, data: { Action?: string; Type?: string }): Promise<unknown>;
  shouldTerminate(): unknown;
  checkAndTerminate(): void;
}

export class Invocation {
  private holds = 0;
  private readonly graces = new Set<Grace>();
  private readonly writes = new Map<string, Promise<unknown>>();

  private constructor(
    private readonly manager: Manager | undefined,
    private readonly termination: { isTerminated: boolean } | undefined,
  ) {
    if (!manager) return;
    const { shouldTerminate, checkpoint } = manager;
    manager.shouldTerminate = () => (this.holds > 0 || this.graced() ? undefined : shouldTerminate.call(manager));
    // The SDK doesn't wait until a context's result is saved.
    manager.checkpoint = (id, data) => {
      const write = checkpoint.call(manager, id, data);
      if (data?.Type === "CONTEXT" && data.Action === "SUCCEED") this.writes.set(id, write);
      return write;
    };
  }

  private static readonly known = new WeakMap<object, Invocation>();
  static of(ctx: durable.DurableContext): Invocation {
    const c = ctx as any;
    const manager = c.checkpoint;
    const termination = c._executionContext?.terminationManager;
    if (typeof manager?.shouldTerminate !== "function" || typeof manager?.checkpoint !== "function" || typeof manager?.checkAndTerminate !== "function" || typeof termination?.isTerminated !== "boolean")
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

  private graced() {
    const now = Date.now();
    for (const g of this.graces) if (g.until <= now) this.graces.delete(g);
    return this.graces.size > 0;
  }

  /** A step gave the function its result: what comes of it may be another step. */
  given(): Grace {
    const grace = { until: Date.now() + GRACE_MS };
    this.graces.add(grace);
    setTimeout(() => this.manager?.checkAndTerminate(), GRACE_MS + 5).unref?.();
    return grace;
  }

  /** The next step came of it. */
  came(grace: Grace | undefined) {
    if (grace) this.graces.delete(grace);
  }

  /** When an operation's result is saved. */
  async written(id: string) {
    await this.writes.get(id);
  }
}
