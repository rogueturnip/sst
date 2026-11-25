import type * as durable from "@aws/durable-execution-sdk-js";
import { createHash } from "crypto";

// Operations by name, not by the order they're called in.
//
// The durable SDK numbers a context's operations as they're called. With branches that run
// at once, `Promise.all([a(), b()])`, that's the order their steps finished in, and on a
// replay, where finished steps return at once, it's another. Then one branch's step is taken
// for another's, and the run fails as non-deterministic. Inngest finds a step by its ID. So
// does this: an operation's number is its name's hash.
//
// It overrides two methods of the SDK's context that aren't public, so the SDK's version is
// pinned, and it's checked that they're there.

interface Internals {
  createStepId(): string;
  peekStepId(): string;
  _stepPrefix?: string;
  _executionContext: { getStepData(id: string): { Status?: string } | undefined };
}

export interface Replay {
  /** The function is returning what's saved, and hasn't come to anything new yet. */
  on: boolean;
}

export interface NamedContext {
  readonly raw: durable.DurableContext;
  readonly lambdaContext: durable.DurableContext["lambdaContext"];
  step<T>(name: string, fn: durable.StepFunc<T>, config?: durable.StepConfig<T>): durable.DurablePromise<T>;
  wait(name: string, duration: durable.Duration): durable.DurablePromise<void>;
  runInChildContext<T>(
    name: string,
    fn: (child: durable.DurableContext) => Promise<T>,
    config?: durable.ChildConfig<T> & { key?: string },
  ): durable.DurablePromise<T>;
}

const contexts = new WeakMap<object, NamedContext>();

export function named(ctx: durable.DurableContext, replay: Replay): NamedContext {
  const known = contexts.get(ctx);
  if (known) return known;
  const c = ctx as unknown as Internals;
  if (typeof c.createStepId !== "function" || typeof c.peekStepId !== "function" || typeof c._executionContext?.getStepData !== "function")
    throw new Error("EventFlow needs the version of @aws/durable-execution-sdk-js that it's built for. Don't override it.");

  let next: string | undefined;
  const { createStepId, peekStepId } = c;
  c.createStepId = function (this: Internals) {
    if (next === undefined) return createStepId.call(c);
    const id = next;
    next = undefined;
    return id;
  };
  c.peekStepId = function (this: Internals) {
    return next ?? peekStepId.call(c);
  };
  const at = <T>(key: string, call: () => T, virtual = false): T => {
    // No "-" in it: that's what nests one in another.
    const token = `n${createHash("sha256").update(key).digest("hex").slice(0, 32)}`;
    next = c._stepPrefix ? `${c._stepPrefix}-${token}` : token;
    const status = virtual ? undefined : c._executionContext.getStepData(next)?.Status;
    if (!virtual && status !== "SUCCEEDED" && status !== "FAILED") replay.on = false;
    try {
      return call();
    } finally {
      next = undefined;
    }
  };
  const context: NamedContext = {
    raw: ctx,
    lambdaContext: ctx.lambdaContext,
    step: (name, fn, config) => at(name, () => ctx.step(name, fn, config)),
    wait: (name, duration) => at(name, () => ctx.wait(name, duration)),
    runInChildContext: (name, fn, config) => {
      const { key, ...rest } = config ?? {};
      return at(key ?? name, () => ctx.runInChildContext(name, fn, rest), rest.virtualContext === true);
    },
  };
  contexts.set(ctx, context);
  return context;
}

/** A logger that doesn't repeat what it said before the replay. In a step it always says it. */
export function replayLogger<T extends object>(logger: T, replay: Replay, inStep: () => boolean): T {
  return new Proxy(logger, {
    get(target, name) {
      const value = (target as any)[name];
      if (typeof value !== "function") return value;
      if (!["log", "info", "warn", "error", "debug"].includes(String(name))) return value.bind(target);
      return (...args: unknown[]) => (replay.on && !inStep() ? undefined : value.apply(target, args));
    },
  });
}
