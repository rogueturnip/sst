import * as durable from "@aws/durable-execution-sdk-js";
import { AsyncLocalStorage } from "async_hooks";
import { createHash } from "crypto";
import { priorityMs, type Definition } from "./definition.js";
import { parseDuration, toDurableSeconds, type DurationInput } from "./duration.js";
import { checkSignal, MESSAGE_TTL_MS, toWireEvent, type Engine, type SendPayload } from "./engine.js";
import { InvokeFailedError, NonRetriableError, RetryAfterError, SignalConflictError, StepFailedError } from "./errors.js";
import { bind, conjuncts, evaluate, getPath, parseExpression, paths, type Expression } from "./expression.js";
import { hash, key, runId as makeRunId } from "./ids.js";
import { Hooks } from "./middleware.js";
import { named, replayLogger, type Replay } from "./named.js";
import { sign, verify } from "./sign.js";
import type { LeaseWaiter, Limit, Memo } from "./store.js";
import { sendSystemEvents, wantedSystemEvents } from "./system.js";
import { infer, saveResponse, toResponse, type AiModel, type SavedResponse } from "./tools.js";
import type { RunPayload, WireEvent } from "./types.js";

// The worker runs every function of the engine, in one durable Lambda function.
// A run is one durable execution. Its name is the run ID.

export interface Registered {
  def: Definition;
  handler: (ctx: any) => unknown;
}

/** A function's return, as the run that invoked it gets it. */
export type InvokeOutcome =
  | { ok: true; result: unknown }
  | { ok: false; error: { name: string; message: string } };

export { NonRetriableError, RetryAfterError, StepFailedError, InvokeFailedError, SignalConflictError } from "./errors.js";

/** The error name of an at-most-once step that stopped part way. */
export const INTERRUPTED = "StepInterrupted";

function interrupted(id: string) {
  return {
    name: INTERRUPTED,
    message: `The step "${id}" stopped part way, and isn't run again because it's atMostOnce. Its side effect may have happened: check it before you retry the run, with force.`,
  };
}

function isInterruption(error: unknown) {
  return error instanceof durable.StepInterruptedError || /interruption/i.test(String((error as any)?.message ?? ""));
}

/**
 * How long an invoke waits when it has no timeout: the run's timeout, at most 20 days.
 * Longer timers overflow Node's setTimeout, as the local test runner uses.
 */
const DEFAULT_INVOKE_TIMEOUT_MS = 20 * 86_400_000;

/** The longest durable wait AWS allows: 366 days. */
export const MAX_WAIT_S = 31_622_400;
/**
 * How long a step waits for a lease before it asks again, in case a wake was lost: 3
 * minutes, longer the more it asks, up to 15. Each time is 3 operations in the run's
 * history, so a step that waits a day in line adds about 300, not 1,500.
 */
export function leaseWaitSeconds(round: number) {
  return Math.min(900, 180 * 2 ** Math.floor(round / 2));
}
/** A waiter whose callback is this, followed by a run's ARN, stops the run. */
export const STOP = "stop:";
/** The largest answer of an invoked run: what the callback API takes. */
const MAX_ANSWER_BYTES = 250 * 1024;
/** How long a received event is kept for a wait that registers just after it. */
export const RECENT_MS = 3_600_000;
/** The largest `if` a wait or cancelOn keeps, with the run's values in it. */
const MAX_CONDITION_BYTES = 16 * 1024;

const json = {
  serialize: async (value: unknown) => (value === undefined ? undefined : JSON.stringify(value)),
  deserialize: async (data: string | undefined) => (data === undefined || data === "" ? null : JSON.parse(data)),
};

// Step results are JSON, as they are with Inngest: a Date becomes a string, and
// undefined becomes null.
function jsonify(value: unknown) {
  return value === undefined ? null : JSON.parse(JSON.stringify(value));
}

/** What's kept of an error: one can be larger than what keeps it. */
const MAX_MESSAGE = 16 * 1024;

export function errorInfo(error: unknown) {
  if (error instanceof Error) return { name: String(error.name).slice(0, 256), message: String(error.message).slice(0, MAX_MESSAGE) };
  return { name: "Error", message: String(error).slice(0, MAX_MESSAGE) };
}

/**
 * An error that isn't the run's: the invocation ends, and Lambda starts another, which goes
 * on from what's saved.
 */
class TryAgain extends Error {
  readonly isUnrecoverable = true;
  readonly isUnrecoverableInvocation = true;
}

/** Read the table outside a step. It's tried again by itself; after that, by Lambda. */
async function read<T>(what: string, fn: () => Promise<T>): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await fn();
    } catch (error) {
      if (attempt >= 3) throw new TryAgain(`Failed to read ${what}: ${errorInfo(error).message}`);
      await new Promise((r) => setTimeout(r, 100 * 2 ** attempt));
    }
  }
}

// Exponential backoff, from 2 seconds, doubling to at most 10 minutes. The jitter
// comes from the run and step, so a replay gets the same delay.
export function backoffMs(attempt: number, seed: string) {
  const base = Math.min(600_000, 2000 * 2 ** (attempt - 1));
  // From the seed's hash, as a number from 0 to 1.
  const jitter = createHash("sha256").update(seed).digest().readUInt32BE(0) / 2 ** 32;
  return Math.round(base * (0.5 + jitter / 2));
}

function retryDelay(error: unknown, attempt: number, retries: number, seed: string) {
  if (error instanceof NonRetriableError || (error as any)?.name === "NonRetriableError") return undefined;
  if (attempt > retries) return undefined;
  let delay = backoffMs(attempt, seed);
  if (error instanceof RetryAfterError) {
    const at = error.retryAfter;
    if (at instanceof Date) delay = at.getTime() - Date.now();
    else if (typeof at === "number") delay = at;
    else {
      const date = Date.parse(at);
      delay = Number.isNaN(date) ? parseDuration(at, "retryAfter") : date - Date.now();
    }
  }
  return Math.min(Math.max(1000, delay), MAX_WAIT_S * 1000);
}

/** A step used as it can't be. Trying the function again wouldn't help. */
class UsageError extends TypeError {}

/** A run that's cancelled, as it didn't start in time, or a later one replaced it. */
export class CancelledError extends NonRetriableError {
  constructor(message: string) {
    super(message);
    // Its own, as it's how it's known after it came out of a step.
    this.name = CANCELLED;
  }
}
const CANCELLED = "sst:Cancelled";

/** Errors that came out of a step: it was tried again already. */
const fromStep = new WeakSet<object>();

type StepInput = string | { id: string; name?: string; atMostOnce?: boolean };

function stepId(input: StepInput) {
  const id = typeof input === "string" ? input : input?.id;
  if (typeof id !== "string" || !id) throw new TypeError("A step needs an ID.");
  return id;
}

function atMostOnce(input: StepInput) {
  return typeof input === "object" && input?.atMostOnce === true;
}

/** The subtype of an at-most-once step, so a retry can see it in the history. */
export const ONCE = "EventFlowOnce";

/** The key of the step that's running, for `idempotencyKey()`. */
const stepKeys = new AsyncLocalStorage<{ key: string; attempt: number }>();

/**
 * A key for a step's side effect, like a payment provider's idempotency key. It comes
 * from the run's root, which a retry or replay of the run keeps, the function or
 * onFailure, and the step's ID with its occurrence, like "charge:2" in a loop. So it's
 * the same in every attempt, retry and replay of that step, and different for every
 * other step, occurrence and run.
 */
export function idempotencyKey(root: string, namespace: string, stepId: string) {
  return `ef_${hash(JSON.stringify([root, namespace, stepId]), 26)}`;
}

function unsupported(name: string) {
  return () => {
    throw new UsageError(`${name} isn't supported by EventFlow.`);
  };
}

const log = (level: "info" | "warn" | "error", message: string, fields: Record<string, unknown>) =>
  console[level](JSON.stringify({ level, message, ...fields }));

interface RunState {
  ctx: durable.DurableContext;
  engine: Engine;
  def: Definition;
  registry: Map<string, Registered>;
  runId: string;
  event: WireEvent;
  memo: Record<string, unknown>;
  counts: Map<string, number>;
  /**
   * The run this one started from: a retry or a replay of a run has the same root,
   * and an invoked run's root comes from its parent's root and step. It's signed in
   * the payload, and idempotency keys and sent event IDs come from it.
   */
  root: string;
  /** "" for the function, "onFailure" for its onFailure, so their keys differ. */
  namespace: string;
  /** When its event came: its steps go before those of runs that came after. */
  at?: number;
  /** Which attempt of the function this is, from 0. */
  attempt: number;
  /** What this attempt's steps returned, for the next attempt. */
  done: Record<string, Memo>;
  /** This attempt's steps that are still running. */
  busy: Set<Promise<unknown>>;
  /** The attempt has ended: its branches that go on start nothing more. */
  fence: { down: boolean };
  replay: Replay;
  /** The step names that are taken. */
  used: Set<string>;
  /** A step's place, where it's been claimed already. */
  claimed?: Claim;
  /** Note that the run started, before its first step runs. */
  start: () => Promise<void>;
  hooks: Hooks;
  /** The context a hook gets. */
  args?: Record<string, unknown>;
  settled: () => Promise<void>;
}

interface Claim {
  id: string;
  /** It's the function's first step. */
  first: boolean;
}

// Inngest gives a step ID used more than once the suffixes ":1", ":2", and so on.
// It's done here too, so IDs that are built in a loop stay distinct in the history.
function occurrence(state: RunState, id: string) {
  // A name is one step's: it's what its result and its idempotency key are found by. Also one
  // that looks like another's second occurrence.
  for (let n = state.counts.get(id) ?? 0; ; n++) {
    const name = n === 0 ? id : `${id}:${n}`;
    if (state.used.has(name)) continue;
    state.counts.set(id, n + 1);
    state.used.add(name);
    return name;
  }
}

// A step's place among the function's, taken as it's called: what a step does before it
// runs can take time, and the next one mustn't get its place, or its idempotency key.
function claim(state: RunState, input: StepInput): Claim {
  if (state.claimed) return state.claimed;
  const first = state.counts.size === 0;
  return { id: occurrence(state, stepId(input)), first };
}

const never = () => new Promise<never>(() => {});

// A DurablePromise starts when it's awaited. Start it now, as Inngest does, so a step
// that isn't awaited still runs, and steps started together run together.
function eager<T>(promise: PromiseLike<T>): Promise<T> {
  return Promise.resolve().then(() => promise);
}

/** A tenant, or a key value, as a short string that's safe as a map key. */
function shortTenant(value: string) {
  return value.length <= 64 ? value : `#${hash(value, 20)}`;
}

/**
 * The tenant of a run, for fair shares: the event's tenant, or the value of the
 * function's first concurrency key.
 */
export function tenantOf(def: Definition, event: WireEvent) {
  if (event.tenant) return event.tenant;
  const keyed = def.concurrency.find((c) => c.key);
  const value = keyed?.key?.({ event });
  return value === undefined ? "" : shortTenant(value);
}

export function limitsFor(def: Definition, event: WireEvent): Limit[] {
  return def.concurrency.map((c) => {
    const value = c.key ? (c.key({ event }) ?? null) : undefined;
    if (c.scope === "fn") {
      const label = value === undefined ? `fn:${def.id}` : `fn:${def.id}:${value}`;
      return { item: key("C", "fn", def.id, value ?? null), limit: c.limit, label };
    }
    return { item: key("C", c.scope, value), limit: c.limit, label: `${c.scope}:${value}` };
  });
}

function makeStep(state: RunState) {
  const { engine, def } = state;
  const ctx = named(state.ctx, state.replay);
  const limits = limitsFor(def, state.event);
  const tenant = tenantOf(def, state.event);
  const priority = priorityMs(def, state.event);

  // A wait can't outlast the run, or AWS's longest wait. Say so, instead of the
  // checkpoint error AWS gives.
  const maxWaitMs = Math.min(MAX_WAIT_S, engine.resource.runTimeoutSeconds ?? MAX_WAIT_S) * 1000;
  const bounded = (ms: number, what: string) => {
    if (ms > maxWaitMs)
      throw new UsageError(
        `The ${what} is ${Math.round(ms / 1000)}s, longer than a run can last (${maxWaitMs / 1000}s). Raise timeout.execution on the EventFlow, up to 366 days.`,
      );
    return toDurableSeconds(ms);
  };

  // A retry passes the results of the steps that succeeded. They aren't run again.
  const memo = (id: string): { hit: true; value: any } | { hit: false } =>
    Object.prototype.hasOwnProperty.call(state.memo, id) ? { hit: true, value: state.memo[id] } : { hit: false };

  // What a step returned, or the error it failed with, for the function's next attempt, if
  // it throws outside a step.
  const record = <T>(id: string, promise: Promise<T>): Promise<T> => {
    const kept = promise.then(
      (v) => {
        state.done[id] = { v: v === undefined ? null : v };
        return v;
      },
      (error) => {
        if (!isDurableInternal(error)) {
          state.done[id] = { e: errorInfo(error) };
          if (error && typeof error === "object") fromStep.add(error);
        }
        throw error;
      },
    );
    state.busy.add(kept);
    const over = () => void state.busy.delete(kept);
    kept.then(over, over);
    return kept;
  };
  // An earlier attempt's step: its result, or the error it failed with.
  const earlier = (id: string): Promise<any> | undefined => {
    const saved = memo(id);
    if (!saved.hit) return undefined;
    const value = saved.value;
    if (value instanceof FailedMemo) {
      state.done[id] = { e: value.error };
      const error = value.error.name === "InvokeError" ? new InvokeFailedError(value.error.message, "") : new StepFailedError(id, value.error);
      fromStep.add(error);
      return Promise.reject(error);
    }
    state.done[id] = { v: value };
    return Promise.resolve(value);
  };

  const leaseMs = () => Math.max(30_000, (ctx.lambdaContext?.getRemainingTimeInMillis?.() ?? 900_000) + 10_000);
  // A wake that throws may not have reached the step: the store keeps it in line.
  const wake = (w: LeaseWaiter) =>
    engine.callback(w.callbackId, { leaseId: w.leaseId }).catch((error) => {
      log("warn", "Failed to wake a step in line. It stays in line.", { run: w.runId, error: errorInfo(error).message });
      throw error;
    });

  // Give back a step's leases, and hand them to the steps in line. An error here
  // mustn't fail the step, which would run its code again: the leases expire.
  const giveBack = async (leaseId: string) => {
    try {
      // Straight to the next step in line, where there's one.
      await engine.store.handoff(limits, leaseId, tenant, wake);
    } catch (error) {
      log("warn", "Failed to give back a lease. It expires on its own.", { run: state.runId, error: errorInfo(error).message });
    }
  };

  // Take a lease on every limit at once, or wait in line until a release can give the
  // step all of them. A step never holds one lease while it waits for another, so two
  // steps can't wait for each other.
  // `place.joined` is when the step first joined the line, so asking again, or after
  // a lease was lost, keeps its place. It comes from a step's result, so a replay has
  // the same.
  const takeLeases = async (child: durable.DurableContext, leaseId: string, tag: string, place: { joined?: number }) => {
    for (let round = 0; ; round++) {
      const t = `${tag}.${round}`;
      const got = await child.step(`lease:${t}`, async () => {
        const result = await engine.store.acquire(limits, leaseId, tenant, leaseMs()).catch((error) => {
          log("warn", "Failed to ask for a lease. Joining the line.", { run: state.runId, error: errorInfo(error).message });
          return "full" as const;
        });
        // When it asked, for its place in line. It's this step's result, so joining the
        // line again after an error is joining in the same place, not twice.
        return { granted: result === "granted", at: Date.now() };
      });
      if (got.granted) return;
      // Runs that came first go first, so what's begun is finished. A priority moves it.
      place.joined ??= (state.at ?? got.at) - priority;
      const [woken, callbackId] = await child.createCallback<{ leaseId: string }>(`queued:${t}`, {
        timeout: { seconds: leaseWaitSeconds(round) },
        serdes: json as any,
      });
      // Join the line, then hand out what's free: the leases may have freed up in
      // between. If they're the step's, it goes on without waiting.
      const mine = await child.step(`enqueue:${t}`, async () => {
        const at = place.joined!;
        const waiter = {
          leaseId,
          limits,
          tenant,
          callbackId,
          runId: state.runId,
          at,
          expiresAt: Date.now() + leaseWaitSeconds(round) * 1000,
        };
        const result = await engine.store.enqueue(waiter);
        if (result === "held") return true;
        let mine = false;
        for (const limit of limits)
          await engine.store.dispatch(limit, async (w) => {
            if (w.callbackId !== callbackId) return wake(w);
            mine = true;
            return true;
          }, waiter);
        return mine;
      });
      if (mine) return;
      try {
        await woken;
        return;
      } catch (error) {
        // Not woken in time: ask again, keeping its place. If the old callback is
        // woken later, the wake fails, and the leases are handed on.
        if (error instanceof durable.CallbackTimeoutError) continue;
        throw error;
      }
    }
  };

  // The key of a step's side effect, for idempotencyKey() called in the step.
  const keyFor = (id: string) => idempotencyKey(state.root, state.namespace, id);
  const inStep = async <T>(id: string, attempt: number, fn: () => T | Promise<T>): Promise<T> => {
    await state.start();
    return stepKeys.run({ key: keyFor(id), attempt: attempt - 1 }, async () => fn());
  };

  const run = <T>(idInput: StepInput, fn: (...args: any[]) => T | Promise<T>, ...args: any[]): Promise<T> => {
    if (state.fence.down) return never();
    const { id } = claim(state, idInput);
    const saved = earlier(id);
    if (saved) return saved as Promise<T>;
    return record(id, runStep(id, idInput, fn, args));
  };

  const runStep = <T>(id: string, idInput: StepInput, fn: (...args: any[]) => T | Promise<T>, args: any[]): Promise<T> => {
    // At most once: AWS saves that the step started before it runs. If the worker
    // stops part way, the step fails instead of running again. It isn't retried.
    const once = atMostOnce(idInput);
    if (!limits.length) {
      if (once)
        return eager(
          ctx.step(id, async (c) => jsonify(await inStep(id, c.attempt, () => fn(...args))), {
            semantics: durable.StepSemantics.AtMostOncePerRetry,
            retryStrategy: () => ({ shouldRetry: false }),
            subType: ONCE,
          }),
        ).catch((error) => {
          throw isInterruption(error) ? new StepFailedError(id, interrupted(id)) : error;
        }) as Promise<T>;
      return eager(
        ctx.step(id, async (c) => jsonify(await inStep(id, c.attempt, () => fn(...args))), {
          retryStrategy: (error, attempt) => {
            const delay = retryDelay(error, attempt, def.retries, `${state.runId}\n${id}\n${attempt}`);
            return delay === undefined ? { shouldRetry: false } : { shouldRetry: true, delay: toDurableSeconds(delay) };
          },
        }),
      ) as Promise<T>;
    }

    // With a concurrency limit, each attempt takes its leases first, and gives them
    // back when it ends. A step that waits for a lease, or for its next attempt, holds
    // none, as with Inngest.
    return eager(
      ctx.runInChildContext(
        id,
        async (child) => {
          for (let attempt = 1; ; attempt++) {
            // A lost lease keeps the step's place in line; a retry joins it again.
            const place: { joined?: number } = {};
            for (let lost = 0; ; lost++) {
              // The lease's ID comes from the run, the step and the attempt, so a lease
              // step that's retried asks for the same lease.
              const leaseId = `${hash(`${state.runId}\n${state.namespace}\n${state.attempt}\n${id}`, 12)}.${attempt}.${lost}`;
              await takeLeases(child, leaseId, `${attempt}.${lost}`, place);
              const outcome = await child.step(
                attempt === 1 && lost === 0 ? "run" : `run:${attempt}.${lost}`,
                async () => {
                  // After a crash, the run step runs again, maybe after its leases
                  // expired. Check they're still held before the work starts.
                  // One that can't be checked, as when DynamoDB keeps failing, counts as
                  // lost: the step takes a new one, and its code hasn't run.
                  const held = await engine.store.renew(limits, leaseId, leaseMs()).catch((error) => {
                    log("warn", "Failed to renew a lease. Taking a new one.", { run: state.runId, error: errorInfo(error).message });
                    return false;
                  });
                  if (!held) return { lost: true as const };
                  try {
                    return { ok: true as const, value: jsonify(await inStep(id, attempt, () => fn(...args))) };
                  } catch (error) {
                    const delay = once ? undefined : retryDelay(error, attempt, def.retries, `${state.runId}\n${id}\n${attempt}`);
                    return { ok: false as const, error: errorInfo(error), delay };
                  } finally {
                    await giveBack(leaseId);
                  }
                },
                {
                  retryStrategy: () => ({ shouldRetry: false }),
                  ...(once ? { semantics: durable.StepSemantics.AtMostOncePerRetry, subType: ONCE } : {}),
                },
              ).then(
                (o) => o,
                // An at-most-once step that the worker stopped part way fails here. It
                // won't run again, so its leases are given back now.
                async (error) => {
                  await child.step(`release:${attempt}.${lost}`, async () => giveBack(leaseId));
                  return {
                    ok: false as const,
                    error: isInterruption(error) ? interrupted(id) : errorInfo(error),
                    delay: undefined,
                  };
                },
              );
              if ("lost" in outcome) {
                // A lease expired before the step ran. Give back the rest.
                await child.step(`release:${attempt}.${lost}`, async () => giveBack(leaseId));
                if (lost >= 5) return { ok: false as const, error: { name: "LeaseError", message: `The step "${id}" kept losing its lease.` } };
                continue;
              }
              if (outcome.ok) return { ok: true as const, value: outcome.value };
              if (outcome.delay === undefined) return { ok: false as const, error: outcome.error };
              await child.wait(`retry:${attempt}`, toDurableSeconds(outcome.delay));
              break;
            }
          }
        },
        { subType: "EventFlowStep" },
      ),
    ).then((outcome) => {
      // An error thrown in a child context comes out as a ChildContextError, so the
      // outcome comes out of it, and the error is thrown here.
      if (!outcome.ok) throw new StepFailedError(id, outcome.error);
      return outcome.value as T;
    });
  };

  // A function with a concurrency limit starts with its first step that has a lease, or here,
  // with a first step that needs none.
  const begin = (first: boolean) => {
    if (!limits.length || !def.timeouts || !first || state.namespace) return;
    void eager(ctx.step("sst:started", async () => state.start())).catch(() => {});
  };
  // A sleep, or a wait, that's done isn't done again by the function's next attempt. One that
  // isn't done is begun again: the attempt doesn't end for it.
  const slept = <T>(id: string, promise: Promise<T>): Promise<T> =>
    promise.then((v) => {
      state.done[id] = { v: v === undefined ? true : v };
      return v;
    });

  const sleep = (idInput: string | { id: string }, duration: DurationInput) => {
    if (state.fence.down) return never();
    const { id, first } = claim(state, idInput);
    if (earlier(id)) return Promise.resolve();
    begin(first);
    const seconds = bounded(parseDuration(duration, `duration of step "${id}"`), `sleep of step "${id}"`);
    return slept(id, eager(ctx.wait(id, seconds)));
  };

  const sleepUntil = (idInput: string | { id: string }, time: Date | string) => {
    if (state.fence.down) return never();
    const { id, first } = claim(state, idInput);
    if (earlier(id)) return Promise.resolve();
    begin(first);
    const at = time instanceof Date ? time.getTime() : Date.parse(time);
    if (!Number.isFinite(at)) throw new UsageError(`The time of step "${id}" isn't a date.`);
    const seconds = bounded(at - Date.now(), `sleep of step "${id}"`);
    return slept(id, eager(ctx.wait(id, seconds)));
  };

  const waitForEvent = (
    idInput: string | { id: string },
    options: { event: string | { name: string }; timeout: DurationInput | Date; match?: string; if?: string },
  ): Promise<WireEvent | null> => {
    if (state.fence.down) return never();
    const { id, first } = claim(state, idInput);
    const saved = earlier(id);
    if (saved) return saved;
    begin(first);
    const name = typeof options.event === "string" ? options.event : options.event?.name;
    if (!name) throw new UsageError(`The step "${id}" waits for an event with no name.`);
    if (options.match && options.if) throw new UsageError(`The step "${id}" has both "match" and "if". Use one.`);
    const timeoutMs =
      options.timeout instanceof Date
        ? options.timeout.getTime() - Date.now()
        : parseDuration(options.timeout, `timeout of step "${id}"`);
    const timeout = bounded(timeoutMs, `timeout of step "${id}"`);
    const { field, value, condition } = matcher(id, state.event, options);

    return slept(id, eager(
      ctx.runInChildContext(
        id,
        async (child) => {
          const [received, callbackId] = await child.createCallback<WireEvent>("event", { timeout, serdes: json as any });
          // The wait may find an event that came while it was registering. Then it uses
          // that event, and doesn't wait for the callback.
          const early = await child.step("listen", async () => {
            const found = await listen(engine, {
              name,
              tenant: state.event.tenant,
              field,
              value,
              condition,
              callbackId,
              runId: state.runId,
              expiresAt: Date.now() + timeoutMs,
              kind: "wait",
            });
            return found.event;
          });
          if (early) return early;
          try {
            return await received;
          } catch (error) {
            if (error instanceof durable.CallbackTimeoutError) return null;
            throw error;
          }
        },
        { subType: "EventFlowWait" },
      ),
    ) as Promise<WireEvent | null>);
  };

  const waitForSignal = (
    idInput: string | { id: string },
    options: { signal: string; timeout: DurationInput | Date; onConflict?: "fail" | "replace" },
  ): Promise<{ signal: string; data: unknown } | null> => {
    if (state.fence.down) return never();
    const { id, first } = claim(state, idInput);
    const saved = earlier(id);
    if (saved) return saved;
    begin(first);
    checkSignal(options);
    if (options.onConflict !== undefined && options.onConflict !== "fail" && options.onConflict !== "replace")
      throw new UsageError(`The step "${id}" has onConflict "${options.onConflict}". Use "fail" or "replace".`);
    const timeoutMs =
      options.timeout instanceof Date ? options.timeout.getTime() - Date.now() : parseDuration(options.timeout, `timeout of step "${id}"`);
    const timeout = bounded(timeoutMs, `timeout of step "${id}"`);
    const item = signalKey(state.event.tenant, options.signal);
    return slept(id, eager(
      ctx.runInChildContext(
        id,
        async (child) => {
          const [received, callbackId] = await child.createCallback<{ signal: string; data: unknown }>("signal", { timeout, serdes: json as any });
          const mine = await child.step("listen", async () => {
            const now = Date.now();
            const wait = { callbackId, runId: state.runId, owner: `${state.runId}\n${state.namespace}\n${id}`, createdAt: now, expiresAt: now + timeoutMs };
            if (await engine.store.signalWait(item, wait, options.onConflict === "replace")) return true;
            // One whose run was stopped keeps its name until it's over: ask whether it waits.
            const held = await engine.store.signalRead(item);
            if (!held) return engine.store.signalWait(item, wait, false);
            if (await engine.waiting(held.callbackId)) return false;
            return engine.store.signalWait(item, wait, { over: held.callbackId });
          });
          if (!mine) return { conflict: true as const };
          let value: { signal: string; data: unknown } | null = null;
          try {
            value = await received;
          } catch (error) {
            if (!(error instanceof durable.CallbackTimeoutError)) throw error;
          }
          // Free before the function goes on, so it can wait for the name again.
          await child.step("stop-listening", async () => engine.store.signalFree(item, callbackId));
          return { value };
        },
        { subType: "EventFlowSignal" },
      ),
    ).then((outcome: any) => {
      if (outcome.conflict) throw new SignalConflictError(options.signal);
      return outcome.value;
    }));
  };

  const invoke = (
    idInput: string | { id: string },
    options: { function: string | { id: string } | { opts?: { id?: string; functionId?: string; appId?: string } }; data?: unknown; timeout?: DurationInput | Date },
  ) => {
    if (state.fence.down) return never();
    const { id, first } = claim(state, idInput);
    const saved = earlier(id);
    if (saved) return saved;
    begin(first);
    const fn = options.function as any;
    const target = typeof fn === "string" ? fn.replace(new RegExp(`^${escape(engine.appId)}-`), (prefix) => (state.registry.has(fn) ? prefix : "")) : (fn?.id ?? fn?.opts?.id ?? fn?.opts?.functionId);
    if (!target) throw new UsageError(`The step "${id}" has no function to invoke.`);
    if (fn?.opts?.appId !== undefined && fn.opts.appId !== engine.appId)
      throw new UsageError(`The step "${id}" invokes a function of the app "${fn.opts.appId}". An EventFlow invokes its own functions: send an event to another, and wait for its answer.`);
    if (!state.registry.has(target))
      throw new UsageError(`The step "${id}" invokes "${target}", which isn't served by this EventFlow.`);
    const timeoutMs =
      options.timeout === undefined
        ? undefined
        : options.timeout instanceof Date
          ? options.timeout.getTime() - Date.now()
          : parseDuration(options.timeout, `timeout of step "${id}"`);
    // Without a timeout, the run waits as long as it can last, at most 20 days, so a
    // child that never answers, like one AWS stopped, doesn't keep it waiting forever.
    const timeout = bounded(timeoutMs ?? Math.min(maxWaitMs, DEFAULT_INVOKE_TIMEOUT_MS), `timeout of step "${id}"`);

    return record(id, eager(
      ctx.runInChildContext(
        id,
        async (child) => {
          const [result, callbackId] = await child.createCallback<InvokeOutcome>("result", { timeout, serdes: json as any });
          // The invoked run goes through the router, like any other, so its flow
          // control applies. The message is signed, so only a run can send one.
          await child.step("start", async () => {
            // The child run is new for each run that invokes it, as a retry of the
            // parent needs its own. Its root, which its idempotency keys come from,
            // is the same for every retry and replay of the parent.
            const identity = `invoke\n${state.runId}\n${state.namespace}\n${state.attempt ? `${state.attempt}\n` : ""}${id}`;
            const root = hash(JSON.stringify([state.root, state.namespace, id]), 26);
            const event: WireEvent = {
              id: hash(identity),
              name: "inngest/function.invoked",
              data: options.data ?? {},
              ts: Date.now(),
              ...(state.event.tenant ? { tenant: state.event.tenant } : {}),
            };
            const childRunId = makeRunId(target, identity);
            await engine.sendMessages([
              {
                message: sign({
                  kind: "invoke" as const,
                  fn: target,
                  event,
                  parent: callbackId,
                  runId: childRunId,
                  root,
                  exp: Date.now() + MESSAGE_TTL_MS,
                }),
                group: event.tenant,
              },
            ]);
            // In the history, so a rerun of this run can find the child, and check it.
            return { runId: childRunId };
          });
          try {
            return await result;
          } catch (error) {
            if (error instanceof durable.CallbackTimeoutError)
              return {
                ok: false,
                error: { name: "InvokeError", message: `The function "${target}" didn't finish within the timeout of step "${id}".` },
              } satisfies InvokeOutcome;
            throw error;
          }
        },
        { subType: "EventFlowInvoke" },
      ),
    ).then((outcome: InvokeOutcome) => {
      if (!outcome.ok) throw new InvokeFailedError(outcome.error.message, target);
      return outcome.result;
    }));
  };

  const sendEvent = (idInput: string | { id: string }, payload: SendPayload | SendPayload[]) => {
    if (state.fence.down) return never();
    const { id, first } = claim(state, idInput);
    const saved = earlier(id);
    if (saved) return saved as Promise<{ ids: string[] }>;
    begin(first);
    const list = Array.isArray(payload) ? payload : [payload];
    const send = async (events: any[]) => {
      // IDs come from the run's root and the step, so a retried step, and a retry
      // or replay of the run, send the same events, and the router skips the ones
      // it has seen.
      const wire = events.map((p, i) =>
        toWireEvent({
          ...p,
          id: p.id ?? hash(JSON.stringify([state.root, state.namespace, id, i]), 26),
          tenant: p.tenant ?? state.event.tenant,
        }),
      );
      await engine.sendEvents(wire);
      return { ids: wire.map((e) => e.id) };
    };
    return record(
      id,
      eager(
        ctx.step(
          id,
          async (c) => {
            if (!state.hooks.any) return send(list);
            const info = state.hooks.info(id, undefined, "sendEvent", undefined, false);
            return handledWith(state, info, c.attempt, async () => {
              const { events } = await state.hooks.transform("transformSendEvent", { events: list.map((e) => ({ ...e })) });
              return state.hooks.wrap("wrapSendEvent", { events }, () => send(events));
            }) as Promise<{ ids: string[] }>;
          },
          {
            retryStrategy: (error, attempt) => {
              const delay = retryDelay(error, attempt, def.retries, `${state.runId}\n${id}\n${attempt}`);
              return delay === undefined ? { shouldRetry: false } : { shouldRetry: true, delay: toDurableSeconds(delay) };
            },
          },
        ),
      ),
    );
  };

  const sendSignal = (idInput: string | { id: string }, options: { signal: string; data?: unknown }) => {
    if (state.fence.down) return never();
    const { id, first } = claim(state, idInput);
    const saved = earlier(id);
    if (saved) return saved as Promise<{ runId: string | undefined }>;
    begin(first);
    checkSignal(options);
    return record(
      id,
      eager(
        ctx.step(id, async () => {
          const item = signalKey(state.event.tenant, options.signal);
          const wait = await engine.store.signalRead(item);
          // The wait it found first, so a try that comes after the run waits again doesn't
          // answer that too.
          const to = await engine.store.successor(
            key("GS", state.runId, state.namespace, state.attempt, id),
            wait ? JSON.stringify([wait.callbackId, wait.runId]) : "",
            86_400_000,
          );
          if (!to) return { runId: undefined };
          const [callbackId, runId] = JSON.parse(to) as [string, string];
          const told = await engine.callback(callbackId, { signal: options.signal, data: options.data ?? null });
          await engine.store.signalFree(item, callbackId);
          return { runId: told || wait?.callbackId !== callbackId ? runId : undefined };
        }),
      ),
    ).then((r: any) => ({ runId: r?.runId ?? undefined }));
  };

  // step.fetch takes what fetch takes. Its ID is where it goes, so it's known in the history.
  const stepFetch = Object.assign(
    (input: any, init?: any): Promise<Response> => {
      const request = typeof input === "object" && input !== null && !(input instanceof URL) ? (input as Request) : undefined;
      const url = new URL(request ? request.url : input);
      const method = String(init?.method ?? request?.method ?? "GET").toUpperCase();
      // Its place is taken now. A request's body is read in the step, each time it's tried.
      return run<SavedResponse>(`fetch ${method} ${url.host}`, async () =>
        saveResponse(await globalThis.fetch(url, { ...(await requestInit(request)), ...init })),
      ).then(toResponse);
    },
    { config: () => stepFetch, fallback: undefined },
  ) as any;

  const ai = {
    infer: (idInput: StepInput, options: { model: AiModel; body: unknown }) => run(idInput, () => infer(options as any)),
    wrap: (idInput: StepInput, fn: (...args: any[]) => unknown, ...args: any[]) => run(idInput, fn, ...args),
    get models() {
      return unsupported("step.ai.models")();
    },
  };

  const tools: Record<string, any> = { run, sleep, sleepUntil, waitForEvent, waitForSignal, invoke, sendEvent, sendSignal, fetch: stepFetch, ai };
  Object.defineProperty(tools, "realtime", { get: unsupported("step.realtime") });
  return tools;
}

// With middleware, each step is in a virtual context of its own: what a hook does before its
// step can take time, and the steps' places in the history mustn't come from who's first.
async function requestInit(request: Request | undefined) {
  if (!request) return {};
  return { method: request.method, headers: request.headers, body: request.body ? Buffer.from(await request.clone().arrayBuffer()) : undefined };
}

function withMiddleware(state: RunState): Record<string, any> {
  const plain = makeStep(state);
  const { hooks } = state;
  if (!hooks.any) return plain;
  const ctx = named(state.ctx, state.replay);
  const tool = (type: string, name: string) => (idInput: StepInput, ...rest: any[]) => {
    if (state.fence.down) return never();
    const claimed = claim(state, idInput);
    const id = stepId(idInput);
    const info = hooks.info(id, typeof idInput === "object" ? idInput.name : undefined, type);
    const inner = async (child: durable.DurableContext) => {
      let options: any = { ...(typeof idInput === "object" ? idInput : { id: idInput }) };
      const takesInput = name === "run" || name === "ai.wrap";
      const [fn, ...input] = takesInput ? rest : [undefined];
      // Only a step that runs code has an input.
      let args = takesInput ? input : rest;
      const changed = await hooks.transform("transformStepInput", {
        stepInfo: { hashedId: info.hashedId, memoized: undefined, stepType: type },
        stepOptions: options,
        input: takesInput ? args : [],
      });
      options = changed.stepOptions;
      // A step's place, and its idempotency key, come from its ID.
      if (stepId(options) !== id) throw new UsageError(`A middleware changes the ID of the step "${id}", which EventFlow doesn't allow.`);
      if (takesInput) args = changed.input;
      const given: any = { ...info, options, input: takesInput ? args : undefined };
      const tools = makeStep({ ...state, ctx: child, claimed });
      const call = name.split(".").reduce((t: any, k) => t[k], tools);
      const handler = takesInput
        ? (...a: any[]) => handledWith(state, given, (stepKeys.getStore()?.attempt ?? 0) + 1, () => fn(...a))
        : undefined;
      return hooks.wrap("wrapStep", { ctx: state.args, stepInfo: given }, () =>
        takesInput ? call(options, handler, ...args) : call(options, ...args),
      );
    };
    // An error out of a virtual context is made another: it's kept apart.
    let outcome: { ok: boolean; value?: any; error?: unknown };
    return eager(
      ctx.runInChildContext(
        id,
        async (child) => {
          try {
            outcome = { ok: true, value: await inner(child) };
          } catch (error) {
            if (isDurableInternal(error)) throw error;
            outcome = { ok: false, error };
          }
        },
        { virtualContext: true, key: `middleware ${claimed.id}` },
      ),
    ).then(() => {
      if (!outcome.ok) throw outcome.error;
      return outcome.value;
    });
  };
  const tools: Record<string, any> = { ...plain };
  for (const name of ["run", "sleep", "sleepUntil", "waitForEvent", "waitForSignal", "invoke", "sendEvent", "sendSignal"]) tools[name] = tool(name, name);
  tools.ai = { infer: tool("ai.infer", "ai.infer"), wrap: tool("ai.wrap", "ai.wrap") };
  Object.defineProperty(tools, "realtime", { get: unsupported("step.realtime") });
  return tools;
}

// A step that runs code for the function, with its hooks.
async function handledWith(state: RunState, info: ReturnType<Hooks["info"]>, attempt: number, handler: () => unknown) {
  const { hooks, def } = state;
  const arg = { ctx: state.args, stepInfo: { ...info, memoized: false } };
  await state.settled();
  await hooks.tell("onStepStart", arg);
  try {
    const output = await hooks.wrap("wrapStepHandler", arg, async () => handler());
    await hooks.tell("onStepComplete", { ...arg, output });
    return output;
  } catch (error) {
    await hooks.tell("onStepError", { ...arg, error, isFinalAttempt: retryDelay(error, attempt, def.retries, "") === undefined });
    throw error;
  }
}

/** The error an earlier attempt's step failed with. */
class FailedMemo {
  constructor(readonly error: { name: string; message: string }) {}
}

function escape(text: string) {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// How a wait finds its event. `match: "data.userId"` matches an event with the same
// value at that path. An `if` with `async.<path> == event.<path>` matches on that
// path too, and the rest of the `if` is checked when the event comes.
export function matcher(
  stepId: string,
  event: WireEvent,
  options: { match?: string; if?: string },
): { field: string; value?: string; condition?: Expression } {
  if (options.match) {
    const path = options.match.replace(/^(event|async)\./, "");
    const value = getPath(event, path.split("."));
    if (value === undefined)
      throw new UsageError(`The step "${stepId}" matches on "${path}", which the run's event doesn't have.`);
    return { field: path, value: JSON.stringify(value) };
  }
  if (options.if) {
    const parsed = parseExpression(options.if, `"if" of step "${stepId}"`);
    // A path of the run's event that it doesn't have never matches. Where the whole `if`
    // needs it, say so instead.
    for (const part of conjuncts(parsed.node)) {
      if (part.kind === "binary" && part.op === "||") continue;
      for (const path of paths(part))
        if (path[0] === "event" && getPath(event, path.slice(1)) === undefined)
          throw new UsageError(`The "if" of step "${stepId}" uses "${path.join(".")}", which the run's event doesn't have.`);
    }
    const expr = bind(parsed, "event", event);
    if (Buffer.byteLength(JSON.stringify(expr)) > MAX_CONDITION_BYTES)
      throw new UsageError(`The "if" of step "${stepId}" is over ${MAX_CONDITION_BYTES} bytes with the run's values in it.`);
    for (const c of conjuncts(expr.node)) {
      if (c.kind !== "binary" || c.op !== "==") continue;
      const [path, literal] =
        c.left.kind === "path" && c.left.path[0] === "async" && c.right.kind === "literal"
          ? [c.left.path, c.right.value]
          : c.right.kind === "path" && c.right.path[0] === "async" && c.left.kind === "literal"
            ? [c.right.path, c.left.value]
            : [undefined, undefined];
      // A key with a "." in it can't be written as one field: it's matched by the `if`.
      if (path && !path.some((p) => p.includes(".")))
        return { field: path.slice(1).join("."), value: JSON.stringify(literal), condition: expr };
    }
    return { field: "*", condition: expr };
  }
  return { field: "*" };
}

export function waitKey(name: string, tenant: string | undefined, field: string, value?: string) {
  return key("W", name, tenant ?? null, field, value ?? null);
}

/** Whether a run with a start timeout started, or was cancelled first. */
export function startKey(runId: string) {
  return key("M", runId);
}

export function signalKey(tenant: string | undefined, signal: string) {
  return key("G", tenant ?? null, signal);
}

export function recentKey(name: string, tenant: string | undefined) {
  return key("R", name, tenant ?? null);
}

// Registering a wait and routing an event each write first and read second:
//
//   wait:   add the field, note the time T, add the waiter, read events since T
//   router: note the time R, read the fields, store the event, read the waiters
//
// So for an event with R >= T, at least one of them sees the other. If the router
// sees the waiter, it resolves the callback. If the wait sees the event, it uses it
// and doesn't wait for the callback. An event with R < T came before the wait
// started, and isn't matched. When both see each other, the run goes on with the
// event the wait found, and the callback is left to time out.
async function listen(
  engine: Engine,
  input: {
    name: string;
    tenant?: string;
    field: string;
    value?: string;
    condition?: Expression;
    callbackId: string;
    runId: string;
    expiresAt: number;
    kind: "wait" | "cancel";
    parent?: string;
  },
): Promise<{ event: WireEvent | null; registered: { key: string; createdAt: number; callbackId: string } }> {
  await engine.store.addWaitField(input.name, input.tenant, input.field, input.expiresAt);
  const createdAt = Date.now();
  const k = waitKey(input.name, input.tenant, input.field, input.value);
  const waiter = {
    callbackId: input.callbackId,
    runId: input.runId,
    createdAt,
    expiresAt: input.expiresAt,
    condition: input.condition ? JSON.stringify(input.condition) : undefined,
    kind: input.kind,
    parent: input.parent,
    info: { name: input.name, tenant: input.tenant, field: input.field },
  };
  await engine.store.putWaiter(k, waiter);
  const registered = { key: k, createdAt, callbackId: input.callbackId };
  const recent = await engine.store.recent(recentKey(input.name, input.tenant), createdAt);
  for (const { event } of recent) {
    if (!eventMatches(event, input.field, input.value, waiter.condition)) continue;
    await engine.store.deleteWaiter(k, waiter).catch(() => {});
    return { event, registered };
  }
  return { event: null, registered };
}

export function eventMatches(event: WireEvent, field: string, value?: string, condition?: string) {
  if (field !== "*") {
    const actual = getPath(event, field.split("."));
    if (actual === undefined || JSON.stringify(actual) !== value) return false;
  }
  if (condition) {
    try {
      return evaluate(JSON.parse(condition) as Expression, { async: event });
    } catch {
      return false;
    }
  }
  return true;
}

function runIdFromArn(arn: string) {
  return arn.match(/\/durable-execution\/([^/]+)\//)?.[1];
}

/** Options of the client that the worker needs. */
export interface WorkerOptions {
  client?: unknown;
  middleware?: any[];
  logger?: any;
}

/**
 * The durable handler: start the function named in the payload.
 */
export function createWorker(registry: Map<string, Registered>, getEngine: () => Engine, options: WorkerOptions = {}) {
  return durable.withDurableExecution(async (payload: RunPayload, ctx: durable.DurableContext) => {
    // Only the router and the worker can sign a run. A payload from anything else, like
    // a direct invoke, is refused before any of it is used.
    if (!payload || payload["sst:eventflow"] !== 1 || !verify(payload))
      throw new NonRetriableError("The EventFlow worker only runs what its router starts.");
    const entry = registry.get(payload.fn);
    const engine = getEngine();
    if (!entry) {
      const message = `The function "${payload.fn}" isn't served by this version of the worker. Was it removed or renamed?`;
      // The run that invoked it would otherwise wait for it.
      if (payload.parent)
        await ctx.step("sst:return", async () => {
          await engine.callback(payload.parent!, { ok: false, error: { name: "InvokeError", message } } satisfies InvokeOutcome);
        });
      throw new NonRetriableError(message);
    }
    if (options.logger) ctx.configureLogger({ customLogger: options.logger });
    const { def } = entry;
    const arn = ctx.executionContext.durableExecutionArn;
    const runId = runIdFromArn(arn) ?? payload.runId;
    const ttlMs = (engine.resource.runTimeoutSeconds ?? MAX_WAIT_S) * 1000;

    // Tell the run that invoked this one how it ended. A run that's gone is fine; any
    // other error is retried, so the result isn't lost.
    const finish = async (outcome: InvokeOutcome) => {
      if (!payload.parent) return;
      await ctx.step("sst:return", async () => {
        const bytes = Buffer.byteLength(JSON.stringify(outcome));
        const answer: InvokeOutcome =
          bytes > MAX_ANSWER_BYTES
            ? {
                ok: false,
                error: {
                  name: "InvokeError",
                  message: `The run succeeded, and its result is ${bytes} bytes, more than the ${MAX_ANSWER_BYTES} an invoke returns. Store it in a bucket, and return its key.`,
                },
              }
            : outcome;
        await engine.callback(payload.parent!, answer);
      }, ENDING);
    };

    // Whichever is written first decides: that it started, or that it didn't in time.
    let starting: Promise<void> | undefined;
    const start = () =>
      (starting ??= (async () => {
        if (!def.timeouts) return;
        const item = startKey(payload.runId);
        const before = await engine.store.owner(item);
        if (before === "started") return;
        if (before === undefined) {
          // The timer first: one more of them than needed does no harm.
          if (def.timeouts.finishMs !== undefined)
            await engine.sendTimer(
              { type: "finish", fn: def.id, runId, arn, parent: payload.parent, tenant: payload.event.tenant },
              Date.now() + def.timeouts.finishMs,
            );
          if ((await engine.store.successor(item, "started", ttlMs)) === "started") return;
        }
        throw new CancelledError("The run didn't start in time.");
      })().catch((error) => {
        starting = undefined;
        throw error;
      }));

    // It has the run's event alone, until the batch is read.
    let events = [payload.event];
    let hooks = new Hooks([], options.client, {});
    let settled: Promise<void> | undefined;
    const tellRun = async (name: string, fn: () => Promise<void>) => {
      await ctx.step(name, fn, ENDING);
    };

    // Whether this invocation goes on from what's saved. The SDK's own logger can't tell, in a
    // virtual context.
    const replay: Replay = { on: Object.keys((ctx as any)._executionContext?._stepData ?? {}).length > 1 };
    const newState = (more: Partial<RunState>): RunState => ({
      ctx,
      engine,
      def,
      registry,
      runId,
      event: payload.event,
      memo: Object.assign(Object.create(null), payload.memo ?? {}),
      // The root is signed with the payload. A payload from before roots is its own.
      root: payload.root ?? payload.runId,
      namespace: "",
      counts: new Map(),
      at: payload.at,
      attempt: 0,
      done: Object.create(null),
      busy: new Set(),
      fence: { down: false },
      replay,
      used: new Set(),
      start,
      hooks,
      settled: async () => {},
      ...more,
    });

    const makeArgs = (forState: RunState, extra: Record<string, unknown> = {}) => {
      const args: Record<string, unknown> = {
        event: events[0],
        events,
        step: withMiddleware(forState),
        group: { parallel: <T>(fn: () => Promise<T>) => fn() },
        runId,
        logger: replayLogger(forState.ctx.logger as object, forState.replay, () => stepKeys.getStore() !== undefined),
        ...extra,
      };
      // Each step is tried at most this many times, as with Inngest.
      args.maxAttempts = def.retries + 1;
      // In a step, the step's attempt, and outside of one, the function's. From 0.
      Object.defineProperty(args, "attempt", {
        enumerable: true,
        get: () => stepKeys.getStore()?.attempt ?? forState.attempt,
      });
      // The key of the step that's running, for its side effect.
      args.idempotencyKey = (...given: unknown[]) => {
        if (given.length)
          throw new TypeError(
            "Call idempotencyKey() with no arguments, inside step.run: it's the key of that step, the same in every attempt, retry and replay.",
          );
        const current = stepKeys.getStore();
        if (!current) throw new TypeError("Call idempotencyKey() inside step.run: it's the key of that step.");
        return current.key;
      };
      // What EventFlow can't give throws when it's read, instead of being undefined.
      const missing: Record<string, string> = {
        defer: "EventFlow doesn't support `defer`.",
        sessions: "EventFlow doesn't support `sessions`.",
      };
      for (const [name, message] of Object.entries(missing))
        Object.defineProperty(args, name, {
          enumerable: false,
          get() {
            throw new UsageError(message);
          },
        });
      return args;
    };

    type Outcome = { ok: true; value: unknown } | { ok: false; error: unknown };
    let cancelKeys: { key: string; createdAt: number; callbackId: string }[] = [];

    // Everything the run does, up to how it ended. What it throws is the run's failure, so
    // it's ended like any other: its parent is told, and its singleton given back.
    const body = async (): Promise<Outcome> => {
      // A singleton's run that lost it to a later one, between being started and running.
      if (payload.lock) {
        const mine = await ctx.step("sst:singleton", async () => (await engine.store.lockRead(payload.lock!))?.run === payload.runId);
        if (!mine) throw new CancelledError("The run was cancelled by a later one, as it's a singleton.");
      }
      // One with a concurrency limit starts with its first lease.
      if (def.timeouts && !def.concurrency.length) await ctx.step("sst:started", async () => start());

      if (payload.batch) {
        const batch = payload.batch;
        events = await read("the run's batch", async () => engine.store.batchEvents(await engine.store.batchRead(batch.item, batch.n)));
        if (!events.length) throw new NonRetriableError("The run's batch is gone.");
      }
      hooks = new Hooks([...(options.middleware ?? []), ...def.middleware], options.client, { id: def.id, name: def.name, opts: { id: def.id } });

      // Listen for the events that cancel the run, before it does anything else.
      if (def.cancelOn.length) {
        const listening = await ctx.step("sst:cancelOn", async () => {
          const keys: typeof cancelKeys = [];
          let found = false;
          for (const c of def.cancelOn) {
            const { field, value, condition } = matcher("cancelOn", payload.event, c);
            const expiresAt = Date.now() + (c.timeoutMs ?? ttlMs);
            const heard = await listen(engine, {
              name: c.name,
              tenant: payload.event.tenant,
              field,
              value,
              condition,
              callbackId: `${STOP}${arn}`,
              runId,
              expiresAt,
              kind: "cancel",
              parent: payload.parent,
            });
            keys.push(heard.registered);
            found ||= !!heard.event;
          }
          return { keys, found };
        }, SETUP);
        cancelKeys = listening.keys;
        // An event that came as it registered cancels it now, as it would have later.
        if (listening.found) throw new CancelledError("The run was cancelled by an event.");
      }

      // An attempt of the function. It's in a virtual context, which costs no operation, so
      // an attempt that failed keeps its place in the history when it isn't run again.
      const attempt = async (n: number, memo: Record<string, unknown>) => {
        const state = newState({ memo, attempt: n });
        state.settled = () => (settled ??= hooks.tell("onMemoizationEnd", { ctx: state.args }));
        let outcome: Outcome | undefined;
        await ctx.runInChildContext(
          "sst:attempt",
          async (child) => {
            state.ctx = child;
            try {
              let args = makeArgs(state);
              state.args = args;
              if (hooks.any) {
                if (n === 0 && hooks.has("onRunStart")) await named(child, replay).step("sst:onRunStart", async () => hooks.tell("onRunStart", { ctx: args }));
                args = (await hooks.transform("transformFunctionInput", { ctx: args, steps: {} })).ctx;
                state.args = args;
              }
              const value = await hooks.wrap("wrapFunctionHandler", { ctx: args }, async () => entry.handler(args));
              if (hooks.any) await state.settled();
              outcome = { ok: true, value: jsonify(value) };
            } catch (error) {
              if (isDurableInternal(error)) throw error;
              outcome = { ok: false, error };
            }
          },
          { virtualContext: true },
        );
        return { state, outcome: outcome! };
      };

      // How earlier attempts failed. It's kept in the table before the wait for the next one,
      // so a replay doesn't run them again, where they might not fail.
      const failures = def.retries > 0 ? await read("how the run's attempts failed", () => engine.store.failures(payload.runId)) : [];
      const memo = Object.assign(Object.create(null), payload.memo ?? {}) as Record<string, unknown>;
      for (let n = 0; ; n++) {
        let failure = failures.find((f) => f.attempt === n);
        let state: RunState | undefined;
        let error: unknown;
        if (failure) {
          await ctx.runInChildContext("sst:attempt", async () => {}, { virtualContext: true });
          error = Object.assign(new Error(failure.error.message), { name: failure.error.name });
        } else {
          const tried = await attempt(n, { ...memo });
          if (tried.outcome.ok) return tried.outcome;
          ({ state } = tried);
          error = tried.outcome.error;
        }
        const final =
          !failure &&
          (error instanceof UsageError ||
            error instanceof StepFailedError ||
            error instanceof InvokeFailedError ||
            (error !== null && typeof error === "object" && fromStep.has(error)));
        const delayMs = failure ? failure.delayMs : final ? undefined : retryDelay(error, n + 1, def.retries, `${runId}\n${n}`);
        // From here its branches that go on start nothing more: the next attempt does.
        if (state && delayMs !== undefined) state.fence.down = true;
        // The same place in the history whether it's told now, or was before a replay.
        if (hooks.has("onRunError"))
          await ctx.step(`sst:onRunError:${n}`, async () =>
            hooks.tell("onRunError", { ctx: state?.args, error, isFinalAttempt: delayMs === undefined }),
          );
        if (delayMs === undefined) return { ok: false, error };
        if (!failure) {
          // Its steps in flight finish first, so the next attempt has their results too.
          while (state!.busy.size) await Promise.allSettled([...state!.busy]);
          log("warn", "The function failed outside a step. It's tried again.", { run: runId, attempt: n + 1, error: errorInfo(error).message });
          failure = { attempt: n, error: errorInfo(error), delayMs, memo: { ...state!.done } };
          const kept = failure;
          await read("how the run's attempt failed", () => engine.store.putFailure(payload.runId, kept, ttlMs + 86_400_000));
        }
        await ctx.wait(`sst:retry:${n + 1}`, toDurableSeconds(failure.delayMs));
        // What every attempt's steps returned, not just the last's: one that failed early
        // didn't come to all of them.
        for (const [id, m] of Object.entries(failure.memo)) memo[id] = "v" in m ? m.v : new FailedMemo(m.e);
      }
    };

    let outcome: Outcome;
    try {
      outcome = await body();
    } catch (error) {
      if (isDurableInternal(error)) throw error;
      outcome = { ok: false, error };
    }

    const isCancelled = !outcome.ok && (outcome.error instanceof CancelledError || errorInfo(outcome.error).name === CANCELLED);
    if (!outcome.ok && def.onFailure && !isCancelled) {
      const { error } = outcome;
      try {
        await ctx.runInChildContext("onFailure", async (child) => {
          const ts = await child.step("sst:failed-at", async () => Date.now());
          const state = newState({ ctx: child, memo: Object.create(null), namespace: "onFailure", start: async () => {} });
          const args = makeArgs(state, {
            error,
            event: {
              id: hash(`failed\n${runId}`),
              name: "inngest/function.failed",
              data: { event: payload.event, error: errorInfo(error), function_id: def.id, run_id: runId },
              ts,
            },
          });
          state.args = args;
          await def.onFailure!(args);
        });
      } catch (failure) {
        if (isDurableInternal(failure)) throw failure;
        log("error", "onFailure failed", { run: runId, error: errorInfo(failure).message });
      }
    }

    // Each of these is done whatever becomes of the others.
    const error = outcome.ok ? undefined : { ...errorInfo(outcome.error), ...(isCancelled ? { name: "Cancelled" } : {}) };
    const ending = async (what: string, fn: () => Promise<void>) => {
      try {
        await fn();
      } catch (failure) {
        if (isDurableInternal(failure)) throw failure;
        log("error", `Failed to ${what}`, { run: runId, error: errorInfo(failure).message });
      }
    };
    if (outcome.ok && hooks.has("onRunComplete")) {
      const output = outcome.value;
      await ending("tell the middleware", () => tellRun("sst:onRunComplete", () => hooks.tell("onRunComplete", { output })));
    }
    if (cancelKeys.length)
      await ending("stop listening", () =>
        tellRun("sst:cancelOn:end", async () => {
          await Promise.all(cancelKeys.map((k) => engine.store.deleteWaiter(k.key, k)));
        }),
      );
    // Give back the singleton, and send the events about the run.
    const ended = outcome.ok ? "completed" : isCancelled ? "cancelled" : "failed";
    const wanted = wantedSystemEvents(registry, engine, { event: payload.event, outcome: ended });
    if (payload.lock || wanted.length)
      await ending("end the run", () =>
        tellRun("sst:end", async () => {
          if (payload.lock) await engine.store.lockFree(payload.lock, payload.runId);
          if (wanted.length)
            await sendSystemEvents(registry, engine, {
              fn: def.id,
              runId: payload.runId,
              event: payload.event,
              events,
              outcome: ended,
              ...(outcome.ok ? { result: outcome.value } : { error }),
            });
        }),
      );
    await ending("tell the run that invoked it", () => finish(outcome.ok ? { ok: true, result: outcome.value } : { ok: false, error: error! }));
    if (!outcome.ok) {
      // It's cancelled, not failed, as it would be by an event that came later.
      if (isCancelled)
        await ctx.step("sst:stop", async () => void (await engine.stopRun(arn)), { retryStrategy: () => ({ shouldRetry: false }) }).catch(() => {});
      throw isCancelled ? Object.assign(new Error(error!.message), { name: "Cancelled" }) : outcome.error;
    }
    return outcome.value;
  });
}

/** What ends a run is tried for as long as an outage may last: what waits for it waits for days. */
const ENDING: durable.StepConfig<any> = {
  retryStrategy: (_error, attempt) => (attempt >= 30 ? { shouldRetry: false } : { shouldRetry: true, delay: { seconds: Math.min(300, 2 ** Math.min(attempt, 8)) } }),
};
/** A function that can't be set up, as for an event without a field, can't later. */
const SETUP: durable.StepConfig<any> = {
  retryStrategy: (error, attempt) =>
    attempt >= 10 || /doesn't have|isn't supported|bytes with the run's/.test(error.message) ? { shouldRetry: false } : { shouldRetry: true, delay: { seconds: Math.min(60, 2 ** attempt) } },
};

function isDurableInternal(error: unknown) {
  if ((error as any)?.isUnrecoverable === true) return true;
  const name = (error as any)?.constructor?.name ?? "";
  return /Unrecoverable|NonDeterministic|Termination/.test(name);
}
