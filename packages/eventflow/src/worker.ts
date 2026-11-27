import * as durable from "@aws/durable-execution-sdk-js";
import { AsyncLocalStorage } from "async_hooks";
import { createHash } from "crypto";
import { fat, fatValue, GoneError, slim, slimValue } from "./blobs.js";
import { led, link, unlinked, type Link } from "./chain.js";
import { Invocation } from "./invocation.js";
import { priorityMs, type Definition } from "./definition.js";
import { parseDuration, toDurableSeconds, type DurationInput } from "./duration.js";
import { arnKey, arnTtl, checkedData, checkEvent, checkSignal, mergeSessions, MESSAGE_TTL_MS, toWireEvent, type Engine, type SendPayload } from "./engine.js";
import { InvokeFailedError, NonRetriableError, RetryAfterError, SignalConflictError, StepFailedError } from "./errors.js";
import { bind, conjuncts, evaluate, getPath, parseExpression, paths, requiredPaths, type Expression } from "./expression.js";
import { callRunId, hash, key, runId as makeRunId } from "./ids.js";
import { Hooks } from "./middleware.js";
import { publish, type TopicRef } from "./realtime.js";
import { named, replayLogger, replayOf, type NamedContext, type Replay } from "./named.js";
import { Failed, Order, ORDER, placesOf, type Places } from "./order.js";
import { keyed, sign, verify } from "./sign.js";
import type { LeaseWaiter, Limit } from "./store.js";
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
export type InvokeOutcome = (
  /** A large result is in the table, or for another EventFlow, isn't returned. */
  | { ok: true; result?: unknown; blob?: string }
  | { ok: false; error: { name: string; message: string } }
) & {
  /** The run whose outcome it is. The one that invoked it takes no other's. */
  by?: string;
};

export { NonRetriableError, RetryAfterError, StepFailedError, InvokeFailedError, SignalConflictError } from "./errors.js";

// How many times a step's try may be started again after an invocation ended in it.
const RESTARTS = 3;
const RESTART_TTL_MS = 30 * 86_400_000;

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
/** A cancelOn heard for an event of a time to come, before its run starts. */
export const DUE = "due:";

/** Where a cancelOn heard for an event of a time to come is kept: for that send of it, by its ts. */
export function dueCancelKey(fn: string, event: { id: string; tenant?: string; ts: number }) {
  return key("DC", fn, event.tenant ?? null, event.id, event.ts);
}
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

/**
 * Read the table outside a step, for as long as the invocation lasts. What's gone, is.
 * Out of time, the invocation ends, and Lambda starts another that reads it: in the function
 * by waiting for that, as an error there would be the function's.
 */
async function read<T>(inv: Invocation, lambda: { getRemainingTimeInMillis?: () => number } | undefined, what: string, fn: () => Promise<T>, inFunction = true): Promise<T> {
  return inv.hold(async () => {
    for (let attempt = 0; ; attempt++) {
      try {
        return await fn();
      } catch (error) {
        if (error instanceof GoneError) throw error;
        const left = lambda?.getRemainingTimeInMillis?.() ?? 60_000;
        if (left < 5000 || attempt > 200) {
          if (inFunction) return never();
          throw new TryAgain(`Failed to read ${what}: ${errorInfo(error).message}`);
        }
        if (attempt === 5) log("warn", `Failed to read ${what}. It's tried again.`, { error: errorInfo(error).message });
        await new Promise((r) => setTimeout(r, Math.min(2000, 100 * 2 ** attempt)));
      }
    }
  });
}

/** Try a write of the table again, in a step, for up to a minute: an outage is as likely as not over by then. */
async function patient<T>(fn: () => Promise<T>): Promise<T> {
  const end = Date.now() + 60_000;
  for (let attempt = 0; ; attempt++) {
    try {
      return await fn();
    } catch (error) {
      if (Date.now() > end) throw error;
      await new Promise((r) => setTimeout(r, Math.random() * Math.min(4000, 100 * 2 ** attempt) + 50));
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
/** A wait that needs what the run's event doesn't have: it never matches. */
export class MissingPathError extends UsageError {}

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
const stepKeys = new AsyncLocalStorage<{ key: string; attempt: number; keyFor: (of: string | number | undefined) => string }>();

/** The step that's running, for tests. */
export const currentStep = () => stepKeys.getStore();

/**
 * A key for a step's side effect, like a payment provider's idempotency key. It comes
 * from the run's root, which a retry or replay of the run keeps, the function or
 * onFailure, and the step's ID, or its ID and what it's of. So it's the same in every
 * attempt, retry and replay of that step, and different for every other step and run.
 */
export function idempotencyKey(root: string, namespace: string, stepId: string, of?: string) {
  return `ef_${hash(JSON.stringify([root, namespace, stepId, ...(of === undefined ? [] : [of])]), 26)}`;
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
  /** What all its attempts share. */
  shared: Shared;
  /** The attempt has ended: its branches that go on start nothing more. */
  fence: { down: boolean };
  replay: Replay;
  inv: Invocation;
  /** The step names that are taken. */
  used: Set<string>;
  /** The order in which the function is given its steps' results. */
  order: Order;
  /** How many times each step ID is used. */
  uses: Map<string, number>;
  /** How many of each signal each step ID has sent. */
  sent: Map<string, number>;
  /** The last call of each step ID that's used, and whether the function has its result. */
  calls: Map<string, Call>;
  /** The step that was called last, as it's called. */
  calling?: Call;
  /** The function is used as it can't be: it fails, whatever it does about that. */
  refuse: (error: UsageError) => void;
  refused: Promise<never>;
  wrong?: UsageError;
  /** A step's place, where it's been claimed already. */
  claimed?: Claim;
  /** The sessions its events share, for what it sends. */
  sessions?: Record<string, string>;
  /** What it defers, by name. */
  defers: Map<string, Deferred>;
  deferCounts: Map<string, number>;
  deferUsed: Set<string>;
  /** Note that the run started, before its first step runs. */
  start: () => Promise<void>;
  hooks: Hooks;
  /** The context a hook gets. */
  args?: Record<string, unknown>;
  settled: () => Promise<void>;
}

/**
 * What a function's attempts share. Each finds the steps that are there, whatever came of
 * them: an attempt that's tried again is a replay of the last.
 */
interface Shared {
  places: Places;
  /** The steps that were begun in this invocation, and go on. */
  ops: Map<string, { kind: string; promise: Promise<any> }>;
  /** Their code, as the last attempt has it. */
  code: Map<string, { fn: (...args: any[]) => unknown; args: any[]; base: string; counts: Map<string, number> }>;
  /** How the function ended is decided: none of its steps start their code any more. */
  ended: boolean;
  /** Its steps of which the code runs now. */
  running: Set<Promise<unknown>>;
  started?: Promise<unknown>;
}

interface Deferred {
  fn: string;
  data: unknown;
  sessions?: Record<string, string>;
}

interface Call {
  link: Link;
  name: string;
  given: boolean;
  /** The ID it was called with. */
  base: string;
}

interface Claim {
  id: string;
  /** The ID it's called with. */
  base: string;
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
  const id = stepId(input);
  const { order } = state;
  const at = link(state);
  // Of which result it came, so the invocation needn't wait for more of that.
  state.inv.came(order.fresh ? order.grace : at.parent?.grace);
  const before = state.calls.get(id);
  // The results come in the same order each time, and so does what comes of them at once. Not
  // what comes of other work in the function, between its steps: which of two branches uses an
  // ID first is whichever's work is faster, and on a replay may be the other's. Then one would
  // get the other's result, and its idempotency key.
  if (before && !before.given && !order.fresh && !led(at, before.link)) {
    const error = new UsageError(
      `The step ID "${id}" is used in two branches that run at once, after work that isn't in a step, so which is which can't be told when the run goes on later. Do that work in a step, or give each an ID of its own, like \`${id}-\${item.id}\`.`,
    );
    state.refuse(error);
    throw error;
  }
  const call = { link: at, name: occurrence(state, id), base: id, given: false };
  at.name = call.name;
  // A step that an earlier attempt ended, on a path that the function didn't take again: its
  // result would be found by its ID, and could be another call's.
  if (state.shared.places.lost(call.name)) {
    const error = new UsageError(
      `The step "${call.name}" ended in an earlier attempt, which took another path, so whose result it is can't be told. Fix what made the path change, and replay the run. Keep the IDs of the steps that ran: their idempotency keys come from them.`,
    );
    state.refuse(error);
    throw error;
  }
  state.uses.set(id, (state.uses.get(id) ?? 0) + 1);
  state.calls.set(id, call);
  state.calling = call;
  return { id: call.name, base: id, first };
}

/** The sessions that all of a run's events are in. */
function sessionsOf(events: WireEvent[]): Record<string, string> | undefined {
  const [first, ...rest] = events;
  const shared = Object.entries(first?.sessions ?? {}).filter(([k, v]) => rest.every((e) => e.sessions?.[k] === v));
  return shared.length ? Object.fromEntries(shared) : undefined;
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
  const { inv } = state;
  const reading = <T>(what: string, fn: () => Promise<T>) => read(inv, ctx.lambdaContext, what, fn);
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

  // An error that comes out of a step was tried again already.
  const record = <T>(promise: Promise<T>): Promise<T> =>
    promise.catch((error) => {
      if (!isDurableInternal(error) && error && typeof error === "object") fromStep.add(error);
      throw error;
    });
  // A step of the run this one is a retry of.
  const earlier = (id: string, _again = false): Promise<any> | undefined => {
    const saved = memo(id);
    if (!saved.hit) return undefined;
    const { value } = saved;
    return value instanceof BlobMemo ? reading("the result", () => fatValue(engine, { blob: value.blob })) : Promise.resolve(value);
  };
  // A step that an earlier attempt of the function began is the same step.
  const op = <T>(id: string, kind: string, begin: () => Promise<T>): Promise<T> => {
    const { ops } = state.shared;
    const before = ops.get(id);
    const was = before?.kind ?? ctx.kind(id);
    if (was !== undefined && was !== kind) {
      const error = new UsageError(`The step "${id}" was another kind of step before. Give it an ID of its own.`);
      state.refuse(error);
      throw error;
    }
    if (before) return before.promise;
    state.shared.places.callers.set(id, state.attempt);
    const promise = begin();
    promise.catch(() => {});
    ops.set(id, { kind, promise });
    return promise;
  };

  const stepOp = <T>(id: string, kind: string, begin: () => Promise<T>) => record(op(id, kind, begin));

  const leaseMs = () => Math.max(30_000, (ctx.lambdaContext?.getRemainingTimeInMillis?.() ?? 900_000) + 10_000);
  // A wake that throws may not have reached the step: the store keeps it in line.
  const wake = (w: LeaseWaiter) =>
    engine.callback(w.callbackId, { leaseId: w.leaseId, placed: w.placed }).catch((error) => {
      log("warn", "Failed to wake a step in line. It stays in line.", { run: w.runId, error: errorInfo(error).message });
      throw error;
    });

  // Give back a step's leases, and hand them to the steps in line. An error here
  // mustn't fail the step, which would run its code again: the leases expire.
  // False when it couldn't: then it's done in a step of its own, for as long as it takes. Not
  // here: the step's work is done, and its result isn't saved until this ends.
  const giveBack = async (leaseId: string) => {
    try {
      // Straight to the next step in line, where there's one.
      await engine.store.handoff(limits, leaseId, tenant, wake);
      return true;
    } catch (error) {
      log("warn", "Failed to give back a lease. It's tried again.", { run: state.runId, error: errorInfo(error).message });
      return false;
    }
  };
  const giveBackAfter = (child: durable.DurableContext, name: string, leaseId: string) =>
    // For as long as the lease may last: after that it has expired. The step's work is done,
    // or wasn't begun: it isn't failed for this.
    child.step(name, async () => engine.store.handoff(limits, leaseId, tenant, wake), RELEASING).then(
      () => {},
      (error) => {
        if (isDurableInternal(error)) throw error;
        log("warn", "Failed to give back a lease. It expires on its own.", { run: state.runId, error: errorInfo(error).message });
      },
    );

  // Take a lease on every limit at once, or wait in line until a release can give the
  // step all of them. A step never holds one lease while it waits for another, so two
  // steps can't wait for each other.
  // `place.joined` is when the step first joined the line, so asking again, or after
  // a lease was lost, keeps its place. It comes from a step's result, so a replay has
  // the same.
  const takeLeases = async (child: durable.DurableContext, leaseId: string, tag: string, place: { joined?: number }, again: boolean): Promise<string[] | undefined> => {
    for (let round = 0; ; round++) {
      const t = `${tag}.${round}`;
      const got = await child.step(`lease:${t}`, async (c) => {
        // It may have asked before it was saved, where the worker stopped.
        const first = round === 0 && c.attempt === 1 && !again;
        const result = await engine.store.acquire(limits, leaseId, tenant, leaseMs(), first).catch((error) => {
          log("warn", "Failed to ask for a lease. Joining the line.", { run: state.runId, error: errorInfo(error).message });
          return "full" as const;
        });
        // When it asked, for its place in line. It's this step's result, so joining the
        // line again after an error is joining in the same place, not twice.
        return { granted: result === "granted", at: Date.now() };
      }, LEASING);
      if (got.granted) return undefined;
      // Runs that came first go first, so what's begun is finished. A priority moves it.
      place.joined ??= (state.at ?? got.at) - priority;
      const [woken, callbackId] = await child.createCallback<{ leaseId: string; placed?: string[] }>(`queued:${t}`, {
        timeout: { seconds: leaseWaitSeconds(round) },
        serdes: json as any,
      });
      // Join the line, then hand out what's free: the leases may have freed up in
      // between. If they're the step's, it goes on without waiting.
      const mine = await child.step(`enqueue:${t}`, async (c) => {
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
        const result = await engine.store.enqueue(waiter, round === 0 && c.attempt === 1);
        // A step that goes on without its callback closes it. An entry of it may be left in
        // line, put back by one that couldn't give it its leases as another did: given others
        // later, it would be told, and they'd be held for no one.
        const close = () => engine.callback(callbackId, { leaseId }).catch(() => false);
        if (result === "held") {
          await close();
          return { placed: undefined };
        }
        let mine: { placed?: string[] } | undefined;
        for (const limit of limits)
          await engine.store.dispatch(limit, async (w) => {
            if (w.callbackId !== callbackId) return wake(w);
            mine = { placed: w.placed };
            await close();
            return true;
          }, waiter);
        return mine ?? null;
      }, LEASING);
      if (mine) return mine.placed;
      try {
        return (await woken)?.placed;
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
  // What a step does, once it's begun: nothing once the function's end is decided.
  const acting = <T>(fn: () => Promise<T>): Promise<T> => {
    if (state.shared.ended) return Promise.reject(new NonRetriableError("The function ended before this step ran."));
    const work = fn();
    const over = () => void state.shared.running.delete(work);
    state.shared.running.add(work);
    work.then(over, over);
    return work;
  };
  const inStep = async <T>(id: string, attempt: number, fn: () => T | Promise<T>, begun = false): Promise<T> => {
    // The invocation has ended, and goes on in a Lambda that's been thawed: the next one runs it.
    if (inv.over) return never();
    await state.start();
    if (inv.over) return never();
    // A step that was begun before, in an invocation that ended, as one longer than the worker's
    // timeout, or a crash. AWS runs it again as the same attempt, so a step that can't fit would
    // run, and do what it does, until the run times out. Each time is counted: after a few, it
    // fails as a try that timed out, and is retried as any other.
    if (begun) {
      let n = 1;
      // By the invocation, so a write whose answer was lost, and is sent again, is this one's.
      const me = ctx.lambdaContext?.awsRequestId ?? crypto.randomUUID();
      while (n <= RESTARTS && !(await patient(() => engine.store.claim(key("SR", state.runId, state.namespace, id, attempt, n), me, RESTART_TTL_MS)))) n++;
      if (n > RESTARTS)
        throw Object.assign(
          new Error(`The step "${id}" was started ${RESTARTS + 1} times without finishing: it doesn't fit in one invocation of the worker. Give the worker a longer invocation timeout, or split the step.`),
          { name: "StepTimedOut" },
        );
    }
    // It was waiting, for its next try or its lease, as the function failed. What's done about
    // that, like a refund, may have been done.
    if (state.shared.ended) throw new NonRetriableError("The function ended before this step ran.");
    const { base, counts } = state.shared.code.get(id) ?? { base: id, counts: state.uses };
    const keyOf = (of: string | number | undefined) => {
      if (of !== undefined) return idempotencyKey(state.root, state.namespace, base, String(of));
      // Which use of an ID is whose can be another in a replay, where a step before it fails, or
      // a list has changed. Then one would be paid for with another's key.
      if ((counts.get(base) ?? 0) > 1)
        throw new NonRetriableError(
          `The step ID "${base}" is used more than once, so its idempotency key can be another use's in a replay. Say what it's of: idempotencyKey(order.id).`,
        );
      return keyFor(id);
    };
    const work = stepKeys.run({ key: keyFor(id), attempt: attempt - 1, keyFor: keyOf }, async () => fn());
    const over = () => void state.shared.running.delete(work);
    state.shared.running.add(work);
    work.then(over, over);
    return work;
  };

  const delayOf = (id: string, attempt: number) => (error: unknown) =>
    retryDelay(error, attempt, def.retries, `${state.runId}\n${id}\n${attempt}`);
  // A step's last error is saved with its place in the order, as a result is.
  const failing = async <T>(id: string, delay: (error: unknown) => number | undefined, fn: () => Promise<T>): Promise<T> => {
    try {
      return await fn();
    } catch (error) {
      if (isDurableInternal(error) || delay(error) !== undefined) throw error;
      throw state.shared.places.failed(id, error);
    }
  };
  const retrying = (id: string) => (error: unknown, attempt: number) => {
    const delay = error instanceof Failed ? undefined : delayOf(id, attempt)(error);
    return delay === undefined ? { shouldRetry: false } : { shouldRetry: true, delay: toDurableSeconds(delay) };
  };

  const run = <T>(idInput: StepInput, fn: (...args: any[]) => T | Promise<T>, ...args: any[]): Promise<T> => {
    if (state.fence.down) return never();
    const { id, base } = claim(state, idInput);
    const saved = earlier(id, !atMostOnce(idInput));
    if (saved) return saved as Promise<T>;
    state.shared.code.set(id, { fn, args, base, counts: state.uses });
    return stepOp(id, limits.length ? "EventFlowStep" : atMostOnce(idInput) ? ONCE : "STEP", () => runStep<T>(id, idInput));
  };

  const runStep = <T>(id: string, idInput: StepInput): Promise<T> => {
    const code = () => {
      const { fn, args } = state.shared.code.get(id)!;
      return fn(...args);
    };
    // At most once: AWS saves that the step started before it runs. If the worker
    // stops part way, the step fails instead of running again. It isn't retried.
    const once = atMostOnce(idInput);
    if (!limits.length) {
      if (once)
        return eager(
          ctx.step(id, (c) => failing(id, () => undefined, async () => jsonify(await inStep(id, c.attempt, code))), {
            semantics: durable.StepSemantics.AtMostOncePerRetry,
            retryStrategy: () => ({ shouldRetry: false }),
            subType: ONCE,
            serdes: state.shared.places.serdes(id),
          }),
        ).catch((error) => {
          throw isInterruption(error) ? new StepFailedError(id, interrupted(id)) : error;
        }) as Promise<T>;
      // Its try was started before this invocation, which ended in it: its code is run again.
      // Not one that waits for its next try.
      const begun = ctx.status(id) === "STARTED";
      return eager(
        ctx.step(id, (c) => failing(id, delayOf(id, c.attempt), async () => jsonify(await inStep(id, c.attempt, code, begun))), {
          retryStrategy: retrying(id),
          serdes: state.shared.places.serdes(id),
        }),
      ) as Promise<T>;
    }

    // With a concurrency limit, each attempt takes its leases first, and gives them
    // back when it ends. A step that waits for a lease, or for its next attempt, holds
    // none, as with Inngest.
    const again = ctx.known(id);
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
              const leaseId = `${hash(`${state.runId}\n${state.namespace}\n${id}`, 12)}.${attempt}.${lost}`;
              const placed = await takeLeases(child, leaseId, `${attempt}.${lost}`, place, again);
              // The run step was started before this invocation, which ended in it.
              const raw = child as any;
              const begun = raw._executionContext?.getStepData?.(raw.peekStepId?.())?.Status === "STARTED";
              const outcome = await child.step(
                attempt === 1 && lost === 0 ? "run" : `run:${attempt}.${lost}`,
                async () => {
                  // After a crash, the run step runs again, maybe after its leases
                  // expired. Check they're still held before the work starts.
                  // One that can't be checked, as when DynamoDB keeps failing, counts as
                  // lost: the step takes a new one, and its code hasn't run.
                  if (inv.over) return never();
                  const held = await patient(() => engine.store.renew(limits, leaseId, leaseMs(), false, placed)).catch((error) => {
                    log("warn", "Failed to renew a lease. Taking a new one.", { run: state.runId, error: errorInfo(error).message });
                    return false;
                  });
                  if (!held) return { lost: true as const };
                  let outcome: { ok: true; value?: any; blob?: string; kept?: true } | { ok: false; error: { name: string; message: string }; delay: number | undefined; kept?: true };
                  try {
                    const value = jsonify(await inStep(id, attempt, code, begun));
                    // In its context's result it's with more: a large one is kept apart.
                    outcome = { ok: true, ...(await patient(() => slimValue(engine, `step\n${state.runId}\n${state.namespace}\n${id}`, value)).catch(() => ({ value }))) };
                  } catch (error) {
                    const delay = once ? undefined : retryDelay(error, attempt, def.retries, `${state.runId}\n${id}\n${attempt}`);
                    outcome = { ok: false, error: errorInfo(error), delay };
                  }
                  if (!(await giveBack(leaseId))) outcome.kept = true;
                  return outcome;
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
                  await giveBackAfter(child, `release:${attempt}.${lost}`, leaseId);
                  return {
                    ok: false as const,
                    error: isInterruption(error) ? interrupted(id) : errorInfo(error),
                    delay: undefined,
                  };
                },
              );
              if ("lost" in outcome) {
                // A lease expired before the step ran. Give back the rest.
                await giveBackAfter(child, `release:${attempt}.${lost}`, leaseId);
                if (lost >= 5) return { ok: false as const, error: { name: "LeaseError", message: `The step "${id}" kept losing its lease.` } };
                continue;
              }
              if ("kept" in outcome && outcome.kept) await giveBackAfter(child, `release:${attempt}.${lost}`, leaseId);
              if (outcome.ok) return { ok: true as const, ...("blob" in outcome ? { blob: outcome.blob } : { value: outcome.value }) };
              if (outcome.delay === undefined) return { ok: false as const, error: outcome.error };
              await child.wait(`retry:${attempt}`, toDurableSeconds(outcome.delay));
              break;
            }
          }
        },
        { subType: "EventFlowStep", serdes: state.shared.places.serdes(id), summaryGenerator: state.shared.places.summary(id) },
      ),
    ).then((outcome: any) => {
      // An error thrown in a child context comes out as a ChildContextError, so the
      // outcome comes out of it, and the error is thrown here.
      if (!outcome.ok) throw new StepFailedError(id, outcome.error);
      return outcome.blob ? (reading("the result", () => fatValue(engine, outcome)) as Promise<T>) : (outcome.value as T);
    });
  };

  // A function with a concurrency limit starts with its first step that has a lease, or here,
  // with a first step that needs none.
  const begin = (first: boolean) => {
    if (!limits.length || !def.timeouts || !first || state.namespace) return;
    state.shared.started ??= eager(ctx.step("sst:started", async () => state.start())).catch(() => {});
  };
  const sleep = (idInput: string | { id: string }, duration: DurationInput) => {
    if (state.fence.down) return never();
    const { id, first } = claim(state, idInput);
    // A sleep called on another result than before is slept again.
    if (earlier(id, true)) return Promise.resolve();
    begin(first);
    const seconds = bounded(parseDuration(duration, `duration of step "${id}"`), `sleep of step "${id}"`);
    return op(id, "WAIT", () => eager(ctx.wait(id, seconds)));
  };

  const sleepUntil = (idInput: string | { id: string }, time: Date | string) => {
    if (state.fence.down) return never();
    const { id, first } = claim(state, idInput);
    // A sleep called on another result than before is slept again.
    if (earlier(id, true)) return Promise.resolve();
    begin(first);
    const at = time instanceof Date ? time.getTime() : Date.parse(time);
    if (!Number.isFinite(at)) throw new UsageError(`The time of step "${id}" isn't a date.`);
    const seconds = bounded(at - Date.now(), `sleep of step "${id}"`);
    return op(id, "WAIT", () => eager(ctx.wait(id, seconds)));
  };

  // An event with its data, where that's large and in the table.
  const whole = (e: any) => (e?.blob ? reading("the event's data", () => fat(engine, e)) : e);

  const waitForEvent = (
    idInput: string | { id: string },
    options: { event: string | { name: string }; timeout: DurationInput | Date; match?: string; if?: string },
  ): Promise<WireEvent | null> => {
    if (state.fence.down) return never();
    const { id, first } = claim(state, idInput);
    const saved = earlier(id);
    if (saved) return saved.then(whole);
    begin(first);
    const name = typeof options.event === "string" ? options.event : options.event?.name;
    if (!name) throw new UsageError(`The step "${id}" waits for an event with no name.`);
    if (options.match && options.if) throw new UsageError(`The step "${id}" has both "match" and "if". Use one.`);
    const timeoutMs =
      options.timeout instanceof Date
        ? options.timeout.getTime() - Date.now()
        : parseDuration(options.timeout, `timeout of step "${id}"`);
    const timeout = bounded(timeoutMs, `timeout of step "${id}"`);
    // One that needs what the run's event doesn't have never matches, and times out with null.
    let m: ReturnType<typeof matcher> | undefined;
    try {
      m = matcher(id, state.event, options);
    } catch (error) {
      if (!(error instanceof MissingPathError)) throw error;
      log("warn", "A wait can't match, and waits for its timeout", { run: state.runId, step: id, reason: error.message.slice(0, 200) });
    }
    const { field, value, condition } = m ?? { field: "", value: undefined, condition: undefined };

    return op(id, "EventFlowWait", () => eager(
      ctx.runInChildContext(
        id,
        async (child) => {
          const [received, callbackId] = await child.createCallback<WireEvent>("event", { timeout, serdes: json as any });
          // The wait may find an event that came while it was registering. Then it uses
          // that event, and doesn't wait for the callback.
          const early = await child.step("listen", async () => {
            if (!m) return undefined;
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
        { subType: "EventFlowWait", serdes: state.shared.places.serdes(id), summaryGenerator: state.shared.places.summary(id) },
      ),
    ).then((e: any) => {
      // Only its event answers it.
      if (e !== null && e?.name !== name) throw new UsageError(`The step "${id}" was answered by something that isn't the event "${name}".`);
      return whole(e);
    }) as Promise<WireEvent | null>);
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
    try {
      checkSignal(options);
    } catch (error) {
      throw new UsageError((error as Error).message);
    }
    if (options.onConflict !== undefined && options.onConflict !== "fail" && options.onConflict !== "replace")
      throw new UsageError(`The step "${id}" has onConflict "${options.onConflict}". Use "fail" or "replace".`);
    const timeoutMs =
      options.timeout instanceof Date ? options.timeout.getTime() - Date.now() : parseDuration(options.timeout, `timeout of step "${id}"`);
    const timeout = bounded(timeoutMs, `timeout of step "${id}"`);
    const item = signalKey(state.event.tenant, options.signal);
    return op(id, "EventFlowSignal", () => eager(
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
        { subType: "EventFlowSignal", serdes: state.shared.places.serdes(id), summaryGenerator: state.shared.places.summary(id) },
      ),
    ).then((outcome: any) => {
      if (outcome.conflict) throw new SignalConflictError(options.signal);
      // Only its signal answers it.
      if (outcome.value !== null && outcome.value?.signal !== options.signal)
        throw new UsageError(`The step "${id}" was answered by something that isn't the signal "${options.signal}".`);
      return outcome.value;
    }));
  };

  const invoke = (
    idInput: string | { id: string },
    options: {
      function: string | { id: string } | { opts?: { id?: string; functionId?: string; appId?: string } };
      data?: unknown;
      meta?: SendPayload["meta"];
      timeout?: DurationInput | Date;
      /** What it's of, like an order's ID: the invoked run's idempotency keys come from it. */
      idempotencyKey?: string | number;
    },
  ) => {
    if (state.fence.down) return never();
    const { id, base, first } = claim(state, idInput);
    const of = options?.idempotencyKey;
    if (of !== undefined && (typeof of === "number" ? !Number.isFinite(of) : typeof of !== "string" || !of))
      throw new UsageError(`The idempotencyKey of step "${id}" is what the run it invokes is of, like an order's ID.`);
    // Which use of an ID is which can be another in a retry or a replay, where a list changed, and
    // then the run it invokes would have another's keys.
    if (of === undefined && (state.uses.get(base) ?? 0) > 1) {
      const error = new UsageError(
        `The step ID "${base}" invokes more than once, so the run it invokes could have another's idempotency keys in a replay. Say what each is of: step.invoke(id, { ..., idempotencyKey: order.id }). The run it invoked first has its keys from the step's ID, and a retry or replay after the change gives it others: check it first.`,
      );
      state.refuse(error);
      throw error;
    }
    const saved = earlier(id);
    if (saved) return saved;
    begin(first);
    const fn = options.function as any;
    const target = typeof fn === "string" ? fn.replace(new RegExp(`^${escape(engine.appId)}-`), (prefix) => (state.registry.has(fn) ? prefix : "")) : (fn?.id ?? fn?.opts?.id ?? fn?.opts?.functionId);
    if (!target) throw new UsageError(`The step "${id}" has no function to invoke.`);
    const sessions = mergeSessions(engine.propagateSessions ? state.sessions : undefined, options.meta);
    const app = fn?.opts?.appId !== undefined && fn.opts.appId !== engine.appId ? String(fn.opts.appId) : undefined;
    if (app !== undefined && !engine.apps[app])
      throw new UsageError(`The step "${id}" invokes a function of the app "${app}". Give it to the client, as "apps: { ${JSON.stringify(app)}: Resource.… }", and link it.`);
    if (app === undefined && !state.registry.has(target))
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

    const identity = `invoke\n${state.runId}\n${state.namespace}\n${id}`;

    try {
      checkEvent({ name: "inngest/function.invoked", ...(sessions ? { sessions } : {}) }, "event", true);
    } catch (error) {
      throw new UsageError((error as Error).message);
    }
    return stepOp(id, "EventFlowInvoke", () => eager(
      ctx.runInChildContext(
        id,
        async (child) => {
          const [result, callbackId] = await child.createCallback<InvokeOutcome>("result", { timeout, serdes: json as any });
          // The invoked run goes through the router, like any other, so its flow
          // control applies. The message is signed, so only a run can send one.
          await child.step("start", () => acting(async () => {
            // The child run is new for each run that invokes it, as a retry of the
            // parent needs its own. Its root, which its idempotency keys come from,
            // is the same for every retry and replay of the parent.
            const root = hash(JSON.stringify(of === undefined ? [state.root, state.namespace, id] : [state.root, state.namespace, base, String(of)]), 26);
            const event: WireEvent = {
              id: hash(identity),
              name: "inngest/function.invoked",
              // As JSON, as it's sent and signed: a Date is its string.
              data: jsonify(options.data ?? {}),
              ts: Date.now(),
              ...(state.event.tenant ? { tenant: state.event.tenant } : {}),
              ...(sessions ? { sessions } : {}),
            };
            if (app !== undefined) {
              await engine.sendMessages([{ message: { kind: "call", fn: target, event, parent: callbackId }, group: event.tenant }], engine.apps[app].queueUrl);
              return {};
            }
            const childRunId = makeRunId(target, identity);
            await engine.sendMessages([
              {
                message: sign({
                  kind: "invoke" as const,
                  fn: target,
                  event: await slim(engine, event),
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
          }));
          // The run whose answer this is: no one else's is taken.
          const by =
            app === undefined ? makeRunId(target, identity) : callRunId(target, { id: hash(identity), tenant: state.event.tenant }, callbackId);
          try {
            const answer = await result;
            if (answer?.by === by) return answer;
            return {
              ok: false,
              error: { name: "InvokeError", message: `The step "${id}" was answered by something that isn't the run it invoked.` },
            } satisfies InvokeOutcome;
          } catch (error) {
            if (error instanceof durable.CallbackTimeoutError)
              return {
                ok: false,
                error: { name: "InvokeError", message: `The function "${target}" didn't finish within the timeout of step "${id}".` },
              } satisfies InvokeOutcome;
            throw error;
          }
        },
        { subType: "EventFlowInvoke", serdes: state.shared.places.serdes(id), summaryGenerator: state.shared.places.summary(id) },
      ),
    ).then((outcome: InvokeOutcome) => {
      if (!outcome.ok) throw new InvokeFailedError(outcome.error.message, target);
      return outcome.blob ? reading("the result", () => fatValue(engine, { blob: outcome.blob })) : outcome.result;
    }));
  };

  const sendEvent = (idInput: string | { id: string }, payload: SendPayload | SendPayload[]) => {
    if (state.fence.down) return never();
    const { id, first } = claim(state, idInput);
    const saved = earlier(id);
    if (saved) return saved as Promise<{ ids: string[] }>;
    begin(first);
    // Each event's ID from the run's root, the step and its place in what's sent, before a
    // middleware changes the list: one it adds has its ID by its own place among those added.
    const payloads = Array.isArray(payload) ? payload : [payload];
    const list = payloads.map((p, i) => ({ ...p, id: p.id ?? hash(JSON.stringify([state.root, state.namespace, id, i]), 26) }));
    // Each is checked against its type's schema, as flow.send() does, and given what it makes of
    // the data. One that doesn't match fails the step, without retries.
    const checked = async () => {
      for (const [i, p] of payloads.entries())
        try {
          list[i].data = await checkedData(p);
        } catch (error) {
          throw new NonRetriableError((error as Error).message);
        }
    };
    const send = async (events: any[]) => {
      let added = 0;
      // IDs come from the run's root and the step, so a retried step, and a retry
      // or replay of the run, send the same events, and the router skips the ones
      // it has seen.
      const wire = events.map((p, i) =>
        toWireEvent({
          ...p,
          // From the run's root and the step, as the step is in the run, so a retried step, a crash
          // and a retry or replay of the run send the same events, and the router skips the ones
          // it has seen.
          id: p.id ?? hash(JSON.stringify([state.root, state.namespace, id, "added", added++]), 26),
          tenant: p.tenant ?? state.event.tenant,
        }, undefined, engine.propagateSessions ? state.sessions : undefined),
      );
      await engine.sendEvents(wire);
      return { ids: wire.map((e) => e.id) };
    };
    return stepOp(
      id,
      "EventFlowSend",
      () => eager(
        ctx.step(
          id,
          (c) => failing(id, delayOf(id, c.attempt), () => acting(async () => {
            await checked();
            if (!state.hooks.any) return send(list);
            const info = state.hooks.info(id, undefined, "sendEvent", undefined, false);
            return handledWith(state, info, c.attempt, async () => {
              const { events } = await state.hooks.transform("transformSendEvent", { events: list.map((e) => ({ ...e })) });
              return state.hooks.wrap("wrapSendEvent", { events }, () => send(events));
            }) as Promise<{ ids: string[] }>;
          })),
          { retryStrategy: retrying(id), serdes: state.shared.places.serdes(id), subType: "EventFlowSend" },
        ),
      ),
    );
  };

  const sendSignal = (idInput: string | { id: string }, options: { signal: string; data?: unknown }) => {
    if (state.fence.down) return never();
    const { id, base, first } = claim(state, idInput);
    // Which signal of the ID this is, by the signal, so a retry where a list changed finds each
    // one's own record. One whose result is reused counts too, so the next isn't taken for it.
    const sk = `${base}\n${options?.signal}`;
    const nth = state.sent.get(sk) ?? 0;
    state.sent.set(sk, nth + 1);
    const saved = earlier(id);
    if (saved) return saved as Promise<{ runId: string | undefined }>;
    begin(first);
    try {
      checkSignal(options);
    } catch (error) {
      throw new UsageError((error as Error).message);
    }
    return stepOp(
      id,
      "EventFlowSignalSend",
      () => eager(
        ctx.step(id, () => acting(async () => {
          const item = signalKey(state.event.tenant, options.signal);
          const wait = await engine.store.signalRead(item);
          // The wait it found first, so a try that comes after the run waits again doesn't
          // answer that too.
          // By the run's root: a retry or replay of the run doesn't tell another wait, nor one that
          // began after it was first sent.
          const ttl = ((engine.resource.runTimeoutSeconds ?? MAX_WAIT_S) + 106 * 86_400) * 1000;
          const record = key("GS", state.root, state.namespace, base, item, nth);
          const to = await engine.store.successor(record, wait ? JSON.stringify([wait.callbackId, wait.runId]) : "", ttl);
          if (!to) return { runId: undefined };
          const [callbackId, runId] = JSON.parse(to) as [string, string];
          // How a try before this one went, if it said: told, or not.
          if (await engine.store.owner(`${record}\ntold`)) return { runId };
          if (await engine.store.owner(`${record}\nnot`)) return { runId: undefined };
          // A try before this one may have told it, and stopped before it said so. If the wait it
          // found is gone, or the run waits again, it was told, by that one.
          const tried = !!(await engine.store.owner(`${record}\ntrying`));
          if (wait && wait.callbackId !== callbackId) return { runId: tried ? runId : undefined };
          if (!tried) await engine.store.claim(`${record}\ntrying`, "trying", ttl);
          const told = (await engine.callback(callbackId, { signal: options.signal, data: options.data ?? null }).catch(async (error) => {
            // Not told, for sure: a try after this doesn't take it that this one did.
            if (!tried && error?.notResumed) await engine.store.unclaim(`${record}\ntrying`, "trying");
            throw error;
          })) || tried;
          // Kept, so a try after this, or a replay, says the same.
          await engine.store.claim(`${record}\n${told ? "told" : "not"}`, "1", ttl);
          await engine.store.signalFree(item, callbackId);
          return { runId: told ? runId : undefined };
        }), { serdes: state.shared.places.serdes(id), subType: "EventFlowSignalSend" }),
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

  const realtime = {
    publish: (idInput: StepInput, ref: TopicRef, data: unknown) =>
      run(idInput, () => {
        if (!engine.realtime) throw new UsageError("step.realtime.publish() needs a Realtime component: link one to the EventFlow.");
        return publish(engine.realtime.resource, engine.realtime.prefix, ref, data, { runId: state.runId, fnId: def.id }, engine.aws);
      }),
  };
  const tools: Record<string, any> = { run, sleep, sleepUntil, waitForEvent, waitForSignal, invoke, sendEvent, sendSignal, fetch: stepFetch, ai, realtime };
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
    const calling = state.calling;
    const id = stepId(idInput);
    const info = hooks.info(id, typeof idInput === "object" ? idInput.name : undefined, type);
    const inner = async (child: durable.DurableContext) => {
      let options: any = { ...(typeof idInput === "object" ? idInput : { id: idInput }) };
      const takesInput = name === "run" || name === "ai.wrap";
      const [fn, ...input] = takesInput ? rest : [undefined];
      // Only a step that runs code has an input.
      let args = takesInput ? input : rest;
      const changed = await state.inv.hold(() =>
        hooks.transform("transformStepInput", {
          stepInfo: { hashedId: info.hashedId, memoized: undefined, stepType: type },
          stepOptions: options,
          input: takesInput ? args : [],
        }),
      );
      options = changed.stepOptions;
      // A step's place, and its idempotency key, come from its ID.
      if (stepId(options) !== id) throw new UsageError(`A middleware changes the ID of the step "${id}", which EventFlow doesn't allow.`);
      if (takesInput) args = changed.input;
      const given: any = { ...info, options, input: takesInput ? args : undefined };
      const tools = makeStep({ ...state, ctx: child, claimed, calling });
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
  tools.ai = { infer: tool("ai.infer", "ai.infer"), wrap: tool("ai.wrap", "ai.wrap"), get models() { return unsupported("step.ai.models")(); } };
  tools.realtime = { publish: tool("realtime.publish", "realtime.publish") };
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

/** Where an earlier run's large result is. */
class BlobMemo {
  constructor(readonly blob: string) {}
}

/** The step tools, as a function gets them: the invocation is told as they're called, and as they answer. */
function told(state: RunState, tools: Record<string, any>): Record<string, any> {
  const tell = (fn: any) => {
    const called = (...args: any[]) => {
      // A step in a step isn't one: it would be found in no history.
      if (stepKeys.getStore()) throw new UsageError("A step can't be called in another step. Call it in the function.");
      state.calling = undefined;
      const answer = fn(...args);
      const call = state.calling as Call | undefined;
      if (!call || typeof answer?.then !== "function") return answer;
      return state.order.give(call.name, answer, (grace) => {
        call.given = true;
        call.link.grace = grace;
      });
    };
    return Object.assign(called, { config: fn.config ? () => called : undefined });
  };
  const out: Record<string, any> = {};
  for (const [name, tool] of Object.entries(tools)) {
    if (typeof tool === "function") out[name] = tell(tool);
    else {
      const group: Record<string, any> = {};
      for (const key of Object.keys(tool)) if (key !== "models") group[key] = tell(tool[key]);
      if (name === "ai") Object.defineProperty(group, "models", { get: unsupported("step.ai.models") });
      out[name] = group;
    }
  }
  return out;
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
      throw new MissingPathError(`The step "${stepId}" matches on "${path}", which the run's event doesn't have.`);
    return { field: path, value: JSON.stringify(value) };
  }
  if (options.if) {
    const parsed = parseExpression(options.if, `"if" of step "${stepId}"`);
    // A path of the run's event that it doesn't have never matches. Where the whole `if`
    // needs it, say so instead.
    for (const part of conjuncts(parsed.node)) {
      // What it reads whatever the values: a branch, as `a || b` or `has(x) ? a : b`, may not.
      for (const path of requiredPaths(part))
        if (path[0] === "event" && getPath(event, path.slice(1)) === undefined)
          throw new MissingPathError(`The "if" of step "${stepId}" uses "${path.join(".")}", which the run's event doesn't have.`);
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

/** That how a run ended is decided: an event that cancels it doesn't stop it any more. */
export function endedKey(runId: string) {
  return key("D", runId);
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
export async function listen(
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
    /** From when an event counts, if not from now. */
    since?: number;
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
  const recent = await engine.store.recent(recentKey(input.name, input.tenant), Math.min(createdAt, input.since ?? createdAt));
  for (const { event: kept } of recent) {
    const event = await fat(engine, kept);
    if (!eventMatches(event, input.field, input.value, waiter.condition)) continue;
    await engine.store.deleteWaiter(k, waiter).catch(() => {});
    // A run has it now: its data is kept as long as the run's.
    return { event: kept.blob ? await slim(engine, event) : kept, registered };
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
          await engine.callback(payload.parent!, { ok: false, error: { name: "InvokeError", message }, by: payload.runId } satisfies InvokeOutcome);
        });
      throw new NonRetriableError(message);
    }
    if (options.logger) ctx.configureLogger({ customLogger: options.logger });
    const { def } = entry;
    const arn = ctx.executionContext.durableExecutionArn;
    const runId = runIdFromArn(arn) ?? payload.runId;
    const ttlMs = (engine.resource.runTimeoutSeconds ?? MAX_WAIT_S) * 1000;

    const inv = Invocation.of(ctx);
    // Whether this invocation goes on from what's saved. The SDK's own logger can't tell, in a
    // virtual context.
    const replay: Replay = replayOf(ctx);
    // Its ARN, where the router may not have kept it, as one that crashed as it started it, so
    // it's found without listing runs. Once, as it begins.
    if (!replay.on && arn) await engine.store.claim(arnKey(payload.runId), arn, arnTtl(engine)).catch(() => {});
    // The run's own operations, by name too, so one that's only sometimes there doesn't move
    // the rest.
    const root = named(ctx, replay);

    // Tell the run that invoked this one how it ended. A run that's gone is fine; any
    // other error is retried, so the result isn't lost.
    const finish = async (outcome: InvokeOutcome) => {
      if (!payload.parent) return;
      await root.step("sst:return", async () => {
        let answer = outcome;
        if (outcome.ok) {
          // A large result is kept for the run that invoked it. One of another EventFlow has
          // no way to read it.
          const kept = await slimValue(engine, `result\n${payload.runId}`, outcome.result);
          if (!("blob" in kept)) answer = outcome;
          else if (payload.external)
            answer = Buffer.byteLength(JSON.stringify(outcome)) > MAX_ANSWER_BYTES
              ? { ok: false, error: { name: "InvokeError", message: "The run succeeded, and its result is more than an invoke of another EventFlow returns. Store it in a bucket, and return its key." } }
              : outcome;
          else answer = { ok: true, blob: kept.blob };
        }
        await engine.callback(payload.parent!, { ...answer, by: payload.runId });
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
    let event = payload.event;
    let hooks = new Hooks([], options.client, {});
    let settled: Promise<void> | undefined;
    const tellRun = async (name: string, fn: () => Promise<void>) => {
      await root.step(name, fn, ENDING);
    };

    // What its functions deferred. Each time the function is replayed it defers the same.
    const defers = new Map<string, Deferred>();
    let shared = sessionsOf(events);
    // What a retry was given of the run it retries.
    const given = () => {
      const memo: Record<string, unknown> = Object.assign(Object.create(null), payload.memo ?? {});
      for (const [id, blob] of Object.entries(payload.memoBlobs ?? {})) memo[id] = new BlobMemo(blob);
      // Only those it had in order: after the first step it runs again, the function may take
      // another path, and a result found by its ID could be another call's.
      if (payload.memoOrder) {
        const ordered = new Set(payload.memoOrder);
        for (const id of Object.keys(memo)) if (!ordered.has(id)) delete memo[id];
      }
      return memo;
    };
    const placesIn = placesOf(ctx);
    const sharedOf = (scope: string, earlier?: string[]): Shared => ({ places: placesIn(scope, earlier), ops: new Map(), code: new Map(), ended: false, running: new Set() });
    const newState = (more: Partial<RunState> & { shared: Shared }): RunState => {
      const state: RunState = {
        ctx,
        engine,
        def,
        registry,
        runId,
        event,
        memo: given(),
        // The root is signed with the payload. A payload from before roots is its own.
        root: payload.root ?? payload.runId,
        namespace: "",
        counts: new Map(),
        at: payload.at,
        attempt: 0,
        fence: { down: false },
        replay,
        inv,
        used: new Set(),
        calls: new Map(),
        uses: new Map(),
        sent: new Map(),
        sessions: shared,
        defers,
        deferCounts: new Map(),
        deferUsed: new Set(),
        start,
        hooks,
        settled: async () => {},
        refuse: () => {},
      refused: undefined as any,
        order: undefined as any,
        ...more,
      };
      state.refused = new Promise<never>((_, reject) => {
        state.refuse = (error) => {
          state.wrong ??= error;
          state.fence.down = true;
          reject(error);
        };
      });
      state.refused.catch(() => {});
      const { places } = state.shared;
      places.attempt = state.attempt;
      state.order = new Order(
        inv,
        places,
        (name, place) => named(state.ctx, replay).step(name, async () => ({ [ORDER]: place() }), LEASING),
        () => ctx.lambdaContext?.getRemainingTimeInMillis?.() ?? 900_000,
        (id) => state.refuse(new UsageError(`The function doesn't call the step "${id}", which it called before. Was it changed as it ran, or does its path depend on more than its event and its steps, like the time?`)),
      );
      return state;
    };

    const makeArgs = (forState: RunState, extra: Record<string, unknown> = {}) => {
      const args: Record<string, unknown> = {
        event: events[0],
        events,
        step: told(forState, withMiddleware(forState)),
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
      args.idempotencyKey = (of?: string | number) => {
        if (of !== undefined && (typeof of === "number" ? !Number.isFinite(of) : typeof of !== "string" || !of))
          throw new TypeError("idempotencyKey() takes what the step's side effect is of, like an order's ID, or nothing.");
        const current = stepKeys.getStore();
        if (!current) throw new TypeError("Call idempotencyKey() inside step.run: it's the key of that step.");
        return current.keyFor(of);
      };
      if (forState.sessions) args.sessions = forState.sessions;
      if (def.deferred) args.parents = events.map((e) => ({ fnSlug: e.from?.fn ?? "", runId: e.from?.runId ?? "" }));
      // Start a deferred function once this run has ended.
      args.defer = (id: string, input: { function: any; data?: unknown; meta?: SendPayload["meta"] }) => {
        if (stepKeys.getStore()) throw new UsageError(`defer("${id}") can't be called in a step. Call it in the function.`);
        const target = input?.function?.id ?? input?.function?.opts?.id;
        if (!target || !registry.get(target)?.def.deferred)
          throw new UsageError(`defer("${id}") needs a function made by createDefer(), and served by this EventFlow.`);
        const schema = (input.function as any).schema?.["~standard"];
        const checked = schema?.validate(input.data);
        if (checked && typeof checked.then === "function") throw new UsageError(`defer("${id}") needs a schema that checks at once.`);
        if (checked?.issues) throw new UsageError(`The data of defer("${id}") doesn't match its schema: ${checked.issues.map((i: any) => i.message).join("; ")}`);
        const sessions = mergeSessions(engine.propagateSessions ? forState.sessions : undefined, input.meta);
        try {
          checkEvent({ name: "inngest/deferred.schedule", ...(sessions ? { sessions } : {}) }, "event", true);
        } catch (error) {
          throw new UsageError((error as Error).message);
        }
        if (forState.fence.down) return { abort: () => {} };
        const data = jsonify(checked ? checked.value : (input.data ?? {}));
        const name = occurrence({ counts: forState.deferCounts, used: forState.deferUsed } as any, stepId(id));
        defers.set(`${forState.namespace}\n${name}`, {
          fn: target,
          data,
          sessions,
        });
        return { abort: () => void defers.delete(`${forState.namespace}\n${name}`) };
      };
      return args;
    };

    type Outcome = { ok: true; value: unknown } | { ok: false; error: unknown };
    let cancelKeys: { key: string; createdAt: number; callbackId: string }[] = [];

    // The run's events, and its middleware.
    const load = async () => {
      if (payload.batch) {
        const batch = payload.batch;
        let refs = 0;
        events = await read(inv, ctx.lambdaContext, "the run's batch", async () => {
          const found = await engine.store.batchRead(batch.item, batch.n);
          refs = found.length;
          return engine.store.batchEvents(found);
        }, false);
        // Part of it isn't the run it was: it's said, not run on what's left.
        if (!events.length || events.length < refs) throw new NonRetriableError(`The run's batch is gone: ${refs - events.length} of its ${refs} events have expired.`);
      }
      if (events.some((e) => e.blob))
        events = await read(inv, ctx.lambdaContext, "the run's event", () => Promise.all(events.map((e) => fat(engine, e))), false);
      event = events[0];
      shared = sessionsOf(events);
      hooks = new Hooks([...(options.middleware ?? []), ...def.middleware], options.client, { id: def.id, name: def.name, opts: { id: def.id } });
    };

    // Everything the run does, up to how it ended. What it throws is the run's failure, so
    // it's ended like any other: its parent is told, and its singleton given back.
    const body = async (): Promise<Outcome> => {
      // A singleton's run that lost it to a later one, between being started and running.
      if (payload.lock) {
        const mine = await root.step("sst:singleton", async () => (await engine.store.lockRead(payload.lock!))?.run === payload.runId, LEASING);
        if (!mine) throw new CancelledError("The run was cancelled by a later one, as it's a singleton.");
      }
      // One with a concurrency limit starts with its first lease.
      if (def.timeouts && !def.concurrency.length) await root.step("sst:started", async () => start(), STARTING);
      await load();

      // Listen for the events that cancel the run, before it does anything else.
      if (def.cancelOn.length) {
        const listening = await root.step(
          "sst:cancelOn",
          async () => {
            const keys: typeof cancelKeys = [];
            let found = false;
            for (const c of def.cancelOn) {
              // One that matches on what the run's event doesn't have, as a cron run's, never
              // cancels it, as with Inngest: the run goes on.
              let m: ReturnType<typeof matcher>;
              try {
                m = matcher("cancelOn", event, c);
              } catch (error) {
                if (!(error instanceof MissingPathError)) throw error;
                log("warn", "A cancelOn can't match this run, which goes on", { fn: def.id, cancelOn: c.name, reason: error.message.slice(0, 200) });
                continue;
              }
              const { field, value, condition } = m;
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
            // One that was heard for its event before it was due, as it started, cancels it too.
            // Not a retry or replay of it, which starts after that, and isn't cancelled by it.
            if (!payload.replayOf) found ||= (await engine.store.owner(dueCancelKey(def.id, payload.event))) === "cancelled";
            return { keys, found };
          },
          SETUP,
        );
        // An event that came as it registered cancels it now, as it would have later.
        if (listening.found) throw new CancelledError("The run was cancelled by an event.");
      }

      return attempts(root, {
        namespace: "",
        shared: sharedOf("", payload.memoOrder?.filter((id) => id in (payload.memo ?? {}) || id in (payload.memoBlobs ?? {}))),
        run: async (state, child) => {
          // What an attempt that failed deferred, the next does again, if it comes to it.
          defers.clear();
          state.settled = () => (settled ??= hooks.tell("onMemoizationEnd", { ctx: state.args }));
          let args = makeArgs(state);
          state.args = args;
          if (hooks.any) {
            if (state.attempt === 0 && hooks.has("onRunStart")) await named(child, replay).step("sst:onRunStart", async () => hooks.tell("onRunStart", { ctx: args }));
            args = (await hooks.transform("transformFunctionInput", { ctx: args, steps: {} })).ctx;
            state.args = args;
          }
          const value = await hooks.wrap("wrapFunctionHandler", { ctx: args }, () => guarded(state, () => entry.handler(args)));
          if (hooks.any) await state.settled();
          return value;
        },
        // At once, in a step: an event that cancels it from here on doesn't stop what's done about
        // how it ended, like onFailure's refund, as the code of its last steps ends.
        decide: () => root.step("sst:decided", async () => engine.store.successor(endedKey(payload.runId), "ended", ttlMs + 86_400_000), LEASING),
        failed: async (n, error, last, state) => {
          if (hooks.has("onRunError"))
            await root.step(`sst:onRunError:${n}`, async () => hooks.tell("onRunError", { ctx: state?.args, error, isFinalAttempt: last }));
        },
      });
    };

    // The function's own code, up to where it's used as it can't be, whatever it does about that.
    const guarded = async (state: RunState, fn: () => unknown) => Promise.race([unlinked(() => state.order.begin(async () => fn())), state.refused]);

    // The code of steps that runs now, until it ends, and they're saved. False if it doesn't.
    const quiet = async (shared: Shared, ms: number) => {
      const end = Date.now() + ms;
      while (shared.running.size) {
        const left = end - Date.now();
        if (left <= 0) return false;
        let timer: any;
        const late = new Promise((r) => (timer = setTimeout(r, left)));
        await inv.hold(() => Promise.race([Promise.allSettled([...shared.running]), late]));
        clearTimeout(timer);
        // What comes of it, like its save, begins.
        await new Promise((r) => setImmediate(r));
      }
      return true;
    };

    // A function, or an onFailure, tried again where it throws outside of its steps. Each
    // attempt is in the same virtual context, which costs no operation, so it finds the steps
    // of the last: it's a replay of it, that goes on where the last threw.
    const attempts = async (
      at: NamedContext,
      options: {
        namespace: string;
        shared: Shared;
        more?: Partial<RunState>;
        run: (state: RunState, child: durable.DurableContext) => Promise<unknown>;
        failed?: (n: number, error: unknown, last: boolean, state: RunState | undefined) => Promise<void>;
        /** How it ended is decided: "stopping" if the router said first that it stops the run. */
        decide?: () => Promise<string>;
      },
    ): Promise<Outcome> => {
      const { shared, namespace } = options;
      for (let n = 0; ; n++) {
        const FAILED = `sst:failed:${n}`;
        let failure: { error: { name: string; message: string }; delayMs: number } | undefined;
        let state: RunState | undefined;
        let error: unknown;
        // How it failed is saved, so a replay doesn't run it again, where it might not fail.
        if (at.saved(FAILED)) {
          failure = (await at.step(FAILED, async () => failure!)) as NonNullable<typeof failure>;
          error = Object.assign(new Error(failure.error.message), { name: failure.error.name });
        } else {
          state = newState({ attempt: n, namespace, shared, ...options.more });
          const current = state;
          let outcome: Outcome | undefined;
          await at.runInChildContext(
            "sst:attempt",
            async (child) => {
              current.ctx = child;
              try {
                const value = await options.run(current, child);
                // Also where something was done about it.
                if (current.fence.down) await current.refused;
                outcome = { ok: true, value: jsonify(value) };
              } catch (failed) {
                if (isDurableInternal(failed)) throw failed;
                outcome = { ok: false, error: current.wrong ?? failed };
              }
            },
            { virtualContext: true, key: "attempt" },
          );
          // It has ended, however it did: its branches that go on start nothing more.
          state.fence.down = true;
          state.order.close();
          if (outcome!.ok) {
            shared.ended = true;
            if (options.decide && (await options.decide()) === "stopping") return { ok: false, error: new CancelledError("The run was stopped.") };
            // The worker stops as the run ends: code that runs would be cut off.
            const left = (ctx.lambdaContext?.getRemainingTimeInMillis?.() ?? 900_000) - 15_000;
            if (!(await quiet(shared, Math.min(600_000, Math.max(0, left)))))
              log("warn", "The code of a step the function didn't wait for still runs, and is cut off.", { run: runId });
            return outcome!;
          }
          error = (outcome as { error: unknown }).error;
        }
        const final =
          !failure &&
          (error instanceof UsageError ||
            error instanceof StepFailedError ||
            error instanceof InvokeFailedError ||
            (error !== null && typeof error === "object" && fromStep.has(error)));
        let delayMs = failure ? failure.delayMs : final ? undefined : retryDelay(error, n + 1, def.retries, `${runId}\n${namespace}${n}`);
        if (!failure) {
          // Nothing of the function starts any more if it isn't tried again, and what runs ends,
          // before what's done about its failure.
          const decide = async () => {
            shared.ended = true;
            if (options.decide && (await options.decide()) === "stopping") error = new CancelledError("The run was stopped.");
          };
          if (delayMs === undefined) await decide();
          if (!(await quiet(shared, 60_000)) && delayMs !== undefined) {
            log("warn", "A step of the function didn't end after the function failed, so it isn't tried again.", { run: runId });
            delayMs = undefined;
            await decide();
          }
        }
        if (!failure && delayMs !== undefined) {
          const info = errorInfo(error);
          log("warn", "The function failed outside a step. It's tried again.", { run: runId, attempt: n + 1, error: info.message });
          const kept = { error: { name: info.name.slice(0, 256), message: info.message.slice(0, 8192) }, delayMs };
          await at.step(FAILED, async () => kept, LEASING);
        }
        await options.failed?.(n, error, delayMs === undefined, state);
        if (delayMs === undefined) return { ok: false, error };
        await at.wait(`sst:retry:${n + 1}`, toDurableSeconds(delayMs));
      }
    };

    // How the run ended is saved before anything is done about it: a replay doesn't run the
    // function again, where it might end in another way.
    const OUTCOME = "sst:outcome";
    const after = (o: Outcome) =>
      !!payload.parent ||
      def.cancelOn.length > 0 ||
      !!payload.lock ||
      defers.size > 0 ||
      hooks.has("onRunComplete") ||
      (!o.ok && !!def.onFailure) ||
      wantedSystemEvents(registry, engine, { event: payload.event, outcome: o.ok ? "completed" : "failed" }).length > 0;
    let outcome: Outcome;
    const wasSaved = root.saved(OUTCOME);
    if (wasSaved) {
      // Its events, for what's told of it. Without them it's still ended.
      await load().catch((error) => {
        if (isDurableInternal(error)) throw error;
      });
      outcome = { ok: true, value: undefined };
    } else {
      try {
        outcome = await body();
      } catch (error) {
        if (isDurableInternal(error)) throw error;
        outcome = { ok: false, error };
      }
    }
    if (wasSaved || after(outcome) || (!outcome.ok && errorInfo(outcome.error).name === CANCELLED)) {
      const ended = outcome;
      const saved = (await root.step(
        OUTCOME,
        async () => {
          // First, so an event that cancels it from now on doesn't stop what's done about it.
          // The router stops a run only if it says it does first: one or the other.
          const decided = await engine.store.successor(endedKey(payload.runId), "ended", ttlMs + 86_400_000);
          // The router said first that it stops the run: it's cancelled, as it would have been a
          // moment before, and nothing's done about how it ended.
          if (decided === "stopping") return { ok: false, stopped: true, error: { name: CANCELLED, message: "The run was stopped." }, defers: [] };
          return {
          ...(ended.ok ? { ok: true, ...(await slimValue(engine, `outcome\n${payload.runId}`, ended.value)) } : { ok: false, error: errorInfo(ended.error) }),
          defers: await Promise.all(
            [...defers].map(async ([name, d]) => [name, { ...d, data: undefined, ...(await slimValue(engine, `defer\n${payload.runId}\n${name}`, d.data)) }] as const),
          ),
        };
        },
        LEASING,
      )) as any;
      if (wasSaved || saved.stopped) {
        defers.clear();
        for (const [name, d] of saved.defers)
          defers.set(name, { fn: d.fn, sessions: d.sessions, data: await read(inv, ctx.lambdaContext, "what it deferred", () => fatValue(engine, d), false) });
        outcome = saved.ok
          ? { ok: true, value: await read(inv, ctx.lambdaContext, "the run's result", () => fatValue(engine, saved), false) }
          : { ok: false, error: Object.assign(new Error(saved.error.message), { name: saved.error.name }) };
      }
      if (def.cancelOn.length && root.saved("sst:cancelOn")) cancelKeys = (await root.step("sst:cancelOn", async () => ({ keys: [] as typeof cancelKeys, found: false }))).keys;
    }

    const endingFirst = async (what: string, fn: () => Promise<void>) => {
      try {
        await fn();
      } catch (failure) {
        if (isDurableInternal(failure)) throw failure;
        log("error", `Failed to ${what}`, { run: runId, error: errorInfo(failure).message });
      }
    };
    // How it ended is decided: an event that cancels it doesn't stop what's done about that, like
    // onFailure's refund, or a parent being told.
    if (cancelKeys.length)
      await endingFirst("stop listening", () =>
        tellRun("sst:cancelOn:end", async () => {
          await Promise.all(cancelKeys.map((k) => engine.store.deleteWaiter(k.key, k)));
        }),
      );
    const isCancelled = !outcome.ok && (outcome.error instanceof CancelledError || errorInfo(outcome.error).name === CANCELLED);
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
    // Give back the singleton, and send the events about the run.
    const ended = outcome.ok ? "completed" : isCancelled ? "cancelled" : "failed";
    const wanted = wantedSystemEvents(registry, engine, { event: payload.event, outcome: ended });
    const end = async () => {
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
    };
    // A failure is told at once, as Inngest's onFailure is a function of its own: the run that
    // invoked it, and the events about it, don't wait for what's done about it.
    const early = !outcome.ok && !!def.onFailure && !isCancelled;
    if (early) await end();
    if (!outcome.ok && def.onFailure && !isCancelled) {
      const { error } = outcome;
      try {
        // What onFailure defers is saved with it, so a replay that doesn't run it again still has them.
        const before = new Set(defers.keys());
        const failed = await root.runInChildContext("onFailure", async (child) => {
          const ts = await child.step("sst:failed-at", async () => Date.now());
          const done = await attempts(named(child, replay), {
            namespace: "onFailure",
            shared: sharedOf("onFailure"),
            more: { memo: Object.create(null), start: async () => {} },
            run: (state) => {
              const args = makeArgs(state, {
                error,
                event: {
                  id: hash(`failed\n${runId}`),
                  name: "inngest/function.failed",
                  data: { event, error: errorInfo(error), function_id: engine.appId ? `${engine.appId}-${def.id}` : def.id, run_id: runId },
                  ts,
                },
              });
              state.args = args;
              return guarded(state, () => def.onFailure!(args));
            },
          });
          const deferred = await Promise.all(
            [...defers].filter(([name]) => !before.has(name)).map(async ([name, d]) => [name, { ...d, data: undefined, ...(await slimValue(engine, `defer\n${payload.runId}\n${name}`, d.data)) }] as const),
          );
          return { deferred, ...(done.ok ? {} : { error: errorInfo(done.error) }) };
        });
        for (const [name, d] of (failed as any)?.deferred ?? [])
          if (!defers.has(name))
            defers.set(name, { fn: d.fn, sessions: d.sessions, data: await read(inv, ctx.lambdaContext, "what onFailure deferred", () => fatValue(engine, d), false) });
        if ((failed as any)?.error) log("error", "onFailure failed", { run: runId, error: (failed as any).error.message });
      } catch (failure) {
        if (isDurableInternal(failure)) throw failure;
        log("error", "onFailure failed", { run: runId, error: errorInfo(failure).message });
      }
    }

    if (outcome.ok && hooks.has("onRunComplete")) {
      const output = outcome.value;
      await ending("tell the middleware", () => tellRun("sst:onRunComplete", () => hooks.tell("onRunComplete", { output })));
    }
    // Start what it deferred, unless it was cancelled.
    if (defers.size && !isCancelled)
      await ending("start what it deferred", () =>
        tellRun("sst:defer", async () => {
          const messages = await Promise.all(
            [...defers].map(async ([name, d]) => {
              const id = keyed(JSON.stringify(["defer", payload.runId, name]));
              const childId = makeRunId(d.fn, `defer\n${payload.runId}\n${name}`);
              return {
                message: sign({
                  kind: "start" as const,
                  fn: d.fn,
                  event: await slim(engine, {
                    id,
                    name: "inngest/deferred.schedule",
                    data: d.data,
                    ts: Date.now(),
                    from: { fn: def.id, runId: payload.runId },
                    ...(payload.event.tenant ? { tenant: payload.event.tenant } : {}),
                    ...(d.sessions ? { sessions: d.sessions } : {}),
                  }),
                  at: Date.now(),
                  runId: childId,
                  root: hash(JSON.stringify([payload.root ?? payload.runId, "defer", name]), 26),
                  exp: Date.now() + MESSAGE_TTL_MS,
                }),
                group: payload.event.tenant,
              };
            }),
          );
          await engine.sendMessages(messages);
        }),
      );
    if (!early) await end();
    if (!outcome.ok) {
      // It's cancelled, not failed, as it would be by an event that came later.
      if (isCancelled)
        await root.step("sst:stop", async () => void (await engine.stopRun(arn)), { retryStrategy: () => ({ shouldRetry: false }) }).catch(() => {});
      throw isCancelled ? Object.assign(new Error(error!.message), { name: "Cancelled" }) : outcome.error;
    }
    return outcome.value;
  });
}

/** What it takes to run a step is tried for as long as an outage may last: it isn't the step's failure. */
const LEASING: durable.StepConfig<any> = {
  retryStrategy: (_error, attempt) => (attempt >= 100 ? { shouldRetry: false } : { shouldRetry: true, delay: { seconds: Math.min(60, 2 ** Math.min(attempt, 6)) } }),
};
const RELEASING: durable.StepConfig<any> = {
  retryStrategy: (_error, attempt) => (attempt >= 8 ? { shouldRetry: false } : { shouldRetry: true, delay: { seconds: Math.min(60, 2 ** attempt) } }),
};
/** Whether it started is read and written until it's known. */
const STARTING: durable.StepConfig<any> = {
  retryStrategy: (error, attempt) => (error?.name === CANCELLED || /didn't start in time/.test(error?.message ?? "") ? { shouldRetry: false } : LEASING.retryStrategy!(error, attempt)),
};
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
