import { parseCron, type Cron } from "./cron.js";
import { parseDuration, type DurationInput } from "./duration.js";
import {
  compileKey,
  evaluate,
  evaluateValue,
  parseExpression,
  type Expression,
  type KeyInput,
} from "./expression.js";

// A function's options, checked and normalized when `createFunction()` runs, so a
// mistake or an option EventFlow doesn't support fails when the module loads, not
// when an event comes.

export interface ConcurrencyOption {
  limit: number;
  key?: KeyInput;
  scope?: "fn" | "env" | "account";
}

export interface FunctionOptions {
  id: string;
  name?: string;
  description?: string;
  triggers?: TriggerInput | TriggerInput[];
  retries?: number;
  concurrency?: number | ConcurrencyOption | ConcurrencyOption[];
  throttle?: { limit: number; period: DurationInput; burst?: number; key?: KeyInput };
  rateLimit?: { limit: number; period: DurationInput; key?: KeyInput };
  idempotency?: KeyInput;
  /**
   * Wait for the events to stop: the run starts once none came for `period`, with the last
   * one. With `timeout`, at the latest that long after the first. Each key, and each
   * tenant, has its own wait.
   */
  debounce?: { period: DurationInput; timeout?: DurationInput; key?: KeyInput };
  /**
   * Start one run for many events, in `events`: up to `maxSize`, or those that came in
   * `timeout`. Each key, and each tenant, has its own batch. An event that `if` isn't true
   * for starts a run by itself.
   */
  batchEvents?: { maxSize: number; timeout: DurationInput; key?: KeyInput; if?: string };
  /**
   * Have one run at a time, of each key and each tenant. While it runs, `"skip"` skips
   * another, and `"cancel"` cancels the one that's running and starts the other. A key that
   * is `false` isn't a singleton.
   */
  singleton?: { mode: "skip" | "cancel"; key?: KeyInput };
  /**
   * Which of the runs that wait for a concurrency limit go first. `run` is how many seconds
   * ahead of its turn, from -600 to 600.
   */
  priority?: { run?: string };
  /**
   * Cancel a run that hasn't started within `start` of its event, as behind a throttle or
   * a concurrency limit, or hasn't finished within `finish` of starting.
   */
  timeouts?: { start?: DurationInput; finish?: DurationInput };
  /** What wraps the function and its steps. See the guide. */
  middleware?: MiddlewareClass[];
  onFailure?: (...args: any[]) => unknown;
  cancelOn?: CancelOnInput[];
  /** Accepted for Inngest compatibility. Steps are always checkpointed. */
  checkpointing?: unknown;
  /** Accepted for Inngest compatibility. It has no effect. */
  optimizeParallelism?: unknown;
}

/** A middleware class, as Inngest's `Middleware.BaseMiddleware`. */
export type MiddlewareClass = new (args: { client: any }) => any;

export interface CancelOnInput {
  /** The event that cancels the run. */
  event: string | { name: string };
  /** Cancel only for an event with the same value at this path as the run's event, like "data.orderId". */
  match?: string;
  /** Cancel only when this is true. `event` is the run's event, and `async` the one that came. */
  if?: string;
  /** Stop listening after this long. By default, until the run ends. */
  timeout?: DurationInput;
}

export interface CancelOn {
  name: string;
  match?: string;
  if?: string;
  timeoutMs?: number;
}

export type TriggerInput =
  | { event: string | { name: string }; if?: string }
  | { cron: string; jitter?: DurationInput }
  | { invoke: true };

export type Trigger =
  | { kind: "event"; name: string; if?: Expression }
  | { kind: "cron"; cron: Cron; jitterMs?: number }
  | { kind: "invoke" };

export interface Limit {
  limit: number;
  scope: "fn" | "env" | "account";
  key?: (scope: Record<string, unknown>) => string | undefined;
  keySource?: string;
}

export interface Bucket {
  intervalMs: number;
  toleranceMs: number;
  key?: (scope: Record<string, unknown>) => string | undefined;
}

export interface Definition {
  id: string;
  name: string;
  triggers: Trigger[];
  retries: number;
  concurrency: Limit[];
  throttle?: Bucket;
  rateLimit?: Bucket;
  idempotency?: (scope: Record<string, unknown>) => string | undefined;
  onFailure?: (...args: any[]) => unknown;
  cancelOn: CancelOn[];
  debounce?: { periodMs: number; timeoutMs?: number; key?: Key };
  batch?: { maxSize: number; timeoutMs: number; key?: Key; if?: Expression };
  singleton?: { mode: "skip" | "cancel"; key?: (scope: Record<string, unknown>) => string | undefined | false };
  priority?: Expression;
  timeouts?: { startMs?: number; finishMs?: number };
  middleware: MiddlewareClass[];
}

type Key = (scope: Record<string, unknown>) => string | undefined;

export const DEFAULT_RETRIES = 4;

const KNOWN = new Set([
  "id",
  "name",
  "description",
  "triggers",
  "retries",
  "concurrency",
  "throttle",
  "rateLimit",
  "idempotency",
  "onFailure",
  "cancelOn",
  "debounce",
  "batchEvents",
  "singleton",
  "priority",
  "timeouts",
  "middleware",
  "checkpointing",
  "optimizeParallelism",
]);

export function define(options: FunctionOptions): Definition {
  if (!options || typeof options !== "object" || typeof (options as any).id !== "string")
    throw new TypeError("createFunction() needs an options object with an `id`.");
  const id = options.id;
  if (!/^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$/.test(id))
    throw new TypeError(
      `The function ID "${id}" must be 1 to 128 letters, digits, ".", "_", ":", "/" and "-".`,
    );
  const where = `function "${id}"`;

  for (const key of Object.keys(options)) {
    if (!KNOWN.has(key))
      throw new TypeError(`The ${where} has an unknown option "${key}".`);
  }

  const retries = options.retries ?? DEFAULT_RETRIES;
  if (!Number.isInteger(retries) || retries < 0 || retries > 20)
    throw new TypeError(`The ${where} has retries ${retries}. Use 0 to 20.`);

  const triggers = normalizeTriggers(options.triggers, where);
  // As with Inngest, a batch's run has no one event for these to look at.
  if (options.batchEvents)
    for (const other of ["idempotency", "rateLimit", "cancelOn", "priority", "debounce", "singleton"] as const)
      if (options[other] !== undefined)
        throw new TypeError(`The ${where} has both "batchEvents" and "${other}", which don't go together.`);
  if (options.middleware !== undefined && (!Array.isArray(options.middleware) || options.middleware.some((m) => typeof m !== "function")))
    throw new TypeError(`The middleware of ${where} must be a list of classes.`);

  const within = (input: DurationInput, label: string, min: number, max: number) => {
    const ms = parseDuration(input, `${label} of ${where}`);
    if (ms < min || ms > max) throw new TypeError(`The ${label} of ${where} must be from ${span(min)} to ${span(max)}.`);
    return ms;
  };
  const keyOf = (key: KeyInput | undefined, label: string) =>
    key === undefined ? undefined : compileKey(key, `${label} key of ${where}`);
  const DAYS_7 = 7 * 86_400_000;

  const concurrency = normalizeConcurrency(options.concurrency, where);

  const bucket = (
    input: { limit: number; period: DurationInput; burst?: number; key?: KeyInput } | undefined,
    label: string,
    burstDefault: number | undefined,
  ): Bucket | undefined => {
    if (!input) return undefined;
    if (!Number.isInteger(input.limit) || input.limit < 1)
      throw new TypeError(`The ${where} has a ${label} limit of ${input.limit}. Use a whole number of at least 1.`);
    const periodMs = parseDuration(input.period, `${label} period of ${where}`);
    if (periodMs < 1000 || periodMs > 7 * 86_400_000)
      throw new TypeError(`The ${label} period of ${where} must be from 1s to 7d.`);
    const intervalMs = periodMs / input.limit;
    const burst = input.burst ?? burstDefault;
    return {
      intervalMs,
      // A rate limit lets `limit` runs start in each period. A throttle lets
      // `burst` start at once, and then one each interval.
      toleranceMs: burst === undefined ? periodMs - intervalMs : burst * intervalMs,
      key: input.key === undefined ? undefined : compileKey(input.key, `${label} key of ${where}`),
    };
  };

  return {
    id,
    name: options.name ?? id,
    triggers,
    retries,
    concurrency,
    throttle: bucket(options.throttle, "throttle", 1),
    rateLimit: bucket(options.rateLimit, "rateLimit", undefined),
    idempotency:
      options.idempotency === undefined
        ? undefined
        : compileKey(options.idempotency, `idempotency key of ${where}`),
    onFailure: options.onFailure,
    debounce: options.debounce && {
      periodMs: within(options.debounce.period, "debounce period", 1000, DAYS_7),
      timeoutMs:
        options.debounce.timeout === undefined ? undefined : within(options.debounce.timeout, "debounce timeout", 1000, 4 * DAYS_7),
      key: keyOf(options.debounce.key, "debounce"),
    },
    batch: options.batchEvents && {
      maxSize: batchSize(options.batchEvents.maxSize, where),
      timeoutMs: within(options.batchEvents.timeout, "batchEvents timeout", 1000, DAYS_7),
      key: keyOf(options.batchEvents.key, "batchEvents"),
      if: options.batchEvents.if === undefined ? undefined : parseExpression(options.batchEvents.if, `batchEvents "if" of ${where}`),
    },
    singleton: options.singleton && normalizeSingleton(options.singleton, where),
    priority: options.priority?.run === undefined ? undefined : parseExpression(options.priority.run, `priority of ${where}`),
    timeouts: options.timeouts && {
      startMs: options.timeouts.start === undefined ? undefined : within(options.timeouts.start, "start timeout", 1000, 366 * 86_400_000),
      finishMs: options.timeouts.finish === undefined ? undefined : within(options.timeouts.finish, "finish timeout", 1000, 366 * 86_400_000),
    },
    middleware: options.middleware ?? [],
    cancelOn: normalizeCancelOn(options.cancelOn, where),
  };
}

/** The most events in a batch. */
export const MAX_BATCH = 500;

function span(ms: number) {
  return ms >= 86_400_000 ? `${ms / 86_400_000}d` : `${ms / 1000}s`;
}

function batchSize(size: number, where: string) {
  if (!Number.isInteger(size) || size < 1 || size > MAX_BATCH)
    throw new TypeError(`The ${where} has a batchEvents maxSize of ${size}. Use 1 to ${MAX_BATCH}.`);
  return size;
}

function normalizeSingleton(input: NonNullable<FunctionOptions["singleton"]>, where: string): Definition["singleton"] {
  if (input.mode !== "skip" && input.mode !== "cancel")
    throw new TypeError(`The singleton of ${where} has mode "${input.mode}". Use "skip" or "cancel".`);
  if (input.key === undefined) return { mode: input.mode };
  const source = input.key;
  if (typeof source === "function")
    return {
      mode: input.mode,
      key: (scope) => {
        const value = source(scope.event);
        return value === false ? false : value === undefined || value === null || value === "" ? undefined : typeof value === "string" ? value : JSON.stringify(value);
      },
    };
  const text = compileKey(source, `singleton key of ${where}`);
  const expr = parseExpression(source, `singleton key of ${where}`);
  return {
    mode: input.mode,
    key: (scope) => {
      try {
        if (evaluateValue(expr, scope) === false) return false;
      } catch {}
      return text(scope);
    },
  };
}

/** How many ms ahead of its turn a run goes. An expression that has no value is none. */
export function priorityMs(def: Definition, event: unknown) {
  if (!def.priority) return 0;
  try {
    const value = Number(evaluateValue(def.priority, { event }));
    return Number.isFinite(value) ? Math.max(-600, Math.min(600, Math.trunc(value))) * 1000 : 0;
  } catch {
    return 0;
  }
}

function normalizeCancelOn(input: FunctionOptions["cancelOn"], where: string): CancelOn[] {
  if (input === undefined) return [];
  if (!Array.isArray(input)) throw new TypeError(`The cancelOn of ${where} must be a list.`);
  return input.map((c) => {
    const name = typeof c?.event === "string" ? c.event : c?.event?.name;
    if (!name) throw new TypeError(`A cancelOn of ${where} has no event name.`);
    if (c.match && c.if) throw new TypeError(`A cancelOn of ${where} has both "match" and "if". Use one.`);
    // Check the expression now, so a mistake fails when the function is created.
    if (c.if) parseExpression(c.if, `cancelOn "if" of ${where}`);
    return {
      name,
      match: c.match,
      if: c.if,
      timeoutMs: c.timeout === undefined ? undefined : parseDuration(c.timeout, `cancelOn timeout of ${where}`),
    };
  });
}

function normalizeTriggers(input: FunctionOptions["triggers"], where: string): Trigger[] {
  const list = input === undefined ? [] : Array.isArray(input) ? input : [input];
  if (list.length > 10) throw new TypeError(`The ${where} has more than 10 triggers.`);
  const triggers = list.map((t): Trigger => {
    if (t && "event" in t) {
      const name = typeof t.event === "string" ? t.event : t.event?.name;
      if (!name) throw new TypeError(`A trigger of ${where} has no event name.`);
      if (name.includes("*") && !/^[^*]+[/.]\*$/.test(name))
        throw new TypeError(`The trigger "${name}" of ${where} has a wildcard that isn't last, after a "/" or ".", like "user/*".`);
      return {
        kind: "event",
        name,
        if: t.if === undefined ? undefined : parseExpression(t.if, `trigger "if" of ${where}`),
      };
    }
    if (t && "cron" in t) {
      const jitterMs = t.jitter === undefined ? undefined : parseDuration(t.jitter, `cron jitter of ${where}`);
      if (jitterMs !== undefined && (jitterMs < 1000 || jitterMs > 3_600_000))
        throw new TypeError(`The cron jitter of ${where} must be from 1s to 1h.`);
      return { kind: "cron", cron: parseCron(t.cron), ...(jitterMs === undefined ? {} : { jitterMs }) };
    }
    if (t && "invoke" in t) return { kind: "invoke" };
    throw new TypeError(`A trigger of ${where} isn't an event or a cron.`);
  });
  // A function with no triggers can only be invoked.
  if (!triggers.length) triggers.push({ kind: "invoke" });
  return triggers;
}

/** The largest concurrency limit. */
export const MAX_CONCURRENCY = 10_000;

function normalizeConcurrency(input: FunctionOptions["concurrency"], where: string): Limit[] {
  if (input === undefined) return [];
  const list = typeof input === "number" ? [{ limit: input }] : Array.isArray(input) ? input : [input];
  if (list.length > 2) throw new TypeError(`The ${where} has more than 2 concurrency limits.`);
  return list
    .filter((c) => c.limit !== 0)
    .map((c) => {
      // Every lease of a limit's shard is in one table item, which DynamoDB caps at 400 KB.
      if (!Number.isInteger(c.limit) || c.limit < 1 || c.limit > MAX_CONCURRENCY)
        throw new TypeError(
          `The ${where} has a concurrency limit of ${c.limit}. Use a whole number from 1 to ${MAX_CONCURRENCY}.`,
        );
      const scope = c.scope ?? "fn";
      if (!["fn", "env", "account"].includes(scope))
        throw new TypeError(`The ${where} has a concurrency scope "${scope}". Use "fn", "env" or "account".`);
      if (scope !== "fn" && c.key === undefined)
        throw new TypeError(`The ${where} has an "${scope}" concurrency limit without a key. Inngest needs a key for it, and so does EventFlow.`);
      return {
        limit: c.limit,
        scope,
        key: c.key === undefined ? undefined : compileKey(c.key, `concurrency key of ${where}`),
        keySource: typeof c.key === "string" ? c.key : undefined,
      };
    });
}

export function matchesTrigger(def: Definition, event: { name: string; data: unknown }) {
  return def.triggers.some((t) => {
    if (t.kind !== "event" || !nameMatches(t.name, event.name)) return false;
    if (!t.if) return true;
    return evaluateSafe(t.if, { event });
  });
}

/** Whether an event's name is a trigger's, or under its wildcard. */
export function nameMatches(trigger: string, name: string) {
  return trigger.endsWith("*") ? name.startsWith(trigger.slice(0, -1)) && name.length >= trigger.length : trigger === name;
}

function evaluateSafe(expr: Expression, scope: Record<string, unknown>) {
  try {
    return evaluate(expr, scope);
  } catch {
    return false;
  }
}
