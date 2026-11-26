import { isMarker, placeOfError, Places, unwrap, type Place } from "./order.js";
import { awsFetch } from "./client.js";
import { fat } from "./blobs.js";
import type { Engine } from "./engine.js";
import { replayId } from "./ids.js";
import type { RunPayload, WireEvent } from "./types.js";
import { key as keyOf } from "./ids.js";
import { takeLock, type LockRuns } from "./singleton.js";
import { sendSystemEvents } from "./system.js";
import { endedKey, ONCE, type InvokeOutcome, type Registered } from "./worker.js";

/** How long a run's retry is remembered: as long as AWS keeps the run. */
const RETENTION_MS = 90 * 86_400_000;

// Runs, by their run ID, as the router's admin API serves them. A run is a durable
// execution of the worker, and its ID is the execution's name, so it can be found
// without its ARN.

export type RunStatus =
  | "RUNNING"
  | "PAUSING"
  | "PAUSED"
  | "SUCCEEDED"
  | "FAILED"
  | "TIMED_OUT"
  | "STOPPED";

export interface RunSummary {
  /** The run ID, which is the durable execution's name. */
  id: string;
  /** The durable execution's ARN. */
  arn: string;
  status: RunStatus;
  startedAt: Date;
  endedAt?: Date;
}

export interface StepRecord {
  /** The step's ID, as the function names it. Steps in a step with a concurrency limit, a wait for an event, or an invoke are under it, like "charge/run". */
  id: string;
  type: "step" | "sleep" | "waitForEvent" | "waitForSignal" | "invoke" | "callback" | "group" | "other";
  status: "running" | "succeeded" | "failed" | "waiting" | "timed out" | "cancelled";
  attempts: number;
  startedAt?: Date;
  endedAt?: Date;
  error?: { name?: string; message?: string };
}

export interface Run extends RunSummary {
  /** The function's ID. */
  function: string;
  /** The event that started the run. */
  event: WireEvent;
  /** The worker version that runs it. A run keeps the version it started on. */
  version?: string;
  output?: unknown;
  error?: { name?: string; message?: string };
  /** For a replay or a retry, the run it replays. */
  replayOf?: string;
  /** For a retry, the steps that weren't run again, since they'd succeeded. */
  reused?: string[];
  /** Each step, in the order it started. */
  steps: StepRecord[];
  /** The first step that failed, if any. */
  failedStep?: StepRecord;
  /** Where to look next. */
  links: { logs: string; logGroup: string };
}

export interface ListInput {
  /** Only the runs of this function. */
  function?: string | { id: string };
  status?: RunStatus;
  from?: Date;
  to?: Date;
  /** At most this many. Defaults to 100. */
  limit?: number;
}


const base = "/2025-12-01";
/** The largest set of saved step results a retry passes to its run. */
const MAX_MEMO_BYTES = 800 * 1024;

async function lambda<T>(engine: Engine, path: string, init: RequestInit = { method: "GET" }): Promise<T> {
  const response = await awsFetch("lambda", `${base}${path}`, init as any, { aws: engine.aws });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`Lambda ${init.method ?? "GET"} ${path.split("?")[0]} failed: ${response.status} ${text}`.trim());
  }
  return (text ? JSON.parse(text) : {}) as T;
}

function time(t: string | number | undefined) {
  if (t === undefined || t === null) return undefined;
  const n = typeof t === "number" ? t : Number(t);
  if (Number.isFinite(n)) return new Date(n < 1e12 ? n * 1000 : n);
  return new Date(t);
}

function summary(e: any): RunSummary {
  return {
    id: e.DurableExecutionName,
    arn: e.DurableExecutionArn,
    status: e.Status,
    startedAt: time(e.StartTimestamp)!,
    endedAt: time(e.EndTimestamp),
  };
}

function parse(text: string | undefined) {
  if (text === undefined || text === null || text === "") return undefined;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

/** The run ID prefix of a function. */
export function runPrefix(fn: string) {
  return `${fn.replace(/[^A-Za-z0-9_-]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 36) || "fn"}-`;
}

export async function listRuns(engine: Engine, input: ListInput = {}): Promise<RunSummary[]> {
  const fn = typeof input.function === "string" ? input.function : input.function?.id;
  const prefix = fn ? runPrefix(fn) : undefined;
  const limit = Math.min(Math.max(1, input.limit ?? 100), 1000);
  const runs: RunSummary[] = [];
  let marker: string | undefined;
  // At most 10 pages, so a filter that matches little can't page forever.
  for (let page = 0; page < 10; page++) {
    // The latest first, as AWS lists them. ReverseOrder is the oldest first.
    const params = new URLSearchParams({ MaxItems: "1000" });
    if (input.status) params.set("Statuses", input.status);
    if (input.from) params.set("StartedAfter", new Date(input.from).toISOString());
    if (input.to) params.set("StartedBefore", new Date(input.to).toISOString());
    if (marker) params.set("Marker", marker);
    const result = await lambda<any>(engine, `/functions/${encodeURIComponent(engine.worker.name)}/durable-executions?${params}`);
    for (const e of result.DurableExecutions ?? []) {
      if (prefix && !String(e.DurableExecutionName).startsWith(prefix)) continue;
      runs.push(summary(e));
    }
    marker = result.NextMarker;
    if (!marker || runs.length >= limit) break;
  }
  return runs.sort((a, b) => b.startedAt.getTime() - a.startedAt.getTime()).slice(0, limit);
}

export async function findRun(engine: Engine, runId: string): Promise<RunSummary | undefined> {
  const params = new URLSearchParams({ DurableExecutionName: runId, MaxItems: "10" });
  const page = await lambda<any>(engine, `/functions/${encodeURIComponent(engine.worker.name)}/durable-executions?${params}`);
  const match = (page.DurableExecutions ?? []).find((e: any) => e.DurableExecutionName === runId);
  return match ? summary(match) : undefined;
}

async function history(engine: Engine, arn: string) {
  const events: any[] = [];
  let marker: string | undefined;
  do {
    const params = new URLSearchParams({ MaxItems: "1000", IncludeExecutionData: "true" });
    if (marker) params.set("Marker", marker);
    const page = await lambda<any>(engine, `/durable-executions/${encodeURIComponent(arn)}/history?${params}`);
    events.push(...(page.Events ?? []));
    marker = page.NextMarker;
  } while (marker);
  return events;
}

async function detail(engine: Engine, arn: string) {
  return lambda<any>(engine, `/durable-executions/${encodeURIComponent(arn)}?IncludeExecutionData=true`);
}

/** Whether a run is running, and what it started with. */
export async function runStatus(engine: Engine, arn: string): Promise<{ running: boolean; status: string; payload?: RunPayload }> {
  const info = await detail(engine, arn);
  return {
    status: String(info.Status),
    running: ["RUNNING", "PAUSED", "PAUSING"].includes(info.Status),
    payload: parse(info.InputPayload) as RunPayload | undefined,
  };
}

export async function getRun(engine: Engine, runId: string): Promise<Run | undefined> {
  const found = await findRun(engine, runId);
  if (!found) return undefined;
  const [info, events] = await Promise.all([detail(engine, found.arn), history(engine, found.arn)]);
  const payload = parse(info.InputPayload) as RunPayload | undefined;
  const steps = toSteps(events);
  const logGroup = `/aws/lambda/${engine.worker.name}`;
  const region = process.env.AWS_REGION ?? "us-east-1";
  return {
    ...found,
    function: payload?.fn ?? "",
    event: (payload?.event && (await fat(engine, payload.event).catch(() => payload.event))) as WireEvent,
    replayOf: payload?.replayOf,
    reused: payload?.memo ? [...Object.keys(payload.memo), ...Object.keys(payload.memoBlobs ?? {})] : undefined,
    version: info.Version,
    output: parse(info.Result),
    error: info.Error ? { name: info.Error.ErrorType, message: info.Error.ErrorMessage } : undefined,
    steps,
    failedStep: steps.find((s) => s.status === "failed"),
    links: {
      logGroup,
      logs: `https://${region}.console.aws.amazon.com/cloudwatch/home?region=${region}#logsV2:log-groups/log-group/${encodeURIComponent(logGroup).replace(/%/g, "$25")}`,
    },
  };
}

export async function cancelRun(engine: Engine, runId: string, registry: Map<string, Registered>, options: { force?: boolean } = {}) {
  const found = await findRun(engine, runId);
  if (!found) throw new Error(`There's no run "${runId}".`);
  // One whose end is decided isn't cut off in what's done about it, like onFailure's refund,
  // or telling the run that invoked it, as a cancelOn event doesn't, unless it's forced.
  const ended = (await engine.store.successor(endedKey(runId), "stopping", (RETENTION_MS + 86_400_000) * 2)) === "ended";
  if (ended && !options.force) {
    const { running } = await runStatus(engine, found.arn);
    if (running)
      throw new Error(`The run "${runId}" has returned or failed, and runs what's done about it, like its onFailure. Cancel it with force to cut that off.`);
    return { id: runId, stopped: false };
  }
  const stopped = await engine.stopRun(found.arn);
  const { status, payload } = await runStatus(engine, found.arn);
  // One that was stopped already is told again: what stopped it may have stopped first.
  const told = keyOf("ST", payload?.runId ?? runId);
  if (stopped || (status === "STOPPED" && !(await engine.store.owner(told)))) {
    // The run that invoked it would otherwise wait for it.
    if (payload?.parent)
      await engine.callback(payload.parent, {
        ok: false,
        error: { name: "Cancelled", message: "The run was cancelled." },
        by: payload.runId,
      } satisfies InvokeOutcome);
    // Its singleton is free for the next.
    if (payload?.lock) await engine.store.lockFree(payload.lock, payload.runId);
    if (payload?.event) await sendSystemEvents(registry, engine, { fn: payload.fn, runId, event: payload.event, outcome: "cancelled" });
    await engine.store.claim(told, "told", RETENTION_MS * 4);
  }
  return { id: runId, stopped };
}

/**
 * Run a function again with the event of an earlier run, as a new run on the latest
 * deployed version. With `reuse`, the steps that succeeded return their saved
 * results instead of running again: the retry picks up at the step that failed.
 */
/**
 * The at-most-once steps of a run, from its history, and the runs it invoked. An
 * at-most-once step that started may have done its side effect, and has no key to
 * stop a second one. It's safe to rerun the run only if the rerun reuses the saved
 * result of the top-level step it's in: `memo`, for a retry. For a replay, which
 * reuses nothing, every one that started is risky. That covers one in onFailure, and
 * one under a concurrency limit, whose results aren't reused.
 *
 * An invoked run is started again, from the start, unless its result is reused, so
 * its at-most-once steps are risky too: `children` are the ones to look at.
 */
export function onceSteps(events: any[], memo?: Record<string, unknown>) {
  const parents = new Map<string, string | undefined>();
  const names = new Map<string, string>();
  for (const e of events) {
    if (!e.Id) continue;
    if (!parents.has(e.Id)) parents.set(e.Id, e.ParentId || undefined);
    if (e.Name && !names.has(e.Id)) names.set(e.Id, e.Name);
  }
  const reused = (id: string) => {
    let top = id;
    for (let depth = 0; depth < 100; depth++) {
      const parent = parents.get(top);
      if (!parent) break;
      top = parent;
    }
    const name = names.get(top);
    return memo !== undefined && name !== undefined && Object.prototype.hasOwnProperty.call(memo, name);
  };
  const started = new Map<string, string>();
  const invokes = new Map<string, { name: string; sent: boolean; runId?: string }>();
  for (const e of events) {
    if (e.SubType === ONCE && e.EventType === "StepStarted" && e.Id) started.set(e.Id, e.Name ?? e.Id);
    if (e.SubType === "EventFlowInvoke" && e.EventType === "ContextStarted" && e.Id) invokes.set(e.Id, { name: e.Name ?? e.Id, sent: false });
  }
  for (const e of events) {
    const invoke = e.Name === "start" && e.ParentId ? invokes.get(e.ParentId) : undefined;
    if (!invoke) continue;
    invoke.sent = true;
    if (e.EventType === "StepSucceeded") {
      const result = parse(e.StepSucceededDetails?.Result?.Payload) as { runId?: string } | undefined;
      if (typeof result?.runId === "string") invoke.runId = result.runId;
    }
  }
  return {
    started: [...started.values()],
    risky: [...started.entries()].filter(([id]) => !reused(id)).map(([, name]) => name),
    children: [...invokes.entries()].filter(([id, v]) => v.sent && !reused(id)).map(([, v]) => ({ name: v.name, runId: v.runId })),
  };
}

/** The at-most-once steps a rerun would run again, in the run and the runs it invoked. */
async function riskySteps(engine: Engine, events: any[], memo: Record<string, unknown> | undefined, depth = 0): Promise<string[]> {
  const found = onceSteps(events, memo);
  const out = [...found.risky];
  for (const child of found.children) {
    if (!child.runId) {
      out.push(`${child.name}, a run it invoked that can't be checked`);
      continue;
    }
    if (depth >= 5) {
      out.push(`${child.name}, a run invoked too deep to check`);
      continue;
    }
    const run = await findRun(engine, child.runId);
    if (!run) continue;
    // The invoked run is started again from the start: it reuses nothing.
    const steps = await riskySteps(engine, await history(engine, run.arn), undefined, depth + 1);
    out.push(...steps.map((s) => `${child.name} → ${s}`));
  }
  return out;
}

export async function rerun(
  engine: Engine,
  lockRuns: LockRuns,
  runId: string,
  reuse: boolean,
  options: { force?: boolean; again?: boolean } = {},
): Promise<{ id: string; replayOf: string; reused: string[]; existing?: boolean }> {
  const found = await findRun(engine, runId);
  if (!found) throw new Error(`There's no run "${runId}".`);
  if (found.status === "RUNNING" || found.status === "PAUSED" || found.status === "PAUSING")
    throw new Error(`The run "${runId}" is still running. Cancel it first.`);
  const [info, events] = await Promise.all([detail(engine, found.arn), history(engine, found.arn)]);
  const payload = parse(info.InputPayload) as RunPayload | undefined;
  if (!payload?.fn) throw new Error(`The run "${runId}" isn't an EventFlow run.`);
  if (payload.parent) throw new Error(`The run "${runId}" was invoked by another run. Retry or replay that run instead.`);

  const found_ = reuse ? memoFrom(events, payload) : undefined;
  const memo = found_ ? { ...(payload.memo ?? {}), ...found_.memo } : undefined;
  const memoBlobs = found_ ? { ...(payload.memoBlobs ?? {}), ...found_.blobs } : undefined;
  if (memo && memoBlobs) for (const id of Object.keys(found_!.memo)) delete memoBlobs[id];
  // Only what it reuses, also of the run that run retried: what it doesn't is run again, and
  // checked as that.
  // One that an earlier retry reused isn't in this run's history, so it can't be told whether
  // it was atMostOnce: running it again is done on purpose only.
  const dropped: string[] = [];
  if (found_ && memo && memoBlobs) {
    const keep = new Set(found_.order);
    for (const m of [memo, memoBlobs])
      for (const id of Object.keys(m))
        if (!keep.has(id)) {
          delete (m as any)[id];
          dropped.push(`${id}, which an earlier retry reused`);
        }
  }
  const reused = [...Object.keys(memo ?? {}), ...Object.keys(memoBlobs ?? {})];
  const memoOrder = found_?.order;
  if (memo && Buffer.byteLength(JSON.stringify(memo)) > MAX_MEMO_BYTES)
    throw new Error(`The saved results of "${runId}" are over ${MAX_MEMO_BYTES} bytes. Replay it instead, to run every step again.`);

  // A rerun that would run an at-most-once step again, here or in a run it invoked,
  // is only done on purpose.
  if (!options.force) {
    const risky = [...dropped, ...(await riskySteps(engine, events, reuse ? Object.fromEntries(reused.map((id) => [id, true])) : undefined))];
    if (risky.length)
      throw new InterruptedRunError(
        `A ${reuse ? "retry" : "replay"} of "${runId}" would run again atMostOnce steps that started: ${risky
          .map((s) => `"${s}"`)
          .join(", ")}. Their side effects may have happened. Check them, then rerun with force.`,
      );
  }
  let id = replayId(runId);

  // A run is rerun once: a second retry, as from a double click or a bulk retry that
  // runs again, gets the first one's run. Ask again on purpose to rerun it again.
  if (!options.again) {
    const owner = await engine.store.successor(keyOf("RR", reuse ? "retry" : "replay", runId), id, RETENTION_MS);
    if (owner !== id) {
      if (await findRun(engine, owner)) return { id: owner, replayOf: runId, reused: [], existing: true };
      // The first rerun was claimed and didn't start: start it, by its name.
      id = owner;
    }
  }
  const { sig: _sig, ...rest } = payload;
  // What it reuses from the table is kept as long again, for it, and a retry of it.
  for (const blob of [...Object.values(memoBlobs ?? {}), ...(payload.event?.blob ? [payload.event.blob.key] : [])]) {
    const body = await engine.store.getBlob(blob);
    if (body === undefined) throw new Error(`A saved result of "${runId}" is gone from the table. Replay it instead.`);
    await engine.store.putBlob(blob, body, ((engine.resource.runTimeoutSeconds ?? 31_622_400) + 106 * 86_400) * 1000);
  }
  // A singleton's rerun needs the singleton, like its run did.
  if (payload.lock) {
    const taken = await takeLock(engine, lockRuns, payload.lock, id, "skip");
    if (!taken) throw new Error(`Another run has the singleton of "${runId}". Rerun it once that one has ended.`);
  }
  await engine.startRun({ ...rest, runId: id, root: payload.root ?? runId, replayOf: runId, memo, memoBlobs, memoOrder, at: Date.now() });
  return { id, replayOf: runId, reused };
}

export class InterruptedRunError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InterruptedRunError";
  }
}

export async function rerunAll(
  engine: Engine,
  lockRuns: LockRuns,
  input: ListInput & { reuse: boolean; force?: boolean },
): Promise<{ id: string; result?: { id: string; existing?: boolean }; error?: string }[]> {
  if (input.status === "SUCCEEDED" && input.reuse)
    throw new Error("A retry reuses what succeeded, so there's nothing to retry in a run that succeeded. Replay it instead.");
  const runs = await listRuns(engine, { ...input, status: input.status ?? "FAILED", limit: Math.min(input.limit ?? 50, 100) });
  const out: { id: string; result?: { id: string }; error?: string }[] = [];
  for (const run of runs) {
    try {
      const result = await rerun(engine, lockRuns, run.id, input.reuse, { force: input.force });
      out.push({ id: run.id, result: { id: result.id, ...(result.existing ? { existing: true } : {}) } });
    } catch (error) {
      out.push({ id: run.id, error: (error as Error).message });
    }
  }
  return out;
}

/**
 * Stop the runs that match: by default, the ones running. Up to 500 at a time, 10 at
 * once. Before an EventFlow with many runs in flight is removed, this is faster than
 * `sst remove`, which stops them one at a time.
 */
export async function cancelAll(engine: Engine, input: ListInput, registry: Map<string, Registered>, options: { force?: boolean } = {}) {
  const runs: RunSummary[] = [];
  for (const status of input.status ? [input.status] : (["RUNNING", "PAUSED"] as RunStatus[]))
    runs.push(...(await listRuns(engine, { ...input, status, limit: Math.min(input.limit ?? 500, 500) })));
  const out: { id: string; stopped?: boolean; error?: string }[] = [];
  for (let i = 0; i < runs.length; i += 10) {
    const results = await Promise.allSettled(runs.slice(i, i + 10).map((r) => cancelRun(engine, r.id, registry, options)));
    results.forEach((r, j) =>
      out.push(
        r.status === "fulfilled"
          ? { id: runs[i + j].id, stopped: r.value.stopped }
          : { id: runs[i + j].id, error: (r.reason as Error)?.message ?? String(r.reason) },
      ),
    );
  }
  return out;
}

/** Whether a step's result is in what's saved: a large one's place is, without it. */
function isWhole(text: string | undefined) {
  if (!text) return false;
  return JSON.parse(text).v !== undefined || unwrap(text).place === undefined;
}

/**
 * The results of a run's top-level steps that succeeded, by step ID, from its
 * history. A retry passes them to its run, so those steps aren't run again.
 */
export function memoFrom(events: any[], earlier: Pick<RunPayload, "memo" | "memoBlobs" | "memoOrder"> = {}): { memo: Record<string, unknown>; blobs: Record<string, string>; order: string[] } {
  const memo: Record<string, unknown> = Object.create(null);
  const blobs: Record<string, string> = Object.create(null);
  const saved: Place[] = [];
  const place = (p: Place | undefined) => {
    if (p?.[0] !== "") return;
    saved.push(p);
  };
  for (const e of events) {
    if (e.ParentId) continue;
    const name: string | undefined = e.Name;
    if (!name) continue;
    const type = String(e.EventType ?? "");
    if (isMarker(name) && type === "StepSucceeded") place(unwrap(e.StepSucceededDetails?.Result?.Payload).place);
    if (type === "StepFailed") place(placeOfError(e.StepFailedDetails?.Error?.Payload?.ErrorData));
    if (name.startsWith("sst:")) continue;
    const payload = (d: any) => {
      if (d?.Result?.Truncated) throw new Error(`The result of step "${name}" is too large to reuse. Replay the run instead.`);
      const text = d?.Result?.Payload;
      if (text === undefined || text === "") return null;
      const found = unwrap(text);
      place(found.place);
      return found.value ?? null;
    };
    // A large result isn't in the context's own event: it's in the one of what's in it.
    const inner = (names: RegExp, type: string) => {
      const found = events.filter((o) => o.ParentId === e.Id && o.EventType === type && names.test(String(o.Name))).pop();
      const text = found?.[`${type}Details`]?.Result?.Payload;
      if (!text) throw new Error(`The result of step "${name}" is too large to reuse. Replay the run instead.`);
      return unwrap(text).value ?? null;
    };
    if (type === "StepSucceeded") memo[name] = payload(e.StepSucceededDetails);
    else if (type === "WaitSucceeded") memo[name] = true;
    else if (type === "ContextSucceeded") {
      const own = /^EventFlow(Step|Wait|Invoke|Signal)$/.test(String(e.SubType));
      const large = own && !isWhole(e.ContextSucceededDetails?.Result?.Payload);
      const whole = payload(e.ContextSucceededDetails);
      const value = !large
        ? whole
        : e.SubType === "EventFlowStep"
          ? inner(/^run(:|$)/, "StepSucceeded")
          : e.SubType === "EventFlowSignal"
            ? { value: inner(/^signal$/, "CallbackSucceeded") }
            : inner(e.SubType === "EventFlowWait" ? /^event$/ : /^result$/, "CallbackSucceeded");
      if (e.SubType === "EventFlowStep") {
        // A large result is in the table, and the memo says where.
        if (value?.ok && value.blob) blobs[name] = value.blob;
        else if (value?.ok) memo[name] = value.value;
      } else if (e.SubType === "EventFlowInvoke") {
        if (value?.ok && value.blob) blobs[name] = value.blob;
        else if (value?.ok) memo[name] = value.result ?? null;
      } else if (e.SubType === "EventFlowWait") memo[name] = value;
      else if (e.SubType === "EventFlowSignal" && !value?.conflict) memo[name] = value?.value ?? null;
    }
  }
  const places = [...new Places("", saved, earlier.memoOrder).all()].sort(([, a], [, b]) => a.seq - b.seq);
  // Up to the first that's run again: what the function does after it may be another.
  const kept = (id: string) => [memo, blobs, earlier.memo ?? {}, earlier.memoBlobs ?? {}].some((m) => Object.prototype.hasOwnProperty.call(m, id));
  const ids = places.map(([id]) => id);
  const first = ids.findIndex((id) => !kept(id));
  const order = first < 0 ? ids : ids.slice(0, first);
  // After it the function may take another path, and a result found by its ID could be another
  // call's: the steps after it run again, with the same idempotency keys.
  const keep = new Set(order);
  for (const id of Object.keys(memo)) if (!keep.has(id)) delete memo[id];
  for (const id of Object.keys(blobs)) if (!keep.has(id)) delete blobs[id];
  return { memo, blobs, order };
}

// The child contexts EventFlow makes, by their subtype.
const SUBTYPES: Record<string, StepRecord["type"]> = {
  EventFlowStep: "step",
  EventFlowWait: "waitForEvent",
  EventFlowInvoke: "invoke",
  EventFlowSignal: "waitForSignal",
  EventFlowSend: "step",
  EventFlowSignalSend: "step",
};

const TYPES: Record<string, StepRecord["type"]> = {
  Step: "step",
  Wait: "sleep",
  Callback: "callback",
  Context: "group",
  ChainedInvoke: "invoke",
};

// Turn the history's events into one record per operation. Operations in a child
// context are named under the context, like "charge/run".
export function toSteps(events: any[]): StepRecord[] {
  const ops = new Map<string, StepRecord & { parent?: string; name: string }>();
  for (const e of events) {
    const type = String(e.EventType ?? "");
    const match = type.match(/^(Step|Wait|Callback|Context|ChainedInvoke)(Started|Succeeded|Failed|TimedOut|Cancelled|Stopped)$/);
    if (!match || !e.Id || isMarker(e.Name)) continue;
    const [, kind, phase] = match;
    let op = ops.get(e.Id);
    if (!op) {
      op = {
        id: e.Name ?? e.Id,
        name: e.Name ?? e.Id,
        parent: e.ParentId,
        type: SUBTYPES[e.SubType] ?? TYPES[kind] ?? "other",
        status: kind === "Wait" || kind === "Callback" ? "waiting" : "running",
        attempts: 0,
      };
      ops.set(e.Id, op);
    }
    const at = time(e.EventTimestamp);
    if (phase === "Started") {
      op.startedAt ??= at;
      if (kind === "Step") op.attempts++;
      continue;
    }
    const details = e[`${kind}${phase}Details`] ?? {};
    const attempt = details.RetryDetails?.CurrentAttempt;
    if (typeof attempt === "number") op.attempts = Math.max(op.attempts, attempt);
    if (phase === "Succeeded") {
      op.status = "succeeded";
      op.endedAt = at;
      op.error = undefined;
      // A step with a concurrency limit, and an invoke, end their context with the
      // outcome, and throw outside it. An outcome that isn't ok is the step failing.
      if (kind === "Context" && (e.SubType === "EventFlowStep" || e.SubType === "EventFlowInvoke")) {
        let outcome: any;
        try {
          outcome = unwrap(details.Result?.Payload).value;
        } catch {}
        if (outcome && outcome.ok === false) {
          op.status = "failed";
          op.error = { name: outcome.error?.name, message: outcome.error?.message };
        }
      }
    } else if (phase === "Failed") {
      // A step that will retry has a next attempt.
      const retrying = details.RetryDetails?.NextAttemptDelaySeconds !== undefined;
      op.status = retrying ? "running" : "failed";
      op.endedAt = retrying ? undefined : at;
      const payload = details.Error?.Payload ?? {};
      op.error = { name: payload.ErrorType, message: payload.ErrorMessage };
    } else if (phase === "TimedOut") {
      op.status = "timed out";
      op.endedAt = at;
    } else {
      op.status = "cancelled";
      op.endedAt = at;
    }
  }

  // Name each operation under its parents.
  const full = (op: StepRecord & { parent?: string; name: string }): string => {
    const parent = op.parent ? ops.get(op.parent) : undefined;
    return parent ? `${full(parent)}/${op.name}` : op.name;
  };
  const list = [...ops.values()].map((op) => ({ ...op, id: full(op) }));
  // A step with a concurrency limit was tried as many times as it has "run" steps.
  for (const op of list)
    if (op.type === "step" && !op.attempts)
      op.attempts = list.filter((o) => o.id === `${op.id}/run` || o.id.startsWith(`${op.id}/run:`)).length;
  // A step with a concurrency limit has "lease" and "run" steps in it. Its failure is
  // the failure of its last "run" step.
  for (const op of list) {
    if (op.status === "failed" && !op.error) {
      const last = list.filter((o) => o.id.startsWith(`${op.id}/run`)).pop();
      if (last) op.error = last.error;
    }
  }
  return list.map(({ parent: _p, name: _n, ...rest }) => rest);
}
