import { createHash } from "crypto";
import type { SQSBatchItemFailure, SQSBatchResponse, SQSEvent, SQSRecord } from "aws-lambda";
import { cronMatches, localMinute } from "./cron.js";
import { matchesTrigger, type Definition } from "./definition.js";
import { fat, slim } from "./blobs.js";
import { checkEvent, checkSignal, cleanEvent, DEDUP_MS, MESSAGE_TTL_MS, type Engine } from "./engine.js";
import { evaluate, getPath } from "./expression.js";
import { callRunId, hash, key, runId as makeRunId, ulid } from "./ids.js";
import { findRun, runStatus } from "./runs.js";
import { deriveRoot, sign, verify } from "./sign.js";
import { LOCK_GRACE_MS, lockTtl, settleLock, singletonKey, takeLock, type LockRuns } from "./singleton.js";
import type { Admission, Pending } from "./store.js";
import { sendSystemEvents } from "./system.js";
import { SYSTEM_EVENTS, type Message, type RunPayload, type Timer, type WireEvent } from "./types.js";
import { eventMatches, MAX_WAIT_S, RECENT_MS, recentKey, signalKey, startKey, STOP, waitKey, type InvokeOutcome, type Registered } from "./worker.js";

// The router reads the engine's queue. For each event it resolves the waits that
// match it, and starts a run of each function it triggers. It also gets the tick of
// the engine's schedule each minute, and starts the cron functions that are due.
//
// It's the only way into the worker. It checks every message: an event must be
// well formed, and its tenant must be its message group; a held or invoked run must
// be signed. What it starts, it signs.

const log = (message: string, fields: Record<string, unknown>) =>
  console.log(JSON.stringify({ level: "info", message, ...fields }));
const warn = (message: string, fields: Record<string, unknown>) =>
  console.warn(JSON.stringify({ level: "warn", message, ...fields }));

/** A message the router won't ever route. It's dropped, not retried. */
class Rejected extends Error {
  /** For a call, who waits for its answer. */
  answer?: { parent: string; by: string };
}

export async function routeBatch(
  sqsEvent: SQSEvent,
  registry: Map<string, Registered>,
  engine: Engine,
): Promise<SQSBatchResponse> {
  const results = await Promise.allSettled(
    sqsEvent.Records.map(async (record) => {
      try {
        await routeRecord(record, registry, engine);
      } catch (error) {
        // A caller isn't left waiting for a call that's dropped.
        if (error instanceof Rejected && error.answer)
          await notifyParent(engine, error.answer.parent, { name: "InvokeError", message: error.message }, error.answer.by);
        throw error;
      }
    }),
  );
  const batchItemFailures: SQSBatchItemFailure[] = [];
  results.forEach((result, i) => {
    if (result.status === "fulfilled") return;
    const record = sqsEvent.Records[i];
    if (result.reason instanceof Rejected) {
      warn("Dropped a message that isn't valid", { messageId: record.messageId, reason: result.reason.message });
      return;
    }
    console.error(
      JSON.stringify({
        level: "error",
        message: "Failed to route a message. It will be retried.",
        messageId: record.messageId,
        receiveCount: record.attributes.ApproximateReceiveCount,
        error: result.reason instanceof Error ? result.reason.message : String(result.reason),
      }),
    );
    batchItemFailures.push({ itemIdentifier: record.messageId });
  });
  return { batchItemFailures };
}

function parse(record: SQSRecord): Message {
  let message: any;
  try {
    message = JSON.parse(record.body);
  } catch {
    throw new Rejected("The body isn't JSON.");
  }
  if (!message || typeof message !== "object") throw new Rejected("The body isn't a message.");
  const group = record.attributes.MessageGroupId;
  if (message.kind === "timer" || message.kind === "system" || message.kind === "start" || message.kind === "invoke") {
    if (!verify(message)) throw new Rejected(`The ${message.kind} message isn't signed by this EventFlow.`);
    if (typeof message.exp !== "number") throw new Rejected(`The ${message.kind} message has no expiry.`);
    return message as Message;
  }
  // Anything else is from a sender. The tenant is the message group, so fair queues and flow
  // control agree on who it belongs to.
  if (message.kind === "signal") {
    try {
      checkSignal(message);
    } catch (error) {
      throw new Rejected((error as Error).message);
    }
    if ((message.tenant ?? undefined) !== (group ?? undefined)) throw new Rejected("The signal's tenant isn't its message group.");
    return { kind: "signal", signal: message.signal, data: message.data, tenant: message.tenant, id: String(message.id ?? "") };
  }
  if (message.kind === "event" || message.kind === "call") {
    const reject = (reason: string) => {
      const error = new Rejected(reason);
      if (message.kind === "call" && typeof message.parent === "string" && message.parent && typeof message.fn === "string")
        error.answer = { parent: message.parent, by: callRunId(message.fn, message.event ?? {}, message.parent) };
      return error;
    };
    try {
      checkEvent(message.event ?? {}, "event", message.kind === "call");
    } catch (error) {
      throw reject((error as Error).message);
    }
    if (typeof message.event.id !== "string") throw reject("The event has no ID.");
    if ((message.event.tenant ?? undefined) !== (group ?? undefined)) throw reject("The event's tenant isn't its message group.");
    // Only what's a sender's to set.
    const event = cleanEvent(message.event);
    if (message.kind === "event") return { kind: "event", event };
    if (typeof message.fn !== "string" || typeof message.parent !== "string" || !message.parent || message.parent.length > 4096)
      throw new Rejected("The call has no function, or no callback.");
    return { kind: "call", fn: message.fn, event, parent: message.parent };
  }
  throw new Rejected(`Unknown message kind "${String(message.kind).slice(0, 20)}".`);
}

async function routeRecord(record: SQSRecord, registry: Map<string, Registered>, engine: Engine) {
  const message = parse(record);
  // When it was sent: the same each time it's delivered.
  const receivedAt = Number(record.attributes.SentTimestamp) || Date.now();
  if (message.kind === "timer") return routeTimer(message, registry, engine);
  if (message.kind === "call") {
    const entry = registry.get(message.fn);
    // The same answer whether it's there or not: it isn't the caller's to find out.
    if (!entry?.def.external)
      return notifyParent(
        engine,
        message.parent,
        { name: "InvokeError", message: `The function "${message.fn.slice(0, 128)}" doesn't take calls from other EventFlows.` },
        callRunId(message.fn, message.event, message.parent),
      );
    return startRun(entry.def, { ...message.event, name: "inngest/function.invoked" }, engine, registry, {
      parent: message.parent,
      runId: callRunId(message.fn, message.event, message.parent),
      identity: "call",
      received: receivedAt,
      external: true,
    });
  }
  if (message.kind === "signal") return void (await deliverSignal(engine, message.tenant, message.signal, message.data, receivedAt));
  if (message.kind === "start" || message.kind === "invoke") {
    // A signed message is good for longer than the dead-letter queue keeps it, so
    // one that's copied can't be sent again much later. The run that invoked it is
    // told, so it doesn't wait.
    if (message.exp < Date.now()) {
      warn("Dropped a message that expired", { kind: message.kind, fn: message.fn });
      if (message.parent)
        await notifyParent(engine, message.parent, { name: "InvokeError", message: `The run of "${message.fn}" expired before it started.` }, message.runId);
      return;
    }
    const entry = registry.get(message.fn);
    if (!entry) {
      warn("Skipped a run of a function that's gone", { fn: message.fn });
      if (message.parent)
        await notifyParent(engine, message.parent, { name: "InvokeError", message: `The function "${message.fn}" isn't served.` }, message.runId);
      return;
    }
    return startRun(entry.def, await fat(engine, message.event), engine, registry, {
      parent: message.parent,
      runId: message.runId,
      root: message.root,
      received: message.kind === "start" ? (message.received ?? receivedAt) : receivedAt,
      held: message.kind === "start" && message.held === true,
      fired: message.kind === "start" ? message.fired : undefined,
      batch: message.kind === "start" ? message.batch : undefined,
      external: message.kind === "start" ? message.external : undefined,
    });
  }

  if (message.kind === "system") {
    if (message.exp < Date.now() || !(SYSTEM_EVENTS as readonly string[]).includes(message.event.name))
      return warn("Dropped a message that expired", { kind: message.kind });
  }
  const event = await fat(engine, message.event);

  // An event ID is used once per tenant in 24 hours. The same message delivered
  // again, or a message whose routing didn't finish, is routed again, since starting
  // a run and resolving a wait are both safe to repeat.
  // What EventFlow sends has names of its own, which a sender can't take first.
  const item = message.kind === "system" ? key("ES", event.tenant ?? null, event.id) : key("E", event.tenant ?? null, event.id);
  const seen = await engine.store.beginEvent(item, record.messageId, DEDUP_MS);
  if (seen === "duplicate") return log("Skipped an event whose ID was already sent", { id: event.id });

  await resolveWaits(event, receivedAt, engine, registry);

  const matched = [...registry.values()].filter((r) => matchesTrigger(r.def, event));
  // Each on its own: one that fails, as when its key throws, doesn't keep the others from
  // starting. The message comes again for it, and the ones that started are found by name.
  const failed: string[] = [];
  for (const { def } of matched) {
    try {
      if (def.batch && inBatch(def, event)) await addToBatch(def, event, receivedAt, engine, registry);
      else await startRun(def, event, engine, registry, { received: receivedAt });
    } catch (error) {
      failed.push(`${def.id}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
  if (failed.length) throw new Error(`Failed to start ${failed.length} of ${matched.length} functions: ${failed.join("; ").slice(0, 2000)}`);
  await engine.store.finishEvent(item, DEDUP_MS);
}

/** Answer the run that invoked another. `by` is that other's ID: it takes no one else's answer. */
async function notifyParent(engine: Engine, parent: string, error: { name: string; message: string }, by: string) {
  await engine.callback(parent, { ok: false, error, by } satisfies InvokeOutcome);
}

async function resolveWaits(event: WireEvent, receivedAt: number, engine: Engine, registry: Map<string, Registered>) {
  const fields = await engine.store.waitFields(event.name, event.tenant);
  if (!fields.length) return;
  // Its data, for waits that start in the next hour, is kept that long, unless one takes it. A
  // sender can't make the table keep what no run uses.
  let kept = await slim(engine, event, RECENT_MS + 86_400_000);
  await engine.store.putRecent(recentKey(event.name, event.tenant), kept, receivedAt, RECENT_MS);
  let taken = false;
  const take = async () => {
    if (!taken) kept = await slim(engine, event);
    taken = true;
    return kept;
  };
  const now = Date.now();
  for (const field of fields) {
    const raw = field === "*" ? undefined : getPath(event, field.split("."));
    if (field !== "*" && raw === undefined) continue;
    const value = raw === undefined ? undefined : JSON.stringify(raw);
    const k = waitKey(event.name, event.tenant, field, value);
    const waiters = (await engine.store.waiters(k, now))
      // A wait that started after the event came finds it itself.
      // With a second's margin for the clocks of SQS and of the worker: one told twice is the same.
      .filter((w) => w.createdAt <= receivedAt + 1000)
      .filter((w) => eventMatches(event, field, value, w.condition));
    // Many runs can wait for one event: some at a time, so none is throttled out.
    for (let i = 0; i < waiters.length; i += 25)
      await Promise.all(
        waiters.slice(i, i + 25).map(async (w) => {
          if (w.kind === "cancel" && w.callbackId.startsWith(STOP)) {
            const arn = w.callbackId.slice(STOP.length);
            if (await stop(engine, registry, { arn, runId: w.runId, parent: w.parent, message: `The run was cancelled by "${event.name}".` }))
              log("Cancelled a run", { run: w.runId, event: event.id });
          } else if (await engine.callback(w.callbackId, await take())) {
            log("Resolved a wait for an event", { run: w.runId, event: event.id });
          }
          await engine.store.deleteWaiter(k, w).catch(() => {});
        }),
      );
  }
}

/**
 * Stop a run, tell the run that invoked it, free its singleton, and send the events about
 * it. False when it had ended by itself. One that was stopped already is told again: what
 * stopped it may have stopped before it told.
 */
export async function stop(
  engine: Engine,
  registry: Map<string, Registered>,
  run: { arn: string; runId: string; parent?: string; message: string },
) {
  const stopped = await engine.stopRun(run.arn);
  const { status, payload } = await runStatus(engine, run.arn);
  if (!stopped && status !== "STOPPED") return false;
  // Once, however often it's stopped: a timer may come long after.
  const told = key("ST", payload?.runId ?? run.runId);
  if (!stopped && (await engine.store.owner(told))) return false;
  const parent = run.parent ?? payload?.parent;
  if (parent && payload) await notifyParent(engine, parent, { name: "Cancelled", message: run.message }, payload.runId);
  if (payload?.lock) await engine.store.lockFree(payload.lock, payload.runId);
  if (payload?.event) await sendSystemEvents(registry, engine, { fn: payload.fn, runId: payload.runId, event: payload.event, outcome: "cancelled" });
  await engine.store.claim(told, "told", ((engine.resource.runTimeoutSeconds ?? MAX_WAIT_S) + 90 * 86_400) * 1000);
  return true;
}

/**
 * Give a signal's data to the run that waits for it. A run that began to wait after the
 * signal was sent doesn't get it, so a message that's delivered twice resumes one wait.
 */
export async function deliverSignal(engine: Engine, tenant: string | undefined, signal: string, data: unknown, sentAt: number) {
  const item = signalKey(tenant, signal);
  const wait = await engine.store.signalRead(item);
  if (!wait || wait.createdAt > sentAt) return undefined;
  const told = await engine.callback(wait.callbackId, { signal, data: data ?? null });
  // One that's over is free for the next, too.
  await engine.store.signalFree(item, wait.callbackId);
  return told ? wait.runId : undefined;
}

// ---------------------------------------------------------------------------
// Batches
// ---------------------------------------------------------------------------

/** The most bytes of events in a batch. */
const MAX_BATCH_BYTES = 10 * 1024 * 1024;

function inBatch(def: Definition, event: WireEvent) {
  if (!def.batch?.if) return true;
  try {
    return evaluate(def.batch.if, { event });
  } catch {
    return false;
  }
}

function batchTtl(engine: Engine) {
  // As long as its run can be looked at, retried and replayed.
  // From when it closes, and as long as its events' data.
  return ((engine.resource.runTimeoutSeconds ?? MAX_WAIT_S) + 106 * 86_400) * 1000;
}

async function addToBatch(def: Definition, event: WireEvent, receivedAt: number, engine: Engine, registry: Map<string, Registered>) {
  const batch = def.batch!;
  const tenant = event.tenant ?? null;
  const item = key("B", def.id, tenant, batch.key?.({ event }) ?? null);
  const order = `${String(receivedAt).padStart(15, "0")}#${hash(event.id, 16)}`;
  await engine.store.batchAdd(item, key("BE", item, event.id), order, await slim(engine, event), receivedAt, batchTtl(engine) + batch.timeoutMs);
  // Each event has a timer of its own, so however this stops, the batch is closed in time.
  await engine.sendTimer({ type: "batch", fn: def.id, item, tenant: event.tenant }, receivedAt + batch.timeoutMs);
  await closeBatches(def, item, engine, registry);
}

/** Start the runs of the batches that are full, or whose time has come. */
async function closeBatches(def: Definition, item: string, engine: Engine, registry: Map<string, Registered>) {
  const { maxSize, timeoutMs } = def.batch!;
  const choose = (pending: Pending[]) => {
    const chosen: Pending[] = [];
    let bytes = 0;
    for (const p of pending) {
      if (chosen.length === maxSize || (chosen.length && bytes + p.bytes > MAX_BATCH_BYTES)) break;
      chosen.push(p);
      bytes += p.bytes;
    }
    const full = chosen.length === maxSize || chosen.length < pending.length;
    return chosen.length && (full || chosen[0].at + timeoutMs <= Date.now()) ? chosen : [];
  };
  for (let i = 0; i < 100; i++) {
    const closed = await engine.store.batchClose(item, maxSize + 1, choose, batchTtl(engine));
    if (!closed) return;
    // One that another router closed, and stopped before it started its run, is started
    // here. Starting it twice is safe: its run has one name.
    const [first] = await engine.store.batchEvents(closed.refs.slice(0, 1));
    if (first)
      await startRun(def, await fat(engine, first), engine, registry, {
        identity: `batch\n${item}\n${closed.n}`,
        batch: { item, n: closed.n },
        fired: true,
        received: closed.at,
      });
    await engine.store.batchDone(item, closed.n);
  }
}

// ---------------------------------------------------------------------------
// Debounce
// ---------------------------------------------------------------------------

async function debounce(def: Definition, event: WireEvent, receivedAt: number, engine: Engine, registry: Map<string, Registered>) {
  const { periodMs, timeoutMs, key: keyOf } = def.debounce!;
  const item = key("D", def.id, event.tenant ?? null, keyOf?.({ event }) ?? null);
  const at = (ms: number) => String(Math.max(0, Math.floor(ms))).padStart(15, "0");
  // The last by its own time, as events come out of order. A time that's still to come
  // doesn't hold its place against the ones sent after it.
  const order = `${at(Math.min(event.ts, receivedAt))}#${at(receivedAt)}#${event.id}`;
  const ttl = Math.max(periodMs, timeoutMs ?? 0) + MESSAGE_TTL_MS;
  const kept = await slim(engine, event);
  for (let attempt = 0; attempt < 5; attempt++) {
    const now = Date.now();
    const merged = await engine.store.debounceMerge(item, kept, order, `${at(receivedAt)}#${event.id}`, now, ttl);
    if (merged === "closing") {
      // The window's run is being started, or a router stopped before it was. Start it, and
      // this event opens the next window.
      await fireDebounce(def, item, engine, registry);
      continue;
    }
    if (merged === "stale") return log("Skipped an event that a later one replaced", { fn: def.id, event: hash(event.id, 10) });
    const due = Math.min(merged.last + periodMs, timeoutMs === undefined ? Infinity : merged.opened + timeoutMs);
    await engine.sendTimer({ type: "debounce", fn: def.id, item, last: merged.last, tenant: event.tenant }, due);
    return;
  }
  throw new Error("A debounce is busy.");
}

async function fireDebounce(def: Definition, item: string, engine: Engine, registry: Map<string, Registered>) {
  const window = await engine.store.debounceRead(item);
  if (!window?.closing) return;
  await startRun(def, await fat(engine, window.event), engine, registry, { fired: true, received: window.last });
  await engine.store.debounceDone(item, window.order);
}

// ---------------------------------------------------------------------------
// Timers
// ---------------------------------------------------------------------------

async function routeTimer(message: Extract<Message, { kind: "timer" }>, registry: Map<string, Registered>, engine: Engine) {
  const { timer, at } = message;
  if (message.exp < Date.now()) return warn("Dropped a timer that expired", { type: timer.type, fn: timer.fn });
  const entry = registry.get(timer.fn);
  const now = Date.now();
  const early = at > now + 500;
  if (timer.type === "debounce") {
    const window = await engine.store.debounceRead(timer.item);
    // Its run started, or an event came since, which has a timer of its own.
    if (!window || (!window.closing && window.last !== timer.last)) return;
    if (early && !window.closing) return engine.sendTimer(timer, at);
    // The function, or its debounce, was taken out: its last event isn't left.
    if (!entry?.def.debounce && !entry) return warn("Skipped a run of a function that's gone", { fn: timer.fn });
    if (!window.closing && !(await engine.store.debounceClose(timer.item, window.order))) return;
    return fireDebounce(entry.def, timer.item, engine, registry);
  }
  if (early) return engine.sendTimer(timer, at);
  if (timer.type === "batch") {
    if (!entry) return warn("Skipped a batch of a function that's gone", { fn: timer.fn });
    // Its batching was taken out: what's left of it starts as one batch.
    const def = entry.def.batch ? entry.def : { ...entry.def, batch: { maxSize: 100, timeoutMs: 0 } };
    return closeBatches(def, timer.item, engine, registry);
  }
  if (timer.type === "finish")
    return void (await stop(engine, registry, { arn: timer.arn, runId: timer.runId, parent: timer.parent, message: "The run didn't finish in time." }));
  // A run that hasn't started: whichever is written first decides, this or its start.
  if ((await engine.store.successor(startKey(timer.runId), "cancelled", (engine.resource.runTimeoutSeconds ?? MAX_WAIT_S) * 1000)) !== "cancelled") return;
  const run = await findRun(engine, timer.runId);
  const message_ = "The run didn't start in time.";
  if (run) return void (await stop(engine, registry, { arn: run.arn, runId: timer.runId, parent: timer.parent, message: message_ }));
  // It's held, and finds that it's cancelled when it's due.
  if (timer.parent) await notifyParent(engine, timer.parent, { name: "Cancelled", message: message_ }, timer.runId);
}

interface StartOptions {
  parent?: string;
  runId?: string;
  root?: string;
  identity?: string;
  /** When its event came. */
  received: number;
  /** It's a debounce's or a batch's run: the event has been through what comes before. */
  fired?: boolean;
  batch?: { item: string; n: number };
  /** Another EventFlow invoked it. */
  external?: boolean;
  /** It was held, and has come back: its timers were sent. */
  held?: boolean;
  /** Not before this, as for a cron's jitter. */
  notBefore?: number;
}

/**
 * Start a run of a function for an event, after its rate limit, debounce, idempotency key,
 * singleton and throttle allow it. What's decided for a function and an event is saved, so
 * a message delivered again gets the same decision, and doesn't take a second slot.
 */
export async function startRun(
  def: Definition,
  event: WireEvent,
  engine: Engine,
  registry: Map<string, Registered>,
  opts: StartOptions,
) {
  const tenant = event.tenant ?? null;
  const id = opts.runId ?? makeRunId(def.id, opts.identity ?? JSON.stringify([tenant, event.id]));
  // The run of a debounce's window is decided on again, when the window closes.
  const item = opts.fired ? key("A", def.id, id, "fired") : key("A", def.id, id);
  const now = Date.now();
  const lockItem = singletonKey(def, event);

  let admission = await engine.store.getAdmission(item);
  if (!admission) {
    const decided = await admit(def, event, engine, registry, id, lockItem, opts);
    const ttl = DEDUP_MS + (decided.decision === "hold" ? decided.at - now : 0);
    admission = await engine.store.putAdmission(item, decided, ttl);
  }

  const skip = async (reason: string, text: string) => {
    log(text, { fn: def.id, event: hash(event.id, 10) });
    if (opts.parent) await notifyParent(engine, opts.parent, { name: "InvokeError", message: `The run was skipped by its ${reason}.` }, id);
  };
  if (admission.decision === "skip") return skip(admission.reason, SKIPPED[admission.reason] ?? "Skipped a run");
  if (admission.decision === "debounce") return debounce(def, event, opts.received, engine, registry);

  if (def.timeouts?.startMs !== undefined) {
    if ((await engine.store.owner(startKey(id))) === "cancelled") {
      if (lockItem) await engine.store.lockFree(lockItem, id);
      return skip("start timeout", "Skipped a run that didn't start in time");
    }
    if (!opts.held)
      await engine.sendTimer(
        { type: "start", fn: def.id, runId: id, parent: opts.parent, tenant: event.tenant },
        opts.received + def.timeouts.startMs,
      );
  }

  if (admission.decision === "hold" && admission.at > Date.now() + 1000) {
    // Hold the run: send it back to the queue, to come again when it's due. A queue
    // delays a message at most 15 minutes, so a longer wait comes back more than once.
    await engine.sendMessages([
      {
        message: sign({
          kind: "start" as const,
          fn: def.id,
          event: await slim(engine, event),
          at: admission.at,
          // The run keeps its ID and root when it comes back, so it's the same run.
          runId: id,
          root: opts.root ?? deriveRoot(id),
          received: opts.received,
          exp: admission.at + MESSAGE_TTL_MS,
          held: true,
          ...(opts.fired ? { fired: true } : {}),
          ...(opts.batch ? { batch: opts.batch } : {}),
          ...(opts.parent ? { parent: opts.parent } : {}),
          ...(opts.external ? { external: true } : {}),
        }),
        group: event.tenant ?? `t-${hash(def.id, 10)}`,
        delaySeconds: Math.min(900, Math.ceil((admission.at - Date.now()) / 1000)),
      },
    ]);
    return;
  }

  // A run that had the singleton, and lost it to a later one while it was held. The runs it
  // took it from are stopped first.
  if (lockItem && (await settleLock(engine, lockRuns(engine, registry), lockItem, id))?.run !== id)
    return skip("singleton", "Skipped a run that a later one replaced");

  const payload: RunPayload = {
    "sst:eventflow": 1,
    fn: def.id,
    runId: id,
    root: opts.root ?? deriveRoot(id),
    event: await slim(engine, event),
    at: opts.received,
    ...(opts.batch ? { batch: opts.batch } : {}),
    ...(lockItem ? { lock: lockItem } : {}),
    ...(opts.parent ? { parent: opts.parent } : {}),
    ...(opts.external ? { external: true } : {}),
  };
  const started = await engine.startRun(payload);
  if (lockItem && started?.arn) await engine.store.lockStarted(lockItem, id, started.arn);
}

const SKIPPED: Record<string, string> = {
  rateLimit: "Skipped a run over the rate limit",
  idempotency: "Skipped a run with an idempotency key already used",
  singleton: "Skipped a run while another has its singleton",
  "start timeout": "Skipped a run that wouldn't start in time",
};

async function admit(
  def: Definition,
  event: WireEvent,
  engine: Engine,
  registry: Map<string, Registered>,
  id: string,
  lockItem: string | undefined,
  opts: StartOptions,
): Promise<Admission> {
  const scope = { event };
  const tenant = event.tenant ?? null;
  const now = Date.now();
  // A limit without a key is per tenant.
  const bucket = (kind: string, k: string | undefined) => key("T", def.id, kind, k ?? tenant);
  if (def.rateLimit && !opts.fired) {
    const item = bucket("rl", def.rateLimit.key?.(scope));
    // A router that stopped after it took the slot, and before it saved what it decided, or
    // may have: the run has it. At worst a run too many starts, and none is lost.
    const mark = key("TK", def.id, id);
    const me = ulid();
    if ((await engine.store.successor(mark, me, DEDUP_MS)) === me) {
      if ((await engine.store.take(item, def.rateLimit.intervalMs, def.rateLimit.toleranceMs)) > 0) {
        // It has none: a delivery after this one, if this stops here, asks again.
        await engine.store.unclaim(mark, me);
        return { decision: "skip", reason: "rateLimit" };
      }
    }
  }
  // A run that's invoked, or a cron's, has nothing to wait for: what invoked it waits.
  if (def.debounce && !opts.fired && !opts.parent && !opts.identity && !opts.runId) return { decision: "debounce" };
  const claimed = def.idempotency?.(scope);
  const idempotency = claimed === undefined ? undefined : key("I", def.id, tenant, claimed);
  if (idempotency && !(await engine.store.claim(idempotency, id, DEDUP_MS))) return { decision: "skip", reason: "idempotency" };
  // A run that's skipped after all gives back what it took.
  const skip = async (reason: string): Promise<Admission> => {
    if (idempotency) await engine.store.unclaim(idempotency, id);
    if (lockItem) await engine.store.lockFree(lockItem, id);
    return { decision: "skip", reason };
  };
  if (lockItem && !(await takeLock(engine, lockRuns(engine, registry), lockItem, id, def.singleton!.mode))) return skip("singleton");
  let at = opts.notBefore ?? 0;
  if (def.throttle) {
    // Each held run takes its own slot, so they come back one at a time, in order. One that
    // would be cancelled before it's due doesn't take one.
    const k = def.throttle.key?.(scope);
    const startMs = def.timeouts?.startMs;
    const due = await engine.store.reserve(
      bucket("th", k),
      def.throttle.intervalMs,
      def.throttle.toleranceMs,
      startMs === undefined ? undefined : Math.max(0, opts.received + startMs - now),
    );
    if (due === Infinity) return skip("start timeout");
    at = Math.max(at, due);
  }
  if (at <= now + 1000) return { decision: "start" };
  // While it's held there's no run to find.
  if (lockItem)
    await engine.store.lockMove(lockItem, id, { run: id, until: at + LOCK_GRACE_MS }, at - now + lockTtl(engine));
  return { decision: "hold", at };
}

// ---------------------------------------------------------------------------
// Singletons
// ---------------------------------------------------------------------------

/** What a singleton needs of runs, for the router. */
export function lockRuns(engine: Engine, registry: Map<string, Registered>): LockRuns {
  return {
    find: (run) => findRun(engine, run),
    running: async (arn) => (await runStatus(engine, arn)).running,
    stop: async (runId, arn) => {
      if (await stop(engine, registry, { arn, runId, message: "The run was cancelled by a later one, as it's a singleton." }))
        log("Cancelled a run that a later one replaced", { run: runId });
    },
  };
}

/**
 * The schedule's tick, once a minute. `time` is when it was due.
 */
export async function tick(time: string, registry: Map<string, Registered>, engine: Engine) {
  const at = new Date(time);
  if (Number.isNaN(at.getTime())) throw new Error(`The tick has no time: "${time}".`);
  const minute = new Date(Math.floor(at.getTime() / 60_000) * 60_000);
  const due = [...registry.values()].flatMap(({ def }) =>
    def.triggers.flatMap((t) => (t.kind === "cron" && cronMatches(t.cron, minute) ? [{ def, cron: t.cron, jitterMs: t.jitterMs }] : [])),
  );
  // Two crons of one function that fire in the same minute start one run.
  const unique = new Map(due.map((d) => [d.def.id, d]));
  const results = await Promise.allSettled(
    [...unique.values()].map(({ def, cron, jitterMs }) => {
      // The run is named after the minute on the cron's clock, so an hour that repeats
      // when the clocks go back starts its run once.
      const identity = `cron\n${cron.timezone}\n${localMinute(cron, minute)}`;
      const event: WireEvent = {
        id: hash(`${def.id}\n${identity}`),
        name: "inngest/scheduled.timer",
        data: { cron: cron.expression },
        ts: minute.getTime(),
      };
      // As late in the jitter as the run's name says, the same each time.
      const notBefore = jitterMs ? minute.getTime() + (parseInt(createHash("sha256").update(`${def.id}\n${identity}`).digest("hex").slice(0, 8), 16) % jitterMs) : undefined;
      return startRun(def, event, engine, registry, { identity, received: minute.getTime(), notBefore });
    }),
  );
  const failed = results.filter((r) => r.status === "rejected") as PromiseRejectedResult[];
  if (failed.length) {
    // The schedule retries the tick, and the runs that started are found by name.
    throw new Error(
      `Failed to start ${failed.length} cron runs: ${failed.map((f) => String(f.reason?.message ?? f.reason)).join("; ")}`,
    );
  }
}
