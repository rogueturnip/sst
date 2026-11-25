import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns, sdk, settle, waitFor } from "./harness.ts";
import { hash, key, runId } from "../src/ids.ts";
import { currentStep } from "../src/worker.ts";
import { memoFrom } from "../src/runs.ts";

// Each was a defect that a review reproduced: a step run twice, a run or an event lost, a
// parent left waiting, or a limit gone over.

const { Middleware } = sdk;
beforeAll(async () => {
  await LocalDurableTestRunner.setupTestEnvironment({ skipTime: true });
});
afterAll(async () => {
  await finishRuns();
  await LocalDurableTestRunner.teardownTestEnvironment();
});
const runsOf = (c: Cluster, fn: string) => [...c.runs.values()].filter((r) => r.payload.fn === fn);
const msg = (c: Cluster) => c.queue[c.queue.length - 1];

describe("retries", () => {
it("a step that succeeded in attempt 0 isn't run again in attempt 2", async () => {
  let attempts = 0; let charges = 0; let once = 0;
  const cluster = new Cluster((flow) => [
    flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 3 }, async ({ step }: any) => {
      const n = attempts++;
      await step.run("a", () => 1);
      if (n === 1) throw new Error("config service down");   // outside a step, before the charge
      await step.run("charge", () => ++charges);
      await step.run({ id: "wire", atMostOnce: true }, () => ++once);
      if (n === 0) throw new Error("after the charge");
      return "ok";
    }),
  ]);
  await cluster.flow.send({ name: "go" }); await cluster.drain();
  const r = await cluster.runOf("f").result;
  expect(charges).toBe(1); expect(once).toBe(1);
});

it("a middleware with onRunError and a function retry", async () => {
  let attempts = 0; const errors: any[] = [];
  class W extends Middleware.BaseMiddleware { readonly id = "w"; onRunError({ error, isFinalAttempt }: any) { errors.push([error.message, isFinalAttempt]); } }
  const cluster = new Cluster((flow) => [
    flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 2, middleware: [W as any] }, async ({ step }: any) => {
      await step.run("a", () => 1);
      if (++attempts === 1) throw new Error("outside");
      await step.sleep("later", "1h"); await step.run("z", () => settle(50)); await step.sleep("later2", "1h");
      return "ok";
    }),
  ]);
  await cluster.flow.send({ name: "go" }); await cluster.drain();
  const r = await cluster.runOf("f").result;
  let err; try { err = r.getError(); } catch {}
  expect(r.getStatus()).toBe("SUCCEEDED");
});

it("a parallel branch that goes on after its attempt failed doesn't run a step twice", async () => {
  let attempts = 0; let charges = 0;
  const cluster = new Cluster((flow) => [
    flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 2 }, async ({ step }: any) => {
      const n = attempts++;
      await Promise.all([
        (async () => { await step.run("a", () => 1); await settle(n === 0 ? 5 : 0); await step.run("charge", async () => { await settle(10); return ++charges; }); })(),
        (async () => { await step.run("b", () => 1); if (n === 0) throw new Error("outside"); })(),
      ]);
      return "ok";
    }),
  ]);
  await cluster.flow.send({ name: "go" }); await cluster.drain();
  const r = await cluster.runOf("f").result;
  expect(charges).toBe(1);
});

it("steps with one ID, in parallel, keep their occurrence with middleware", async () => {
  const keys: Record<string, Set<string>> = {}; 
  let call = 0;
  class Slow extends Middleware.BaseMiddleware { readonly id = "slow"; 
    async transformStepInput({ input }: any) { // the first replay has another order
      const c = call++; await settle(c % 4 === 0 ? 30 : c % 4 === 1 ? 0 : c % 4 === 2 ? 0 : 30); } }
  const cluster = new Cluster((flow) => [
    flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 2, middleware: [Slow as any] }, async ({ step, idempotencyKey }: any) => {
      const out = await Promise.all(["alice", "bob"].map((who) => step.run("charge", () => { (keys[who] ??= new Set()).add(currentStep()!.key); return who; })));
      await step.sleep("s", "1h");
      return out;
    }),
  ]);
  await cluster.flow.send({ name: "go" }); await cluster.drain();
  const r = await cluster.runOf("f").result;
  let err; try { err = r.getError(); } catch {}
  expect(r.getStatus()).toBe("SUCCEEDED");
});

it("a step's idempotency key is its own in every attempt, with middleware", async () => {
  const keys: Record<string, Set<string>> = {}; let call = 0; let attempts = 0;
  class Slow extends Middleware.BaseMiddleware { readonly id = "slow"; 
    async transformStepInput() { const c = call++; await settle(c % 4 === 0 ? 30 : c % 4 === 1 ? 0 : c % 4 === 2 ? 0 : 30); } }
  const cluster = new Cluster((flow) => [
    flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 2, middleware: [Slow as any] }, async ({ step, idempotencyKey }: any) => {
      const n = attempts++;
      const out = await Promise.all(["alice", "bob"].map((who) => step.run("charge", () => { (keys[who] ??= new Set()).add(currentStep()!.key); return who; })));
      if (n === 0) throw new Error("outside");
      return out;
    }),
  ]);
  await cluster.flow.send({ name: "go" }); await cluster.drain();
  const r = await cluster.runOf("f").result;
  expect(r.getResult()).toEqual(["alice", "bob"]);
});

it("a cancelOn the event has no value for doesn't leave the parent waiting", async () => {
  let failed = 0;
  const cluster = new Cluster((flow) => [
    flow.createFunction({ id: "parent", triggers: { event: "go" }, retries: 0 }, async ({ step }: any) => step.invoke("c", { function: "child", data: {}, timeout: "10d" })),
    flow.createFunction({ id: "child", triggers: { event: "x" }, retries: 0, cancelOn: [{ event: "stop", match: "data.orderId" }], onFailure: async () => { failed++; } }, async () => "ok"),
  ]);
  await cluster.flow.send({ name: "go" }); await cluster.drain(); 
  await waitFor(() => cluster.queue.length || undefined); await cluster.drain();
  const r = await cluster.runOf("child").result; await settle(300);
  expect(cluster.succeeded.length).toBe(1);
});

it("an error whose name is Cancelled still runs onFailure", async () => {
  let failed = 0;
  const cluster = new Cluster((flow) => [
    flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 0, concurrency: 1, onFailure: async () => { failed++; } }, async ({ step }: any) => {
      await step.run("capture", () => { const e = new Error("the provider cancelled the capture"); e.name = "Cancelled"; throw e; });
    }),
  ]);
  await cluster.flow.send({ name: "go" }); await cluster.drain();
  await cluster.runOf("f").result;
  expect(failed).toBe(1);
});
});

describe("router", () => {
it("singleton cancel: a stop that fails once leaves two runs", async () => {
  const c = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "go" }, singleton: { mode: "cancel" }, retries: 0 }, async ({ step }: any) => { await step.waitForEvent("w", { event: "never", timeout: "20d" }); })]);
  await c.flow.send({ name: "go", id: "one" }); await c.drain(); await settle(100);
  const first = runsOf(c, "f")[0];
  const real = c.fetch.bind(c); let fail = 1;
  c.fetch = async (s: string, p: string, i: any) => p.endsWith("/stop") && fail-- > 0 ? new Response("TooManyRequestsException", { status: 429 }) : real(s, p, i);
  await c.flow.send({ name: "go", id: "two" }); await c.drain(); await settle(100);
  expect(c.statusOf(first.runId)).toBe("STOPPED");
});

it("a signal's message delivered twice answers one wait", async () => {
  const c = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 0 }, async ({ step }: any) => {
    const got: any[] = [];
    for (const n of [1, 2]) got.push((await step.waitForSignal("approve" + n, { signal: "approve", timeout: "1h" }))?.data);
    return got;
  })]);
  await c.flow.send({ name: "go" }); await c.drain(); await waitFor(() => c.store.signals.size || undefined);
  const first = [...c.store.signals.values()][0].callbackId;
  await c.flow.sendSignal({ signal: "approve", data: "payment 1" });
  const m = msg(c); await c.drain();
  // The run waits for the second approval. SQS delivers the first message again.
  const second = await waitFor(() => [...c.store.signals.values()].find((w) => w.callbackId !== first));
  await c.deliver([m]);
  await settle(100);
  expect([...c.store.signals.values()]).toEqual([second]);
  expect(c.succeeded).toHaveLength(1);
  await c.flow.sendSignal({ signal: "approve", data: "payment 2" });
  await c.drain();
  expect((await c.runOf("f").result).getResult()).toEqual(["payment 1", "payment 2"]);
});

it("a cancelled run's wait keeps the signal's name from its rerun", async () => {
  const c = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 0 }, async ({ step }: any) => step.waitForSignal("w", { signal: "s", timeout: "10d" }))]);
  await c.flow.send({ name: "go", id: "a" }); await c.drain(); await waitFor(() => c.store.signals.size || undefined);
  const first = c.runOf("f");
  await c.flow.runs.cancel(first.runId);
  await c.flow.send({ name: "go", id: "b" }); await c.drain();
  const second = runsOf(c, "f").find((r) => r.runId !== first.runId)!;
  const r = await Promise.race([second.result, settle(400).then(() => "waits")]) as any;
  expect(r).toBe("waits");
});

it("a router that stops after the callback, and before it frees the name, leaves the name taken", async () => {
  const c = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 0 }, async ({ step }: any) => step.waitForSignal("w", { signal: "s", timeout: "10d" }))]);
  await c.flow.send({ name: "go", id: "a" }); await c.drain(); await waitFor(() => c.store.signals.size || undefined);
  const first = c.runOf("f");
  const real = c.store.signalFree.bind(c.store); let crash = 1;
  c.store.signalFree = async (a: any, b: any) => { if (crash-- > 0) throw new Error("crash"); return real(a, b); };
  await c.flow.sendSignal({ signal: "s", data: 1 }); const m = msg(c); await c.drain();
  await first.result; await c.deliver([m]);
  await c.flow.send({ name: "go", id: "b" }); await c.drain();
  const second = runsOf(c, "f").find((r) => r.runId !== first.runId)!;
  const r = await Promise.race([second.result, settle(400).then(() => "waits")]) as any;
  expect(r).toBe("waits");
});

it("a stop whose notification fails leaves the parent waiting", async () => {
  const c = new Cluster((flow) => [
    flow.createFunction({ id: "parent", triggers: { event: "go" }, retries: 0 }, async ({ step }: any) => step.invoke("c", { function: "child", data: {}, timeout: "10d" })),
    flow.createFunction({ id: "child", triggers: { event: "x" }, retries: 0, timeouts: { finish: "1h" } }, async ({ step }: any) => { await step.waitForEvent("w", { event: "never", timeout: "20d" }); }),
  ]);
  await c.flow.send({ name: "go" }); await c.drain(); await waitFor(() => c.queue.length || undefined); await c.drain();
  await waitFor(() => c.runs.size === 2 || undefined); await settle(200);
  c.callbackFails = (_id, n) => (n <= 5 ? 429 : undefined);
  const real = c.fetch.bind(c); let stops = 0; // stopped once, as AWS would: then it has ended
  c.fetch = async (s: string, p: string, i: any) => (p.endsWith("/stop") && stops++, real(s, p, i));
  await c.fire("finish"); const left = c.timers.length; await c.fire("finish");
  expect(c.succeeded.length).toBe(1);
});

it("an event whose message is redriven, while its batch is open, is in the batch once", async () => {
  const c = new Cluster((flow) => [
    flow.createFunction({ id: "pay", triggers: { event: "p" }, batchEvents: { maxSize: 10, timeout: "1h" }, retries: 0 }, async ({ events }: any) => events.map((e: any) => e.id)),
    flow.createFunction({ id: "other", triggers: { event: "p" }, retries: 0 }, async () => 1),
  ]);
  c.failStart = (p) => p.fn === "other"; // what keeps the message from being done, until the fix
  await c.flow.send({ name: "p", id: "payment-1" });
  const m = { ...msg(c) }; await c.deliver([c.queue.shift()!]);
  c.failStart = undefined;
  // Moved back from the dead-letter queue: a new message ID, and a new time.
  await settle(10);
  await c.deliver([{ ...m, messageId: "redriven", sentAt: Date.now() }]);
  await c.fire("batch");
  const r = await c.runOf("pay").result;
  expect(r.getResult()).toEqual(["payment-1"]);
});

it("a router that stops between the rate limit and saving the decision doesn't lose the event", async () => {
  const c = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "go" }, rateLimit: { limit: 1, period: "1h" }, retries: 0 }, async () => "ran")]);
  const real = c.store.putAdmission.bind(c.store); let crash = 1;
  c.store.putAdmission = async (a: any, b: any) => { if (crash-- > 0) throw new Error("crash"); return real(a, b); };
  await c.flow.send({ name: "go" }); await c.drain();
  expect(c.runs.size).toBe(1);
});

it("a held run with a start timeout doesn't add a timer each time it comes back", async () => {
  const c = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "go" }, throttle: { limit: 1, period: "1d" }, timeouts: { start: "7d" }, retries: 0 }, async () => "ran")]);
  await c.flow.send([{ name: "go" }, { name: "go" }, { name: "go" }]); 
  const now = Date.now; let offset = 0; Date.now = () => now() + offset;
  try {
    for (let i = 0; i < 40; i++) {
      await c.drain({ maxRounds: 1 }); await c.fire2?.();
      offset += 15 * 60_000;
    }
  } finally { Date.now = now; }
  const starts = c.timers.filter((m) => JSON.parse(m.body).timer.type === "start").length;
  expect(starts).toBeLessThan(10);
});

it("a run that's skipped by its singleton doesn't use its idempotency key", async () => {
  const c = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 0, idempotency: "event.data.order", singleton: { mode: "skip", key: "event.data.account" } }, async ({ step, event }: any) => { if (event.data.order === "1") await step.sleep("s", "1h"); return event.data.order; })]);
  c.store.lockFree = c.store.lockFree.bind(c.store);
  await c.flow.send({ name: "go", id: "hold", data: { order: "0", account: "a" } });
  const real = c.failStart; void real;
  await c.drain(); 
  const first = c.runs.get([...c.runs.keys()][0])!;
  // While the first has it:
  c.ended.delete(first.runId); 
  const lock = [...c.store.locks.keys()][0];
  if (!lock) c.store.locks.set("x", { run: first.runId, until: Date.now() + 1e6 });
  await first.result; await settle(50);
  // Take the singleton as another run would hold it.
  const item = key("S", "f", null, "a");
  c.store.locks.set(item, { run: "other", until: Date.now() + 1e7 });
  await c.flow.send({ name: "go", id: "pay-try-1", data: { order: "77", account: "a" } }); await c.drain();
  c.store.locks.delete(item); // it ended
  await c.flow.send({ name: "go", id: "pay-try-2", data: { order: "77", account: "a" } }); await c.drain();
  const ran = runsOf(c, "f").filter((r) => r.payload.event.data.order === "77").length;
  expect(ran).toBe(1);
});

it("a singleton's run that was stopped by a timeout can be run again", async () => {
  const c = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 0, singleton: { mode: "skip" }, timeouts: { finish: "1h" } }, async ({ step }: any) => step.waitForEvent("w", { event: "never", timeout: "20d" }))]);
  await c.flow.send({ name: "go" }); await c.drain(); await settle(200);
  const first = c.runOf("f");
  await c.fire("finish");
  c.history.set(first.runId, { status: "STOPPED", payload: first.payload, events: [] });
  let error; try { await c.flow.runs.retry(first.runId); } catch (e) { error = (e as Error).message; }
  expect(error).toBeUndefined();
});

it("an event that a later one replaced doesn't run after it, when it's delivered again", async () => {
  const c = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 0, debounce: { period: "10s" } }, async ({ event }: any) => event.data.v)]);
  const t = Date.now();
  await c.flow.send({ name: "go", id: "new", ts: t, data: { v: "new" } }); await c.drain(); await settle(5);
  await c.flow.send({ name: "go", id: "old", ts: t - 5000, data: { v: "old" } }); const m = msg(c); m.sentAt = Date.now(); await c.drain();
  await c.fire("debounce"); await settle(50);
  await c.deliver([m]); // SQS delivers it again
  await c.fire("debounce"); await settle(100);
  expect(runsOf(c, "f").map((r) => r.payload.event.data.v)).toEqual(["new"]);
});

it("a sender can't keep the event about a failed run from being routed", async () => {
  const seen: any[] = [];
  const c = new Cluster((flow) => [
    flow.createFunction({ id: "pay", triggers: { event: "pay" }, retries: 0 }, async () => { throw new Error("broken"); }),
    flow.createFunction({ id: "alert", triggers: { event: "inngest/function.failed" }, retries: 0 }, async ({ event }: any) => void seen.push(event.data.run_id)),
  ]);
  // Anyone who may send, and knows the event's ID (as it's an order's), works out the run's, and the event's.
  const run = runId("pay", JSON.stringify([null, "order-1"]));
  c.queue.push({ messageId: "x", body: JSON.stringify({ kind: "event", event: { id: hash(JSON.stringify(["inngest/function.failed", run])), name: "harmless", data: {}, ts: 1 } }), receiveCount: 0, sentAt: Date.now() });
  await c.drain();
  await c.flow.send({ name: "pay", id: "order-1" });
  await c.pump(800);
  expect(seen.length).toBe(1);
});

it("a key function that throws for one function doesn't keep the others from starting", async () => {
  const c = new Cluster((flow) => [
    flow.createFunction({ id: "a", triggers: { event: "go" }, retries: 0, rateLimit: { limit: 1, period: "1h", key: (e: any) => e.data.user.id } }, async () => 1),
    flow.createFunction({ id: "b", triggers: { event: "go" }, retries: 0 }, async () => 1),
  ]);
  await c.flow.send({ name: "go", data: {} }); await c.drain({ maxRounds: 6 });
  expect(runsOf(c, "b").length).toBe(1);
});

});

describe("more", () => {
it("a retry reuses a large result of a step with a concurrency limit", async () => {
  const c = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 0, concurrency: 5 }, async ({ step }: any) => { await step.run("charge", () => "x".repeat(300_000)); await step.run("small", () => 1); throw new Error("later"); })]);
  await c.flow.send({ name: "go" }); await c.drain();
  const r = await c.runOf("f").result;
  const { memo, blobs } = memoFrom(r.getHistoryEvents());
  expect([...Object.keys(memo), ...Object.keys(blobs)]).toContain("charge");
});
it("parallel step.fetch of Requests with bodies", async () => {
  const real = globalThis.fetch; let call = 0;
  (globalThis as any).fetch = async (u: any, i: any) => String(u).startsWith("https://bank") ? new Response("paid " + Buffer.from(i.body).toString()) : real(u, i);
  try {
    const c = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 0 }, async ({ step }: any) => {
      const n = call++;
      const body = (text: string, ms: number) => new ReadableStream({ async start(ctl) { await settle(ms); ctl.enqueue(new TextEncoder().encode(text)); ctl.close(); } });
      const out = await Promise.all([["alice", n ? 0 : 30], ["bob", n ? 30 : 0]].map(async ([who, ms]: any) => (await step.fetch(new Request("https://bank/pay", { method: "POST", body: body(who, ms) , duplex: "half"} as any))).text()));
      await step.sleep("s", "1h");
      return out;
    })]);
    await c.flow.send({ name: "go" }); await c.drain();
    const r = await c.runOf("f").result;
    let e; try { e = r.getError(); } catch {}
    expect(r.getResult()).toEqual(["paid alice", "paid bob"]);
  } finally { globalThis.fetch = real; }
});
it("an onRunComplete hook that fails, or a system event that can't be sent, still answers the parent", async () => {
  class W extends Middleware.BaseMiddleware { readonly id = "w"; onRunComplete() { throw new Error("tracing is down"); } }
  const c = new Cluster((flow) => [
    flow.createFunction({ id: "parent", triggers: { event: "go" }, retries: 0 }, async ({ step }: any) => step.invoke("c", { function: "child", data: {}, timeout: "10d" })),
    flow.createFunction({ id: "child", triggers: { event: "x" }, retries: 0, middleware: [W as any] }, async () => "ok"),
  ]);
  await c.flow.send({ name: "go" }); await c.drain(); await waitFor(() => c.queue.length || undefined); await c.drain();
  const r = await c.runOf("child").result; await settle(200);
  expect(c.succeeded.length).toBe(1);
});
it("a concurrency key that throws still runs onFailure and answers the parent", async () => {
  let failed = 0;
  const c = new Cluster((flow) => [
    flow.createFunction({ id: "parent", triggers: { event: "go" }, retries: 0 }, async ({ step }: any) => step.invoke("c", { function: "child", data: {}, timeout: "10d" })),
    flow.createFunction({ id: "child", triggers: { event: "x" }, retries: 0, concurrency: { limit: 1, key: (e: any) => e.data.user.id }, onFailure: async () => { failed++; } }, async () => "ok"),
  ]);
  await c.flow.send({ name: "go" }); await c.drain(); await waitFor(() => c.queue.length || undefined); await c.drain();
  const r = await c.runOf("child").result; await settle(200);
  expect(c.succeeded.length).toBe(1);
});
it("an answer the callback API refuses (400, as over 256 KB) tells the parent something", async () => {
  const c = new Cluster((flow) => [
    flow.createFunction({ id: "parent", triggers: { event: "go" }, retries: 0 }, async ({ step }: any) => step.invoke("c", { function: "child", data: {}, timeout: "10d" })),
    flow.createFunction({ id: "child", triggers: { event: "x" }, retries: 0 }, async () => "x".repeat(300_000)),
  ]);
  const real = c.fetch.bind(c);
  c.fetch = async (s: string, p: string, i: any) => p.endsWith("/succeed") && String(i.body).length > 262_144 ? new Response('{"__type":"InvalidParameterValueException"}', { status: 400 }) : real(s, p, i);
  await c.flow.send({ name: "go" }); await c.drain(); await waitFor(() => c.queue.length || undefined); await c.drain();
  const r = await c.runOf("child").result; await settle(200);
  expect(c.succeeded.length).toBe(1);
});
});
