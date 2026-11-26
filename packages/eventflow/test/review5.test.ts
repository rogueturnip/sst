import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns, sdk, settle, waitFor } from "./harness.ts";

const { NonRetriableError, Middleware } = sdk as any;
beforeAll(async () => {
  await LocalDurableTestRunner.setupTestEnvironment({ skipTime: false });
});
afterAll(async () => {
  await finishRuns();
  await LocalDurableTestRunner.teardownTestEnvironment();
});
const status = async (r: any) => {
  const x = await r.result;
  let e, v;
  try { e = x.getError(); } catch {}
  try { v = x.getResult(); } catch {}
  return { s: x.getStatus(), e, v };
};
const st = (r: any, ms = 20_000) => Promise.race([status(r), settle(ms).then(() => ({ s: "HANG" }) as any)]);

// Each was a defect that the tenth review reproduced.
describe('unclaim', () => {
  it("a retried invoke that its singleton skips frees the idempotency key of the run still charging", async () => {
    const charged: string[] = [];
    const c = new Cluster((flow: any) => {
      const charge = flow.createFunction({ id: "charge", triggers: { event: "charge" }, retries: 0, idempotency: "event.data.orderId", singleton: { key: "event.data.account", mode: "skip" } },
        async ({ step, event, runId }: any) => {
          await step.run("pay", () => { charged.push(`${event.data.orderId}@${runId}`); });
          await step.waitForEvent("settle", { event: "never", timeout: "3s" });
          return "ok";
        });
      return [charge, flow.createFunction({ id: "checkout", triggers: { event: "go" }, retries: 0 }, async ({ step }: any) =>
        step.invoke("charge", { function: charge, data: { orderId: 7, account: "x" }, timeout: "1s" }))];
    });
    await c.flow.send({ name: "go" }); await c.pump(2500);
    const p = c.runOf("checkout"); const r1 = await st(p); void ("first", r1.s);
    c.history.set(p.runId, { status: "FAILED", payload: p.payload, events: (await p.result).getHistoryEvents() });
    const again = await c.flow.runs.retry(p.runId);
    const pr = st(c.run(again.id)); await c.pump(1500); const r2 = await pr;
    void ("retry", r2.s, r2.e?.errorMessage);
    await settle(3000); await c.pump(500); // the first charge ends
    await c.flow.send({ name: "charge", data: { orderId: 7, account: "x" } }); await c.pump(1500);
    void ("charged", charged);
    expect(charged.length).toBe(1);
  }, 40_000);

});

describe('sigshift', () => {
  it("a retry's signal for B is answered with the run that A's went to, and B's wait isn't told", async () => {
    let first = true;
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "waiter", triggers: { event: "w" }, retries: 0 }, async ({ step, event }: any) =>
        (await step.waitForSignal("s", { signal: `approve-${event.data.id}`, timeout: "6s" }))?.data ?? "TIMED OUT"),
      flow.createFunction({ id: "order", triggers: { event: "a" }, retries: 0 }, async ({ step }: any) => {
        try { await step.run("loyalty", () => { if (first) throw new Error("down"); }); } catch {}
        const items = await step.run("cart", () => (first ? ["A"] : ["B"]));
        const got: any[] = [];
        for (const item of items) got.push([item, (await step.sendSignal("notify", { signal: `approve-${item}`, data: item })).runId]);
        await step.run("ship", () => { if (first) throw new NonRetriableError("carrier down"); });
        return got;
      })]);
    await c.flow.send([{ name: "w", data: { id: "A" } }, { name: "w", data: { id: "B" } }]);
    await c.drain();
    await waitFor(() => (c.store.signalItems?.size ?? [...(c.store as any).items?.keys?.() ?? []].length) >= 0 ? true : undefined);
    await settle(1500);
    await c.flow.send({ name: "a" }); await c.pump(2500);
    const run = c.runOf("order"); const r1 = await st(run); void ("first", r1.s, JSON.stringify(r1.v));
    c.history.set(run.runId, { status: "FAILED", payload: run.payload, events: (await run.result).getHistoryEvents() });
    first = false;
    const again = await c.flow.runs.retry(run.runId);
    const p = st(c.run(again.id)); await c.pump(3000);
    const r = await p;
    const waiters = [...c.runs.values()].filter((x) => x.payload.fn === "waiter");
    const outs = await Promise.all(waiters.map(async (w) => [w.payload.event.data.id, w.runId, (await st(w)).v]));
    void ("retry", r.s, JSON.stringify(r.v), "waiters", JSON.stringify(outs));
    expect(outs.find((o) => o[0] === "B")![2]).toBe("B");
  }, 40_000);

});

describe('mwsend', () => {
  it("a middleware that adds an event in a retry moves the event IDs: the charge is sent twice", async () => {
    let first = true; const charged: string[] = []; const audits: number[] = [];
    const Audit = class extends Middleware.BaseMiddleware {
      readonly id = "audit";
      transformSendEvent({ events }: any) { return { events: first ? events : [{ name: "audit", data: {} }, ...events] }; }
    };
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "charge", triggers: { event: "charge" }, retries: 0 }, async ({ event, runId }: any) => { charged.push(runId); }),
      flow.createFunction({ id: "audit", triggers: { event: "audit" }, retries: 0 }, async () => { audits.push(1); }),
      flow.createFunction({ id: "order", triggers: { event: "a" }, retries: 0, middleware: [Audit] }, async ({ step }: any) => {
        try { await step.run("loyalty", () => { if (first) throw new Error("down"); }); } catch {}
        await step.sendEvent("bill", { name: "charge", data: { order: 7 } });
        await step.run("ship", () => { if (first) throw new NonRetriableError("carrier down"); });
      })]);
    await c.flow.send({ name: "a" }); await c.pump(2500);
    const run = c.runOf("order"); void ("first", (await st(run)).s);
    c.history.set(run.runId, { status: "FAILED", payload: run.payload, events: (await run.result).getHistoryEvents() });
    first = false;
    const again = await c.flow.runs.retry(run.runId);
    const p = st(c.run(again.id)); await c.pump(3000); void ("retry", (await p).s);
    void ("charge runs", charged, "audit runs", audits.length);
    expect(charged.length).toBe(1);
  }, 40_000);

});

describe('blobgone', () => {
  it("a retry refused for a gone blob keeps the singleton", async () => {
    const runs: string[] = [];
    const c = new Cluster((flow: any) => [flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 0, singleton: { key: "event.data.acct", mode: "skip" }, concurrency: [{ limit: 5 }] }, async ({ step, runId }: any) => {
      runs.push(runId);
      await step.run("big", () => "x".repeat(150_000));
      await step.run("fail", () => { throw new NonRetriableError("no"); });
    })]);
    await c.flow.send({ name: "go", data: { acct: 1 } }); await c.pump(1500);
    const run = c.runOf("f"); void ("first", (await st(run)).s, "blobs", c.store.blobs.size);
    c.history.set(run.runId, { status: "FAILED", payload: run.payload, events: (await run.result).getHistoryEvents() });
    c.store.blobs.clear();
    try { await c.flow.runs.retry(run.runId); } catch (e) { void ("retry refused:", (e as Error).message); }
    await c.flow.send({ name: "go", data: { acct: 1 } }); await c.pump(1500);
    void ("runs", runs.length);
    expect(runs.length).toBe(2);
  }, 40_000);

});

describe('stopping2', () => {
  it("a run being stopped as it ends is cancelled, not cut off in its onFailure", async () => {
    const log: string[] = [];
    const c = new Cluster((flow: any) => [flow.createFunction({
      id: "pay", triggers: { event: "go" }, retries: 0, timeouts: { finish: "1s" },
      onFailure: async ({ step }: any) => {
        await step.run("refund-1", () => { log.push("refund-1"); });
        await step.sleep("between", "1500ms");
        await step.run("refund-2", () => { log.push("refund-2"); });
      },
    }, async ({ step }: any) => {
      await step.run("charge", () => { log.push("charge"); });
      await step.sleep("wait", "1500ms");
      await step.run("fail", () => { throw new NonRetriableError("declined"); });
    })]);
    let throttled = 0;
    const real = c.fetch.bind(c);
    c.fetch = async (s: any, p: any, i: any) => {
      if (/\/stop$/.test(p) && throttled++ === 0) { log.push("stop throttled"); return new Response('{"__type":"TooManyRequestsException"}', { status: 429 }); }
      if (/\/stop$/.test(p)) log.push("stopped");
      return real(s, p, i);
    };
    await c.flow.send({ name: "go", data: { id: "x" } });
    await c.drain();
    await waitFor(() => (log.includes("charge") ? true : undefined), 600);
    await settle(200);
    await c.fire("finish");
    await settle(2500);
    await c.fire("finish");
    const run = c.runOf("pay"); await Promise.race([run.result, settle(5000)]);
    void (log, "status", c.statusOf(run.runId), "stopped", c.stopped);
    // It was being stopped as it ended: it's cancelled, and onFailure doesn't run, as for one
    // that's timed out.
    expect(log).not.toContain("refund-1");
  }, 40_000);

});
