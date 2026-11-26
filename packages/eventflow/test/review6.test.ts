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

// Each was a defect that the eleventh review reproduced.
describe('sigdup', () => {

  it("a retry where a list changed sends a signal again to a wait that began after", async () => {
    let first = true;
    const approvals: string[] = [];
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "waiter", triggers: { event: "w" }, retries: 0 }, async ({ step, event, runId }: any) => {
        const got = await step.waitForSignal("s", { signal: `approve-${event.data.id}`, timeout: "8s" });
        if (got) await step.run("pay", () => { approvals.push(`${event.data.id}#${event.data.n}`); });
        return got?.data ?? "TIMED OUT";
      }),
      flow.createFunction({ id: "order", triggers: { event: "a" }, retries: 0 }, async ({ step }: any) => {
        try { await step.run("loyalty", () => { if (first) throw new Error("down"); }); } catch {}
        const items = await step.run("cart", () => (first ? ["A", "B"] : ["B", "A"]));
        const got: any[] = [];
        for (const item of items) got.push([item, (await step.sendSignal("notify", { signal: `approve-${item}`, data: item })).runId]);
        await step.run("ship", () => { if (first) throw new NonRetriableError("carrier down"); });
        return got;
      })]);
    await c.flow.send([{ name: "w", id: "w1", data: { id: "A", n: 1 } }, { name: "w", id: "w2", data: { id: "B", n: 1 } }]);
    await c.drain(); await settle(1500);
    await c.flow.send({ name: "a" }); await c.pump(2500);
    const run = c.runOf("order"); const r1 = await st(run);
    void ("first", r1.s, JSON.stringify(r1.v), approvals);
    c.history.set(run.runId, { status: "FAILED", payload: run.payload, events: (await run.result).getHistoryEvents() });
    // Later orders of A and B, waiting for their own approval.
    await c.flow.send([{ name: "w", id: "w3", data: { id: "A", n: 2 } }, { name: "w", id: "w4", data: { id: "B", n: 2 } }]);
    await c.drain(); await settle(1500);
    first = false;
    const again = await c.flow.runs.retry(run.runId);
    const p = st(c.run(again.id)); await c.pump(3000);
    const r = await p;
    await c.pump(1500);
    void ("retry", r.s, JSON.stringify(r.v), "approvals", approvals);
    expect(approvals.filter((a) => a.endsWith("#2"))).toEqual([]);
  }, 60_000);

});

describe('singleton', () => {

  // As in Inngest, where onFailure is a function of its own, a later run may start beside it; it
  // never cuts it off.
  it("a later run of a cancel singleton doesn't cut off the earlier one's onFailure", async () => {
    const log: string[] = [];
    let busy = 0, most = 0;
    const enter = (s: string) => { busy++; most = Math.max(most, busy); log.push(`+${s}`); };
    const leave = (s: string) => { busy--; log.push(`-${s}`); };
    const c = new Cluster((flow: any) => [flow.createFunction({
      id: "sync", triggers: { event: "go" }, retries: 0, singleton: { key: "event.data.acct", mode: "cancel" },
      onFailure: async ({ step, event }: any) => {
        await step.run("undo-1", () => enter(`undo-${event.data.event.data.n}`));
        await step.sleep("between", "2s");
        await step.run("undo-2", () => leave(`undo-${event.data.event.data.n}`));
      },
    }, async ({ step, event }: any) => {
      await step.run("work", async () => { log.push(`work-${event.data.n}`); });
      if (event.data.n === 1) throw new NonRetriableError("declined");
      await step.run("write", async () => { enter(`write-${event.data.n}`); await settle(300); leave(`write-${event.data.n}`); });
    })]);
    await c.flow.send({ name: "go", data: { acct: "x", n: 1 } });
    await c.drain();
    await waitFor(() => (log.includes("+undo-1") ? true : undefined), 600);
    await c.flow.send({ name: "go", data: { acct: "x", n: 2 } });
    await c.pump(4000);
    await waitFor(() => (log.includes("-undo-1") ? true : undefined), 600);
    expect(c.stopped).toEqual([]);
    expect(log.filter((l) => l.startsWith("-"))).toEqual(expect.arrayContaining(["-undo-1", "-write-2"]));
    void most;
  }, 40_000);

});

describe('failedcancel', () => {

  it("a run that failed, while a step's code still runs, is cancelled by an event, and onFailure doesn't run", async () => {
    const log: string[] = [];
    const c = new Cluster((flow: any) => [flow.createFunction({
      id: "pay", triggers: { event: "go" }, retries: 0,
      cancelOn: [{ event: "stop", match: "data.id" }],
      onFailure: async ({ step }: any) => { await step.run("refund", () => { log.push("refund"); }); },
    }, async ({ step }: any) => {
      const charge = step.run("charge", async () => { log.push("charge started"); await settle(3000); log.push("charged"); });
      const first = await Promise.race([charge.then(() => "charged"), step.sleep("limit", "1s").then(() => "late")]);
      if (first === "late") { log.push("failed"); throw new NonRetriableError("the charge took too long"); }
    })]);
    await c.flow.send({ name: "go", data: { id: "x" } });
    await c.drain();
    await waitFor(() => (log.includes("failed") ? true : undefined), 600);
    await settle(300);
    await c.flow.send({ name: "stop", data: { id: "x" } });
    await c.drain();
    const run = c.runOf("pay"); await Promise.race([run.result, settle(8000)]);
    await settle(500);
    void (log, "status", c.statusOf(run.runId), "stopped", c.stopped);
    expect(log).toContain("refund");
  }, 40_000);

});

describe('mwadd', () => {

  it("an event a middleware appends is sent again in a retry where the list is longer", async () => {
    let first = true; const audits: string[] = [];
    const Audit = class extends Middleware.BaseMiddleware {
      readonly id = "audit";
      transformSendEvent({ events }: any) { return { events: [...events, { name: "audit", data: { batch: 1 } }] }; }
    };
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "audit", triggers: { event: "audit" }, retries: 0 }, async ({ runId }: any) => { audits.push(runId); }),
      flow.createFunction({ id: "order", triggers: { event: "a" }, retries: 0, middleware: [Audit] }, async ({ step }: any) => {
        try { await step.run("loyalty", () => { if (first) throw new Error("down"); }); } catch {}
        const items = await step.run("cart", () => (first ? ["A"] : ["A", "B"]));
        await step.sendEvent("notify", items.map((i: string) => ({ name: "noop", data: { i }, id: `n-${i}` })));
        await step.run("ship", () => { if (first) throw new NonRetriableError("carrier down"); });
      })]);
    await c.flow.send({ name: "a" }); await c.pump(2500);
    const run = c.runOf("order"); await st(run);
    c.history.set(run.runId, { status: "FAILED", payload: run.payload, events: (await run.result).getHistoryEvents() });
    first = false;
    const again = await c.flow.runs.retry(run.runId);
    const p = st(c.run(again.id)); await c.pump(3000); void ("retry", await p);
    await c.pump(1000);
    void ("audit runs", audits);
    expect(audits.length).toBe(1);
  }, 40_000);

});
