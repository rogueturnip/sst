import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns, sdk, settle, waitFor } from "./harness.ts";

const { NonRetriableError } = sdk as any;
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

// Each was a defect that the thirteenth review reproduced.
describe("signal nth", () => {
  it("a retry's second send of a signal by one ID, which the first run didn't reach, is delivered", async () => {
    let first = true;
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "waiter", triggers: { event: "w" }, retries: 0 }, async ({ step }: any) => {
        const a = await step.waitForSignal("s1", { signal: "tick", timeout: "30s" });
        const b = await step.waitForSignal("s2", { signal: "tick", timeout: "10s" });
        return [a?.data ?? null, b?.data ?? null];
      }),
      flow.createFunction({ id: "sender", triggers: { event: "a" }, retries: 0 }, async ({ step }: any) => {
        const r1 = await step.sendSignal("notify", { signal: "tick", data: 1 });
        await step.run("between", () => { if (first) throw new NonRetriableError("down"); });
        await step.run("pause", async () => { await settle(1500); });
        const r2 = await step.sendSignal("notify", { signal: "tick", data: 2 });
        return [r1.runId ?? null, r2.runId ?? null];
      }),
    ]);
    await c.flow.send({ name: "w", id: "w1" });
    await c.drain();
    await settle(1500);
    await c.flow.send({ name: "a" });
    await c.pump(2500);
    const run = c.runOf("sender");
    expect((await st(run)).s).toBe("FAILED");
    c.history.set(run.runId, { status: "FAILED", payload: run.payload, events: (await run.result).getHistoryEvents() });
    first = false;
    const again = await c.flow.runs.retry(run.runId);
    const p = st(c.run(again.id));
    await c.pump(4000);
    const r = await p;
    const w = await st(c.runOf("waiter"), 20_000);
    expect(w.v).toEqual([1, 2]);
    expect(r.v[1]).toBe(c.runOf("waiter").runId);
  }, 90_000);
});

describe("singleton ending", () => {
  const make = (log: string[]) =>
    new Cluster((flow: any) => [
      flow.createFunction({
        id: "sync", triggers: { event: "go" }, retries: 0, singleton: { key: "event.data.acct", mode: "cancel" },
        onFailure: async ({ step, event }: any) => {
          await step.run("undo-1", () => { log.push(`undo-${event.data.event.data.n}`); });
          await step.waitForEvent("approve", { event: "approve", timeout: "1h" });
          await step.run("undo-2", () => { log.push(`undone-${event.data.event.data.n}`); });
        },
      }, async ({ step, event }: any) => {
        await step.run("work", async () => { log.push(`work-${event.data.n}`); });
        if (event.data.n === 1) throw new NonRetriableError("declined");
        await step.run("write", async () => { log.push(`write-${event.data.n}`); });
      }),
    ]);

  it("a later run waits for the old one's onFailure without using its message's tries, then starts", async () => {
    const log: string[] = [];
    const c = make(log);
    await c.flow.send({ name: "go", data: { acct: "x", n: 1 } });
    await c.drain();
    await waitFor(() => (log.includes("undo-1") ? true : undefined), 600);
    await settle(500);
    await c.flow.send({ name: "go", data: { acct: "x", n: 2 } });
    // As SQS would deliver it, many times over, while the old onFailure waits.
    for (let i = 0; i < 8; i++) await c.drain({ maxRounds: 1 });
    expect(c.queue.every((m) => m.receiveCount <= 1)).toBe(true);
    expect(log).toEqual(["work-1", "undo-1"]);
    await c.flow.send({ name: "approve" });
    await c.pump(6000);
    await waitFor(() => (log.includes("write-2") ? true : undefined), 600);
    expect(log).toEqual(["work-1", "undo-1", "undone-1", "work-2", "write-2"]);
    expect(c.stopped).toEqual([]);
  }, 90_000);

  it("an earlier event that comes late doesn't take the singleton from a later one's run", async () => {
    const log: string[] = [];
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "sync", triggers: { event: "go" }, retries: 0, singleton: { key: "event.data.acct", mode: "cancel" } }, async ({ step, event }: any) => {
        await step.run("work", async () => { log.push(`work-${event.data.n}`); });
        await step.sleep("hold", "3s");
        await step.run("write", async () => { log.push(`write-${event.data.n}`); });
      }),
    ]);
    await c.flow.send({ name: "go", id: "e2", data: { acct: "x", n: 2 } });
    await settle(20);
    await c.flow.send({ name: "go", id: "e3", data: { acct: "x", n: 3 } });
    const [m2, m3] = c.queue.splice(0);
    await c.deliver([m3]);
    await waitFor(() => (log.includes("work-3") ? true : undefined), 600);
    await c.deliver([m2]);
    await c.pump(5000);
    await waitFor(() => (log.includes("write-3") ? true : undefined), 600);
    expect(log).toEqual(["work-3", "write-3"]);
    expect(c.stopped).toEqual([]);
  }, 60_000);

  it("an admin cancel doesn't cut off onFailure, unless it's forced", async () => {
    const log: string[] = [];
    const c = make(log);
    await c.flow.send({ name: "go", data: { acct: "x", n: 1 } });
    await c.drain();
    await waitFor(() => (log.includes("undo-1") ? true : undefined), 600);
    await settle(500);
    const run = c.runOf("sync");
    await expect(c.flow.runs.cancel(run.runId)).rejects.toThrow(/onFailure/);
    expect(c.stopped).toEqual([]);
    expect((await c.flow.runs.cancel(run.runId, { force: true })).stopped).toBe(true);
    expect(c.stopped).toEqual([`arn/${run.runId}`]);
  }, 60_000);
});
