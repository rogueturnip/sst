import { afterAll, beforeAll, expect, it } from "bun:test";
import { ExecutionStatus, LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns, sdk, settle, waitFor } from "./harness.ts";

// Each was a defect that the eighth review reproduced.

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

it("one approve event, redelivered, satisfies two waits", async () => {
  const log: string[] = [];
  const c = new Cluster((flow: any) => [
    flow.createFunction({ id: "installments", triggers: { event: "plan/start" }, retries: 0 }, async ({ step }: any) => {
      for (let i = 0; i < 2; i++) {
        const ok = await step.waitForEvent(`approve-${i}`, { event: "plan/approve", match: "data.id", timeout: "1h" });
        if (!ok) return "timed out";
        await step.run(`charge-${i}`, () => void log.push(`charge-${i} by ${ok.id}`));
      }
    }),
    // Another function on the same event, whose start fails once (a throttle).
    flow.createFunction({ id: "audit", triggers: { event: "plan/approve" }, retries: 0 }, async () => {}),
  ]);
  await c.flow.send({ name: "plan/start", data: { id: "p1" }, tenant: "acme" });
  await c.drain();
  await waitFor(() => [...c.store.waiterItems.values()].flat().length > 0 || undefined);
  let fails = 1;
  c.failStart = (p: any) => p.fn === "audit" && fails-- > 0;
  await c.flow.send({ name: "plan/approve", data: { id: "p1" }, tenant: "acme", id: "approval-1" });
  const msg = c.queue.splice(0, 1)[0];
  const r1 = await c.deliver([msg]);
  void ("first delivery failures", r1.batchItemFailures.length);
  // The run resumes and starts its second wait, within a second of the event.
  await waitFor(() => [...c.store.waiterItems.values()].flat().some((w: any) => true) && log.length === 1 || undefined);
  await settle(150);
  void ("since sent ms", Date.now() - msg.sentAt, "waiters", [...c.store.waiterItems.values()].flat().length);
  const r2 = await c.deliver([msg]);  // SQS redelivers the same message
  void ("second delivery failures", r2.batchItemFailures.length);
  await settle(1500);
  void ("log", log);
  expect(log.length).toBe(1);
}, 30_000);

it("a redelivered event cancels the run it started", async () => {
  const log: string[] = [];
  const c = new Cluster((flow: any) => [
    flow.createFunction({ id: "sync", triggers: { event: "order/updated" }, retries: 0,
      cancelOn: [{ event: "order/updated", match: "data.id" }] }, async ({ step, event }: any) => {
      await step.sleep("settle", "2s");
      await step.run("apply", () => void log.push(`apply v${event.data.v}`));
    }),
    flow.createFunction({ id: "audit", triggers: { event: "order/updated" }, retries: 0 }, async () => {}),
  ]);
  let fails = 1;
  c.failStart = (p: any) => p.fn === "audit" && fails-- > 0;
  await c.flow.send({ name: "order/updated", data: { id: "o1", v: 1 }, tenant: "acme" });
  const msg = c.queue.splice(0, 1)[0];
  await c.deliver([msg]);
  await waitFor(() => [...c.store.waiterItems.values()].flat().length > 0 || undefined);
  void ("since sent ms", Date.now() - msg.sentAt);
  await c.deliver([msg]); // the same message again, as SQS does
  await settle(3500);
  void ("stopped", c.stopped, "log", log);
  expect(c.stopped.length).toBe(0);
}, 30_000);

it("a loop over a list that a re-run step gives, with one item each time", async () => {
  let first = true;
  const charged: string[] = []; const receipts: string[] = [];
  const c = new Cluster((flow: any) => [
    flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 0 }, async ({ step }: any) => {
      // The list of unpaid orders; when the API is down, the order in the event.
      const unpaid: string[] = await step.run("fetch-unpaid", () => { if (first) throw new Error("api down"); return ["B"]; }).catch(() => ["A"]);
      for (const o of unpaid) {
        const r = await step.run("charge", () => { charged.push(o); return { order: o, paid: 100 }; });
        await step.run(`receipt-${o}`, () => void receipts.push(`${o} paid by charge of ${r.order}`));
      }
      await step.run("ship", () => { if (first) throw new NonRetriableError("carrier down"); return "ok"; });
      return "ok";
    }),
  ]);
  await c.flow.send({ name: "go" }); await c.drain();
  const run = c.runOf("f"); await st(run);
  c.history.set(run.runId, { status: "FAILED", payload: run.payload, events: (await run.result).getHistoryEvents() });
  first = false;
  const again = await c.flow.runs.retry(run.runId);
  const result = await st(c.run(again.id));
  void ("reused", again.reused, "result", result.s, "charged", charged, "receipts", receipts);
  // Another list: the charge isn't given A's result, it runs again, for B.
  expect(result.v).toBe("ok");
  expect(charged).toEqual(["A", "B"]);
  expect(receipts).toEqual(["A paid by charge of A", "B paid by charge of B"]);
}, 30_000);
