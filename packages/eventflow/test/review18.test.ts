import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns } from "./harness.ts";

beforeAll(async () => {
  await LocalDurableTestRunner.setupTestEnvironment({ skipTime: true });
});
afterAll(async () => {
  await finishRuns();
  await LocalDurableTestRunner.teardownTestEnvironment();
});

// Found by the twenty-fifth review: Inngest schedules the runs of an event whose `ts` is still
// to come for that time, a common way to delay a job. EventFlow takes `ts` and starts them at once.
describe("an event whose ts is still to come", () => {
  it("doesn't start its runs before then", async () => {
    let started = 0;
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "later", triggers: { event: "remind" } }, async () => {
        started++;
      }),
    ]);
    await c.flow.send({ name: "remind", ts: Date.now() + 3_600_000 });
    await c.drain();
    // An hour from now, so nothing has started yet.
    expect(c.runs.size).toBe(0);
    expect(started).toBe(0);
  }, 60_000);
});

// With Inngest, `match: "data.orderId"` is `event.data.orderId == async.data.orderId`: a run whose
// event doesn't have the field waits, and gets null at the timeout. EventFlow fails the run, at once
// and without retries, where the docs of waits don't say so.
describe("a wait that matches on a field the run's event doesn't have", () => {
  it("times out with null, and doesn't fail the run", async () => {
    let got: unknown = "unset";
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "approve", triggers: { event: "order/placed" } }, async ({ step }: any) => {
        got = await step.waitForEvent("approval", { event: "order/approved", timeout: "2s", match: "data.orderId" });
        return "done";
      }),
    ]);
    // An order placed without an orderId, as one sent by an older client.
    await c.flow.send({ name: "order/placed", data: {} });
    await c.drain();
    const result = await [...c.runs.values()][0].result;
    expect(result.getStatus()).toBe("SUCCEEDED");
    expect(got).toBeNull();
  }, 60_000);
});
