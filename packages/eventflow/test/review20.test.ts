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

/** Run `fn` with the clock `ms` ahead. */
async function later<T>(ms: number, fn: () => Promise<T>) {
  const now = Date.now;
  Date.now = () => now() + ms;
  try {
    return await fn();
  } finally {
    Date.now = now;
  }
}

// Found by the twenty-seventh review: an event of a time to come starts its run only when it's
// due, and the run listens for its cancelOn only then, for events received after that. So a
// cancel sent while it's still to come is missed, and the run goes on. With Inngest, a future ts
// is as `step.sleepUntil(event.ts)` at the start of the run, and a cancelOn stops it meanwhile.
describe("an event whose ts is still to come, for a function with cancelOn", () => {
  it("is cancelled by a matching event sent before it's due", async () => {
    const reminded: string[] = [];
    const c = new Cluster((flow: any) => [
      flow.createFunction(
        { id: "reminder", triggers: { event: "reminder/scheduled" }, cancelOn: [{ event: "reminder/cancelled", match: "data.id" }] },
        async ({ event, step }: any) => step.run("remind", () => void reminded.push(event.data.id)),
      ),
    ]);
    await c.flow.send({ name: "reminder/scheduled", data: { id: "r1" }, ts: Date.now() + 86_400_000 });
    await c.drain();
    // Two hours on, the user cancels it.
    await later(7_200_000, async () => {
      await c.flow.send({ name: "reminder/cancelled", data: { id: "r1" } });
      await c.drain();
    });
    // A day on, it's due.
    await later(86_402_000, () => c.drain());
    for (const run of c.runs.values()) await (run as any).result;
    expect(reminded).toEqual([]);
  }, 60_000);
});

// A large event of a time to come goes back to the queue every 15 minutes with all its data,
// up to 1 MB, where a held run's message has a reference to it. SQS bills a message by 64 KB,
// so each of its 96 trips a day costs 16 requests to send, and 16 to receive.
describe("a large event whose ts is still to come", () => {
  it("goes back to the queue without its data", async () => {
    const c = new Cluster((flow: any) => [flow.createFunction({ id: "later", triggers: { event: "big" } }, async () => "ok")]);
    await c.flow.send({ name: "big", data: { blob: "x".repeat(900_000) }, ts: Date.now() + 86_400_000 });
    await c.drain({ maxRounds: 1 });
    expect(c.queue).toHaveLength(1);
    expect(c.queue[0].body.length).toBeLessThan(200_000);
  }, 60_000);
});
