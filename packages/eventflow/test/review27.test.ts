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

const DAY = 86_400_000;

// Found by the thirty-fourth review. A hop of an event of a time to come is claimed by its SQS
// messageId before the next hop is sent (router.ts:189). When the send fails, as when SQS
// throttles, the message is retried with the same messageId, and then moved to the dead-letter
// queue. A message moved back with `sst eventflow dead-letters --redrive` is a new message with
// a new messageId, as AWS documents, so its claim of the same hop fails, and it's dropped
// without a word for 24 hours: the event never comes back, and its runs never start. The guide
// says redriven events are "routed again, as if they'd just been sent".
describe("a hop of an event of a time to come that's moved back from the dead-letter queue", () => {
  it("still goes on", async () => {
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "later", triggers: { event: "later" } }, async () => "ok"),
    ]);
    await c.flow.send({ name: "later", data: {}, ts: Date.now() + DAY });
    await c.deliver(c.queue.splice(0, 1));
    expect(c.queue.length).toBe(1);
    const hop = c.queue.splice(0, 1)[0];
    // SQS fails the send of the next hop, each time the hop is tried.
    const fetch = c.fetch.bind(c);
    c.fetch = async (service, path, init) => {
      if ((init.headers as any)?.["X-Amz-Target"] === "AmazonSQS.SendMessageBatch") return new Response('{"__type":"ServiceUnavailable"}', { status: 503 });
      return fetch(service, path, init);
    };
    for (let i = 0; i < 5; i++) {
      const result = await later(900_000 + i * 360_000, () => c.deliver([hop]));
      expect(result.batchItemFailures.length).toBe(1);
    }
    c.fetch = fetch;
    // It's in the dead-letter queue, and moved back after the fix: a new message, with a new ID.
    c.deadLetters.push({ ...hop, messageId: `${hop.messageId}-redriven`, receiveCount: 0 });
    await c.flow.admin.redrive();
    const redriven = c.queue.splice(0, 1);
    await later(3_600_000, () => c.deliver(redriven));
    // The next hop is sent, or the event is gone.
    expect(c.queue.length).toBe(1);
  }, 60_000);
});

// Found by the thirty-fourth review. The thirty-third review's fix, a hop taken once, is only for
// "due" messages. A run held by a throttle goes back to the queue the same way, 15 minutes at a
// time (router.ts:587), with nothing that says the hop was taken: a held message that SQS
// delivers twice re-sends it twice, and the copies don't fold back together until the run
// starts. A throttle of one a day holds a run ~96 hops, each duplicate on the way adding a
// lineage of its own, with its SQS and router cost.
describe("a run held by a throttle whose message is delivered twice", () => {
  it("still goes back to the queue once", async () => {
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "slow", triggers: { event: "slow" }, throttle: { limit: 1, period: "1d" } }, async () => "ok"),
    ]);
    // Two start, as burst + 1, and the third is held for a day.
    await c.flow.send([{ name: "slow", data: {} }, { name: "slow", data: {} }, { name: "slow", data: {} }]);
    await c.deliver(c.queue.splice(0));
    const held = c.queue.filter((m) => JSON.parse(m.body).kind === "start" && JSON.parse(m.body).held);
    expect(held.length).toBe(1);
    c.queue = c.queue.filter((m) => !held.includes(m));
    // The same SQS message, delivered twice.
    await later(900_000, () => c.deliver([held[0]]));
    await later(900_000, () => c.deliver([held[0]]));
    expect(c.queue.filter((m) => JSON.parse(m.body).kind === "start").length).toBe(1);
  }, 60_000);
});

// Found by the thirty-fourth review. "Runs they start send none, so they don't go on forever"
// (system.ts:28) looks only at the ended run's own event. A run started by
// inngest/function.finished that invokes a helper: the helper's event is
// inngest/function.invoked, so its end sends inngest/function.finished, which starts the first
// function again, which invokes the helper again, forever, a pair of runs every few seconds
// until someone notices the bill. A guard on the function's own ID, as one would write, doesn't
// stop it.
describe("a run started by inngest/function.finished that invokes another", () => {
  it("doesn't go on forever", async () => {
    const c = new Cluster((flow: any) => {
      const helper = flow.createFunction({ id: "helper", triggers: { event: "never" } }, async () => "h");
      const audit = flow.createFunction({ id: "audit", triggers: { event: "inngest/function.finished" } }, async ({ event, step }: any) => {
        if (event.data.function_id === "test-audit") return;
        return step.invoke("record", { function: helper, data: {} });
      });
      const job = flow.createFunction({ id: "job", triggers: { event: "job" } }, async () => "j");
      return [helper, audit, job];
    });
    await c.flow.send({ name: "job", data: {} });
    await c.pump(4000);
    expect([...c.runs.values()].filter((r) => r.payload.fn === "helper").length).toBe(1);
  }, 60_000);
});
