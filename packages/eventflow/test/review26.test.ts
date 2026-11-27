import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns } from "./harness.ts";
import { evaluate, parseExpression } from "../src/expression.ts";

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

// Found by the thirty-third review. An event of a time to come goes back to the queue every
// 15 minutes as a new "due" message (router.ts:186), with nothing that says this hop was
// taken. A due message that SQS delivers twice, as a standard queue may, or that is delivered
// again because the router's invocation timed out after it re-sent it, re-sends it twice. The
// copies never fold back together until the event is due, and each one copies itself again on
// the next redelivery: an event a month ahead takes ~2,900 hops, so the copies, and their SQS
// and router cost, grow with each duplicate along the way. Only the last step, beginEvent on
// "ED", makes them start one run.
describe("an event of a time to come whose due message is delivered twice", () => {
  it("still goes back to the queue once", async () => {
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "later", triggers: { event: "later" } }, async () => "ok"),
    ]);
    await c.flow.send({ name: "later", data: {}, ts: Date.now() + DAY });
    await c.deliver(c.queue.splice(0, 1));
    expect(c.queue.length).toBe(1);
    const hop = c.queue.splice(0, 1)[0];
    // The same SQS message, delivered twice.
    await later(900_000, () => c.deliver([hop]));
    await later(900_000, () => c.deliver([hop]));
    // Two copies of the event go on, each re-sent every 15 minutes until it's due.
    expect(c.queue.length).toBe(1);
  }, 60_000);
});

// Found by the thirty-third review. regex.ts reads "[[:alpha:]]", RE2's POSIX class, as the set
// "[", ":", "a", "l", "p", "h" followed by a literal "]", and doesn't throw. A trigger, wait or
// cancelOn `if` copied from Inngest with one silently never matches what it should, and matches
// what it shouldn't. The guide says matches() takes a pattern as RE2 does, and anything else
// throws when the function is created.
describe("matches() with a POSIX class", () => {
  it("matches as RE2 does, or throws", () => {
    const is = (source: string, data: unknown) => evaluate(parseExpression(source, "test"), { event: { data } });
    let result: [boolean, boolean] | undefined;
    try {
      result = [is('event.data.s.matches("^[[:alpha:]]+$")', { s: "abc" }), is('event.data.s.matches("^[[:alpha:]]+$")', { s: "a]" })];
    } catch {}
    if (result) expect(result).toEqual([true, false]);
  });
});
