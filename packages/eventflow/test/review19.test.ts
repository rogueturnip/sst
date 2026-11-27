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

// Found by the twenty-sixth review: the start timeout's timer is sent for when the event came,
// not for its `ts`, so an event of a time to come further off than the start timeout is
// cancelled before it's due, always.
describe("an event whose ts is still to come, for a function with a start timeout", () => {
  it("starts its run then", async () => {
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "later", triggers: { event: "remind" }, timeouts: { start: "10m" } }, async () => "ok"),
    ]);
    await c.flow.send({ name: "remind", ts: Date.now() + 3_600_000 });
    await c.drain();
    // Ten minutes on, the start timeout's timer comes.
    await c.fire("start");
    // An hour on, the held run comes back.
    await later(3_700_000, () => c.drain());
    expect(c.runs.size).toBe(1);
  }, 60_000);
});

// A throttle's slot is taken when the event comes, and the run is held until the later of the
// slot and `ts`. So events of the same time to come all start together, at `ts`.
describe("events whose ts is still to come, for a throttled function", () => {
  it("start one a minute, as the throttle says", async () => {
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "throttled", triggers: { event: "remind" }, throttle: { limit: 1, period: "1m" } }, async () => "ok"),
    ]);
    const ts = Date.now() + 3_600_000;
    await c.flow.send([1, 2, 3, 4, 5].map((n) => ({ name: "remind", id: `r${n}`, ts })));
    await c.drain();
    // Just after ts: the burst, burst + 1 = 2 as the docs say, may have started, not five.
    await later(3_602_000, () => c.drain());
    expect(c.runs.size).toBeLessThanOrEqual(2);
  }, 60_000);
});

// Any finite `ts` is taken, and the run goes back to the queue every 15 minutes until then. A
// `ts` in microseconds, as some languages give, is 50,000 years away: the run never starts, and
// nothing says so.
describe("an event whose ts is in microseconds", () => {
  it("is refused, or starts its run", async () => {
    const c = new Cluster((flow: any) => [flow.createFunction({ id: "now", triggers: { event: "job" } }, async () => "ok")]);
    let refused = false;
    try {
      await c.flow.send({ name: "job", ts: Date.now() * 1000 });
    } catch {
      refused = true;
    }
    await c.drain();
    expect(refused || c.runs.size === 1).toBe(true);
  }, 60_000);
});

// A singleton's lock is taken when the event comes, and kept while its run is held. So an event
// for tomorrow, with mode "skip", skips every run of its key until then, while nothing runs. One
// with a ts of the year 3000 skips them for good.
describe("an event whose ts is still to come, for a singleton that skips", () => {
  it("doesn't skip a run for now while nothing runs", async () => {
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "single", triggers: { event: "sync" }, singleton: { key: "event.data.user", mode: "skip" } }, async () => "ok"),
    ]);
    await c.flow.send({ name: "sync", id: "tomorrow", data: { user: "u1" }, ts: Date.now() + 86_400_000 });
    await c.drain();
    await c.flow.send({ name: "sync", id: "now", data: { user: "u1" } });
    await c.drain();
    expect(c.runs.size).toBe(1);
  }, 60_000);
});
