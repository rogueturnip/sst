import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns, sdk } from "./harness.ts";
import { matcher } from "../src/worker.ts";
import { evaluate } from "../src/expression.ts";

beforeAll(async () => {
  await LocalDurableTestRunner.setupTestEnvironment({ skipTime: false });
});
afterAll(async () => {
  await finishRuns();
  await LocalDurableTestRunner.teardownTestEnvironment();
});

void sdk;

// Found by the twentieth review.
describe("a cancelOn that matches on a field the run's event doesn't have", () => {
  it("doesn't fail the run before its first step", async () => {
    const c = new Cluster((flow: any) => [
      flow.createFunction(
        { id: "remind", triggers: [{ event: "go" }], retries: 0, cancelOn: [{ event: "stop", match: "data.reminderId" }] },
        async ({ step }: any) => step.run("work", () => "done"),
      ),
    ]);
    // An event without data.reminderId: the run can't be cancelled by one, but it should run.
    await c.flow.send({ name: "go", data: {} });
    await c.pump(3000);
    const result = await c.runOf("remind").result;
    expect(result.getStatus()).toBe("SUCCEEDED");
    expect(result.getResult()).toBe("done");
  }, 30_000);
});

describe("a wait's `if` that guards a field of the run's event", () => {
  it("isn't refused when a branch that doesn't read it can match", () => {
    // Where the run's event has no coupon, any "order/paid" matches.
    const source = 'has(event.data.coupon) ? async.data.coupon == event.data.coupon : async.data.kind == "any"';
    const run = { id: "e", name: "x", data: {}, ts: 0 } as any;
    const { condition } = matcher("wait", run, { if: source });
    expect(evaluate(condition!, { async: { data: { kind: "any" } } })).toBe(true);
  });
});

describe("a cron's run name", () => {
  it("is the minute on its clock, as before, except the second time of an hour that repeats", async () => {
    const { parseCron, localMinute } = await import("../src/cron.ts");
    const hourly = parseCron("TZ=America/New_York 0 * * * *");
    expect(localMinute(parseCron("0 * * * *"), new Date("2026-11-27T10:00:00Z"))).toBe("2026-11-27T10:00");
    expect(localMinute(hourly, new Date("2026-11-01T05:00:00Z"))).toBe("2026-11-01T01:00");
    expect(localMinute(hourly, new Date("2026-11-01T06:00:00Z"))).toBe("2026-11-01T01:00+1h");
    expect(localMinute(parseCron("TZ=America/New_York 0 1 * * *"), new Date("2026-11-01T06:00:00Z"))).toBe("2026-11-01T01:00");
  });
});

describe("finding a run", () => {
  it("is by the ARN kept when it started, not by listing runs", async () => {
    const c = new Cluster((flow: any) => [flow.createFunction({ id: "f", triggers: { event: "go" } }, async ({ step }: any) => step.sleep("s", "5s"))]);
    await c.flow.send({ name: "go" });
    await c.drain();
    const id = c.runOf("f").runId;
    c.listings = 0;
    expect((await c.flow.runs.get(id))?.id).toBe(id);
    expect(c.listings).toBe(0);
    expect((await c.flow.runs.cancel(id)).stopped).toBe(true);
    expect(c.listings).toBe(0);
  }, 30_000);
});
