import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns, sdk } from "./harness.ts";

beforeAll(async () => {
  await LocalDurableTestRunner.setupTestEnvironment({ skipTime: false });
});
afterAll(async () => {
  await finishRuns();
  await LocalDurableTestRunner.teardownTestEnvironment();
});

const noop = async () => null;

// Found by the nineteenth review.
describe("an hourly cron in a time zone, when the clocks go back", () => {
  it("runs in each of the two hours that read 01:xx", async () => {
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "hourly", triggers: { cron: "TZ=America/New_York 0 * * * *" }, retries: 0 }, noop),
    ]);
    // 2026-11-01: 05:00Z is 01:00 EDT, and 06:00Z is 01:00 EST, an hour later.
    await c.handler({ "sst:eventflow:tick": "2026-11-01T05:00:00Z" }, {});
    await c.handler({ "sst:eventflow:tick": "2026-11-01T06:00:00Z" }, {});
    // Two hours went by, and an hourly cron ran once.
    expect(c.runs.size).toBe(2);
  });
});

describe("step.sendEvent with an event type", () => {
  it("checks the data against its schema, as flow.send() does", async () => {
    const strict = {
      "~standard": {
        version: 1,
        vendor: "test",
        validate: (value: any) => (typeof value?.email === "string" ? { value } : { issues: [{ message: "email is required" }] }),
      },
    } as const;
    const userCreated = sdk.eventType("user/created", { schema: strict as any });
    // flow.send() refuses it.
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "sender", triggers: { event: "go" }, retries: 0 }, async ({ step }: any) => {
        try {
          await step.sendEvent("s", userCreated.create({} as any));
          return "sent";
        } catch (error: any) {
          return `refused: ${error.message}`;
        }
      }),
    ]);
    await expect(c.flow.send(userCreated.create({} as any))).rejects.toThrow("schema");
    await c.flow.send({ name: "go" });
    await c.pump(3000);
    const result = (await c.runOf("sender").result).getResult();
    expect(result).toContain("refused");
  }, 30_000);
});

describe("listing the runs of a function whose ID is longer than 36 characters", () => {
  it("doesn't read every one of its runs, one at a time, to list a few", async () => {
    const id = "send-weekly-digest-email-to-subscribers";
    const c = new Cluster((flow: any) => [flow.createFunction({ id, triggers: { event: "a" }, retries: 0 }, noop)]);
    for (let i = 0; i < 300; i++) {
      const runId = c.flow.runs.idFor(id, `e${i}`);
      c.history.set(runId, { status: "FAILED", payload: { fn: id }, events: [], startedAt: i + 1 });
    }
    let reads = 0;
    const fetch = c.fetch.bind(c);
    c.fetch = async (service: string, path: string, init: RequestInit) => {
      if (/^\/2025-12-01\/durable-executions\/[^/?]+\?/.test(path)) reads++;
      return fetch(service, path, init);
    };
    const listed = await c.flow.runs.list({ function: id, status: "FAILED", limit: 5 });
    expect(listed.length).toBe(5);
    // One GetDurableExecution per run of the function, in series, in the router, whose
    // timeout is a minute: with a few thousand runs, `sst eventflow retry --failed` times out.
    expect(reads).toBeLessThan(50);
  });
});

describe("the runs of `a-b`, beside a function `a.b`", () => {
  it("aren't the runs of `a.b`, which have the same prefix", async () => {
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "a-b", triggers: { event: "a" }, retries: 0 }, noop),
      flow.createFunction({ id: "a.b", triggers: { event: "b" }, retries: 0 }, noop),
    ]);
    const mine = c.flow.runs.idFor("a-b", "e1");
    const other = c.flow.runs.idFor("a.b", "e2");
    c.history.set(mine, { status: "FAILED", payload: { fn: "a-b" }, events: [], startedAt: 1 });
    c.history.set(other, { status: "FAILED", payload: { fn: "a.b" }, events: [], startedAt: 2 });
    // Listing "a.b" checks whose each run is. Listing "a-b" doesn't, and takes the runs of "a.b".
    expect((await c.flow.runs.list({ function: "a.b" })).map((r) => r.id)).toEqual([other]);
    expect((await c.flow.runs.list({ function: "a-b" })).map((r) => r.id)).toEqual([mine]);
  });
});
