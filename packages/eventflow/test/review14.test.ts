import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns, sdk, settle } from "./harness.ts";

beforeAll(async () => {
  await LocalDurableTestRunner.setupTestEnvironment({ skipTime: false });
});
afterAll(async () => {
  await finishRuns();
  await LocalDurableTestRunner.teardownTestEnvironment();
});

void sdk;

// Found by the twenty-first review: what still lists runs, in the account's 15 a second.
describe("finding a run that isn't in the table", () => {
  it("doesn't list runs each time a run that hasn't started is polled", async () => {
    // The docs' own pattern: the ID is known before the run starts, and runs.get is polled.
    const c = new Cluster((flow: any) => [flow.createFunction({ id: "f", triggers: { event: "go" } }, async () => {})]);
    const id = c.flow.runs.idFor("f", "not-sent-yet");
    c.listings = 0;
    for (let i = 0; i < 5; i++) expect(await c.flow.runs.get(id)).toBeUndefined();
    // The first lists, for a run that started before ARNs were kept. Those after don't, for a while.
    expect(c.listings).toBe(1);
  }, 30_000);

  it("doesn't list runs for each event while a singleton's run is held", async () => {
    const c = new Cluster((flow: any) => [
      flow.createFunction(
        { id: "t", triggers: { event: "go" }, singleton: { mode: "skip" }, throttle: { limit: 1, period: "1m", burst: 0 } as any, retries: 0 },
        async () => {},
      ),
    ]);
    // The first takes the throttle's slot, and ends, giving the singleton back.
    await c.flow.send({ name: "go", id: "a" });
    await c.drain({ maxRounds: 1 });
    await c.runOf("t").result;
    await settle(200);
    // The second takes the singleton, and is held by the throttle: the lock has no ARN.
    await c.flow.send({ name: "go", id: "b" });
    await c.drain({ maxRounds: 1 });
    c.queue = [];
    c.listings = 0;
    // Each event that comes while it's held asks for the held run by listing.
    for (const id of ["c", "d", "e"]) {
      await c.flow.send({ name: "go", id });
      await c.drain({ maxRounds: 1 });
    }
    expect(c.listings).toBe(0);
  }, 30_000);

  it("doesn't list runs when GetDurableExecution is throttled", async () => {
    const c = new Cluster((flow: any) => [flow.createFunction({ id: "f", triggers: { event: "go" } }, async ({ step }: any) => step.sleep("s", "5s"))]);
    await c.flow.send({ name: "go" });
    await c.drain();
    const id = c.runOf("f").runId;
    // GetDurableExecution answers 429, as when it's over its 300 a second after the client's retries.
    const fetch = c.fetch.bind(c);
    c.fetch = async (service, path, init) =>
      service === "lambda" && /^\/2025-12-01\/durable-executions\/[^/]+\?IncludeExecutionData=false/.test(path)
        ? new Response('{"__type":"TooManyRequestsException"}', { status: 429 })
        : fetch(service, path, init);
    c.listings = 0;
    // The throttle moves onto the 15 a second, which the router's own lookups share.
    await c.flow.runs.get(id).catch(() => {});
    expect(c.listings).toBe(0);
  }, 30_000);
});

describe("a run whose ARN wasn't kept when it started", () => {
  it("is still found without listing runs", async () => {
    const c = new Cluster((flow: any) => [flow.createFunction({ id: "f", triggers: { event: "go" } }, async ({ step }: any) => step.sleep("s", "5s"))]);
    // The table throttles the claim of the ARN, which startRun drops. The worker knows its ARN.
    const claim = c.store.claim.bind(c.store);
    let failed = 0;
    c.store.claim = async (item: string, owner: string, ttl: number) => {
      if (owner.startsWith("arn/")) {
        failed++;
        throw new Error("ProvisionedThroughputExceededException");
      }
      return claim(item, owner, ttl);
    };
    await c.flow.send({ name: "go" });
    await c.drain();
    await settle(300);
    c.store.claim = claim;
    expect(failed).toBe(1);
    const id = c.runOf("f").runId;
    c.listings = 0;
    for (let i = 0; i < 3; i++) expect((await c.flow.runs.get(id))?.id).toBe(id);
    expect(c.listings).toBe(0);
  }, 30_000);
});
