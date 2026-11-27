import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns, settle, waitFor } from "./harness.ts";

beforeAll(async () => {
  await LocalDurableTestRunner.setupTestEnvironment({ skipTime: false });
});
afterAll(async () => {
  await finishRuns();
  await LocalDurableTestRunner.teardownTestEnvironment();
});

// Found by the seventeenth review.
describe("signal throttled, told by another", () => {
  it("a send whose callback was throttled, retried after another sender told the wait, doesn't say it told it", async () => {
    const log: string[] = [];
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "waiter", triggers: { event: "w" }, retries: 0 }, async ({ step }: any) => {
        const a = await step.waitForSignal("s1", { signal: "tick", timeout: "60s" });
        await step.run("got", () => { log.push(`got ${a?.data}`); });
        const b = await step.waitForSignal("s2", { signal: "tick", timeout: "60s" });
        await step.run("got2", () => { log.push(`got2 ${b?.data}`); });
        return null;
      }),
      flow.createFunction({ id: "sender", triggers: { event: "a" }, retries: 0 }, async ({ step, event }: any) => {
        const r = await step.sendSignal("notify", { signal: "tick", data: event.data.n });
        if (!r.runId) await step.run("fallback", () => { log.push(`fallback ${event.data.n}`); });
        else await step.run("sent", () => { log.push(`sent ${event.data.n}`); });
        return r.runId ?? null;
      }),
    ]);
    await c.flow.send({ name: "w", id: "w1" });
    await c.drain();
    await settle(1500);
    // The callback API throttles the first sender's tries: it's never resumed by them.
    let first: string | undefined;
    c.callbackFails = (id, attempt) => {
      first ??= id;
      return id === first && attempt <= 5 ? 429 : undefined;
    };
    await c.flow.send({ name: "a", id: "a1", data: { n: 1 } });
    await c.pump(200);
    await waitFor(() => ((c.callbackAttempts.get(first ?? "") ?? 0) >= 5 ? true : undefined), 600);
    // Another sender tells the wait, before the first's step is tried again.
    await c.flow.send({ name: "a", id: "a2", data: { n: 2 } });
    await c.pump(12_000);
    await waitFor(() => (log.filter((l) => l.startsWith("sent") || l.startsWith("fallback")).length >= 2 ? true : undefined), 1500);
    // The waiter was told once, by the second sender. Its next wait got nothing.
    expect(log).toContain("got 2");
    expect(log).not.toContain("got 1");
    expect(log).not.toContain("got2 1");
    // The first sender's data reached no one: it says so, and falls back.
    expect(log).toContain("fallback 1");
  }, 90_000);
});
