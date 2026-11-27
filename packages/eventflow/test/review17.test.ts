import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { ExecutionStatus, LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns, waitFor } from "./harness.ts";

beforeAll(async () => {
  await LocalDurableTestRunner.setupTestEnvironment({ skipTime: true });
});
afterAll(async () => {
  await finishRuns();
  await LocalDurableTestRunner.teardownTestEnvironment();
});

// Found by the twenty-fourth review: a step waiting for its next try is known and not saved,
// so the try that then starts is counted as a restart of it.
describe("a step's restarts", () => {
  // A step is started again up to three times after the worker stopped in it, and then fails as
  // a try that timed out. In a retry, the try's own first start is counted as the first restart,
  // so it's failed after two, and the message says it started four times where it started three.
  it("can be started again three times in a retry, as in its first try", async () => {
    let calls = 0;
    const gates: (() => void)[] = [];
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "work", triggers: { event: "go" }, retries: 1 }, async ({ step }: any) =>
        step.run("long", async () => {
          const n = ++calls;
          if (n === 1) throw new Error("once");
          // In its retry, the worker stops here three times.
          if (n <= 4) await new Promise<void>((r) => gates.push(r));
          return "ok";
        }),
      ),
    ]);
    await c.flow.send({ name: "go" });
    await c.drain();
    const run = await waitFor(() => [...c.runs.values()][0]);
    for (let crash = 0; crash < 3; crash++) {
      await waitFor(() => (gates.length ? true : undefined));
      // Its next checkpoint, the step's result, is refused, so its work is abandoned.
      const paused = run.runner.pauseExecution();
      gates.shift()!();
      await paused;
      await run.runner.resumeExecution();
    }
    const result = await run.result;
    expect(calls).toBe(5);
    expect(result.getStatus()).toBe(ExecutionStatus.SUCCEEDED);
  }, 60_000);

  it("aren't counted for a retry that starts for the first time", async () => {
    let calls = 0;
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "work", triggers: { event: "go" }, retries: 2 }, async ({ step }: any) =>
        step.run("flaky", async () => {
          if (++calls === 1) throw new Error("once");
          return "ok";
        }),
      ),
    ]);
    const claimed: string[] = [];
    const claim = c.store.claim.bind(c.store);
    (c.store as any).claim = async (item: string, owner: string, ttl: number) => {
      claimed.push(item);
      return claim(item, owner, ttl);
    };
    await c.flow.send({ name: "go" });
    await c.drain();
    await Promise.all([...c.runs.values()].map((r) => r.result));
    expect(calls).toBe(2);
    // Nothing was restarted: no restart is counted, and none of the three is used up.
    expect(claimed.filter((k) => k.includes("SR"))).toEqual([]);
  }, 60_000);
});
