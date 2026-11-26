import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { ExecutionStatus, LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns, sdk, settle } from "./harness.ts";

// Each was a defect that the seventh review reproduced: a retry after a failure the function
// caught, which reuses what comes after it where the function comes to it the same way.

const { NonRetriableError, Middleware } = sdk as any;
beforeAll(async () => {
  await LocalDurableTestRunner.setupTestEnvironment({ skipTime: false });
});
afterAll(async () => {
  await finishRuns();
  await LocalDurableTestRunner.teardownTestEnvironment();
});
const status = async (r: any) => {
  const x = await r.result;
  let e, v;
  try { e = x.getError(); } catch {}
  try { v = x.getResult(); } catch {}
  return { s: x.getStatus(), e, v };
};
const st = (r: any, ms = 20_000) => Promise.race([status(r), settle(ms).then(() => ({ s: "HANG" }) as any)]);

// A run that fails at "ship", after a failure it caught, and its retry with it fixed.
async function retried(options: any, body: (step: any, first: boolean, charge: () => number) => Promise<void>) {
  let first = true;
  let charges = 0;
  const c = new Cluster((flow: any) => [
    flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 0, ...options }, async ({ step }: any) => {
      await body(step, first, () => ++charges);
      await step.run("ship", () => { if (first) throw new NonRetriableError("carrier down"); return "ok"; });
      return "ok";
    }),
  ]);
  await c.flow.send({ name: "go" });
  await c.drain();
  const run = c.runOf("f");
  await st(run);
  c.history.set(run.runId, { status: "FAILED", payload: run.payload, events: (await run.result).getHistoryEvents() });
  first = false;
  const again = await c.flow.runs.retry(run.runId);
  return { again, result: await st(c.run(again.id)), charges: () => charges };
}

describe("a retry after a failure the function caught", () => {
  it("reuses a step after a join, in whatever order its results come", async () => {
    const { again, result, charges } = await retried({}, async (step, first, charge) => {
      await Promise.allSettled([
        step.run("loyalty", async () => { if (first) throw new Error("down"); await settle(50); }),
        step.run("quote", async () => { await settle(100); return 1; }),
      ]);
      await step.run("charge", charge);
    });
    expect(again.reused).toContain("charge");
    expect(result.v).toBe("ok");
    expect(charges()).toBe(1);
  }, 30_000);

  it("reuses a step whose ID ends in a number", async () => {
    const { again, result, charges } = await retried({}, async (step, first, charge) => {
      try { await step.run("loyalty", () => { if (first) throw new Error("down"); }); } catch {}
      await step.run("charge:42", charge);
    });
    expect(again.reused).toContain("charge:42");
    expect(result.v).toBe("ok");
    expect(charges()).toBe(1);
  }, 30_000);

  it("reuses the steps of a join, with middleware", async () => {
    class Slow extends Middleware.BaseMiddleware { readonly id = "slow"; async transformStepInput() { await settle(5); } }
    const { again, result, charges } = await retried({ middleware: [Slow] }, async (step, first, charge) => {
      try { await step.run("loyalty", () => { if (first) throw new Error("down"); }); } catch {}
      await Promise.all([step.run("charge", charge), step.run("other", () => 1)]);
    });
    expect(again.reused).toEqual(expect.arrayContaining(["charge", "other"]));
    expect(result.v).toBe("ok");
    expect(charges()).toBe(1);
  }, 30_000);

  it("reuses a step after other work, however long it takes", async () => {
    let slow = true;
    const { result, charges } = await retried({}, async (step, first, charge) => {
      try { await step.run("loyalty", () => { if (first) throw new Error("down"); }); } catch {}
      // A secret fetched in a cold Lambda, and from a cache in a warm one.
      if (slow) await settle(20);
      slow = false;
      await step.run("charge", charge);
    });
    expect(result.v).toBe("ok");
    expect(charges()).toBe(1);
  }, 30_000);
});

it("an event that cancels a run doesn't stop its onFailure", async () => {
  const log: string[] = [];
  const c = new Cluster((flow: any) => [
    flow.createFunction(
      {
        id: "f",
        triggers: { event: "order/placed" },
        retries: 0,
        cancelOn: [{ event: "order/cancelled", match: "data.id" }],
        onFailure: async ({ step }: any) => {
          await step.run("refund-1", () => void log.push("refund-1"));
          await step.sleep("s", "2s");
          await step.run("refund-2", () => void log.push("refund-2"));
        },
      },
      async ({ step }: any) => {
        await step.run("charge", () => void log.push("charge"));
        throw new NonRetriableError("out of stock");
      },
    ),
  ]);
  await c.flow.send({ name: "order/placed", data: { id: 1 } });
  await c.drain();
  for (let i = 0; i < 100 && !log.includes("refund-1"); i++) await settle(20);
  await c.flow.send({ name: "order/cancelled", data: { id: 1 } });
  await c.drain();
  expect((await st(c.runOf("f"))).s).toBe(ExecutionStatus.FAILED);
  expect(c.stopped).toEqual([]);
  expect(log).toEqual(["charge", "refund-1", "refund-2"]);
}, 30_000);
