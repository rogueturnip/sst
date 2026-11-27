import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns, sdk, settle, waitFor } from "./harness.ts";

const { NonRetriableError } = sdk as any;
beforeAll(async () => {
  await LocalDurableTestRunner.setupTestEnvironment({ skipTime: false });
});
afterAll(async () => {
  await finishRuns();
  await LocalDurableTestRunner.teardownTestEnvironment();
});

// Found by the fifteenth review.
describe("singleton ended, invoked", () => {
  for (const mode of ["skip", "cancel"] as const)
    it(`an invoke message that comes again while the invoked run's onFailure runs doesn't answer the parent, with "${mode}"`, async () => {
      const log: string[] = [];
      let child: any;
      const c = new Cluster((flow: any) => {
        child = flow.createFunction({
          id: "child", triggers: { event: "go" }, retries: 0, singleton: { key: "event.data.acct", mode },
          onFailure: async ({ step, event }: any) => {
            await step.run("undo-1", () => { log.push(`undo-${event.data.event.data.n}`); });
            await step.waitForEvent("approve", { event: "approve", timeout: "1h" });
          },
        }, async ({ step, event }: any) => {
          await step.run("work", async () => { log.push(`work-${event.data.n}`); });
          if (event.data.n === 1) throw new NonRetriableError("declined");
          return "done";
        });
        const parent = flow.createFunction({ id: "parent", triggers: { event: "p" }, retries: 0 }, async ({ step }: any) => {
          const r = await step.invoke("call", { function: child, data: { acct: "x", n: 1 }, timeout: "1h" }).then((v: any) => `ok:${v}`, (e: any) => `err:${e?.message}`);
          await step.run("told", () => { log.push(`parent ${r}`); });
        });
        return [child, parent];
      });
      const invokes: any[] = [];
      const deliver = c.deliver.bind(c);
      c.deliver = async (batch: any[]) => {
        for (const m of batch) if (JSON.parse(m.body).kind === "invoke") invokes.push({ ...m });
        return deliver(batch);
      };
      await c.flow.send({ name: "p" });
      await c.pump(3000);
      await waitFor(() => (log.includes("undo-1") ? true : undefined), 600);
      expect(invokes.length).toBe(1);
      // A later event of the same key starts beside the onFailure.
      await c.flow.send({ name: "go", data: { acct: "x", n: 2 } });
      await c.pump(2000);
      await waitFor(() => (log.includes("work-2") ? true : undefined), 600);
      // SQS delivers the invoke message again (at least once), or the parent's send step ran twice.
      await c.deliver([{ ...invokes[0], messageId: "again", receiveCount: 0 }]);
      await c.pump(2000);
      // The parent is still waiting for its child, which hasn't ended: it isn't told the run was skipped.
      expect(log.filter((l) => l.startsWith("parent"))).toEqual([]);
    }, 90_000);
});

describe("signal told", () => {
  it("a send whose told mark fails to save after the wait was told still says who it went to", async () => {
    const log: string[] = [];
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "waiter", triggers: { event: "w" }, retries: 0 }, async ({ step }: any) => {
        const a = await step.waitForSignal("s1", { signal: "tick", timeout: "30s" });
        await step.run("got", () => { log.push(`got ${a?.data}`); });
        return a?.data ?? null;
      }),
      flow.createFunction({ id: "sender", triggers: { event: "a" }, retries: 2 }, async ({ step }: any) => {
        const r = await step.sendSignal("notify", { signal: "tick", data: 1 });
        // A common shape: no one waited, so do it another way.
        if (!r.runId) await step.run("fallback", () => { log.push("fallback"); });
        return r.runId ?? null;
      }),
    ]);
    await c.flow.send({ name: "w", id: "w1" });
    await c.drain();
    await settle(1500);
    const claim = c.store.claim.bind(c.store);
    let failed = false;
    (c.store as any).claim = async (item: string, owner: string, ttl: number) => {
      // DynamoDB, or the worker, fails right after the waiter was told.
      if (!failed && item.endsWith("\ntold")) {
        failed = true;
        throw new Error("ProvisionedThroughputExceededException");
      }
      return claim(item, owner, ttl);
    };
    await c.flow.send({ name: "a" });
    await c.pump(8000);
    await waitFor(() => (log.includes("got 1") ? true : undefined), 600);
    const r = await c.runOf("sender").result;
    expect(failed).toBe(true);
    expect(log).toEqual(["got 1"]);
    expect(r.getResult()).toBe(c.runOf("waiter").runId);
  }, 90_000);
});
