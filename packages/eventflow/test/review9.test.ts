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

// Found by the sixteenth review.
describe("singleton ended, invoked, held", () => {
  it("a held start message that comes again while the invoked run's onFailure runs doesn't answer the parent", async () => {
    const log: string[] = [];
    let child: any;
    const c = new Cluster((flow: any) => {
      child = flow.createFunction({
        id: "child", triggers: { event: "go" }, retries: 0, singleton: { key: "event.data.acct", mode: "skip" },
        throttle: { limit: 1, period: "2s", burst: 0 },
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
    const held: any[] = [];
    const deliver = c.deliver.bind(c);
    c.deliver = async (batch: any[]) => {
      for (const m of batch) {
        const b = JSON.parse(m.body);
        if (b.kind === "start" && b.held && b.parent) held.push({ ...m });
      }
      return deliver(batch);
    };
    // Takes the throttle's slot, so the invoked run is held.
    await c.flow.send({ name: "go", data: { acct: "y", n: 0 } });
    await c.pump(500);
    await c.flow.send({ name: "p" });
    await c.pump(4000);
    await waitFor(() => (log.includes("undo-1") ? true : undefined), 600);
    expect(held.length).toBeGreaterThan(0);
    await c.flow.send({ name: "go", data: { acct: "x", n: 2 } });
    await c.pump(4000);
    await waitFor(() => (log.includes("work-2") ? true : undefined), 600);
    expect(log.filter((l) => l.startsWith("parent"))).toEqual([]);
    // SQS delivers the held start message again.
    await c.deliver([{ ...held[held.length - 1], messageId: "again", receiveCount: 0 }]);
    await c.pump(2000);
    // The child hasn't ended: its onFailure waits. The parent isn't told it was skipped.
    expect(log.filter((l) => l.startsWith("parent"))).toEqual([]);
  }, 90_000);
});

describe("signal told, waits again", () => {
  it("a send whose told mark fails to save, retried after the waiter waits again, still says who it told", async () => {
    const log: string[] = [];
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "waiter", triggers: { event: "w" }, retries: 0 }, async ({ step }: any) => {
        const a = await step.waitForSignal("s1", { signal: "tick", timeout: "30s" });
        await step.run("got", () => { log.push(`got ${a?.data}`); });
        const b = await step.waitForSignal("s2", { signal: "tick", timeout: "30s" });
        await step.run("got2", () => { log.push(`got2 ${b?.data}`); });
        return a?.data ?? null;
      }),
      flow.createFunction({ id: "sender", triggers: { event: "a" }, retries: 2 }, async ({ step }: any) => {
        const r = await step.sendSignal("notify", { signal: "tick", data: 1 });
        if (!r.runId) await step.run("fallback", () => { log.push("fallback"); });
        return r.runId ?? null;
      }),
    ]);
    await c.flow.send({ name: "w", id: "w1" });
    await c.drain();
    await settle(1500);
    const claim = c.store.claim.bind(c.store);
    const signalRead = c.store.signalRead.bind(c.store);
    let failed = false;
    (c.store as any).claim = async (item: string, owner: string, ttl: number) => {
      if (!failed && item.endsWith("\ntold")) {
        failed = true;
        throw new Error("ProvisionedThroughputExceededException");
      }
      return claim(item, owner, ttl);
    };
    (c.store as any).signalRead = async (item: string) => {
      // The step's retry comes after the waiter woke and waits again, as with a retry's delay.
      if (failed) await waitFor(() => (log.includes("got 1") ? true : undefined), 600).then(() => settle(1500));
      return signalRead(item);
    };
    await c.flow.send({ name: "a" });
    await c.pump(10_000);
    await waitFor(() => (log.includes("got 1") ? true : undefined), 600);
    const r = await c.runOf("sender").result;
    // The waiter was told once, by this send.
    expect(log).toContain("got 1");
    expect(failed).toBe(true);
    expect(log.filter((l) => l === "fallback")).toEqual([]);
    expect(r.getResult()).toBe(c.runOf("waiter").runId);
  }, 90_000);
});

describe("signal not told, replayed", () => {
  it("a replay of a send that found the wait gone doesn't say it told it", async () => {
    const log: string[] = [];
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "waiter", triggers: { event: "w" }, retries: 0 }, async ({ step }: any) => {
        const a = await step.waitForSignal("s1", { signal: "tick", timeout: "20s" });
        return a?.data ?? null;
      }),
      flow.createFunction({ id: "sender", triggers: { event: "a" }, retries: 0 }, async ({ step }: any) => {
        const r = await step.sendSignal("notify", { signal: "tick", data: 1 });
        if (!r.runId) await step.run("fallback", () => { log.push("fallback"); });
        throw new NonRetriableError(`sent to ${r.runId ?? "no one"}`);
      }),
    ]);
    await c.flow.send({ name: "w", id: "w1" });
    await c.drain();
    await settle(1500);
    // The waiter's run was stopped, and its wait is closed: the API answers 404.
    c.callbackFails = () => 404;
    await c.flow.send({ name: "a", id: "a1" });
    await c.pump(3000);
    const sender = c.runOf("sender");
    const first = await sender.result;
    expect(first.getError()?.errorMessage).toBe("sent to no one");
    expect(log).toEqual(["fallback"]);
    c.history.set(sender.runId, { status: "FAILED", payload: sender.payload, events: [] });
    const { id } = await c.flow.runs.replay(sender.runId);
    await c.pump(3000);
    const again = await c.run(id).result;
    // The replay finds what the first found: no one was told.
    expect(again.getError()?.errorMessage).toBe("sent to no one");
    expect(log).toEqual(["fallback", "fallback"]);
  }, 90_000);
});
