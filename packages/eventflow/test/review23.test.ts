import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns, sdk } from "./harness.ts";

const { createDefer } = sdk;

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

// Found by the thirtieth review. The router's listener for an event of a time to come looks
// back to receivedAt + 1 in recent(), but resolveWaits() only gives an event to a waiter with
// `createdAt <= receivedAt`, and the waiter's createdAt is when the reminder was routed. A
// cancel that SQS took after the reminder, but before the reminder was routed, and that is
// routed after it (the usual order), is seen by neither side: recent() ran before the cancel
// was stored, and resolveWaits() skips the waiter as one that started after the cancel came.
describe("a cancel sent just after a reminder, routed after it", () => {
  it("cancels the reminder", async () => {
    const reminded: string[] = [];
    const c = new Cluster((flow: any) => [
      flow.createFunction(
        { id: "reminder", triggers: { event: "reminder/scheduled" }, cancelOn: [{ event: "reminder/cancelled", match: "data.id" }] },
        async ({ event, step }: any) => step.run("remind", () => void reminded.push(event.data.id)),
      ),
    ]);
    await c.flow.send({ name: "reminder/scheduled", data: { id: "r1" }, ts: Date.now() + DAY });
    // 100 ms later the user cancels it.
    await later(100, () => c.flow.send({ name: "reminder/cancelled", data: { id: "r1" } }));
    const [scheduled, cancelled] = c.queue.splice(0, 2);
    expect(cancelled.sentAt).toBeGreaterThan(scheduled.sentAt);
    // The queue is a second behind: the reminder is routed, then the cancel.
    await later(1000, () => c.deliver([scheduled]));
    await later(1050, () => c.deliver([cancelled]));
    await later(DAY + 2000, async () => {
      await c.drain();
      for (const run of c.runs.values()) await (run as any).result;
    });
    expect(reminded).toEqual([]);
  }, 60_000);
});

// defer() names each use of an ID by its occurrence, "refund", "refund:1", and the deferred run's
// root, which all its idempotency keys come from, is hash([root, "defer", name]). invoke() refuses
// an ID used twice without an idempotencyKey for this reason; defer() takes none, and doesn't
// refuse. A replay whose list changed, as one loaded in a step, gives the refund of B the root,
// and so the payment provider's key, that the refund of A had.
describe("a defer() in a loop, replayed with another list", () => {
  it("gives each deferred run the keys of what it's for", async () => {
    const keys: [string, string][] = [];
    let items = ["A", "B"];
    const c = new Cluster((flow: any) => {
      const refund = createDefer(flow, { id: "refund", retries: 0 }, async ({ event, step, idempotencyKey }: any) =>
        step.run("refund", () => void keys.push([event.data.item, idempotencyKey()])),
      );
      return [
        refund,
        flow.createFunction({ id: "cancel-order", triggers: { event: "order/cancelled" }, retries: 0 }, async ({ step, defer }: any) => {
          const list: string[] = await step.run("load", () => items);
          for (const item of list) defer("refund", { function: refund, data: { item }, idempotencyKey: item });
        }),
      ];
    });
    await c.flow.send({ name: "order/cancelled" });
    let runId = "";
    for (let i = 0; i < 6; i++) {
      await c.drain();
      for (const run of c.runs.values()) {
        if (run.payload?.fn === "cancel-order") runId ||= run.runId;
        await (run as any).result;
      }
    }
    const first = new Map(keys);
    expect(first.size).toBe(2);
    // A was refunded by hand; the run is replayed for B.
    items = ["B"];
    const run = c.runs.get(runId)!;
    c.history.set(runId, { status: "FAILED", payload: run.payload, events: [] });
    keys.length = 0;
    const { id } = await c.flow.runs.replay(runId);
    await (c.run(id) as any).result;
    for (let i = 0; i < 6; i++) {
      await c.drain();
      for (const r of c.runs.values()) await (r as any).result;
    }
    // By what it's of, B keeps its key, and doesn't get A's.
    expect(keys[0]?.[1]).not.toBe(first.get("A"));
    expect(keys).toEqual([["B", first.get("B")!]]);
  }, 60_000);
});

describe("a defer() used more than once without what it's of", () => {
  it("is refused", async () => {
    let error = "";
    const c = new Cluster((flow: any) => {
      const refund = createDefer(flow, { id: "refund", retries: 0 }, async () => {});
      return [
        refund,
        flow.createFunction({ id: "cancel-order", triggers: { event: "order/cancelled" }, retries: 0 }, async ({ defer }: any) => {
          try {
            for (const item of ["A", "B"]) defer("refund", { function: refund, data: { item } });
          } catch (e: any) {
            error = e.message;
          }
        }),
      ];
    });
    await c.flow.send({ name: "order/cancelled" });
    await c.drain();
    for (const run of c.runs.values()) await (run as any).result;
    expect(error).toContain("idempotencyKey");
  }, 60_000);
});

// requiredPaths() takes both sides of an `&&` that isn't a top-level conjunct, as under `!`. But
// `a && b` is false when b is, whatever a is, so `!(event.data.vip && async.data.flagged)` matches
// an event that isn't flagged, though the run's event has no vip. The wait is refused instead,
// and times out.
describe("a wait whose if has an && under a !", () => {
  it("resumes on an event that decides it without the missing path", async () => {
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "review", triggers: { event: "order/placed" } }, async ({ step }: any) => {
        const ok = await step.waitForEvent("checked", {
          event: "order/checked",
          timeout: "2s",
          if: "async.data.orderId == event.data.orderId && !(event.data.vip && async.data.flagged)",
        });
        return ok ? "checked" : "timed out";
      }),
    ]);
    await c.flow.send({ name: "order/placed", data: { orderId: "o1" } });
    await c.drain();
    const run = await (async () => {
      for (let i = 0; i < 200 && !c.runs.size; i++) await new Promise((r) => setTimeout(r, 10));
      return [...c.runs.values()][0];
    })();
    await new Promise((r) => setTimeout(r, 300));
    await c.flow.send({ name: "order/checked", data: { orderId: "o1", flagged: false } });
    await c.drain();
    expect((await (run as any).result).getResult()).toBe("checked");
  }, 60_000);
});
