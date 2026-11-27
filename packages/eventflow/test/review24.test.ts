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

async function settle(c: Cluster, rounds = 6) {
  for (let i = 0; i < rounds; i++) {
    await c.drain();
    for (const run of c.runs.values()) await (run as any).result?.catch?.(() => {});
  }
}

// Found by the thirty-first review. defer() refuses a second use of an ID without an
// idempotencyKey by throwing, not by state.refuse() as invoke() and the other refusals do. The
// first use is already in `defers`, so the run that's refused still starts it, with the keys of
// the name "refund". Fixed as the message says, by an idempotencyKey, and replayed, A's refund is
// "refund@\"A\"", whose root, and payment provider key, is another: A is refunded twice.
describe("a defer() refused for a second use of its ID", () => {
  it("starts nothing, so a replay with the fix doesn't refund the first again", async () => {
    const keys: [string, string][] = [];
    let keyed = false;
    const c = new Cluster((flow: any) => {
      const refund = createDefer(flow, { id: "refund", retries: 0 }, async ({ event, step, idempotencyKey }: any) =>
        step.run("refund", () => void keys.push([event.data.item, idempotencyKey()])),
      );
      return [
        refund,
        flow.createFunction({ id: "cancel-order", triggers: { event: "order/cancelled" }, retries: 0 }, async ({ defer }: any) => {
          for (const item of ["A", "B"]) defer("refund", { function: refund, data: { item }, ...(keyed ? { idempotencyKey: item } : {}) });
        }),
      ];
    });
    await c.flow.send({ name: "order/cancelled" });
    let runId = "";
    let status = "";
    for (let i = 0; i < 6; i++) {
      await c.drain();
      for (const run of c.runs.values()) {
        if (run.payload?.fn === "cancel-order") runId ||= run.runId;
        const r = await (run as any).result;
        if (run.payload?.fn === "cancel-order") status = r.getStatus?.() ?? status;
      }
    }
    expect(status).toBe("FAILED");
    const before = [...keys];
    // The fix the message asks for, and a replay.
    keyed = true;
    const run = c.runs.get(runId)!;
    c.history.set(runId, { status: "FAILED", payload: run.payload, events: [] });
    const { id } = await c.flow.runs.replay(runId);
    await (c.run(id) as any).result;
    await settle(c);
    const a = keys.filter(([item]) => item === "A");
    // A refused run starts none of what it deferred; else A has two keys.
    expect(before).toEqual([]);
    expect(new Set(a.map(([, k]) => k)).size).toBe(1);
  }, 60_000);

  it("fails the run, though the function catches what defer() throws", async () => {
    const refunded: string[] = [];
    let status = "";
    const c = new Cluster((flow: any) => {
      const refund = createDefer(flow, { id: "refund", retries: 0 }, async ({ event }: any) => void refunded.push(event.data.item));
      return [
        refund,
        flow.createFunction({ id: "cancel-order", triggers: { event: "order/cancelled" }, retries: 0 }, async ({ defer }: any) => {
          // Each on its own, so one that fails doesn't keep the others: B is never deferred.
          for (const item of ["A", "B"])
            try {
              defer("refund", { function: refund, data: { item } });
            } catch {}
          return "done";
        }),
      ];
    });
    await c.flow.send({ name: "order/cancelled" });
    for (let i = 0; i < 6; i++) {
      await c.drain();
      for (const run of c.runs.values()) {
        const r = await (run as any).result;
        if (run.payload?.fn === "cancel-order") status = r.getStatus?.() ?? status;
      }
    }
    // As invoke() is refused: the run fails, and B isn't silently dropped from a completed run.
    expect(status).toBe("FAILED");
  }, 60_000);
});

// listen() now sets the waiter's createdAt to `since`, receivedAt + 1, for the router's listener
// of an event of a time to come. The waiter's sort key is createdAt#callbackId, and the callback
// is `due:` + dueCancelKey(fn, event): the same for each cancelOn of the function. Two cancelOns
// on one event name, indexed by one field and value, now always have the same key, and the
// second replaces the first. Before, Date.now() set them apart by the time of the writes.
describe("an event of a time to come, with two cancelOns on one event", () => {
  it("is cancelled by the first, as by the second", async () => {
    const reminded: string[] = [];
    const c = new Cluster((flow: any) => [
      flow.createFunction(
        {
          id: "reminder",
          triggers: { event: "reminder/scheduled" },
          cancelOn: [
            { event: "order/updated", if: "async.data.id == event.data.id && async.data.status == 'cancelled'" },
            { event: "order/updated", if: "async.data.id == event.data.id && async.data.status == 'refunded'" },
          ],
        },
        async ({ event, step }: any) => step.run("remind", () => void reminded.push(event.data.id)),
      ),
    ]);
    await c.flow.send({ name: "reminder/scheduled", data: { id: "r1" }, ts: Date.now() + DAY });
    await c.drain();
    await later(60_000, async () => {
      await c.flow.send({ name: "order/updated", data: { id: "r1", status: "cancelled" } });
      await c.drain();
    });
    await later(DAY + 2000, async () => {
      await c.drain();
      for (const run of c.runs.values()) await (run as any).result;
    });
    expect(reminded).toEqual([]);
  }, 60_000);
});

// The function's attempts clear `defers` as each begins: "What an attempt that failed deferred,
// the next does again, if it comes to it." onFailure's attempts don't. One that defers, then
// throws outside a step, is tried again; what the next does instead, the failed one's defer
// still starts with it.
describe("an onFailure tried again", () => {
  it("starts only what its last attempt deferred", async () => {
    const started: string[] = [];
    const c = new Cluster((flow: any) => {
      const notify = createDefer(flow, { id: "notify", retries: 0 }, async ({ event }: any) => void started.push(event.data.what));
      return [
        notify,
        flow.createFunction(
          {
            id: "pay",
            triggers: { event: "go" },
            retries: 1,
            onFailure: async ({ defer, attempt }: any) => {
              const what = attempt === 0 ? "first" : "second";
              defer(what, { function: notify, data: { what } });
              if (attempt === 0) throw new Error("flaky");
            },
          },
          async () => {
            throw new Error("boom");
          },
        ),
      ];
    });
    await c.flow.send({ name: "go" });
    await settle(c, 8);
    expect(started).toEqual(["second"]);
  }, 60_000);
});
