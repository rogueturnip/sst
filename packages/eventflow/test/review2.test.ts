import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { ExecutionStatus, LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns, sdk, settle, waitFor } from "./harness.ts";
import { memoFrom, onceSteps } from "../src/runs.ts";

// Each was a defect that the second review reproduced. In real time, as some come of it.

const { Middleware, createDefer, referenceFunction } = sdk;
beforeAll(async () => {
  await LocalDurableTestRunner.setupTestEnvironment({ skipTime: false });
});
afterAll(async () => {
  await finishRuns();
  await LocalDurableTestRunner.teardownTestEnvironment();
});
const status = async (r: any): Promise<{ s: string; e?: any; v?: any }> => {
  const x = await r.result;
  let e, v;
  try { e = x.getError(); } catch {}
  try { v = x.getResult(); } catch {}
  return { s: x.getStatus(), e, v };
};
const st = (r: any, ms = 8000) => Promise.race([status(r), settle(ms).then(() => ({ s: "HANG" }) as any)]);
const one = async (options: any, handler: any, client: any = {}) => {
  const c = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 0, ...options }, handler)], client);
  await c.flow.send({ name: "go" });
  await c.drain();
  return c;
};

describe("a step ID in two branches", () => {
  it("is refused, not guessed", async () => {
    for (const speeds of [[150, 5], [5, 150]]) {
      const calls: string[] = [];
      const c = await one({ retries: 2 }, async ({ step }: any) => {
        const branch = async (who: string, ms: number) => {
          await step.run(`check-${who}`, () => settle(ms));
          return step.run("charge", () => (calls.push(who), `charged ${who}`));
        };
        return Promise.all([branch("alice", speeds[0]), branch("bob", speeds[1])]);
      });
      const r = await st(c.runOf("f"));
      expect(r.s).toBe(ExecutionStatus.FAILED);
      expect(r.e.errorMessage).toContain("two branches");
      expect(calls.length).toBeLessThanOrEqual(1);
    }
  }, 30_000);

  it("is fine in a loop, a list that's started at once, and a race that's run again", async () => {
    const c = await one({}, async ({ step }: any) => {
      const out: any[] = [];
      for (let i = 0; i < 3; i++) out.push(await step.run("loop", () => i));
      out.push(...(await Promise.all([1, 2, 3].map((i) => step.run("list", async () => (await settle(30 - i * 10), i))))));
      for (let i = 0; i < 2; i++) out.push(await Promise.race([step.sleep("timeout", "30s").then(() => "late"), step.run("work", () => i)]));
      const branch = async (n: string) => { const r = []; for (let i = 0; i < 2; i++) r.push(await step.run(`${n}-poll`, async () => (await settle(Math.random() * 20), i))); return r; };
      out.push(await Promise.all([branch("a"), branch("b")]));
      await step.run("x", () => 1).then(() => step.run("x", () => 2));
      return out;
    });
    const r = await st(c.runOf("f"), 20_000);
    expect(r.v).toEqual([0, 1, 2, 1, 2, 3, 0, 1, [[0, 1], [0, 1]]]);
  }, 30_000);
});

describe("how a run ended", () => {
  it("isn't decided again as its onFailure goes on", async () => {
    let n = 0;
    const log: string[] = [];
    const c = await one(
      { onFailure: async ({ step }: any) => { await step.run("refund", () => log.push("refund")); await step.sleep("s", "2s"); await step.run("mail", () => log.push("mail")); } },
      async ({ step }: any) => { await step.run("reserve", () => log.push("reserve")); if (n++ === 0) throw new Error("config service down"); await step.run("charge", () => log.push("charge")); return "ok"; },
    );
    expect((await st(c.runOf("f"))).s).toBe(ExecutionStatus.FAILED);
    expect(log).toEqual(["reserve", "refund", "mail"]);
    expect(n).toBe(1);
  }, 20_000);

  it("isn't decided again as its parent is told", async () => {
    let n = 0;
    const log: string[] = [];
    const c = new Cluster((flow) => [
      flow.createFunction({ id: "parent", triggers: { event: "go" }, retries: 0 }, async ({ step }: any) => step.invoke("c", { function: "pay", timeout: "10d" })),
      flow.createFunction({ id: "pay", retries: 0, onFailure: async () => { log.push("refund"); } }, async ({ step }: any) => { await step.run("charge", () => { log.push("charge"); }); if (n++ > 0) throw new Error("config service down"); return "ok"; }),
    ]);
    c.callbackFails = (_id, k) => (k <= 5 ? 429 : undefined);
    await c.flow.send({ name: "go" });
    await c.pump(1500);
    expect(await st(c.runOf("pay"), 15_000)).toMatchObject({ s: ExecutionStatus.SUCCEEDED, v: "ok" });
    expect(await st(c.runOf("parent"), 5000)).toMatchObject({ v: "ok" });
    expect(log).toEqual(["charge"]);
  }, 40_000);

  it("stops its other branches", async () => {
    const log: string[] = [];
    const c = await one(
      { onFailure: async ({ step }: any) => { await step.run("refund", async () => { log.push("refund"); await settle(300); }); } },
      async ({ step }: any) => {
        await Promise.all([
          (async () => { await step.run("a", () => settle(50)); await step.run("charge", () => log.push("charge")); })(),
          (async () => { await step.run("b", () => 1); throw new Error("outside"); })(),
        ]);
      },
    );
    await st(c.runOf("f"));
    expect(log).toEqual(["refund"]);
  }, 20_000);
});

describe("work between steps", () => {
  class Slow extends Middleware.BaseMiddleware {
    readonly id = "slow";
    async transformStepInput({ stepOptions }: any) { if (stepOptions.id === "a") await settle(150); }
  }
  it("of a middleware isn't cut off by a branch that sleeps", async () => {
    const t0 = Date.now();
    const at: number[] = [];
    const c = await one({ middleware: [Slow] }, async ({ step }: any) => { await Promise.all([step.sleep("long", "5s"), step.run("a", () => void at.push(Date.now() - t0))]); return "ok"; });
    expect((await st(c.runOf("f"), 12_000)).v).toBe("ok");
    expect(at).toHaveLength(1);
    expect(at[0]).toBeLessThan(3000);
  }, 30_000);

  it("of a retry isn't put off by a branch that sleeps", async () => {
    const t0 = Date.now();
    const tries: number[] = [];
    const c = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 1 }, async ({ step }: any) => {
      tries.push(Date.now() - t0);
      const p = step.sleep("long", "9s");
      await step.run("a", () => 1);
      if (tries.length < 2) throw new Error("outside");
      await p;
      return "ok";
    })]);
    const real = c.store.putFailure.bind(c.store);
    c.store.putFailure = async (a: any, b: any) => { await settle(150); return real(a, b); };
    await c.flow.send({ name: "go" });
    await c.drain();
    await st(c.runOf("f"), 25_000);
    expect(tries[1]).toBeLessThan(6000);
  }, 40_000);

  it("of a large event's data isn't cut off, and the next step runs once", async () => {
    const t0 = Date.now();
    const runs: number[] = [];
    const c = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 0 }, async ({ step }: any) => {
      await Promise.all([step.sleep("long", "6s"), (async () => { await step.waitForEvent("w", { event: "big", timeout: "1h" }); await step.run("charge", () => void runs.push(Date.now() - t0)); })()]);
      return "ok";
    })]);
    const real = c.store.getBlob.bind(c.store);
    c.store.getBlob = async (k: string) => { await settle(150); return real(k); };
    await c.flow.send({ name: "go" });
    await c.drain();
    await settle(800);
    await c.flow.send({ name: "big", data: { x: "y".repeat(150_000) } });
    await c.drain();
    expect((await st(c.runOf("f"), 14_000)).v).toBe("ok");
    expect(runs).toHaveLength(1);
    expect(runs[0]).toBeLessThan(4500);
  }, 30_000);

  it("of a function, for a moment, isn't cut off either", async () => {
    const at: number[] = [];
    const t0 = Date.now();
    const c = await one({}, async ({ step }: any) => { await Promise.all([step.sleep("long", "5s"), (async () => { await step.run("a", () => 1); await settle(200); await step.run("b", () => void at.push(Date.now() - t0)); })()]); return "ok"; });
    expect((await st(c.runOf("f"), 12_000)).v).toBe("ok");
    expect(at).toHaveLength(1);
    expect(at[0]).toBeLessThan(3000);
  }, 30_000);
});

describe("an outage of the table", () => {
  for (const caught of [false, true])
    it(`as a result is read isn't the function's error${caught ? ", and it can't be caught" : ""}`, async () => {
      let failed = 0;
      const c = new Cluster((flow) => [
        flow.createFunction({ id: "parent", triggers: { event: "go" }, retries: 1, onFailure: async () => { failed++; } }, async ({ step }: any) => {
          if (!caught) return (await step.invoke("c", { function: "child", data: {} })).length;
          try { return (await step.invoke("c", { function: "child", data: {} })).length; } catch { await step.run("compensate", () => 1); return "compensated"; }
        }),
        flow.createFunction({ id: "child", triggers: { event: "x" }, retries: 0 }, async () => "x".repeat(150_000)),
      ]);
      const real = c.store.getBlob.bind(c.store);
      let outage = 6;
      c.store.getBlob = async (k: string) => { if (outage-- > 0) throw new Error("dynamo down"); return real(k); };
      await c.flow.send({ name: "go" });
      await c.pump(1500);
      expect((await st(c.runOf("parent"), 12_000)).v).toBe(150_000);
      expect(failed).toBe(0);
    }, 30_000);
});

describe("a run that's retried", () => {
  it("reuses large results, and doesn't invoke again", async () => {
    let childRuns = 0;
    const c = new Cluster((flow) => [
      flow.createFunction({ id: "parent", triggers: { event: "go" }, retries: 0 }, async ({ step }: any) => {
        const e = await step.waitForEvent("w", { event: "big", timeout: "1h" });
        const r = await step.invoke("c", { function: "child", data: {} });
        throw new Error(`after ${e?.data?.x?.length} ${r.length}`);
      }),
      flow.createFunction({ id: "child", triggers: { event: "x" }, retries: 0 }, async ({ step }: any) => { await step.run({ id: "wire", atMostOnce: true }, () => { childRuns++; }); return "x".repeat(150_000); }),
    ]);
    await c.flow.send({ name: "go" });
    await c.drain();
    await settle(300);
    await c.flow.send({ name: "big", data: { x: "y".repeat(150_000) } });
    await c.pump(2000);
    const run = c.runOf("parent");
    const events = (await run.result).getHistoryEvents();
    const { memo, blobs } = memoFrom(events);
    expect(blobs.c).toBeString();
    expect(onceSteps(events, { ...memo, ...blobs }).children).toEqual([]);
    c.history.set(run.runId, { status: "FAILED", payload: run.payload, events, error: { ErrorType: "Error", ErrorMessage: "x" } });
    const out = await c.flow.runs.retry(run.runId);
    expect(out.reused.sort()).toEqual(["c", "w"]);
    await c.pump(1000);
    expect((await st(c.run(out.id))).e.errorMessage).toBe("after 150000 150000");
    expect(childRuns).toBe(1);
  }, 30_000);

  it("stops the runs its singleton was taken from", async () => {
    const c = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "go" }, singleton: { mode: "cancel" }, retries: 0 }, async ({ step }: any) => step.waitForEvent("w", { event: "never", timeout: "4s" }))]);
    await c.flow.send({ name: "go", id: "old" });
    await c.drain();
    await settle(200);
    const A = [...c.runs.values()][0];
    c.history.set(A.runId + "x", { status: "FAILED", payload: { ...A.payload, runId: A.runId + "x" }, events: [] });
    const real = c.fetch.bind(c);
    c.fetch = async (s: string, p: string, i: any) => (p.endsWith("/stop") ? new Response("Throttled", { status: 429 }) : real(s, p, i));
    await c.flow.send({ name: "go", id: "B" });
    await c.drain({ maxRounds: 3 });
    c.queue = [];
    c.fetch = real;
    const item = [...c.store.locks.keys()][0];
    (c.store.locks.get(item) as any).until = Date.now() - 1;
    await c.flow.runs.replay(A.runId + "x").catch(() => {});
    expect(c.statusOf(A.runId)).toBe("STOPPED");
  }, 20_000);
});

describe("defer", () => {
  it("in a step is refused", async () => {
    const c = new Cluster((flow) => {
      const audit = createDefer(flow, { id: "audit", retries: 0 }, async () => {});
      return [audit, flow.createFunction({ id: "pay", triggers: { event: "pay" }, retries: 0 }, async ({ step, defer }: any) => { await step.run("charge", () => { defer("a", { function: audit }); }); })];
    });
    await c.flow.send({ name: "pay" });
    await c.drain();
    expect((await st(c.runOf("pay"))).e.errorMessage).toContain("can't be called in a step");
  });

  it("is kept, though the function isn't run again as the run ends", async () => {
    const ran: any[] = [];
    const c = new Cluster((flow) => {
      const audit = createDefer(flow, { id: "audit", retries: 0 }, async ({ event }: any) => void ran.push(event.data.x.length));
      return [
        audit,
        flow.createFunction({ id: "parent", triggers: { event: "go" }, retries: 0 }, async ({ step }: any) => step.invoke("c", { function: "pay", timeout: "10d" })),
        flow.createFunction({ id: "pay", retries: 0 }, async ({ defer, step }: any) => { defer("a", { function: audit, data: { x: "y".repeat(150_000) } }); await step.run("w", () => 1); return "ok"; }),
      ];
    });
    c.callbackFails = (_id, k) => (k <= 5 ? 429 : undefined);
    await c.flow.send({ name: "go" });
    await c.pump(1500);
    expect((await st(c.runOf("pay"), 15_000)).v).toBe("ok");
    await c.pump(800);
    expect((await st(c.runOf("parent"), 5000)).v).toBe("ok");
    expect(ran).toEqual([150_000]);
  }, 40_000);

  it("has its start timeout", async () => {
    const c = new Cluster((flow) => {
      const audit = createDefer(flow, { id: "audit", retries: 0, concurrency: 1, timeouts: { start: "1m" } } as any, async () => {});
      return [audit, flow.createFunction({ id: "pay", triggers: { event: "pay" }, retries: 0 }, async ({ defer }: any) => { defer("a", { function: audit }); })];
    });
    await c.flow.send({ name: "pay" });
    await c.drain();
    await st(c.runOf("pay"));
    await c.drain();
    expect(c.timers.map((m) => JSON.parse(m.body).timer.type)).toContain("start");
  });
});

describe("signals", () => {
  it("are refused when they're larger than a callback takes, and don't end the wait", async () => {
    const c = await one({}, async ({ step }: any) => step.waitForSignal("w", { signal: "s", timeout: "10s" }));
    await waitFor(() => c.store.signals.size || undefined);
    await expect(c.flow.sendSignal({ signal: "s", data: "x".repeat(300_000) })).rejects.toThrow("200 KB");
    c.queue.push({ messageId: "big", body: JSON.stringify({ kind: "signal", signal: "s", data: "x".repeat(300_000), id: "i" }), receiveCount: 0, sentAt: Date.now() });
    await c.drain();
    expect(c.store.signals.size).toBe(1);
    await c.flow.sendSignal({ signal: "s", data: "small" });
    await c.drain();
    expect((await st(c.runOf("f"))).v).toEqual({ signal: "s", data: "small" });
  }, 20_000);

  it("and invokes are answered by nothing else", async () => {
    const c = await one({}, async ({ step }: any) => { const s = await step.waitForSignal("w", { signal: "approve", timeout: "1h" }); return s ? "APPROVED" : "no"; });
    const w: any = await waitFor(() => [...c.store.signals.values()][0]);
    c.queue.push({ messageId: "x", body: JSON.stringify({ kind: "call", fn: "nothing", parent: w.callbackId, event: { id: "e", name: "n", data: {}, ts: 1 } }), receiveCount: 0, sentAt: Date.now() });
    await c.drain();
    const r = await st(c.runOf("f"));
    expect(r.s).toBe(ExecutionStatus.FAILED);
    expect(r.e.errorMessage).toContain("answered by something");
  }, 20_000);
});

describe("calls to another EventFlow", () => {
  it("are checked before they're sent, and answered when they're dropped", async () => {
    const shop = new Cluster((flow) => [flow.createFunction({ id: "buy", triggers: { event: "buy" }, retries: 0 }, async ({ step }: any) => step.invoke("c", { function: referenceFunction({ appId: "billing", functionId: "charge" }), meta: { sessions: { chat: "x".repeat(300) } }, timeout: "2h" }))], { id: "shop", apps: { billing: { queueUrl: "https://sqs/billing", routerName: "r2" } } });
    await shop.flow.send({ name: "buy" });
    await shop.drain();
    expect((await st(shop.runOf("buy"))).e.errorMessage).toContain("sessions");
    const billing = new Cluster((flow) => [flow.createFunction({ id: "charge", external: true, retries: 0 }, async () => "ok")], { id: "billing" });
    const answers: any[] = [];
    const real = billing.fetch.bind(billing);
    billing.fetch = async (s, p, i) => (p.endsWith("/succeed") ? (answers.push(JSON.parse(String(i.body))), new Response(null, { status: 200 })) : real(s, p, i));
    await billing.deliver([{ messageId: "m", body: JSON.stringify({ kind: "call", fn: "charge", parent: "cb", event: { id: "e", name: "inngest/function.invoked", data: {}, ts: 1, sessions: { chat: "x".repeat(300) } } }), receiveCount: 0, sentAt: Date.now() }]);
    expect(answers).toHaveLength(1);
    expect(answers[0].ok).toBe(false);
  }, 20_000);
});

describe("a step in flight as the function fails", () => {
  it("that waits for a branch that has ended doesn't hang the run", async () => {
    let n = 0;
    const c = await one({ retries: 2 }, async ({ step }: any) => {
      n++;
      let open: any;
      const gate = new Promise((r) => (open = r));
      await Promise.all([
        step.run("consume", async () => { await gate; return 1; }),
        (async () => { await step.run("b", () => settle(50)); await step.run("produce", () => { open(); }); })(),
        (async () => { await settle(10); throw new Error("outside"); })(),
      ]);
    });
    const r = await st(c.runOf("f"), 75_000);
    expect(r.s).toBe(ExecutionStatus.FAILED);
    expect(n).toBe(1);
  }, 90_000);
});

describe("logs", () => {
  it("of what comes after a wait are written, once", async () => {
    const lines: string[] = [];
    const c = await one(
      {},
      async ({ step, logger }: any) => { logger.info("start"); await step.sleep("s", "2s"); logger.info("woke"); await step.run("b", () => 1); logger.info("after b"); await step.sleep("s2", "2s"); logger.info("end"); },
      { logger: { info: (m: string) => lines.push(m), warn() {}, error() {}, debug() {} } },
    );
    await st(c.runOf("f"));
    expect(lines).toEqual(["start", "woke", "after b", "end"]);
  }, 20_000);
});

describe("a run that was stopped", () => {
  it("is told of once", async () => {
    const c = new Cluster((flow) => [flow.createFunction({ id: "pay", triggers: { event: "pay" }, retries: 0, timeouts: { finish: "7d" } }, async ({ step }: any) => step.waitForEvent("w", { event: "never", timeout: "4s" }))], { systemEvents: true });
    await c.flow.send({ name: "pay" });
    await c.drain();
    await settle(300);
    await c.flow.runs.cancel(c.runOf("pay").runId);
    const told = () => c.queue.filter((m) => JSON.parse(m.body).event?.name === "inngest/function.cancelled").length;
    expect(told()).toBe(1);
    await c.fire("finish");
    expect(told()).toBe(1);
  }, 20_000);
});

describe("an event", () => {
  it("is bounded in all of it", async () => {
    const c = new Cluster(() => []);
    await expect(c.flow.send({ name: "go", v: "v".repeat(1000) } as any)).rejects.toThrow("v of event");
  });
});
