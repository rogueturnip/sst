import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { ExecutionStatus, LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns, sdk, settle } from "./harness.ts";
import { currentStep } from "../src/worker.ts";
import { missing } from "../src/order.ts";
import { memoFrom, toSteps } from "../src/runs.ts";

// A function is given its steps' results in one order, live and on each replay. In real time,
// as which finishes first is what it's about.

const { Middleware, NonRetriableError } = sdk;
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
const st = (r: any, ms = 20_000) => Promise.race([status(r), settle(ms).then(() => ({ s: "HANG" }) as any)]);
const one = async (options: any, handler: any, client: any = {}) => {
  const c = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 0, ...options }, handler)], client);
  await c.flow.send({ name: "go" });
  await c.drain();
  return c;
};
class Slow extends Middleware.BaseMiddleware {
  readonly id = "slow";
  async transformStepInput() {
    await settle(Math.random() * 10);
  }
}
const kinds: [string, any][] = [
  ["", {}],
  [", with a concurrency limit", { concurrency: 6 }],
  [", with middleware", { middleware: [Slow] }],
  [", with both", { concurrency: 6, middleware: [Slow] }],
];
// As p-limit 3 does it: the next task runs in the last one's async context.
const limiter = (n: number) => {
  let active = 0;
  const queue: (() => void)[] = [];
  const next = () => { active--; queue.shift()?.(); };
  const go = async (fn: any, resolve: any) => {
    active++;
    const result = (async () => fn())();
    resolve(result);
    try { await result; } catch {}
    next();
  };
  return (fn: any) =>
    new Promise<any>((resolve) => {
      queue.push(() => void go(fn, resolve));
      void (async () => { await Promise.resolve(); if (active < n && queue.length) queue.shift()!(); })();
    });
};

for (const [name, options] of kinds)
  describe(`which finished first${name}`, () => {
    it("is the same on a replay, for a race of steps", async () => {
      const did: string[] = [];
      const c = await one(options, async ({ step }: any) => {
        const work = step.run("work", async () => { await settle(300); return "work"; });
        const check = step.run("check", async () => { await settle(10); return "fraud"; });
        if ((await Promise.race([work, check])) === "fraud") await step.run("refund", () => void did.push("refund"));
        else await step.run("ship", () => void did.push("ship"));
        await work;
        await step.sleep("later", "1s");
      });
      expect((await st(c.runOf("f"))).s).toBe(ExecutionStatus.SUCCEEDED);
      expect(did).toEqual(["refund"]);
    }, 30_000);

    it("is the same on a replay, for a race with a sleep", async () => {
      const did: string[] = [];
      const c = await one(options, async ({ step }: any) => {
        const pay = step.run("pay", async () => { await settle(2500); return "paid"; });
        const first = await Promise.race([pay, step.sleep("timeout", "1s").then(() => "late")]);
        if (first === "late") {
          await step.run("tell", () => void did.push("late"));
          await pay;
          await step.run("refund", () => void did.push("refund"));
        } else await step.run("ship", () => void did.push("ship"));
        await step.sleep("later", "1s");
      });
      expect((await st(c.runOf("f"))).s).toBe(ExecutionStatus.SUCCEEDED);
      expect(did).toEqual(["late", "refund"]);
    }, 30_000);

    it("is the same on a replay, for a step that fails", async () => {
      const did: string[] = [];
      const c = await one(options, async ({ step }: any) => {
        const bad = step.run("bad", async () => { await settle(10); throw new Error("declined"); });
        const good = step.run("good", async () => { await settle(250); return "paid"; });
        const first = await Promise.race([bad.catch((e: any) => e.message), good]);
        await step.run("did", () => void did.push(first));
        await good;
        await step.sleep("later", "1s");
        await step.run("did", () => void did.push(first));
      });
      expect((await st(c.runOf("f"))).s).toBe(ExecutionStatus.SUCCEEDED);
      expect(did).toEqual(["declined", "declined"]);
    }, 30_000);

    it("is the same when the function is tried again", async () => {
      const did: string[] = [];
      const keys = new Map<string, string>();
      let tries = 0;
      const c = await one({ ...options, retries: 2 }, async ({ step, idempotencyKey }: any) => {
        const n = tries++;
        const speeds: any = n ? { alice: 10, bob: 250 } : { alice: 250, bob: 10 };
        const pending = new Map(["alice", "bob"].map((who) => [who, step.run(`get-${who}`, () => settle(speeds[who])).then(() => who)]));
        const out: string[] = [];
        while (pending.size) {
          const who: any = await Promise.race(pending.values());
          pending.delete(who);
          out.push(await step.run("charge", () => { did.push(who); keys.set(currentStep()!.key, who); return who; }));
        }
        if (tries === 1) throw new Error("outside a step");
        return out;
      });
      const r = await st(c.runOf("f"));
      expect(r.v).toEqual(["bob", "alice"]);
      expect(did).toEqual(["bob", "alice"]);
      expect([...keys.values()]).toEqual(["bob", "alice"]);
    }, 30_000);

    it("decides which of a step ID is whose, through a limiter", async () => {
      const did: string[] = [];
      let fails = 1;
      const c = await one({ ...options, retries: 3 }, async ({ step }: any) => {
        const limit = limiter(1);
        const speeds: any = { bob: 60, alice: 10 };
        const branch = async (who: string) => {
          await step.run(`check-${who}`, () => settle(speeds[who]));
          return limit(() => step.run("charge", async () => {
            if (who === "bob" && fails-- > 0) throw new Error("timeout");
            await settle(150);
            did.push(who);
            return who;
          }));
        };
        const r = await Promise.all([branch("bob"), step.sleep("other", "1s").then(() => {}), branch("alice")]);
        await step.sleep("s", "1s");
        return r;
      });
      const r = await st(c.runOf("f"));
      expect(r.v).toEqual(["bob", null, "alice"]);
      expect(did.sort()).toEqual(["alice", "bob"]);
    }, 30_000);
  });

describe("a step ID used in ordinary code", () => {
  const shapes: Record<string, (step: any, log: string[]) => Promise<any>> = {
    "a helper in a loop": async (step, log) => {
      const item = async (i: number) => { await step.run("validate", () => i); await settle(5); return step.run("charge", () => void log.push(`${i}`)); };
      for (let i = 0; i < 3; i++) await item(i);
    },
    "a helper that begins with other work": async (step, log) => {
      const item = async (i: number) => { await settle(5); await step.run("validate", () => i); return step.run("charge", () => void log.push(`${i}`)); };
      for (let i = 0; i < 3; i++) await item(i);
    },
    "a generator": async (step, log) => {
      async function* pages() { for (let i = 0; i < 3; i++) { await step.run("page", () => i); yield await step.run("rows", () => i); } }
      for await (const i of pages()) await step.run("charge", () => void log.push(`${i}`));
    },
    recursion: async (step, log) => {
      const go = async (i: number): Promise<void> => { if (i === 3) return; await step.run("a", () => i); await step.run("charge", () => void log.push(`${i}`)); await go(i + 1); };
      await go(0);
    },
    "a chain of then": (step, log) => [0, 1, 2].reduce((p, i) => p.then(() => step.run("a", () => i)).then(() => step.run("charge", () => void log.push(`${i}`))), Promise.resolve() as Promise<any>),
    "timers one after another": async (step, log) => {
      for (let i = 0; i < 3; i++) await new Promise<void>((resolve, reject) => setTimeout(() => step.run("charge", () => void log.push(`${i}`)).then(resolve, reject), 5));
    },
    "rounds of Promise.all": async (step, log) => {
      for (let i = 0; i < 3; i++) await Promise.all([0].map(async () => { await step.run("a", () => i); await step.run("charge", () => void log.push(`${i}`)); }));
    },
    "a try/catch for each": async (step, log) => {
      for (let i = 0; i < 3; i++) try { await (async () => { await step.run("a", () => i); await step.run("charge", () => void log.push(`${i}`)); })(); } catch {}
    },
  };
  for (const [name, fn] of Object.entries(shapes))
    it(`works in ${name}`, async () => {
      const log: string[] = [];
      const c = await one({}, async ({ step }: any) => { await fn(step, log); await step.sleep("s", "1s"); return "ok"; });
      expect((await st(c.runOf("f"))).v).toBe("ok");
      expect(log).toEqual(["0", "1", "2"]);
    }, 20_000);

  it("isn't guessed where work outside of steps decides, whatever the function does about it", async () => {
    const log: string[] = [];
    const c = await one({ retries: 2 }, async ({ step }: any) => {
      const branch = async (who: string, ms: number) => { await settle(ms); return step.run("charge", async () => { await settle(100); log.push(who); return who; }); };
      const r = await Promise.allSettled([branch("alice", 30), branch("bob", 5)]);
      await step.run("ship", () => void log.push("ship"));
      return r;
    });
    const r = await st(c.runOf("f"));
    expect(r.s).toBe(ExecutionStatus.FAILED);
    expect(r.e.errorMessage).toContain("isn't in a step");
    expect(log).toEqual(["bob"]);
  }, 20_000);
});

describe("a run that has failed", () => {
  for (const [name, options] of [["", {}], [", with a concurrency limit", { concurrency: 5 }]] as [string, any][])
    it(`starts no step that was waiting for its next try${name}`, async () => {
      const log: string[] = [];
      const c = await one(
        { ...options, retries: 3, onFailure: async ({ step }: any) => { await step.run("refund", async () => { log.push("refund"); await settle(4000); }); } },
        async ({ step }: any) => {
          let n = 0;
          await Promise.all([
            step.run("charge", () => { if (n++ === 0) throw new Error("again"); log.push("charged"); }),
            step.run("check", async () => { await settle(200); throw new NonRetriableError("fraud"); }),
          ]);
        },
      );
      expect((await st(c.runOf("f"))).s).toBe(ExecutionStatus.FAILED);
      expect(log).toEqual(["refund"]);
    }, 30_000);

  it("waits for code that runs before its onFailure", async () => {
    const log: string[] = [];
    const c = await one(
      { onFailure: async () => void log.push("refund") },
      async ({ step }: any) => {
        await Promise.all([
          step.run("charge", async () => { await settle(400); log.push("charged"); }),
          step.run("check", async () => { await settle(50); throw new NonRetriableError("fraud"); }),
        ]);
      },
    );
    await st(c.runOf("f"));
    expect(log).toEqual(["charged", "refund"]);
  }, 20_000);
});

it("a step that fails before the function awaits it isn't an error of the process", async () => {
  const seen: unknown[] = [];
  const on = (e: unknown) => void seen.push(e);
  process.on("unhandledRejection", on);
  const c = await one({}, async ({ step }: any) => {
    const p = step.run("a", () => { throw new Error("declined"); });
    void step.run("alone", () => { throw new Error("no one awaits it"); });
    await step.run("b", () => settle(200));
    await step.sleep("s", "1s");
    try { await p; return "no"; } catch (e: any) { return e.message; }
  });
  expect((await st(c.runOf("f"))).v).toBe("declined");
  await settle(20);
  process.off("unhandledRejection", on);
  expect(seen).toEqual([]);
}, 20_000);

it("work between two steps isn't put off by a branch that calls another", async () => {
  const t0 = Date.now();
  const at: number[] = [];
  const c = await one({}, async ({ step }: any) => {
    await Promise.all([
      (async () => { await step.run("a", () => 1); await settle(200); await step.run("a2", () => void at.push(Date.now() - t0)); })(),
      (async () => { await step.run("b", () => settle(30)); await step.sleep("long", "5s"); })(),
    ]);
  });
  await st(c.runOf("f"));
  expect(at).toHaveLength(1);
  expect(at[0]).toBeLessThan(3000);
}, 20_000);

describe("a step the function doesn't call any more", () => {
  const ms = missing.ms;
  beforeAll(() => void (missing.ms = 1500));
  afterAll(() => void (missing.ms = ms));

  it("ends the run, where it called it in the same attempt", async () => {
    let n = 0;
    const log: string[] = [];
    const c = await one({ retries: 2 }, async ({ step }: any) => {
      if (n++ === 0) await step.run("removed", () => 1);
      await step.run("b", () => 2);
      await step.sleep("s", "1s");
      await step.run("charge", () => void log.push("charge"));
    });
    const r = await st(c.runOf("f"));
    expect(r.s).toBe(ExecutionStatus.FAILED);
    expect(r.e.errorMessage).toContain(`doesn't call the step "removed"`);
    expect(log).toEqual([]);
  }, 30_000);

  it("is waited for, however late it comes", async () => {
    const did: string[] = [];
    let n = 0;
    const c = await one({}, async ({ step }: any) => {
      const slow = n++ > 0;
      const card = (async () => { await settle(slow ? 900 : 5); return step.run("card", () => "card"); })();
      const bank = step.run("bank", () => settle(150).then(() => "bank"));
      const first = await Promise.race([card, bank]);
      await step.run(`ship-${first}`, () => void did.push(first as string));
      await Promise.all([card, bank]);
      await step.sleep("s", "1s");
    });
    expect((await st(c.runOf("f"))).s).toBe(ExecutionStatus.SUCCEEDED);
    expect(did).toEqual(["card"]);
  }, 30_000);

  it("is gone on without by a later attempt, once for all", async () => {
    const at: number[] = [];
    const c = await one({ retries: 2 }, async ({ step, attempt }: any) => {
      if (attempt === 0) {
        for (let i = 0; i < 4; i++) await step.run(`warm${i}`, () => 1);
        throw new Error("outside");
      }
      at.push(Date.now());
      await step.run("b", () => 1);
      await step.sleep("s", "1s");
      await step.run("c", () => 1);
      return "ok";
    });
    expect((await st(c.runOf("f"))).v).toBe("ok");
    expect(at).toHaveLength(2);
    expect(at[1] - at[0]).toBeLessThan(5000);
  }, 30_000);
});

describe("a function that's tried again after it was paused", () => {
  for (const [name, options] of kinds)
    it(`does nothing twice${name}`, async () => {
      const log: string[] = [];
      let passes = 0;
      const c = new Cluster((flow) => [
        flow.createFunction({ id: "f", triggers: { event: "go" }, ...options, retries: 3 }, async ({ step }: any) => {
          // It throws as it goes on, before it comes to the steps that are done.
          if (++passes === 2) throw new Error("the config service is down");
          const branch = async (who: string, ms: number) => { await step.run(`check-${who}`, () => settle(ms)); return step.run("charge", () => { log.push(who); return who; }); };
          const charged = await Promise.all([branch("alice", 120), branch("bob", 5)]);
          await step.run({ id: "wire", atMostOnce: true }, () => void log.push("wire"));
          await step.invoke("child", { function: "child", timeout: "1h" });
          await step.sleep("a-day", "2s");
          return charged;
        }),
        flow.createFunction({ id: "child", retries: 0 }, async ({ step }: any) => step.run({ id: "pay", atMostOnce: true }, () => void log.push("child"))),
      ]);
      await c.flow.send({ name: "go" });
      await c.pump(4000);
      expect((await st(c.runOf("f"))).v).toEqual(["alice", "bob"]);
      expect(passes).toBeGreaterThan(2);
      expect(log).toEqual(["bob", "alice", "wire", "child"]);
    }, 40_000);

  it("is its onFailure, too", async () => {
    const log: string[] = [];
    let passes = 0;
    const c = await one(
      { retries: 2, onFailure: async ({ step }: any) => {
        passes++;
        await step.run("refund", () => void log.push("refund"));
        if (passes === 1) throw new Error("the config service is down");
        await step.run("mail", () => void log.push("mail"));
      } },
      async () => { throw new NonRetriableError("no"); },
    );
    expect((await st(c.runOf("f"))).s).toBe(ExecutionStatus.FAILED);
    expect(log).toEqual(["refund", "mail"]);
  }, 30_000);
});

describe("a run that's retried", () => {
  it("gives no result to another branch, and charges each once", async () => {
    const charged = new Map<string, string>();
    let tries = 0;
    const c = new Cluster((flow) => [
      flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 0 }, async ({ step }: any) => {
        const first = tries++ === 0;
        (globalThis as any).currentKey = (who: string) => currentStep()!.keyFor(who);
        const speeds: any = { carol: 5, bob: 60, alice: 150 };
        const branch = async (who: string) => {
          await step.run(`check-${who}`, async () => { await settle(speeds[who]); if (first && who === "carol") throw new Error("check failed"); });
          return step.run("charge", () => {
            if (first && who === "alice") throw new Error("charge failed");
            // A provider that charges once for each key.
            const key = (globalThis as any).currentKey(who);
            if (!charged.has(key)) charged.set(key, who);
            return who;
          });
        };
        const all = await Promise.allSettled(["alice", "bob", "carol"].map(branch));
        if (all.some((r) => r.status === "rejected")) throw new NonRetriableError("not all");
        return all.map((r: any) => r.value);
      }),
    ]);
    await c.flow.send({ name: "go" });
    await c.drain();
    const run = c.runOf("f");
    await st(run);
    c.history.set(run.runId, { status: "FAILED", payload: run.payload, events: (await run.result).getHistoryEvents() });
    const again = await c.flow.runs.retry(run.runId);
    const r = await st(c.run(again.id));
    expect(r.v).toEqual(["alice", "bob", "carol"]);
    expect([...charged.values()].sort()).toEqual(["alice", "bob", "carol"]);
  }, 30_000);

  it("has the same order for a result that's read from the table", async () => {
    const did: string[] = [];
    const big = { x: "y".repeat(150_000) };
    const c = new Cluster((flow) => [
      flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 0, concurrency: 3 }, async ({ step }: any) => {
        const quote = step.run("quote", () => big).then(() => "quote");
        const check = step.run("check", () => settle(30)).then(() => "check");
        const first = await Promise.race([quote, check]);
        await step.run(`pay-${first}`, () => void did.push(first));
        await step.sleep("s", "1s");
        return first;
      }),
    ]);
    await c.flow.send({ name: "go" });
    await c.drain();
    const run = c.runOf("f");
    await st(run);
    // Its order isn't known, as that of what came after a step that's run again.
    const { blobs } = memoFrom((await run.result).getHistoryEvents());
    expect(Object.keys(blobs)).toEqual(["quote"]);
    did.length = 0;
    const getBlob = c.store.getBlob.bind(c.store);
    let reads = 0;
    c.store.getBlob = async (k: string) => { await settle(reads++ ? 400 : 5); return getBlob(k); };
    c.history.set(run.runId, { status: "FAILED", payload: { ...run.payload }, events: (await run.result).getHistoryEvents().filter((e: any) => e.Name === "quote" || e.ParentId) });
    const again = await c.flow.runs.retry(run.runId);
    expect((await st(c.run(again.id))).s).toBe(ExecutionStatus.SUCCEEDED);
    expect(did).toHaveLength(1);
  }, 30_000);
});

describe("what a step sends", () => {
  it("has IDs by what it is, so a replay with another list sends each once", async () => {
    let list = ["A", "B"];
    const c = new Cluster((flow) => [
      flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 0 }, async ({ step }: any) => {
        const orders = await step.run("list", () => list);
        for (const o of orders) await step.sendEvent("charge-order", { name: "order/charge", data: { o } });
      }),
    ]);
    await c.flow.send({ name: "go" });
    await c.drain();
    const run = c.runOf("f");
    await st(run);
    const ids = () => c.queue.filter((m) => JSON.parse(m.body).kind === "event").flatMap((m) => [JSON.parse(m.body)].map((b) => [b.event?.id ?? b.id, b.event?.data?.o]));
    const first = new Map(ids().map(([id, o]) => [o, id]));
    c.history.set(run.runId, { status: "FAILED", payload: run.payload, events: (await run.result).getHistoryEvents() });
    list = ["C", "A", "B"];
    const again = await c.flow.runs.replay(run.runId);
    await st(c.run(again.id));
    const all = ids();
    expect(all.filter(([, o]) => o === "A").every(([id]) => id === first.get("A"))).toBe(true);
    expect(all.filter(([, o]) => o === "B").every(([id]) => id === first.get("B"))).toBe(true);
    expect(new Set(all.map(([id]) => id)).size).toBe(3);
  }, 30_000);

  it("isn't sent once the run has failed", async () => {
    const log: string[] = [];
    let tries = 0;
    const t0 = Date.now();
    const c = new Cluster((flow) => [
      flow.createFunction(
        { id: "f", triggers: { event: "go" }, retries: 3, onFailure: async ({ step }: any) => { await step.run("refund", async () => { log.push("refund"); await settle(4000); }); } },
        async ({ step }: any) => {
          await Promise.all([
            step.sendEvent("ship", { name: "order/ship" }),
            step.run("check", async () => { await settle(100); throw new NonRetriableError("fraud"); }),
          ]);
        },
      ),
    ]);
    const real = c.fetch.bind(c);
    c.fetch = async (service: string, path: string, init: RequestInit) => {
      if (service === "sqs" && String(init.body).includes("order/ship") && Date.now() - t0 < 600) return (tries++, new Response("{}", { status: 500 }));
      return real(service, path, init);
    };
    await c.flow.send({ name: "go" });
    await c.drain();
    expect((await st(c.runOf("f"))).s).toBe(ExecutionStatus.FAILED);
    expect(log).toEqual(["refund"]);
    expect(tries).toBeGreaterThan(0);
    expect(c.queue.some((m) => m.body.includes("order/ship"))).toBe(false);
  }, 30_000);
});

it("an explicit ID that looks like a second use has a key of its own", async () => {
  const keys: string[] = [];
  const c = await one({}, async ({ step, idempotencyKey }: any) => {
    for (const o of ["a", "b"]) await step.run("charge", () => keys.push(idempotencyKey(o)));
    await step.run("charge:1", () => keys.push(idempotencyKey()));
    return "ok";
  });
  expect((await st(c.runOf("f"))).v).toBe("ok");
  expect(new Set(keys).size).toBe(3);
}, 20_000);

it("code that runs as the function ends isn't cut off", async () => {
  const log: string[] = [];
  const c = await one({}, async ({ step }: any) => {
    const pay = step.run("pay", async () => { await settle(400); log.push("paid"); });
    await Promise.race([pay, step.run("check", () => settle(5))]);
    return "ok";
  });
  const r = await c.runOf("f").result;
  expect(r.getResult()).toBe("ok");
  expect(log).toEqual(["paid"]);
  expect(toSteps(r.getHistoryEvents()).find((s) => s.id === "pay")).toMatchObject({ status: "succeeded" });
}, 20_000);

describe("what's saved", () => {
  it("has the order for a rerun, up to the first step that's run again", async () => {
    const c = await one({}, async ({ step }: any) => {
      const run = (id: string, ms: number, fail = false) => step.run(id, async () => { await settle(ms); if (fail) throw new Error("no"); return id; });
      await Promise.allSettled([run("1-slow", 150), run("2", 10), run("3-fails", 200, true), run("0", 60), run("after", 300), step.sleep("z", "1s")]);
      return "ok";
    });
    await st(c.runOf("f"));
    const events = (await c.runOf("f").result).getHistoryEvents();
    const { memo, order } = memoFrom(events);
    expect(order).toEqual(["2", "0", "1-slow"]);
    // What came after the step that failed runs again.
    expect(memo).toEqual({ "2": "2", "0": "0", "1-slow": "1-slow" });
    expect(toSteps(events).some((s) => s.id.includes("sst:order"))).toBe(false);
    expect(toSteps(events).find((s) => s.id === "3-fails")).toMatchObject({ status: "failed", error: { message: "no" } });
  }, 20_000);

  it("is whole for results too large for their context", async () => {
    const did: string[] = [];
    const big = "x".repeat(261_900);
    const c = await one({ concurrency: 2 }, async ({ step }: any) => {
      const a = step.run("a", async () => { await settle(200); return big; });
      const b = step.run("b", async () => { await settle(10); return "b"; });
      const first: any = await Promise.race([a, b]);
      await step.run("did", () => void did.push(first.slice(0, 1)));
      const v = await a;
      await step.sleep("s", "1s");
      await step.run("did", () => void did.push(first.slice(0, 1)));
      return v.length;
    });
    expect((await st(c.runOf("f"))).v).toBe(big.length);
    expect(did).toEqual(["b", "b"]);
  }, 20_000);
});

// Random functions, of which the path depends on what finishes first. Each time one is
// replayed, it must come to what it came to before, in the same order.
describe("any function", () => {
  const N = Number(process.env.EVENTFLOW_ORDER_RUNS ?? 12);
  for (const [name, options] of kinds)
    it(`takes the same path on each replay${name}`, async () => {
      const checks: ((c: Cluster) => Promise<void>)[] = [];
      const c = new Cluster((flow) =>
        Array.from({ length: N }, (_, k) => {
          const traces: string[][] = [];
          const ran = new Map<string, Set<string>>();
          const seed = Math.random();
          // Choices that are the function's own are the same each time: they come from the seed.
          const rng = () => { let s = Math.floor(seed * 2 ** 31) + k; return () => ((s = (s * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff); };
          let outer = 0;
          const fails = new Set<string>();
          checks.push(check);
          return flow.createFunction({ id: `f${k}`, triggers: { event: "go" }, ...options, retries: 2 }, async ({ step, idempotencyKey, attempt }: any) => {
            const r = rng();
            const trace: string[] = [];
            traces.push(trace);
            const limit = limiter(2);
            const act = async (id: string, who: string) => {
              const v = await step.run(id, async () => {
                const key = currentStep()!.key;
                ran.set(key, (ran.get(key) ?? new Set()).add(who));
                await settle(Math.random() * 60);
                if (Math.random() < 0.1 && !fails.has(key)) { fails.add(key); throw new Error("again"); }
                return who;
              });
              trace.push(`${who}=${v}`);
              return v;
            };
            const plans = Array.from({ length: 4 }, () => Array.from({ length: 3 }, () => r()));
            const branch = async (b: number) => {
              for (let i = 0; i < 3; i++) {
                const p = plans[b][i];
                const who = `${b}.${i}`;
                if (p < 0.2) { await step.sleep(`nap-${who}`, "1s"); trace.push(`nap ${who}`); }
                else if (p < 0.4) await limit(() => act("limited", who));
                else if (p < 0.6) {
                  const first = await Promise.race([act(`x-${who}`, `x${who}`), act(`y-${who}`, `y${who}`)]);
                  await act("winner", first);
                } else if (p < 0.8) await act("shared", who);
                else { const all = await Promise.all([1, 2, 3].map((j) => act("list", `${who}.${j}`))); trace.push(all.join()); }
              }
            };
            await Promise.all([0, 1, 2, 3].map(branch));
            if (outer++ === 0) throw new Error("outside a step");
            await step.sleep("end", "1s");
            return trace.length;
          });
          async function check(c: Cluster) {
          const result = await st(c.runOf(`f${k}`), 80_000);
          expect(result.s).toBe(ExecutionStatus.SUCCEEDED);
          expect(traces.length).toBeGreaterThan(1);
          for (let i = 1; i < traces.length; i++) {
            const n = Math.min(traces[i].length, traces[i - 1].length);
            expect(traces[i].slice(0, n)).toEqual(traces[i - 1].slice(0, n));
          }
          // A step, by its key, was always the same one's.
          for (const who of ran.values()) expect(who.size).toBe(1);
          // And each was told its own result.
          for (const t of traces) for (const line of t) if (line.includes("=")) expect(line.split("=")[0]).toBe(line.split("=")[1]);
          }
        }),
      );
      await c.flow.send({ name: "go" });
      await c.drain();
      await Promise.all(checks.map((check) => check(c)));
    }, 90_000);
});
