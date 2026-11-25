import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { ExecutionStatus, LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns, sdk, settle, waitFor } from "./harness.ts";

const { NonRetriableError, Middleware, referenceFunction, SignalConflictError } = sdk;

beforeAll(async () => {
  await LocalDurableTestRunner.setupTestEnvironment({ skipTime: true });
});
afterAll(async () => {
  await finishRuns();
  await LocalDurableTestRunner.teardownTestEnvironment();
});

const runsOf = (c: Cluster, fn: string) => [...c.runs.values()].filter((r) => r.payload.fn === fn);
const define = (options: any) => () => new sdk.EventFlow({ id: "x" }).createFunction({ id: "f", triggers: { event: "go" }, ...options }, async () => {});

describe("options", () => {
  it("are checked when the function is created", () => {
    for (const [options, message] of [
      [{ debounce: { period: "10ms" } }, "debounce period"],
      [{ debounce: { period: "8d" } }, "debounce period"],
      [{ batchEvents: { maxSize: 0, timeout: "5s" } }, "maxSize"],
      [{ batchEvents: { maxSize: 501, timeout: "5s" } }, "maxSize"],
      [{ batchEvents: { maxSize: 5, timeout: "0s" } }, "batchEvents timeout"],
      [{ batchEvents: { maxSize: 5, timeout: "5s" }, debounce: { period: "5s" } }, "don't go together"],
      [{ batchEvents: { maxSize: 5, timeout: "5s" }, cancelOn: [{ event: "x" }] }, "don't go together"],
      [{ singleton: { mode: "queue" } }, "mode"],
      [{ priority: { run: "event.data." } }, "priority"],
      [{ timeouts: { start: "1ms" } }, "start timeout"],
      [{ triggers: { cron: "* * * * *", jitter: "2h" } }, "jitter"],
      [{ triggers: { event: "*" } }, "wildcard"],
      [{ triggers: { event: "user/*.created" } }, "wildcard"],
      [{ triggers: { event: "user*" } }, "wildcard"],
      [{ middleware: ["x"] }, "middleware"],
    ] as [any, string][])
      expect(define(options)).toThrow(message);
  });

  it("don't let a sender name an event as EventFlow's own", async () => {
    const cluster = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "inngest/function.failed" } }, async () => {})]);
    await expect(cluster.flow.send({ name: "inngest/function.failed" })).rejects.toThrow("inngest/");
    // Nor in a message of its own.
    for (const kind of ["event", "system"])
      cluster.queue.push({ messageId: `forged-${kind}`, body: JSON.stringify({ kind, exp: Date.now() + 1e9, event: { id: "e", name: "inngest/function.failed", data: {}, ts: 1 } }), receiveCount: 0, sentAt: Date.now() });
    await cluster.drain();
    expect(cluster.runs.size).toBe(0);
  });
});

describe("wildcard triggers", () => {
  it("match what's under them", async () => {
    const cluster = new Cluster((flow) => [
      flow.createFunction({ id: "users", triggers: { event: "app/user.*" } }, async ({ event }) => event.name),
      flow.createFunction({ id: "app", triggers: { event: "app/*" } }, async ({ event }) => event.name),
    ]);
    await cluster.flow.send([{ name: "app/user.created" }, { name: "app/order" }, { name: "app/user" }, { name: "application/x" }, { name: "app/user." }]);
    await cluster.drain();
    expect(runsOf(cluster, "users").map((r) => r.payload.event.name).sort()).toEqual(["app/user.created"]);
    expect(runsOf(cluster, "app").map((r) => r.payload.event.name).sort()).toEqual(["app/order", "app/user", "app/user.", "app/user.created"]);
  });
});

describe("cron jitter", () => {
  it("holds a run as long as its name says, the same each time", async () => {
    const cluster = new Cluster((flow) => [flow.createFunction({ id: "nightly", triggers: sdk.cron("* * * * *", { jitter: "10m" }) }, async () => {})]);
    const tick = { "sst:eventflow:tick": new Date(Math.ceil(Date.now() / 60_000) * 60_000).toISOString() };
    await cluster.handler(tick, {});
    await cluster.handler(tick, {});
    const held = cluster.queue.map((m) => JSON.parse(m.body));
    expect(held).toHaveLength(2);
    expect(held[0].kind).toBe("start");
    expect(held[0].at).toBe(held[1].at);
    expect(held[0].runId).toBe(held[1].runId);
    expect(held[0].at - Date.parse(tick["sst:eventflow:tick"])).toBeLessThan(600_000);
  });
});

describe("debounce", () => {
  const make = (options: any = {}) =>
    new Cluster((flow) => [
      flow.createFunction({ id: "save", triggers: { event: "doc/edited" }, debounce: { period: "10s", key: "event.data.doc", ...options }, ...options.more }, async ({ event }) => event.data.n),
    ]);
  const edit = (c: Cluster, n: number, more: any = {}) => c.flow.send({ name: "doc/edited", data: { doc: "a", n, ...more.data }, ...more, ...(more.data ? { data: { doc: "a", n, ...more.data } } : {}) });

  it("starts one run, with the last event, once they stop", async () => {
    const cluster = make();
    for (const n of [1, 2, 3]) {
      await edit(cluster, n);
      await cluster.drain();
      await settle(3);
    }
    expect(cluster.runs.size).toBe(0);
    await cluster.fire("debounce");
    await cluster.drain();
    expect(runsOf(cluster, "save").map((r) => r.payload.event.data.n)).toEqual([3]);
    // The timers of the others find nothing to do.
    await cluster.fire("debounce");
    expect(cluster.runs.size).toBe(1);
  });

  it("opens a new window after its run starts", async () => {
    const cluster = make();
    await edit(cluster, 1);
    await cluster.drain();
    await cluster.fire("debounce");
    await settle(3);
    await edit(cluster, 2);
    await cluster.drain();
    expect(cluster.runs.size).toBe(1);
    await cluster.fire("debounce");
    expect(runsOf(cluster, "save").map((r) => r.payload.event.data.n)).toEqual([1, 2]);
  });

  it("keeps keys, and tenants, apart", async () => {
    const cluster = make();
    await edit(cluster, 1);
    await edit(cluster, 2, { data: { doc: "b" } });
    await edit(cluster, 3, { tenant: "t1" });
    await edit(cluster, 4, { tenant: "t2" });
    await cluster.drain();
    await cluster.fire("debounce");
    expect(runsOf(cluster, "save").map((r) => r.payload.event.data.n).sort()).toEqual([1, 2, 3, 4]);
  });

  it("keeps the last by its time, when events come out of order", async () => {
    const cluster = make();
    const ts = Date.now() - 1000;
    await edit(cluster, 2, { ts: ts + 10 });
    await cluster.drain();
    await edit(cluster, 1, { ts });
    await cluster.drain();
    await cluster.fire("debounce");
    expect(runsOf(cluster, "save").map((r) => r.payload.event.data.n)).toEqual([2]);
  });

  it("doesn't let a time that's to come hold its place", async () => {
    const cluster = make();
    await edit(cluster, 1, { ts: Date.now() + 86_400_000 });
    await cluster.drain();
    await settle(3);
    await edit(cluster, 2);
    await cluster.drain();
    await cluster.fire("debounce");
    expect(runsOf(cluster, "save").map((r) => r.payload.event.data.n)).toEqual([2]);
  });

  it("starts at its timeout, though events keep coming", async () => {
    const cluster = make({ timeout: "15s" });
    const now = Date.now;
    try {
      for (let i = 0; i < 3; i++) {
        Date.now = () => now() + i * 6000;
        await edit(cluster, i);
        await cluster.drain();
      }
    } finally {
      Date.now = now;
    }
    const due = cluster.timers.map((m) => JSON.parse(m.body).at - now());
    // 10 s, 16 s, and then 15 s, not 22.
    expect(Math.round(due[due.length - 1] / 1000)).toBe(15);
  });

  it("doesn't lose its event when a router stops as it starts the run", async () => {
    const cluster = make();
    await edit(cluster, 1);
    await cluster.drain();
    let fail = true;
    cluster.failStart = () => fail;
    await cluster.fire("debounce");
    expect(cluster.runs.size).toBe(0);
    fail = false;
    await settle(3);
    // The next event finds the window closing, and starts its run before it opens its own.
    await edit(cluster, 2);
    await cluster.drain();
    expect(runsOf(cluster, "save").map((r) => r.payload.event.data.n)).toEqual([1]);
    await cluster.fire("debounce");
    expect(runsOf(cluster, "save").map((r) => r.payload.event.data.n)).toEqual([1, 2]);
  });

  it("starts nothing for an event that's delivered again after its window closed", async () => {
    const cluster = make();
    await edit(cluster, 1);
    const [message] = cluster.queue;
    await cluster.drain();
    await cluster.fire("debounce");
    await cluster.deliver([message]);
    await cluster.fire("debounce");
    expect(cluster.runs.size).toBe(1);
  });

  it("takes one from the rate limit for each event", async () => {
    const cluster = make({ more: { rateLimit: { limit: 1, period: "1h" } } });
    await edit(cluster, 1);
    await cluster.drain();
    await settle(3);
    await edit(cluster, 2);
    await cluster.drain();
    await cluster.fire("debounce");
    expect(runsOf(cluster, "save").map((r) => r.payload.event.data.n)).toEqual([1]);
  });
});

describe("batchEvents", () => {
  const make = (options: any = {}) =>
    new Cluster((flow) => [
      flow.createFunction({ id: "bulk", triggers: { event: "log/line" }, batchEvents: { maxSize: 3, timeout: "30s", ...options }, retries: 0 }, async ({ event, events, step }) => {
        const n = await step.run("write", () => events.map((e: any) => e.data.n));
        return { n, first: event.data.n };
      }),
    ]);
  const lines = (c: Cluster, from: number, to: number, more: any = {}) =>
    c.flow.send(Array.from({ length: to - from + 1 }, (_, i) => ({ name: "log/line", data: { n: from + i }, ...more })));
  const results = async (c: Cluster) => (await Promise.all(runsOf(c, "bulk").map(async (r) => (await r.result).getResult().n))) as number[][];

  it("starts a run for each full batch, with each event in one", async () => {
    const cluster = make();
    await lines(cluster, 1, 7);
    await cluster.drain();
    const got = await results(cluster);
    expect(got.map((b) => b.length)).toEqual([3, 3]);
    await cluster.fire("batch");
    const all = (await results(cluster)).flat().sort((a, b) => a - b);
    expect(all).toEqual([1, 2, 3, 4, 5, 6, 7]);
  });

  it("starts what there is once its time comes, once", async () => {
    const cluster = make();
    await lines(cluster, 1, 2);
    await cluster.drain();
    expect(cluster.runs.size).toBe(0);
    await cluster.fire("batch");
    await cluster.fire("batch");
    expect((await results(cluster)).map((b) => b.length)).toEqual([2]);
  });

  it("keeps keys, and tenants, apart", async () => {
    const cluster = make({ key: "event.data.k" });
    await cluster.flow.send([1, 2, 3, 4].map((n) => ({ name: "log/line", data: { n, k: n % 2 } })));
    await lines(cluster, 5, 5, { tenant: "a" });
    await lines(cluster, 6, 6, { tenant: "b" });
    await cluster.drain();
    await cluster.fire("batch");
    const got = await results(cluster);
    expect(got.map((b) => b.slice().sort()).sort()).toEqual([[1, 3], [2, 4], [5], [6]]);
  });

  it("starts an event that its `if` isn't true for by itself, at once", async () => {
    const cluster = make({ if: "event.data.n < 100" });
    await lines(cluster, 1, 1);
    await lines(cluster, 200, 200);
    await cluster.drain();
    expect(await results(cluster)).toEqual([[200]]);
  });

  it("loses nothing, and has nothing twice, when routers stop and messages come again", async () => {
    const cluster = make();
    await lines(cluster, 1, 9);
    const messages = [...cluster.queue];
    let failures = 4;
    cluster.failStart = () => failures-- > 0;
    await cluster.drain();
    // All of them again, as SQS may.
    for (const m of messages) await cluster.deliver([{ ...m }]);
    await cluster.fire("batch");
    await cluster.drain();
    await cluster.fire("batch");
    const all = (await results(cluster)).flat().sort((a, b) => a - b);
    expect(all).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });

  it("starts a full batch whose router stopped before its run started", async () => {
    const cluster = make();
    await lines(cluster, 1, 3);
    let fail = true;
    cluster.failStart = () => fail;
    await cluster.drain({ maxRounds: 1 });
    cluster.queue = cluster.queue.filter((m) => JSON.parse(m.body).kind === "timer");
    expect(cluster.runs.size).toBe(0);
    fail = false;
    // Its messages are gone, as after too many tries: its timer starts it.
    await cluster.fire("batch");
    expect((await results(cluster)).flat().sort()).toEqual([1, 2, 3]);
  });

  it("has one run for events that a retry of the run has again", async () => {
    const cluster = make();
    await lines(cluster, 1, 3);
    await cluster.drain();
    const run = cluster.runOf("bulk");
    await run.result;
    cluster.history.set(run.runId, { status: "FAILED", payload: run.payload, events: [] });
    const { id } = await cluster.flow.runs.replay(run.runId);
    expect((await cluster.run(id).result).getResult().n.sort()).toEqual([1, 2, 3]);
  });
});

describe("singleton", () => {
  const make = (mode: string, options: any = {}) => {
    const gates: (() => void)[] = [];
    const cluster = new Cluster((flow) => [
      flow.createFunction({ id: "sync", triggers: { event: "sync" }, singleton: { mode, key: "event.data.k", ...options }, retries: 0, ...options.more }, async ({ event, step }: any) => {
        await step.run("work", () => (event.data.wait ? new Promise<void>((r) => gates.push(r)) : undefined));
        return event.data.n;
      }),
    ]);
    return { cluster, gates };
  };
  const go = async (c: Cluster, n: number, data: any = {}, more: any = {}) => {
    await c.flow.send({ name: "sync", data: { k: "a", n, ...data }, ...more });
    await c.drain();
  };

  it("skips a run while another runs, and not after", async () => {
    const { cluster, gates } = make("skip");
    await go(cluster, 1, { wait: true });
    await waitFor(() => gates.length);
    await go(cluster, 2);
    expect(cluster.runs.size).toBe(1);
    gates[0]();
    await cluster.runOf("sync").result;
    await go(cluster, 3);
    expect(runsOf(cluster, "sync").map((r) => r.payload.event.data.n)).toEqual([1, 3]);
  });

  it("keeps keys and tenants apart, and isn't one where the key is false", async () => {
    const { cluster, gates } = make("skip", { key: "event.data.k" });
    await go(cluster, 1, { wait: true });
    await go(cluster, 2, { k: "b" });
    await go(cluster, 3, {}, { tenant: "t" });
    await go(cluster, 4, { k: false });
    await go(cluster, 5, { k: false });
    expect(runsOf(cluster, "sync").map((r) => r.payload.event.data.n)).toEqual([1, 2, 3, 4, 5]);
    await waitFor(() => gates.length);
    gates[0]();
  });

  it("cancels the one that's running, and starts the other", async () => {
    const { cluster, gates } = make("cancel");
    await go(cluster, 1, { wait: true });
    await waitFor(() => gates.length);
    const first = cluster.runOf("sync").runId;
    await go(cluster, 2);
    expect(cluster.stopped).toEqual([`arn/${first}`]);
    expect(runsOf(cluster, "sync").map((r) => r.payload.event.data.n)).toEqual([1, 2]);
    gates[0]();
  });

  it("is free again after its run failed, was stopped, or its worker was", async () => {
    const { cluster, gates } = make("skip");
    await go(cluster, 1, { wait: true });
    await waitFor(() => gates.length);
    // Stopped from outside: nothing gave the singleton back.
    cluster.stopped.push(`arn/${cluster.runOf("sync").runId}`);
    await go(cluster, 2);
    expect(runsOf(cluster, "sync").map((r) => r.payload.event.data.n)).toEqual([1, 2]);
    gates[0]();
  });

  it("starts the same run when its message comes again, and skips it once another has run", async () => {
    const { cluster, gates } = make("skip");
    await cluster.flow.send({ name: "sync", data: { k: "a", n: 1, wait: true } });
    const [message] = cluster.queue;
    let fail = true;
    cluster.failStart = () => fail;
    await cluster.drain({ maxRounds: 1 });
    expect(cluster.runs.size).toBe(0);
    fail = false;
    // Another comes before its message is back: it's to start yet, so the other's skipped.
    await cluster.flow.send({ name: "sync", data: { k: "a", n: 2 } });
    await cluster.deliver(cluster.queue.splice(0).filter((m) => m.messageId !== message.messageId));
    expect(cluster.runs.size).toBe(0);
    await cluster.deliver([message]);
    await cluster.deliver([message]);
    expect(runsOf(cluster, "sync").map((r) => r.payload.event.data.n)).toEqual([1]);
    await waitFor(() => gates.length);
    gates[0]();
  });

  it("doesn't run one that lost it between being started and running", async () => {
    const { cluster } = make("cancel");
    await cluster.flow.send({ name: "sync", data: { k: "a", n: 1 } });
    const lock = cluster.store.lockRead.bind(cluster.store);
    // The router sees that it has it, and then a later run takes it.
    cluster.store.lockRead = async (item) => (cluster.runs.size === 0 ? lock(item) : { run: "later", until: Date.now() + 1e6 });
    await cluster.drain();
    const result = await cluster.runOf("sync").result;
    expect(result.getStatus()).toBe(ExecutionStatus.FAILED);
    expect(result.getError().errorMessage).toContain("later one");
  });

  it("tells the run that invoked one it skips", async () => {
    const gates: (() => void)[] = [];
    const cluster = new Cluster((flow) => {
      const one = flow.createFunction({ id: "one", singleton: { mode: "skip" }, retries: 0 }, async ({ step }: any) => step.run("w", () => new Promise<void>((r) => gates.push(r))));
      return [one, flow.createFunction({ id: "caller", triggers: { event: "call" }, retries: 0 }, async ({ step }) => step.invoke("i", { function: one }))];
    });
    await cluster.flow.send([{ name: "call" }, { name: "call" }]);
    await cluster.pump(600);
    const done = await Promise.race(runsOf(cluster, "caller").map((r) => r.result));
    expect(done.getError().errorMessage).toContain("singleton");
    gates.forEach((g) => g());
    await cluster.pump(300);
  });
});

describe("timeouts", () => {
  it("cancel a run that hasn't finished in time", async () => {
    const gates: (() => void)[] = [];
    const cluster = new Cluster((flow) => [
      flow.createFunction({ id: "slow", triggers: { event: "go" }, timeouts: { finish: "30s" }, retries: 0 }, async ({ step }: any) => {
        await step.run("w", () => new Promise<void>((r) => gates.push(r)));
      }),
    ]);
    await cluster.flow.send({ name: "go" });
    await cluster.drain();
    await waitFor(() => gates.length);
    expect(cluster.stopped).toHaveLength(0);
    await cluster.fire("finish");
    expect(cluster.stopped).toHaveLength(1);
    gates[0]();
  });

  it("cancel a run that waits for a lease too long, and not one that has started", async () => {
    const gates: (() => void)[] = [];
    const ran: number[] = [];
    const cluster = new Cluster((flow) => [
      flow.createFunction({ id: "one", triggers: { event: "go" }, concurrency: 1, timeouts: { start: "30s" }, retries: 0 }, async ({ event, step }: any) => {
        await step.run("w", () => (ran.push(event.data.n), event.data.n === 1 ? new Promise<void>((r) => gates.push(r)) : 1));
      }),
    ]);
    await cluster.flow.send({ name: "go", data: { n: 1 } });
    await cluster.drain();
    await waitFor(() => gates.length);
    await cluster.flow.send({ name: "go", data: { n: 2 } });
    await cluster.drain();
    await settle(100);
    await cluster.fire("start");
    const second = [...cluster.runs.values()].find((r) => r.payload.event.data.n === 2)!;
    expect(cluster.stopped).toEqual([`arn/${second.runId}`]);
    // The local runner can't stop it: given its lease after all, it doesn't run.
    gates[0]();
    await second.result;
    expect(ran).toEqual([1]);
  });

  it("don't have a throttle hold a run that would be cancelled, or use its slot", async () => {
    const cluster = new Cluster((flow) => [
      flow.createFunction({ id: "t", triggers: { event: "go" }, throttle: { limit: 1, period: "1m", burst: 0 } as any, timeouts: { start: "90s" } }, async () => {}),
    ]);
    for (let n = 0; n < 5; n++) await cluster.flow.send({ name: "go", data: { n } });
    const batch = cluster.queue.splice(0);
    await cluster.deliver(batch);
    const held = cluster.queue.filter((m) => JSON.parse(m.body).kind === "start");
    expect(cluster.runs.size).toBe(1);
    expect(held).toHaveLength(1);
  });
});

describe("priority", () => {
  it("moves a run's steps ahead in line", async () => {
    const order: string[] = [];
    let open!: () => void;
    const gate = new Promise<void>((r) => (open = r));
    const cluster = new Cluster((flow) => [
      flow.createFunction({ id: "p", triggers: { event: "go" }, concurrency: 1, priority: { run: "event.data.vip ? 120 : 0" }, retries: 0 }, async ({ event, step }: any) => {
        await step.run("w", async () => {
          order.push(event.data.n);
          if (event.data.n === "first") await gate;
        });
      }),
    ]);
    await cluster.flow.send({ name: "go", data: { n: "first" } });
    await cluster.drain();
    await waitFor(() => order.length);
    await cluster.flow.send({ name: "go", data: { n: "normal" } });
    await cluster.drain();
    await settle(150);
    await cluster.flow.send({ name: "go", data: { n: "vip", vip: true } });
    await cluster.drain();
    await settle(150);
    open();
    await Promise.all([...cluster.runs.values()].map((r) => r.result));
    expect(order).toEqual(["first", "vip", "normal"]);
  });
});

describe("retries of the function", () => {
  it("try it again when it throws outside a step, and don't run its steps again", async () => {
    const calls: string[] = [];
    let attempts = 0;
    const cluster = new Cluster((flow) => [
      flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 3 }, async (ctx: any) => {
        const a = await ctx.step.run("a", () => (calls.push("a"), "A"));
        await ctx.step.sleep("pause", "1m");
        expect(ctx.attempt).toBe(attempts);
        if (++attempts < 3) throw new Error("outside");
        const b = await ctx.step.run("b", () => (calls.push("b"), "B"));
        return a + b + ctx.attempt;
      }),
    ]);
    await cluster.flow.send({ name: "go" });
    await cluster.drain();
    const result = await cluster.runOf("f").result;
    expect(result.getResult()).toBe("AB2");
    expect(calls).toEqual(["a", "b"]);
    expect(attempts).toBe(3);
  });

  it("give up after the retries, and run onFailure once", async () => {
    let attempts = 0;
    const failures: any[] = [];
    const cluster = new Cluster((flow) => [
      flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 2, onFailure: async ({ error }: any) => void failures.push(error.message) }, async () => {
        attempts++;
        throw new Error("always");
      }),
    ]);
    await cluster.flow.send({ name: "go" });
    await cluster.drain();
    const result = await cluster.runOf("f").result;
    expect(result.getStatus()).toBe(ExecutionStatus.FAILED);
    expect(result.getError().errorMessage).toContain("always");
    expect(attempts).toBe(3);
    expect(failures).toEqual(["always"]);
  });

  it("don't try it again for a NonRetriableError, a step's error, or how a step is used", async () => {
    for (const body of [
      async () => {
        throw new NonRetriableError("no");
      },
      async ({ step }: any) => step.run("s", () => Promise.reject(new Error("step"))),
      async ({ step }: any) => step.sleepUntil("s", "not a date"),
      async ({ step }: any) => step.waitForSignal("s", { signal: "", timeout: "1h" }),
    ]) {
      let calls = 0;
      const cluster = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 1 }, async (ctx: any) => (calls++, body(ctx)))]);
      await cluster.flow.send({ name: "go" });
      await cluster.drain();
      const result = await cluster.runOf("f").result;
      // A step that's tried again starts the function again, from its saved steps. The
      // function itself isn't tried again.
      expect(result.getHistoryEvents().some((e: any) => /^sst:failed/.test(e.Name ?? ""))).toBe(false);
      expect(calls).toBeLessThanOrEqual(2);
    }
  });

  it("give the next attempt the error a step failed with, when the function caught it", async () => {
    let runs = 0;
    let attempts = 0;
    const cluster = new Cluster((flow) => [
      flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 1 }, async ({ step }: any) => {
        const caught = await step.run("s", () => (runs++, Promise.reject(new NonRetriableError("card declined")))).catch((e: Error) => e.message);
        if (++attempts === 1) throw new Error("outside");
        return caught;
      }),
    ]);
    await cluster.flow.send({ name: "go" });
    await cluster.drain();
    expect((await cluster.runOf("f").result).getResult()).toContain("card declined");
    expect(runs).toBe(1);
  });

  it("wait for the steps in flight, so they aren't run twice", async () => {
    const calls: string[] = [];
    let attempts = 0;
    const cluster = new Cluster((flow) => [
      flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 2 }, async ({ step }: any) => {
        const slow = step.run("charge", async () => (await settle(80), calls.push("charge"), "charged"));
        slow.catch(() => {});
        if (++attempts === 1) throw new Error("outside");
        return slow;
      }),
    ]);
    await cluster.flow.send({ name: "go" });
    await cluster.drain();
    expect((await cluster.runOf("f").result).getResult()).toBe("charged");
    expect(calls).toEqual(["charge"]);
  });

  it("keep a step's idempotency key", async () => {
    const keys: string[] = [];
    let attempts = 0;
    const cluster = new Cluster((flow) => [
      flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 2, concurrency: 1 }, async ({ step, idempotencyKey }: any) => {
        await step.run("a", () => keys.push(idempotencyKey()));
        if (++attempts === 1) throw new Error("outside");
        await step.run("b", () => keys.push(idempotencyKey()));
      }),
    ]);
    await cluster.flow.send({ name: "go" });
    await cluster.drain();
    await cluster.runOf("f").result;
    expect(new Set(keys).size).toBe(2);
    expect(keys).toHaveLength(2);
  });

  it("carry more than one item holds", async () => {
    let attempts = 0;
    const cluster = new Cluster((flow) => [
      flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 1 }, async ({ step }: any) => {
        const parts = [];
        for (let i = 0; i < 4; i++) parts.push(await step.run(`p${i}`, () => "x".repeat(150_000)));
        if (++attempts === 1) throw new Error("outside");
        return parts.join("").length;
      }),
    ]);
    await cluster.flow.send({ name: "go" });
    await cluster.drain();
    expect((await cluster.runOf("f").result).getResult()).toBe(600_000);
  });

  it("number a step's attempts, in the step", async () => {
    for (const concurrency of [undefined, 1]) {
      const seen: number[] = [];
      const cluster = new Cluster((flow) => [
        flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 3, concurrency }, async (ctx: any) => {
          await ctx.step.run("s", () => {
            seen.push(ctx.attempt);
            if (seen.length < 3) throw new Error("again");
          });
          return ctx.attempt;
        }),
      ]);
      await cluster.flow.send({ name: "go" });
      await cluster.drain();
      expect((await cluster.runOf("f").result).getResult()).toBe(0);
      expect(seen).toEqual([0, 1, 2]);
    }
  });
});

describe("signals", () => {
  const waiting = (c: Cluster) => waitFor(() => (c.store.signals.size ? c.store.signals : undefined));
  const make = (onConflict?: string) =>
    new Cluster((flow) => [
      flow.createFunction({ id: "wait", triggers: { event: "wait" }, retries: 0 }, async ({ step, event }: any) => step.waitForSignal("s", { signal: "go", timeout: "1h", onConflict: event.data.onConflict ?? onConflict })),
      flow.createFunction({ id: "send", triggers: { event: "send" } }, async ({ step }: any) => step.sendSignal("s", { signal: "go", data: { ok: 1 } })),
    ]);

  it("resume the run that waits, from a run and from the client", async () => {
    for (const from of ["run", "client"]) {
      const cluster = make();
      await cluster.flow.send({ name: "wait", data: {} });
      await cluster.drain();
      await waiting(cluster);
      if (from === "run") await cluster.flow.send({ name: "send" });
      else await cluster.flow.sendSignal({ signal: "go", data: { ok: 1 } });
      await cluster.drain();
      expect((await cluster.runOf("wait").result).getResult()).toEqual({ signal: "go", data: { ok: 1 } });
      if (from === "run") expect((await cluster.runOf("send").result).getResult()).toEqual({ runId: cluster.runOf("wait").runId });
      expect(cluster.store.signals.size).toBe(0);
    }
  });

  it("say when no run waits, and are per tenant", async () => {
    const cluster = make();
    await cluster.flow.send({ name: "send" });
    await cluster.drain();
    expect((await cluster.runOf("send").result).getResult().runId ?? null).toBe(null);
    await cluster.flow.send({ name: "wait", data: {}, tenant: "a" });
    await cluster.drain();
    await waiting(cluster);
    await cluster.flow.sendSignal({ signal: "go", tenant: "b" });
    await cluster.drain();
    expect(cluster.store.signals.size).toBe(1);
    await cluster.flow.sendSignal({ signal: "go", tenant: "a" });
    await cluster.drain();
    expect((await cluster.runOf("wait").result).getResult()).toEqual({ signal: "go", data: null });
  });

  it("fail the second run that waits, or have it take the first's place", async () => {
    const cluster = make();
    await cluster.flow.send({ name: "wait", data: {} });
    await cluster.drain();
    await waiting(cluster);
    const first = cluster.runOf("wait");
    await cluster.flow.send({ name: "wait", data: {} });
    await cluster.drain();
    const second = [...cluster.runs.values()].find((r) => r.runId !== first.runId)!;
    expect((await second.result).getError().errorMessage).toContain("Another run waits");
    await cluster.flow.send({ name: "wait", data: { onConflict: "replace" } });
    await cluster.drain();
    await settle(150);
    await cluster.flow.sendSignal({ signal: "go", data: 1 });
    await cluster.drain();
    const third = [...cluster.runs.values()].find((r) => r.payload.event.data.onConflict)!;
    expect((await third.result).getResult()).toEqual({ signal: "go", data: 1 });
    void SignalConflictError;
  });

  it("refuse one whose tenant isn't its message's group", async () => {
    const cluster = make();
    await cluster.flow.send({ name: "wait", data: {}, tenant: "a" });
    await cluster.drain();
    await waiting(cluster);
    cluster.queue.push({ messageId: "forged", body: JSON.stringify({ kind: "signal", signal: "go", data: 1, tenant: "a", id: "x" }), group: "b", receiveCount: 0, sentAt: Date.now() });
    await cluster.drain();
    expect(cluster.store.signals.size).toBe(1);
    await cluster.flow.sendSignal({ signal: "go", tenant: "a" });
    await cluster.drain();
  });
});

describe("system events", () => {
  const make = (options: any = {}, names = ["inngest/function.failed", "inngest/function.finished", "inngest/function.cancelled"]) => {
    const seen: any[] = [];
    const cluster = new Cluster(
      (flow) => [
        flow.createFunction({ id: "ok", triggers: { event: "ok" } }, async () => "done"),
        flow.createFunction({ id: "bad", triggers: { event: "bad" }, retries: 0 }, async () => Promise.reject(new Error("broken"))),
        ...names.map((name, i) =>
          flow.createFunction({ id: `watch-${i}`, triggers: { event: name }, retries: 0 }, async ({ event }: any) => {
            seen.push([event.name, event.data.function_id, event.data._inngest.status, event.data.error?.message ?? event.data.result]);
            throw new Error("the watcher fails too");
          }),
        ),
      ],
      options,
    );
    return { cluster, seen };
  };

  it("say how a run ended, and not how their own runs did", async () => {
    const { cluster, seen } = make();
    await cluster.flow.send([{ name: "ok" }, { name: "bad" }]);
    await cluster.pump(800);
    expect(seen.sort()).toEqual([
      ["inngest/function.failed", "bad", "Failed", "broken"],
      ["inngest/function.finished", "bad", "Failed", "broken"],
      ["inngest/function.finished", "ok", "Completed", "done"],
    ]);
    // The watchers failed: that isn't told, or it would go on forever.
    await cluster.pump(300);
    expect(seen).toHaveLength(3);
  });

  it("aren't sent where nothing wants them", async () => {
    const { cluster } = make({}, []);
    await cluster.flow.send([{ name: "ok" }, { name: "bad" }]);
    await cluster.drain();
    await Promise.all([...cluster.runs.values()].map((r) => r.result));
    expect(cluster.queue).toHaveLength(0);
  });

  it("are sent for every run when they're asked for, and for one that's cancelled", async () => {
    const { cluster } = make({ systemEvents: true }, []);
    await cluster.flow.send({ name: "ok" });
    await cluster.drain();
    const run = cluster.runOf("ok");
    await run.result;
    expect(cluster.queue.map((m) => JSON.parse(m.body).event.name)).toEqual(["inngest/function.finished"]);
    cluster.queue.length = 0;
    cluster.history.set(run.runId, { status: "RUNNING", payload: run.payload, events: [] });
    cluster.ended.delete(run.runId);
    await cluster.flow.runs.cancel(run.runId);
    expect(cluster.queue.map((m) => JSON.parse(m.body).event.name).sort()).toEqual(["inngest/function.cancelled", "inngest/function.finished"]);
  });
});

describe("invoke", () => {
  it("takes a function's ID, or a reference, of this app only", async () => {
    for (const fn of ["child", "test-child", referenceFunction({ functionId: "child" }), referenceFunction({ functionId: "child", appId: "test" })]) {
      const cluster = new Cluster((flow) => [
        flow.createFunction({ id: "child" }, async ({ event }: any) => event.data.n * 2),
        flow.createFunction({ id: "parent", triggers: { event: "go" }, retries: 0 }, async ({ step }: any) => step.invoke("c", { function: fn, data: { n: 4 } })),
      ]);
      await cluster.flow.send({ name: "go" });
      await cluster.pump(500);
      expect((await cluster.runOf("parent").result).getResult()).toBe(8);
    }
    const cluster = new Cluster((flow) => [
      flow.createFunction({ id: "parent", triggers: { event: "go" } }, async ({ step }: any) => step.invoke("c", { function: referenceFunction({ functionId: "child", appId: "other" }) })),
    ]);
    await cluster.flow.send({ name: "go" });
    await cluster.drain();
    expect((await cluster.runOf("parent").result).getError().errorMessage).toContain('the app "other"');
  });
});

describe("step.fetch and step.ai", () => {
  const withServer = async (handler: (req: Request) => Response | Promise<Response>, test: (url: string) => Promise<void>) => {
    const server = Bun.serve({ port: 0, fetch: handler });
    try {
      await test(`http://localhost:${server.port}`);
    } finally {
      server.stop(true);
    }
  };

  it("saves a response, and gives it back as one", async () => {
    let hits = 0;
    await withServer(
      async (req) => (hits++, new URL(req.url).pathname === "/bin" ? new Response(new Uint8Array([255, 0, 128])) : Response.json({ method: req.method, body: await req.text() }, { status: 201, headers: { "x-a": "b" } })),
      async (url) => {
        const cluster = new Cluster((flow) => [
          flow.createFunction({ id: "f", triggers: { event: "go" } }, async ({ step }: any) => {
            const res = await step.fetch(`${url}/json`, { method: "POST", body: "hi" });
            const bin = await step.fetch(`${url}/bin`);
            await step.sleep("pause", "1h");
            return { status: res.status, header: res.headers.get("x-a"), body: await res.json(), bytes: [...new Uint8Array(await bin.arrayBuffer())] };
          }),
        ]);
        await cluster.flow.send({ name: "go" });
        await cluster.drain();
        expect((await cluster.runOf("f").result).getResult()).toEqual({ status: 201, header: "b", body: { method: "POST", body: "hi" }, bytes: [255, 0, 128] });
        expect(hits).toBe(2);
      },
    );
  });

  it("calls a model with its key, tries a rate limit again, and not a bad request", async () => {
    const seen: any[] = [];
    await withServer(
      async (req) => {
        const body: any = await req.json();
        seen.push([req.headers.get("x-api-key") ?? req.headers.get("authorization"), body.model]);
        if (body.messages === "bad") return new Response("bad request", { status: 400 });
        return seen.length === 1 ? new Response("slow down", { status: 429, headers: { "retry-after": "2" } }) : Response.json({ text: "hello" });
      },
      async (url) => {
        const model = (format: string) => ({ format, url, authKey: "sk-secret", headers: {}, options: {}, onCall: (_: any, body: any) => void (body.model = "m-1") });
        const cluster = new Cluster((flow) => [
          flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 3 }, async ({ step, event }: any) => step.ai.infer("ask", { model: model(event.data.format), body: { messages: event.data.messages } })),
        ]);
        await cluster.flow.send({ name: "go", data: { format: "anthropic", messages: [] } });
        await cluster.drain();
        const run = cluster.runOf("f");
        expect((await run.result).getResult()).toEqual({ text: "hello" });
        expect(seen).toEqual([["sk-secret", "m-1"], ["sk-secret", "m-1"]]);
        // The key isn't in the history.
        expect(JSON.stringify((await run.result).getOperations?.() ?? "")).not.toContain("sk-secret");
        await cluster.flow.send({ name: "go", data: { format: "openai-chat", messages: "bad" } });
        await cluster.drain();
        const bad = [...cluster.runs.values()].find((r) => r !== run)!;
        expect((await bad.result).getError().errorMessage).toContain("400");
        expect(seen[2]).toEqual(["Bearer sk-secret", "m-1"]);
        expect(seen).toHaveLength(3);
      },
    );
  });

  it("runs a function with its input", async () => {
    const cluster = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "go" } }, async ({ step }: any) => step.ai.wrap("w", (a: number, b: number) => a + b, 2, 3))]);
    await cluster.flow.send({ name: "go" });
    await cluster.drain();
    expect((await cluster.runOf("f").result).getResult()).toBe(5);
  });
});

describe("middleware", () => {
  it("has its hooks, in order, around functions, steps and sends", async () => {
    const log: string[] = [];
    const make = (name: string) =>
      class extends Middleware.BaseMiddleware {
        readonly id = name;
        async transformFunctionInput({ ctx }: any) {
          return { ctx: { ...ctx, [name]: `injected ${name}` } };
        }
        async wrapFunctionHandler({ next }: any) {
          return `${name}(${await next()})`;
        }
        async transformStepInput({ stepOptions, input }: any) {
          await settle(Math.random() * 5);
          return { stepOptions, input: input.map((x: any) => `${x}+${name}`) };
        }
        async wrapStep({ next }: any) {
          return `${name}<${await next()}>`;
        }
        async wrapStepHandler({ next }: any) {
          return `${name}[${await next()}]`;
        }
        transformSendEvent({ events }: any) {
          return { events: events.map((e: any) => ({ ...e, data: { ...e.data, by: [...(e.data?.by ?? []), name] } })) };
        }
        onRunStart() {
          log.push(`${name} start`);
        }
        onStepStart({ stepInfo }: any) {
          log.push(`${name} step ${stepInfo.options.id}`);
        }
        onStepComplete({ output }: any) {
          log.push(`${name} done ${output}`);
        }
        onRunComplete({ output }: any) {
          log.push(`${name} end ${output}`);
        }
      };
    const cluster = new Cluster(
      (flow) => [
        flow.createFunction({ id: "f", triggers: { event: "go" }, middleware: [make("fn") as any] }, async ({ step, event, client, fn }: any) => {
          const r = await step.run("s", (x: string) => x, "in");
          await step.sleep("pause", "1m");
          return `${r} ${client} ${fn} ${event.data.by}`;
        }),
      ],
      { middleware: [make("client")] },
    );
    await cluster.flow.send({ name: "go", data: {} });
    await cluster.drain();
    const out = "client(fn(client<fn<client[fn[in+client+fn]]>> injected client injected fn client))";
    expect((await cluster.runOf("f").result).getResult()).toBe(out);
    expect(log).toEqual(["client start", "fn start", "client step s", "fn step s", "client done client[fn[in+client+fn]]", "fn done client[fn[in+client+fn]]", `client end ${out}`, `fn end ${out}`]);
  });

  it("keeps steps in their places in the history, however long its hooks take", async () => {
    class Slow extends Middleware.BaseMiddleware {
      readonly id = "slow";
      async transformStepInput() {
        await settle(Math.random() * 30);
      }
    }
    const calls: string[] = [];
    const cluster = new Cluster((flow) => [
      flow.createFunction({ id: "f", triggers: { event: "go" }, middleware: [Slow as any], concurrency: 3 }, async ({ step }: any) => {
        const out = [];
        for (let round = 0; round < 3; round++) {
          out.push(...(await Promise.all(["a", "b", "c", "d"].map((id) => step.run(`${id}${round}`, () => (calls.push(`${id}${round}`), `${id}${round}`))))));
          await step.sleep(`pause${round}`, "1h");
        }
        return out.join("");
      }),
    ]);
    await cluster.flow.send({ name: "go" });
    await cluster.drain();
    expect((await cluster.runOf("f").result).getResult()).toBe("a0b0c0d0a1b1c1d1a2b2c2d2");
    expect(new Set(calls).size).toBe(12);
    expect(calls).toHaveLength(12);
  });

  it("hears of errors, and which is the last", async () => {
    const errors: any[] = [];
    class Watch extends Middleware.BaseMiddleware {
      readonly id = "watch";
      onStepError({ error, isFinalAttempt }: any) {
        errors.push(["step", error.message, isFinalAttempt]);
      }
      onRunError({ error, isFinalAttempt }: any) {
        errors.push(["run", error.message, isFinalAttempt]);
      }
    }
    const cluster = new Cluster((flow) => [
      flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 1, middleware: [Watch as any] }, async ({ step }: any) => step.run("s", () => Promise.reject(new Error("no")))),
    ]);
    await cluster.flow.send({ name: "go" });
    await cluster.drain();
    await cluster.runOf("f").result;
    expect(errors).toEqual([["step", "no", false], ["step", "no", true], ["run", "no", true]]);
  });

  it("refuses hooks that have no meaning here", () => {
    class Req extends Middleware.BaseMiddleware {
      readonly id = "req";
      async wrapRequest() {}
    }
    expect(() => new sdk.EventFlow({ id: "x", middleware: [Req as any] })).toThrow("wrapRequest");
  });
});

describe("logger", () => {
  it("is the one that's given, and doesn't repeat itself", async () => {
    const lines: string[] = [];
    const logger = { info: (m: string) => lines.push(m), warn: () => {}, error: () => {}, debug: () => {} };
    const cluster = new Cluster(
      (flow) => [
        flow.createFunction({ id: "f", triggers: { event: "go" } }, async ({ step, logger }: any) => {
          logger.info("before");
          await step.sleep("pause", "1h");
          logger.info("after");
        }),
      ],
      { logger },
    );
    await cluster.flow.send({ name: "go" });
    await cluster.drain();
    await cluster.runOf("f").result;
    expect(lines).toEqual(["before", "after"]);
  });
});
