import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { ExecutionStatus, LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { z } from "zod";
import { Cluster, finishRuns, sdk, settle, waitFor } from "./harness.ts";

const { channel, createDefer, referenceFunction, Middleware } = sdk;
beforeAll(async () => {
  await LocalDurableTestRunner.setupTestEnvironment({ skipTime: true });
});
afterAll(async () => {
  await finishRuns();
  await LocalDurableTestRunner.teardownTestEnvironment();
});
const runsOf = (c: Cluster, fn: string) => [...c.runs.values()].filter((r) => r.payload.fn === fn);

describe("sessions", () => {
  it("are those that all of a run's events share, and go to what it sends and invokes", async () => {
    const seen: Record<string, any> = {};
    const make = (options: any = {}) =>
      new Cluster(
        (flow) => {
          const child = flow.createFunction({ id: "child" }, async ({ sessions }: any) => void (seen.child = sessions));
          return [
            child,
            flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 0 }, async ({ step, sessions }: any) => {
              seen.f = sessions;
              await step.sendEvent("e", [{ name: "next", id: "n1" }, { name: "next", id: "n2", meta: { sessions: { chat: null, extra: 7 } } }, { name: "next", id: "n3", meta: { sessions: null } }]);
              await step.invoke("i", { function: child });
            }),
            flow.createFunction({ id: "next", triggers: { event: "next" } }, async ({ event, sessions }: any) => void (seen[event.id] = sessions)),
          ];
        },
        options,
      );
    let c = make();
    await c.flow.send({ name: "go", meta: { sessions: { chat: "c1", user: 5 } } });
    await c.pump(800);
    expect(seen).toEqual({ f: { chat: "c1", user: "5" }, child: { chat: "c1", user: "5" }, n1: { chat: "c1", user: "5" }, n2: { user: "5", extra: "7" }, n3: undefined });
    c = make({ sessionPropagation: false });
    await c.flow.send({ name: "go", meta: { sessions: { chat: "c1" } } });
    await c.pump(800);
    expect([seen.f, seen.child, seen.n1]).toEqual([{ chat: "c1" }, undefined, undefined]);
  });

  it("are checked", async () => {
    const c = new Cluster(() => []);
    await expect(c.flow.send({ name: "go", meta: { sessions: { [`x${"y".repeat(200)}`]: "a" } } })).rejects.toThrow("sessions");
    await expect(c.flow.send({ name: "go", meta: { sessions: Object.fromEntries(Array.from({ length: 30 }, (_, i) => [`s${i}`, "a"])) } })).rejects.toThrow("sessions");
  });
});

describe("defer", () => {
  const make = (handler: any, options: any = {}) => {
    const ran: any[] = [];
    const c = new Cluster((flow) => {
      const audit = createDefer(flow, { id: "audit", retries: 0, schema: z.object({ what: z.string() }) as any }, async ({ event, parents, step }: any) => {
        await step.run("save", () => ran.push([event.data, parents[0].fnSlug, parents[0].runId.slice(0, 2)]));
      });
      return [audit, flow.createFunction({ id: "pay", triggers: { event: "pay" }, retries: 0, ...options }, (ctx: any) => handler(ctx, audit))];
    });
    return { c, ran };
  };
  const go = async (c: Cluster) => {
    await c.flow.send({ name: "pay" });
    for (let i = 0; i < 4; i++) {
      await c.drain();
      await Promise.all([...c.runs.values()].map((r) => r.result));
    }
  };

  it("starts once the run has ended, once, with its data and its run", async () => {
    let during = -1;
    const { c, ran } = make(async ({ defer, step }: any, audit: any) => {
      defer("a", { function: audit, data: { what: "one" } });
      defer("a", { function: audit, data: { what: "two" } });
      const dropped = defer("b", { function: audit, data: { what: "dropped" } });
      await step.sleep("pause", "1h");
      await step.run("w", () => void (during = ran.length));
      dropped.abort();
    });
    await go(c);
    expect(during).toBe(0);
    expect(ran.sort()).toEqual([[{ what: "one" }, "pay", "pa"], [{ what: "two" }, "pay", "pa"]]);
  });

  it("starts after a run that failed, and after one that was tried again, once", async () => {
    const failed = make(async ({ defer }: any, audit: any) => {
      defer("a", { function: audit, data: { what: "x" } });
      throw new Error("no");
    });
    await go(failed.c);
    expect(failed.ran).toEqual([[{ what: "x" }, "pay", "pa"]]);
    let attempts = 0;
    const again = make(
      async ({ defer }: any, audit: any) => {
        defer("a", { function: audit, data: { what: `attempt ${attempts}` } });
        if (attempts++ === 0) defer("only-first", { function: audit, data: { what: "x" } });
        if (attempts === 1) throw new Error("again");
      },
      { retries: 1 },
    );
    await go(again.c);
    expect(again.ran.map((r) => r[0].what)).toEqual(["attempt 1"]);
  });

  it("refuses data that isn't its schema's, and a function that isn't a deferred one", async () => {
    for (const body of [
      ({ defer }: any, audit: any) => defer("a", { function: audit, data: { what: 1 } }),
      ({ defer }: any) => defer("a", { function: { id: "pay" }, data: {} }),
    ]) {
      const { c, ran } = make(async (ctx: any, audit: any) => void body(ctx, audit));
      await go(c);
      expect((await c.runOf("pay").result).getStatus()).toBe(ExecutionStatus.FAILED);
      expect(ran).toHaveLength(0);
    }
    expect(() => createDefer(new sdk.EventFlow({ id: "x" }), { id: "a", onFailure: () => {} } as any, async () => {})).toThrow("onFailure");
  });
});

describe("realtime", () => {
  it("publishes to its topic, once, with a message its schema takes", async () => {
    const sent: { url: string; body: any; auth: string | null }[] = [];
    const real = globalThis.fetch;
    process.env.AWS_ACCESS_KEY_ID = "AKIDEXAMPLE";
    process.env.AWS_SECRET_ACCESS_KEY = "secret";
    process.env.AWS_REGION ??= "ca-central-1";
    (globalThis as any).fetch = async (input: any, init?: any) => {
      const req = input instanceof Request ? input : new Request(input, init);
      if (!req.url.startsWith("https://abc-ats.iot")) return real(input, init);
      sent.push({ url: req.url, body: await req.json(), auth: req.headers.get("authorization") });
      return new Response("{}");
    };
    try {
      const order = channel({ name: (id: string) => `order:${id}`, topics: { status: { schema: z.object({ state: z.string() }) } } });
      const c = new Cluster(
        (flow) => [
          flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 0 }, async ({ step, event }: any) => {
            await step.realtime.publish("tell", order("a/#+1").status, { state: event.data.state });
            await step.sleep("pause", "1h");
            return "ok";
          }),
        ],
        { realtime: { resource: { endpoint: "abc-ats.iot.ca-central-1.amazonaws.com" }, prefix: "app/dev" } },
      );
      await c.flow.send({ name: "go", data: { state: "paid" } });
      await c.drain();
      expect((await c.runOf("f").result).getResult()).toBe("ok");
      expect(sent).toHaveLength(1);
      // Nothing of the name is special in a topic.
      expect(decodeURIComponent(new URL(sent[0].url).pathname)).toBe("/topics/app/dev/order%3Aa%2F%23%2B1/status");
      expect(c.flow.realtime.topic(order("a/#+1").status)).toBe("app/dev/order%3Aa%2F%23%2B1/status");
      expect(sent[0].body).toMatchObject({ channel: "order:a/#+1", topic: "status", data: { state: "paid" }, kind: "data", fnId: "f" });
      expect(sent[0].auth).toContain("/iotdata/aws4_request");
      await c.flow.realtime.publish(order("2").status, { state: "now" });
      expect(sent).toHaveLength(2);
      await c.flow.send({ name: "go", id: "bad", data: { state: 1 } });
      await c.drain();
      const bad = [...c.runs.values()].find((r) => r.payload.event.id === "bad")!;
      expect((await bad.result).getError().errorMessage).toContain("schema");
      expect(sent).toHaveLength(2);
    } finally {
      globalThis.fetch = real;
    }
  });
});

describe("another EventFlow's functions", () => {
  // Two in one cluster's queue would be one: the call is taken to the other by hand.
  const make = (external: boolean, result: any = (n: number) => n * 2) => {
    const billing = new Cluster((flow) => [flow.createFunction({ id: "charge", external, retries: 0 }, async ({ event }: any) => result(event.data.n)), flow.createFunction({ id: "secret" }, async () => "no")], { id: "billing" });
    const shop = new Cluster((flow) => [flow.createFunction({ id: "buy", triggers: { event: "buy" }, retries: 0 }, async ({ step }: any) => step.invoke("c", { function: referenceFunction({ appId: "billing", functionId: "charge" }), data: { n: 21 }, timeout: "2h" }))], {
      id: "shop",
      apps: { billing: { queueUrl: "https://sqs/billing", routerName: "r2" } },
    });
    return { billing, shop };
  };
  const carry = async (shop: Cluster, billing: Cluster) => {
    await shop.flow.send({ name: "buy", tenant: "t1" });
    Cluster.use(shop);
    await shop.drain();
    const call = await waitFor(() => shop.queue.find((m) => JSON.parse(m.body).kind === "call"));
    shop.queue = [];
    expect(JSON.parse(call.body).sig).toBeUndefined();
    Cluster.use(billing);
    return call;
  };

  it("are invoked, and answer", async () => {
    const { billing, shop } = make(true);
    const call = await carry(shop, billing);
    const answers: any[] = [];
    const real = billing.fetch.bind(billing);
    billing.fetch = async (s, p, i) => (p.endsWith("/succeed") ? (answers.push([decodeURIComponent(p.split("/")[3]), JSON.parse(String(i.body))]), new Response(null, { status: 200 })) : real(s, p, i));
    await billing.deliver([call]);
    await billing.deliver([call]);
    const run = billing.runOf("charge");
    await run.result;
    expect(run.payload.event).toMatchObject({ name: "inngest/function.invoked", tenant: "t1", data: { n: 21 } });
    expect(answers).toEqual([[JSON.parse(call.body).parent, { ok: true, result: 42 }]]);
  });

  it("aren't, unless they say so, and it isn't said which there are", async () => {
    const { billing, shop } = make(false);
    const call = await carry(shop, billing);
    const answers: any[] = [];
    const real = billing.fetch.bind(billing);
    billing.fetch = async (s, p, i) => (p.endsWith("/succeed") ? (answers.push(JSON.parse(String(i.body))), new Response(null, { status: 200 })) : real(s, p, i));
    for (const fn of ["charge", "secret", "nothing"]) await billing.deliver([{ ...call, body: JSON.stringify({ ...JSON.parse(call.body), fn }) }]);
    expect(billing.runs.size).toBe(0);
    expect(new Set(answers.map((a) => a.error.message.replace(/"[^"]*"/, "X"))).size).toBe(1);
    // Nor for another tenant than the message's.
    await billing.deliver([{ ...call, group: "t2" }]);
    expect(answers).toHaveLength(3);
  });

  it("need the app to be given", async () => {
    const c = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "go" } }, async ({ step }: any) => step.invoke("c", { function: referenceFunction({ appId: "nope", functionId: "x" }) }))]);
    await c.flow.send({ name: "go" });
    await c.drain();
    expect((await c.runOf("f").result).getError().errorMessage).toContain('"nope"');
  });
});

describe("large events", () => {
  const big = (n: number) => "é".repeat(n / 2);
  it("reach runs, waits, batches, debounces and invokes whole", async () => {
    const seen: Record<string, number> = {};
    const note = (name: string, v: any) => void (seen[name] = v?.length ?? -1);
    const c = new Cluster((flow) => {
      const child = flow.createFunction({ id: "child" }, async ({ event }: any) => (note("child", event.data.text), { text: event.data.text }));
      return [
        child,
        flow.createFunction({ id: "run", triggers: { event: "big", if: "size(event.data.text) > 10" }, retries: 0 }, async ({ event, step }: any) => {
          note("run", event.data.text);
          const r: any = await step.invoke("i", { function: child, data: { text: event.data.text } });
          note("invoke", r.text);
        }),
        flow.createFunction({ id: "wait", triggers: { event: "wait" }, retries: 0 }, async ({ step }: any) => {
          const e = await step.waitForEvent("w", { event: "big", timeout: "1h", if: "async.data.k == event.data.k" });
          note("wait", e?.data.text);
        }),
        flow.createFunction({ id: "batch", triggers: { event: "big" }, batchEvents: { maxSize: 2, timeout: "1m" }, retries: 0 }, async ({ events }: any) => note("batch", events[1].data.text)),
        flow.createFunction({ id: "debounce", triggers: { event: "big" }, debounce: { period: "5s", key: "event.data.k" }, retries: 0 }, async ({ event }: any) => note("debounce", event.data.text)),
        flow.createFunction({ id: "held", triggers: { event: "big" }, throttle: { limit: 1, period: "1s", burst: 0 } as any, retries: 0 }, async ({ event }: any) => note("held", event.data.text)),
      ];
    });
    await c.flow.send({ name: "wait", data: { k: "a" } });
    await c.drain();
    await settle(150);
    await c.flow.send([1, 2].map((i) => ({ name: "big", id: `b${i}`, data: { k: "a", text: big(480_000) } })));
    await c.pump(1200);
    await c.fire("debounce");
    await c.pump(600);
    expect(seen).toEqual({ run: 240_000, child: 240_000, invoke: 240_000, wait: 240_000, batch: 240_000, debounce: 240_000, held: 240_000 });
    // What's passed on is small, and the event isn't in it.
    for (const r of c.runs.values()) expect(JSON.stringify(r.payload).length).toBeLessThan(10_000);
    for (const s of c.succeeded) expect(JSON.stringify(s.body).length).toBeLessThan(10_000);
    const run = runsOf(c, "run")[0];
    c.history.set(run.runId, { status: "SUCCEEDED", payload: run.payload, events: [] });
    expect(((await c.flow.runs.get(run.runId))!.event.data as any).text).toHaveLength(240_000);
  });

  it("are refused over 1 MB, and a sender can't refer to another's", async () => {
    const c = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "big" }, retries: 0 }, async ({ event }: any) => event)]);
    await expect(c.flow.send({ name: "big", data: { text: "x".repeat(1_100_000) } })).rejects.toThrow("bytes");
    await c.flow.send({ name: "big", id: "victim", tenant: "a", data: { secret: "s".repeat(200_000) } });
    await c.drain();
    const { blob } = c.runOf("f").payload.event;
    expect(blob.key).toBeString();
    c.queue.push({ messageId: "x", group: "evil", body: JSON.stringify({ kind: "event", event: { id: "e", name: "big", data: { mine: 1 }, ts: 1, tenant: "evil", blob, from: { fn: "x", runId: "y" } } }), receiveCount: 0, sentAt: Date.now() });
    await c.drain();
    const forged = [...c.runs.values()].find((r) => r.payload.event.tenant === "evil")!;
    expect(forged.payload.event.blob).toBeUndefined();
    expect(forged.payload.event.from).toBeUndefined();
    expect((await forged.result).getResult().data).toEqual({ mine: 1 });
  });
});

describe("middleware's functions", () => {
  it("are served", async () => {
    class Cleaner extends Middleware.BaseMiddleware {
      readonly id = "cleaner";
      static functions({ client }: any) {
        return [client.createFunction({ id: "clean", triggers: { event: "clean" } }, async () => "cleaned")];
      }
    }
    const c = new Cluster(() => [], { middleware: [Cleaner] });
    await c.flow.send({ name: "clean" });
    await c.drain();
    expect((await c.runOf("clean").result).getResult()).toBe("cleaned");
  });
});
