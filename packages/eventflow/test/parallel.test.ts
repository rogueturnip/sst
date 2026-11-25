import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { readFileSync } from "node:fs";
import { Cluster, finishRuns, sdk, settle } from "./harness.ts";

// Steps are found by their names, as Inngest's are, so branches that run at once can finish
// in any order, and in another when the run is replayed.

const { Middleware } = sdk;
beforeAll(async () => {
  await LocalDurableTestRunner.setupTestEnvironment({ skipTime: true });
});
afterAll(async () => {
  await finishRuns();
  await LocalDurableTestRunner.teardownTestEnvironment();
});
const run = async (options: any, handler: any, client: any = {}) => {
  const c = new Cluster((flow) => [flow.createFunction({ id: "f", triggers: { event: "go" }, retries: 0, ...options }, handler)], client);
  await c.flow.send({ name: "go" });
  await c.drain();
  return (await c.runOf("f").result) as any;
};
class Slow extends Middleware.BaseMiddleware {
  readonly id = "slow";
  async transformStepInput() {
    await settle(Math.random() * 10);
  }
}

describe("branches that run at once", () => {
  for (const [name, options] of [
    ["", {}],
    [", with a concurrency limit", { concurrency: 4 }],
    [", with middleware", { middleware: [Slow] }],
    [", with both", { concurrency: 3, middleware: [Slow] }],
  ] as [string, any][])
    it(`can have many steps each${name}`, async () => {
      const calls: string[] = [];
      const result = await run(options, async ({ step }: any) => {
        const branch = async (name: string, ms: () => number) => {
          const out: string[] = [];
          for (let i = 0; i < 4; i++) out.push(await step.run(`${name}${i}`, async () => (await settle(ms()), calls.push(`${name}${i}`), `${name}${i}`)));
          return out.join("");
        };
        const random = () => Math.random() * 20;
        const one = await Promise.all(["a", "b", "c"].map((n) => branch(n, random)));
        await step.sleep("pause", "1h");
        const two = await Promise.all(["d", "e"].map((n) => branch(n, random)));
        await step.sleep("pause-2", "1h");
        return [...one, ...two];
      });
      expect(result.getResult()).toEqual(["a", "b", "c", "d", "e"].map((n) => `${n}0${n}1${n}2${n}3`));
      expect(calls).toHaveLength(20);
      expect(new Set(calls).size).toBe(20);
    }, 30_000);

  it("can sleep and retry on their own", async () => {
    const tries: Record<string, number> = {};
    const result = await run({ retries: 3 }, async ({ step }: any) => {
      const branch = async (name: string, fails: number) => {
        await step.sleep(`${name}-wait`, name === "a" ? "2h" : "1h");
        const value = await step.run(`${name}-work`, () => {
          tries[name] = (tries[name] ?? 0) + 1;
          if (tries[name] <= fails) throw new Error("again");
          return name;
        });
        await step.sleep(`${name}-rest`, "1m");
        return step.run(`${name}-end`, () => value + tries[name]);
      };
      return Promise.all([branch("a", 2), branch("b", 0), branch("c", 1)]);
    });
    expect(result.getResult()).toEqual(["a3", "b1", "c2"]);
  }, 30_000);

  it("can be tried again by the function, with what they had", async () => {
    const calls: string[] = [];
    let attempts = 0;
    const result = await run({ retries: 2 }, async ({ step }: any) => {
      const n = attempts++;
      const branch = async (name: string, ms: number) => {
        const a = await step.run(`${name}-1`, async () => (await settle(ms), calls.push(`${name}-1`), name));
        return step.run(`${name}-2`, async () => (await settle(ms), calls.push(`${name}-2`), a + "2"));
      };
      const out = await Promise.all([branch("x", 25), branch("y", 3)]);
      if (n === 0) throw new Error("outside");
      return out;
    });
    expect(result.getResult()).toEqual(["x2", "y2"]);
    expect(calls.sort()).toEqual(["x-1", "x-2", "y-1", "y-2"]);
  });
});

describe("step names", () => {
  it("are one step's each, also those that look like an occurrence", async () => {
    const keys: string[] = [];
    const result = await run({}, async ({ step, idempotencyKey }: any) => {
      const go = (id: string, v: number) => step.run(id, () => (keys.push(idempotencyKey()), v));
      const out = [await go("a", 1), await go("a:1", 2), await go("a", 3), await go("a:1", 4), await go("a", 5)];
      await step.sleep("pause", "1h");
      return out;
    });
    expect(result.getResult()).toEqual([1, 2, 3, 4, 5]);
    expect(new Set(keys).size).toBe(5);
  });

  it("can be of two kinds of step", async () => {
    const result = await run({}, async ({ step }: any) => {
      await step.sleep("x", "1m");
      const a = await step.run("x", () => 1);
      await step.sleep("x", "1m");
      return a;
    });
    expect(result.getResult()).toBe(1);
  });
});

describe("the durable SDK", () => {
  it("is the version this is built for", () => {
    const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
    const installed = JSON.parse(readFileSync(new URL("../node_modules/@aws/durable-execution-sdk-js/package.json", import.meta.url), "utf8"));
    expect(pkg.dependencies["@aws/durable-execution-sdk-js"]).toBe(installed.version);
  });
});
