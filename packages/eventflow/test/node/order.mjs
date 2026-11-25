// What depends on the runtime's promises and async contexts, on Node, as the worker runs.
// `npm run test:node` builds it first.
import assert from "node:assert/strict";
import { setTimeout as settle } from "node:timers/promises";
process.env.SST_EVENTFLOW_KEY = "test-signing-key";
const { LocalDurableTestRunner } = await import("@aws/durable-execution-sdk-js-testing");
const { createWorker } = await import("../../dist/worker.js");
const { sign } = await import("../../dist/sign.js");

const unhandled = [];
process.on("unhandledRejection", (e) => unhandled.push(e));
const run = async (handler, options = {}) => {
  const def = { id: "f", name: "f", retries: 2, concurrency: [], cancelOn: [], middleware: [], triggers: [], ...options };
  const failures = [];
  const store = { failures: async () => failures, putFailure: async (_run, f) => void failures.push(JSON.parse(JSON.stringify(f))) };
  const engine = { resource: {}, store, apps: {}, propagateSessions: false, systemEvents: false };
  const worker = createWorker(new Map([["f", { def, handler }]]), () => engine, {});
  const payload = sign({ "sst:eventflow": 1, fn: "f", runId: "r1", root: "r1", event: { id: "e", name: "go", data: {}, ts: 1 } });
  const result = await new LocalDurableTestRunner({ handlerFunction: worker }).run({ payload });
  return result.getStatus() === "SUCCEEDED" ? result.getResult() : Promise.reject(result.getError());
};
const charges = (n, fn) => async () => {
  const log = [];
  await run(async (args) => {
    await fn(args.step, (i) => args.step.run("charge", async () => (await settle(Math.random() * 15), log.push(i), i)));
    await args.step.sleep("pause", "1s");
  });
  assert.deepEqual(log.sort(), Array.from({ length: n }, (_, i) => i));
};
const limiter = (n) => {
  let active = 0;
  const queue = [];
  const next = () => (active--, queue.shift()?.());
  const go = async (fn, resolve) => {
    active++;
    const r = (async () => fn())();
    resolve(r);
    try { await r; } catch {}
    next();
  };
  return (fn) => new Promise((resolve) => {
    queue.push(() => go(fn, resolve));
    (async () => { await Promise.resolve(); if (active < n && queue.length) queue.shift()(); })();
  });
};
const cases = {
  "a helper in a loop": charges(3, async (step, charge) => { const item = async (i) => { await step.run("a", () => i); await settle(2); await charge(i); }; for (let i = 0; i < 3; i++) await item(i); }),
  "an async generator": charges(3, async (step, charge) => { async function* g() { for (let i = 0; i < 3; i++) { await step.run("a", () => i); yield await step.run("b", () => i); } } for await (const i of g()) await charge(i); }),
  "timers one after another": charges(3, async (step, charge) => { for (let i = 0; i < 3; i++) await new Promise((ok, no) => setTimeout(() => charge(i).then(ok, no), 2)); }),
  "a limiter": charges(4, async (step, charge) => { const l = limiter(2); await Promise.all([0, 1, 2, 3].map(async (i) => { await step.run(`s${i}`, () => settle(Math.random() * 15)); await l(() => charge(i)); })); }),
  "a pool": charges(5, async (step, charge) => { const q = [0, 1, 2, 3, 4]; const w = async () => { while (q.length) await charge(q.shift()); }; await Promise.all([w(), w()]); }),
  "results as they come": charges(3, async (step, charge) => { const p = new Map([0, 1, 2].map((i) => [i, step.run(`s${i}`, () => settle(30 - i * 10)).then(() => i)])); while (p.size) { const i = await Promise.race(p.values()); p.delete(i); assert.equal(await charge(i), i); await step.sleep("gap", "1s"); } }),
  "a race that's replayed": async () => {
    const did = [];
    await run(async ({ step }) => {
      const a = step.run("a", () => settle(150)).then(() => "a");
      const b = step.run("b", () => settle(5)).then(() => "b");
      const first = await Promise.race([a, b]);
      await step.run(`did-${first}`, () => void did.push(first));
      await a;
      await step.sleep("s", "1s");
    });
    assert.deepEqual(did, ["b"]);
  },
  "a step that fails before it's awaited": async () => {
    const r = await run(async ({ step }) => { const p = step.run("a", () => { throw new Error("declined"); }); await step.run("b", () => settle(50)); try { await p; } catch (e) { return e.message; } }, { retries: 0 });
    assert.equal(r, "declined");
  },
};
await LocalDurableTestRunner.setupTestEnvironment({ skipTime: false });
let failed = 0;
for (const [name, fn] of Object.entries(cases)) {
  try { await fn(); console.log(`ok    ${name}`); }
  catch (e) { failed++; console.log(`FAIL  ${name}`, e); }
}
if (unhandled.length) { failed++; console.log("FAIL  unhandled rejections", unhandled); }
console.log(`node ${process.version}: ${Object.keys(cases).length - failed} pass, ${failed} fail`);
await LocalDurableTestRunner.teardownTestEnvironment();
process.exit(failed ? 1 : 0);
