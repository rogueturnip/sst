// The leases with faults put in, on DynamoDB Local:
//
//   - a call that isn't made, and one that's made and whose answer is lost
//   - a wake that fails, and one that reaches the step and whose answer is lost
//   - a worker that crashes part way through any of it, and doesn't come back
//
// It checks that no limit is ever gone over by steps that hold their leases, that every
// step runs, and that nothing's held or in line at the end. Leases and callbacks are
// short here, so what AWS's timeouts heal in minutes is seen.
//
//   EVENTFLOW_TEST_ENDPOINT=http://localhost:8000 bun scripts/fault-leases.ts [cap of the shared limit] [cap of each tenant's]
import { Agent, request } from "node:http";
import { DynamoStore, type LeaseWaiter, type Limit } from "../src/store.ts";
import { AwsError } from "../src/aws.ts";

const endpoint = process.env.EVENTFLOW_TEST_ENDPOINT ?? "http://localhost:8000";
const sharedCap = Number(process.argv[2] ?? 8);
const ownCap = Number(process.argv[3] ?? 3);
const runs = Number(process.argv[4] ?? 10);
const work = Number(process.env.WORK ?? 120);
const rate = Number(process.env.FAULTS ?? 0.05);
const crashes = Number(process.env.CRASHES ?? 0.004);
const LEASE = Number(process.env.LEASE ?? 12_000);
const CALLBACK = Number(process.env.CALLBACK ?? 10_000);
const table = `fault-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;

const agent = new Agent({ keepAlive: true, maxSockets: 64 });
const post = (action: string, body: any) =>
  new Promise<any>((resolve, reject) => {
    const req = request(endpoint, {
      method: "POST",
      agent,
      headers: {
        "Content-Type": "application/x-amz-json-1.0",
        "X-Amz-Target": `DynamoDB_20120810.${action}`,
        Authorization: "AWS4-HMAC-SHA256 Credential=local/20260101/local/dynamodb/aws4_request, SignedHeaders=host, Signature=0",
      },
    }, (res) => {
      let t = "";
      res.on("data", (c) => (t += c));
      res.on("end", () => resolve({ status: res.statusCode, data: JSON.parse(t || "{}") }));
    });
    req.on("error", reject);
    req.end(JSON.stringify(body));
  });
await post("CreateTable", {
  TableName: table, BillingMode: "PAY_PER_REQUEST",
  AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }, { AttributeName: "sk", AttributeType: "S" }],
  KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }, { AttributeName: "sk", KeyType: "RANGE" }],
});

let faults = true;
let crashed = 0;
class Crash extends Error {}
/** A worker: a store of its own, which may crash for good at any call. */
function worker() {
  const store = new DynamoStore(table) as any;
  store.grantMs = LEASE;
  let dead = false;
  store.alive = () => {
    if (dead) throw new Crash();
  };
  store.db = async (action: string, body: any, options?: { retries?: number }) => {
    if (dead) return new Promise(() => {});
    if (faults && Math.random() < crashes) {
      dead = true;
      crashed++;
      // Before the call, or after it: the worker never hears.
      if (Math.random() < 0.5) await post(action, { TableName: table, ...body }).catch(() => {});
      store.onDeath?.();
      return new Promise(() => {});
    }
    await new Promise((r) => setTimeout(r, Math.random() * 8));
    const tries = options?.retries ?? 10;
    for (let attempt = 0; ; attempt++) {
      const fault = faults && Math.random() < rate;
      if (fault && Math.random() < 0.5) {
        if (attempt < tries) continue;
        throw new AwsError("dynamodb", action, 500, "InternalServerError", "");
      }
      const { status, data } = await post(action, { TableName: table, ...body });
      // Made, and the answer is lost: sent again, or thrown.
      if (fault && attempt < tries) continue;
      if (fault) throw new AwsError("dynamodb", action, 500, "InternalServerError", "");
      if (status !== 200) throw new AwsError("dynamodb", action, status, String(data.__type).split("#").pop()!, data.message ?? "", [], data);
      return data;
    }
  };
  return store;
}

const id = Math.random().toString(36).slice(2);
const shared: Limit = { item: `C#f-${id}`, limit: sharedCap };
const tenants = Array.from({ length: 6 }, (_, i) => ({ name: `t${i}`, own: { item: `C#f-${id}-${i}`, limit: ownCap } }));
/** The leases' holders that are running their code, as far as their lease lasts. */
const active = new Map<string, number>();
const most = new Map<string, number>();
let over = 0, reasked = 0, lost = 0, done = 0, restarts = 0;
const wakes = new Map<string, () => void>();
const wake = async (w: LeaseWaiter) => {
  await new Promise((r) => setTimeout(r, Math.random() * 10));
  if (faults && Math.random() < 0.02) throw new Error("wake");
  const resume = wakes.get(w.callbackId);
  if (!resume) return false;
  wakes.delete(w.callbackId);
  resume();
  // It reached the step, and the answer was lost.
  if (faults && Math.random() < 0.02) throw new Error("wake");
  return true;
};

async function attemptStep(leaseId: string, tenant: (typeof tenants)[number], ms: number, at: number, attempt: number) {
  const store = worker();
  const limits = [tenant.own, shared];
  // When it crashes, whatever it was doing hangs: Lambda starts it again.
  const death = new Promise<"died">((resolve) => (store.onDeath = () => resolve("died")));
  const work = async () => {
    for (let round = 0; ; round++) {
      if ((await store.acquire(limits, leaseId, tenant.name, LEASE).catch(() => "full")) === "granted") break;
      const callbackId = `cb-${leaseId}-${attempt}-${round}`;
      const woken = new Promise<boolean>((resolve) => {
        wakes.set(callbackId, () => resolve(true));
        setTimeout(() => wakes.delete(callbackId) && resolve(false), CALLBACK);
      });
      const w: LeaseWaiter = { leaseId, limits, tenant: tenant.name, callbackId, runId: "r", at, expiresAt: Date.now() + 300_000 };
      let mine = false;
      for (let tries = 0; tries < 8; tries++) {
        try {
          if ((await store.enqueue(w)) === "held") mine = true;
          else for (const l of limits) await store.dispatch(l, async (o: LeaseWaiter) => (o.callbackId === callbackId ? (mine = true) : wake(o)), w);
          break;
        } catch {}
      }
      if (mine || (await woken)) break;
      reasked++;
    }
    const until = Date.now() + LEASE;
    if (!(await store.renew(limits, leaseId, LEASE).catch(() => false))) {
      lost++;
      await store.handoff(limits, leaseId, tenant.name, wake).catch(() => {});
      return "again" as const;
    }
    for (const l of limits) {
      const n = (active.get(l.item) ?? 0) + 1;
      active.set(l.item, n);
      most.set(l.item, Math.max(most.get(l.item) ?? 0, n));
      if (n > l.limit) over++;
    }
    await new Promise((r) => setTimeout(r, ms));
    for (const l of limits) active.set(l.item, active.get(l.item)! - 1);
    if (Date.now() > until) throw new Error("The test is too slow for its leases");
    done++;
    await store.handoff(limits, leaseId, tenant.name, wake).catch(() => {});
    return "done" as const;
  };
  const before = done;
  const outcome = await Promise.race([work(), death]);
  // It had done its work, and crashed giving its leases back.
  if (outcome === "died") return store.didWork ? "done" : "died";
  void before;
  return outcome;
}

async function step(name: string, tenant: (typeof tenants)[number], ms: number) {
  const at = Date.now();
  (globalThis as any).busy?.set(name, at);
  for (let i = 0; ; i++) {
    // A crash after the work is a step Lambda runs again, as a step is at least once.
    const outcome = await attemptStep(`${name}.${outcomeSuffix(i)}`, tenant, ms, at, i);
    if (outcome === "done") return void (globalThis as any).busy?.delete(name);
    if (outcome === "died") restarts++;
    await new Promise((r) => setTimeout(r, 50));
  }
}
// A crashed step asks again with the same lease, as its ID comes from the run.
const outcomeSuffix = (_i: number) => "0";

const started = Date.now();
const all = Promise.all(
  tenants.flatMap((t) =>
    Array.from({ length: runs }, async (_, i) => {
      for (const [n, ms] of [2, work, work].entries()) await step(`${t.name}-${i}.${n}`, t, ms);
    }),
  ),
);
const busy = new Map<string, number>();
(globalThis as any).busy = busy;
const slow = setInterval(async () => {
  if (Date.now() - started < 30_000) return;
  const look = worker();
  const s = await look.readLimit(shared);
  console.error(`${((Date.now() - started) / 1000).toFixed(0)}s done ${done}; running ${[...active.values()].reduce((a, b) => a + b, 0)}; steps ${[...busy].slice(0, 6).map(([k, t]) => `${k} ${((Date.now() - t) / 1000).toFixed(0)}s`).join(", ")}; shared leases ${JSON.stringify(Object.fromEntries(Object.entries<any>(s.leases).slice(0, 8).map(([k, v]) => [k, [v.granted ? "g" : "o", Math.round((v.expires - Date.now()) / 1000)]])))} missing ${s.missing.length} free ${s.free}`);
}, 10_000);
const stuck = await Promise.race([all.then(() => false), new Promise((r) => setTimeout(() => r(true), 240_000))]);
faults = false;
clearInterval(slow);
// Leases of crashed workers run out, and the next to look gives them back.
await new Promise((r) => setTimeout(r, LEASE + 500));
const left: string[] = [];
const clean = worker();
for (const l of [shared, ...tenants.map((t) => t.own)]) {
  await clean.dispatch(l, async () => false);
  const s = await clean.readLimit(l);
  const live = Object.values<any>(s.leases);
  if (live.length) left.push(`${l.item.slice(-4)}: ${live.length} leases`);
}
const expected = tenants.length * runs * 3;
const ok = !stuck && over === 0 && done >= expected && !left.length;
console.log(
  `${ok ? "ok" : "FAILED"} ${((Date.now() - started) / 1000).toFixed(0)}s: ${done} of ${expected} steps ran, over a limit ${over}, most shared ${most.get(shared.item)} of ${sharedCap}, ` +
    `crashes ${crashed} (${restarts} restarts), asked again ${reasked}, lost before work ${lost}${left.length ? `, left: ${left.join(", ")}` : ""}`,
);
process.exit(ok ? 0 : 1);
