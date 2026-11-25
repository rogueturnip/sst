// How many steps a second one concurrency limit can do, on DynamoDB Local with AWS's
// limit on a partition key put in: 1,000 write units and 3,000 read units a second, by
// the size of the item. DynamoDB Local has none, so without this every design looks fast.
// It's a model, not a measurement of AWS: it has no adaptive capacity, and it's the
// shape that counts.
//
//   EVENTFLOW_TEST_ENDPOINT=http://localhost:8000 bun scripts/bench-leases.ts [cap] [ms of work] [steps] [tenants]
import { Agent, request } from "node:http";
import { DynamoStore, type LeaseWaiter, type Limit } from "../src/store.ts";
import { AwsError, isTransient } from "../src/aws.ts";

const endpoint = process.env.EVENTFLOW_TEST_ENDPOINT ?? "http://localhost:8000";
const cap = Number(process.argv[2] ?? 100);
const work = Number(process.argv[3] ?? 1000);
const steps = Number(process.argv[4] ?? cap * 10);
const tenants = Number(process.argv[5] ?? 10);
const latency = 4;

const agent = new Agent({ keepAlive: true, maxSockets: 256 });
function post(action: string, body: any) {
  return new Promise<any>((resolve, reject) => {
    const req = request(endpoint, {
      method: "POST",
      agent,
      headers: {
        "Content-Type": "application/x-amz-json-1.0",
        "X-Amz-Target": `DynamoDB_20120810.${action}`,
        Authorization: "AWS4-HMAC-SHA256 Credential=local/20260101/local/dynamodb/aws4_request, SignedHeaders=host, Signature=0",
      },
    }, (res) => {
      let text = "";
      res.setEncoding("utf8");
      res.on("data", (c) => (text += c));
      res.on("end", () => resolve({ status: res.statusCode!, data: JSON.parse(text || "{}") }));
    });
    req.on("error", reject);
    req.end(JSON.stringify(body));
  });
}

const buckets = new Map<string, { w: number; r: number; t: number }>();
const by = new Map<string, { n: number; failed: number; units: number }>();
function kind(action: string, body: any) {
  const u = String(body.UpdateExpression ?? "");
  const sk = body.Key?.sk?.S ?? body.Item?.sk?.S ?? "";
  const what = u.startsWith("REMOVE leases.#o") ? "pass" : u.startsWith("REMOVE leases") ? "release" : u.startsWith("REMOVE tenants") ? "dropTenant"
    : u.includes("ADD active") ? "take" : u.startsWith("SET leases") ? "renew" : sk.startsWith("w#") ? "mark" : sk === "-" ? "limit" : "entry/line";
  return `${action} ${what}`;
}
let wcu = 0, rcu = 0, throttled = 0, calls = 0, failedWrites = 0;
function bucket(pk: string) {
  const now = Date.now();
  let b = buckets.get(pk);
  if (!b) buckets.set(pk, (b = { w: 1000, r: 3000, t: now }));
  b.w = Math.min(1000, b.w + (now - b.t));
  b.r = Math.min(3000, b.r + 3 * (now - b.t));
  b.t = now;
  return b;
}

const table = `bench-${Date.now()}`;
await post("CreateTable", {
  TableName: table, BillingMode: "PAY_PER_REQUEST",
  AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }, { AttributeName: "sk", AttributeType: "S" }],
  KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }, { AttributeName: "sk", KeyType: "RANGE" }],
});

const store = new DynamoStore(table);
(store as any).db = async (action: string, body: any) => {
  for (let attempt = 0; ; attempt++) {
    calls++;
    await new Promise((r) => setTimeout(r, latency * (0.5 + Math.random())));
    const pk = body.Key?.pk?.S ?? body.Item?.pk?.S ?? body.ExpressionAttributeValues?.[":pk"]?.S ?? "";
    const b = bucket(pk);
    const write = /Put|Update|Delete/.test(action);
    if (write ? b.w <= 0 : b.r <= 0) {
      throttled++;
      if (attempt >= 13) throw new AwsError("dynamodb", action, 400, "ThrottlingException", "");
      await new Promise((r) => setTimeout(r, Math.random() * Math.min(1000, 20 * 2 ** attempt) + 10));
      continue;
    }
    const { status, data } = await post(action, { TableName: table, ReturnConsumedCapacity: "TOTAL", ...body });
    const units = data.ConsumedCapacity?.CapacityUnits ?? 1;
    const k = kind(action, body), e0 = by.get(k) ?? { n: 0, failed: 0, units: 0 };
    e0.n++; e0.units += units; if (status !== 200) e0.failed++;
    by.set(k, e0);
    if (write) (b.w -= units), (wcu += units);
    else (b.r -= units), (rcu += units);
    if (status !== 200) {
      if (write) failedWrites++;
      const e = new AwsError("dynamodb", action, status, String(data.__type).split("#").pop()!, data.message ?? "", [], data);
      if (attempt < 13 && isTransient(e)) continue;
      throw e;
    }
    return data;
  }
};

const limits: Limit[] = [{ item: "C#bench", limit: cap }];
const waiting = new Map<string, () => void>();
const wake = async (w: LeaseWaiter) => {
  await new Promise((r) => setTimeout(r, 15));
  const go = waiting.get(w.callbackId);
  if (!go) return false;
  waiting.delete(w.callbackId);
  go();
  return true;
};
let running = 0, most = 0, done = 0, asked = 0;
async function step(i: number) {
  const tenant = `t${i % tenants}`, leaseId = `lease-${String(i).padStart(6, "0")}`;
  const at = Date.now() + i / 1000;
  for (let round = 0; ; round++) {
    if ((await store.acquire(limits, leaseId, tenant, 60_000, round === 0)) !== "granted") {
      if (round) asked++;
      const callbackId = `cb-${i}-${round}`;
      const woken = new Promise<boolean>((resolve) => {
        waiting.set(callbackId, () => resolve(true));
        setTimeout(() => resolve(false), 60_000);
      });
      let mine = false;
      const w0 = { leaseId, limits, tenant, callbackId, runId: `r${i}`, at, expiresAt: Date.now() + 300_000 };
      if ((await store.enqueue(w0, round === 0)) === "held") mine = true;
      else await store.dispatch(limits[0], async (w) => (w.callbackId === callbackId ? (mine = true) : wake(w)), w0);
      if (!mine && !(await woken)) continue;
    }
    if (!(await store.renew(limits, leaseId, 60_000))) continue;
    most = Math.max(most, ++running);
    await new Promise((r) => setTimeout(r, work));
    running--;
    done++;
    await store.handoff(limits, leaseId, tenant, wake);
    return;
  }
}
const start = Date.now();
const timer = setInterval(() => console.error(`${((Date.now() - start) / 1000).toFixed(0)}s done ${done} running ${running} throttled ${throttled}`), 10_000);
const errors: string[] = [];
// One that can't keep up is stopped, and says how far it got.
const giveUp = new Promise((r) => setTimeout(r, Number(process.env.BENCH_SECONDS ?? 120) * 1000));
await Promise.race([giveUp, Promise.all(Array.from({ length: steps }, (_, i) => step(i).catch((e) => errors.push(String(e.message)))))]);
clearInterval(timer);
const s = (Date.now() - start) / 1000;
const ideal = (done * work) / cap / 1000;
console.log(JSON.stringify({
  cap, workMs: work, steps, done, tenants, seconds: +s.toFixed(1), idealSeconds: +ideal.toFixed(1),
  stepsPerSecond: +(done / s).toFixed(0), usedPercent: +((ideal / s) * 100).toFixed(0), most, askedAgain: asked,
  wcuPerStep: +(wcu / done).toFixed(1), rcuPerStep: +(rcu / done).toFixed(1), throttled, errors: errors.length, error: errors[0],
}));
for (const [k, v] of [...by.entries()].sort((a, b) => b[1].units - a[1].units).slice(0, 9))
  console.error(`${k}: ${(v.n / done).toFixed(1)} a step, ${(v.failed / done).toFixed(1)} failed, ${(v.units / done).toFixed(1)} units`);
process.exit(0);
