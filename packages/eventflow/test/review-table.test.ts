import { describe, expect, it, setDefaultTimeout } from "bun:test";
import { DynamoStore, type Limit } from "../src/store.ts";
import { AwsError } from "../src/aws.ts";

// On DynamoDB Local, as the contract: `npm run test:table`. Each was a defect a review
// reproduced, where an answer is lost or two versions share a limit.

setDefaultTimeout(60_000);
const table = process.env.EVENTFLOW_TEST_TABLE;
const endpoint = process.env.EVENTFLOW_TEST_ENDPOINT;
async function post(action: string, body: any) {
  const r = await fetch(endpoint!, { method: "POST", headers: { "Content-Type": "application/x-amz-json-1.0", "X-Amz-Target": "DynamoDB_20120810." + action, Authorization: "AWS4-HMAC-SHA256 Credential=local/20260101/local/dynamodb/aws4_request, SignedHeaders=host, Signature=0" }, body: JSON.stringify(body) });
  return { status: r.status, data: (await r.json()) as any };
}
// `lose(action, body)` true: the write is made, its answer is a 500, and the client sends it
// again, as aws4fetch does, unless it's told not to.
function make(lose?: (action: string, body: any) => boolean) {
  const s = new DynamoStore(table!);
  (s as any).db = async (action: string, body: any, options?: any) => {
    const b = { TableName: table, ...body };
    if (lose?.(action, body)) {
      await post(action, b);
      if (options?.retries === 0) throw new AwsError("dynamodb", action, 500, "InternalServerError", "");
    }
    const { status, data } = await post(action, b);
    if (status !== 200) throw new AwsError("dynamodb", action, status, String(data.__type).split("#").pop()!, data.message ?? "", [], data);
    return data;
  };
  return s;
}
const run = Date.now().toString(36);

describe.skipIf(!table || !endpoint)("review", () => {
it("a limit that's raised, or lowered, across a shard isn't gone over", async () => {
  let over = false;
  for (const [from, to] of [[25, 26], [26, 25], [100, 20]]) {
    const store = make(); const item = "C#r2-" + run + "-" + from + "-" + to;
    const old: Limit = { item, limit: from }, now: Limit = { item, limit: to };
    let a = 0, b = 0;
    for (let i = 0; i < from + 5; i++) if ((await store.acquire([old], "old" + i, "t", 600_000)) === "granted") a++;
    for (let i = 0; i < to + 5; i++) if ((await store.acquire([now], "new" + i, "t", 600_000)) === "granted") b++;
    over ||= a + b > Math.max(from, to);
  }
  expect(over).toBe(false);
});

it("a rate limit whose answer is lost doesn't lose the event it let through", async () => {
  let first = true;
  const s2 = make((a, b) => { if (a === "UpdateItem" && /SET tat/.test(b.UpdateExpression) && first) { first = false; return true; } return false; });
  const wait = await s2.take("T#r2-" + run, 3_600_000, 0);
  expect(wait).toBe(0);
});

it("a singleton's move whose answer is lost says it moved", async () => {
  let first = true;
  const s = make((a, b) => { if (a === "UpdateItem" && /#run = :run/.test(b.UpdateExpression) && first) { first = false; return true; } return false; });
  const item = "S#r2-" + run;
  await s.lockTake(item, { run: "old", until: Date.now() + 1e6 }, 1e7);
  const moved = await s.lockMove(item, "old", { run: "new", until: Date.now() + 1e6 }, 1e7);
  expect(moved).toBe(true);
});

it("an event redelivered with a new time, while its batch is open, is in it once (DynamoStore)", async () => {
  const s = make(); const item = "B#r2-" + run; const ev: any = { id: "e", name: "p", data: {}, ts: 1 };
  const at = Date.now();
  await s.batchAdd(item, "BE#r2-" + run, String(at).padStart(15, "0") + "#h", ev, at, 1e8);
  await s.batchAdd(item, "BE#r2-" + run, String(at + 9).padStart(15, "0") + "#h", ev, at + 9, 1e8);
  const closed = await s.batchClose(item, 11, (p) => p, 1e8);
  expect(closed!.refs.length).toBe(1);
});

it("batches under concurrent routers, lost answers and crashes: each event once", async () => {
  const item = "B#s1-" + run; const N = 120, max = 7; const starts = new Map<number, string[]>();
  const choose = (p: any[]) => (p.length >= max ? p.slice(0, max) : []);
  let dice = 0;
  const route = async (i: number) => {
    const s = make(() => Math.random() < 0.05);
    const real = (s as any).db; (s as any).db = async (...a: any[]) => { if (Math.random() < 0.04) throw new Error("crash"); const r = await real(...a); if (Math.random() < 0.02) throw new Error("crash after"); return r; };
    for (let attempt = 0; attempt < 60; attempt++) {
      try {
        await s.batchAdd(item, "BE#s1-" + run + "-" + i, String(1e12 + i).padStart(15, "0") + "#" + i, { id: "e" + i, name: "p", data: {}, ts: 1 } as any, 1e12 + i, 1e9);
        for (let k = 0; k < 50; k++) {
          const c = await s.batchClose(item, max + 1, choose, 1e9);
          if (!c) return;
          const prev = starts.get(c.n); if (prev && JSON.stringify(prev) !== JSON.stringify(c.refs)) throw new Error("DIFFERENT " + c.n);
          starts.set(c.n, c.refs);
          await s.batchDone(item, c.n);
        }
        return;
      } catch (e) { if (String(e).includes("DIFFERENT")) throw e; dice++; await new Promise((r) => setTimeout(r, Math.random() * 30)); }
    }
  };
  await Promise.all(Array.from({ length: N }, (_, i) => route(i)));
  const s = make(); for (;;) { const c = await s.batchClose(item, max + 1, (p) => p.slice(0, max), 1e9); if (!c) break; starts.set(c.n, c.refs); await s.batchDone(item, c.n); }
  const all = [...starts.values()].flat();
  expect(all.length).toBe(N); expect(new Set(all).size).toBe(N);
});
});
