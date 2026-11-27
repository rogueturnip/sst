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

// Found by the twenty-third review: what onFailure defers isn't saved, and its context
// isn't run again once it has ended.
describe("a defer() in onFailure", () => {
  it("starts, though the first try to start it fails", async () => {
    const ran: any[] = [];
    const c = new Cluster((flow: any) => {
      const audit = createDefer(flow, { id: "audit", retries: 0 }, async ({ event }: any) => void ran.push(event.data));
      return [
        audit,
        flow.createFunction(
          { id: "pay", triggers: { event: "pay" }, retries: 0, onFailure: async ({ defer }: any) => void defer("a", { function: audit, data: { what: "refund" } }) },
          async () => {
            throw new Error("declined");
          },
        ),
      ];
    });
    // SQS fails once as the worker starts what it deferred.
    let failed = false;
    const fetch = c.fetch.bind(c);
    c.fetch = async (service: string, path: string, init: RequestInit) => {
      if (!failed && service === "sqs" && String(init.body).includes("deferred.schedule")) {
        failed = true;
        return new Response(JSON.stringify({ __type: "InternalError", message: "down" }), { status: 500 });
      }
      return fetch(service, path, init);
    };
    await c.flow.send({ name: "pay" });
    for (let i = 0; i < 6; i++) {
      await c.drain();
      await Promise.all([...c.runs.values()].map((r) => r.result));
    }
    expect(failed).toBe(true);
    expect(ran).toEqual([{ what: "refund" }]);
  }, 60_000);
});
