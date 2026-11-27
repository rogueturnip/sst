import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns, sdk } from "./harness.ts";

const { createDefer, NonRetriableError } = sdk as any;

beforeAll(async () => {
  await LocalDurableTestRunner.setupTestEnvironment({ skipTime: true });
});
afterAll(async () => {
  await finishRuns();
  await LocalDurableTestRunner.teardownTestEnvironment();
});

async function settle(c: Cluster, rounds = 6) {
  for (let i = 0; i < rounds; i++) {
    await c.drain();
    for (const run of c.runs.values()) await (run as any).result?.catch?.(() => {});
  }
}

// Found by the thirty-second review. defer()'s refusal calls defers.clear() on the run's one
// `defers` map, which onFailure shares with the function. A refusal in onFailure drops what the
// function itself deferred before it failed, though that was decided, and saved in sst:outcome:
// the function's own audit never starts. (In a replay after the onFailure context is saved, as
// when the worker stops before sst:defer, sst:outcome brings it back and it does start.)
describe("an onFailure whose defer() is refused", () => {
  it("still starts what the function deferred", async () => {
    const started: string[] = [];
    const c = new Cluster((flow: any) => {
      const audit = createDefer(flow, { id: "audit", retries: 0 }, async ({ event }: any) => void started.push(event.data.what));
      return [
        audit,
        flow.createFunction(
          {
            id: "pay",
            triggers: { event: "go" },
            retries: 0,
            onFailure: async ({ defer }: any) => {
              for (const what of ["n1", "n2"]) defer("notify", { function: audit, data: { what } });
            },
          },
          async ({ defer }: any) => {
            defer("audit", { function: audit, data: { what: "audit" } });
            throw new NonRetriableError("boom");
          },
        ),
      ];
    });
    await c.flow.send({ name: "go" });
    await settle(c, 8);
    expect(started).toEqual(["audit"]);
  }, 60_000);
});
