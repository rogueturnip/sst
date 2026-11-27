import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns, settle, waitFor } from "./harness.ts";

beforeAll(async () => {
  await LocalDurableTestRunner.setupTestEnvironment({ skipTime: false });
});
afterAll(async () => {
  await finishRuns();
  await LocalDurableTestRunner.teardownTestEnvironment();
});

// Found by the twenty-second review: an invoked run's onFailure, which Inngest runs as a
// function of its own, holds back what the run that invoked it is told.
describe("an invoked run that fails, and has an onFailure", () => {
  it("tells the run that invoked it how it failed, without waiting for its onFailure", async () => {
    const log: string[] = [];
    const c = new Cluster((flow: any) => {
      const child = flow.createFunction(
        {
          id: "child",
          retries: 0,
          onFailure: async ({ step }: any) => {
            await step.run("refund", async () => {
              await settle(4000);
              log.push("refunded");
            });
          },
        },
        async () => {
          throw new Error("nope");
        },
      );
      const parent = flow.createFunction({ id: "parent", triggers: { event: "go" } }, async ({ step }: any) => {
        try {
          await step.invoke("call", { function: child, timeout: "2s" });
          return "no error";
        } catch (error: any) {
          return `${log.length ? "after" : "before"} onFailure: ${error.message}`;
        }
      });
      return [child, parent];
    });
    await c.flow.send({ name: "go" });
    await c.pump(500);
    const result = await c.runOf("parent").result;
    // The child failed at once. The parent is told it timed out, as its onFailure still ran.
    expect(result.getResult()).toBe("before onFailure: nope");
  }, 30_000);
});
