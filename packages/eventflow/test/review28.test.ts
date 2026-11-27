import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns } from "./harness.ts";

beforeAll(async () => {
  await LocalDurableTestRunner.setupTestEnvironment({ skipTime: true });
});
afterAll(async () => {
  await finishRuns();
  await LocalDurableTestRunner.teardownTestEnvironment();
});

// Found by the thirty-fifth review. The `about` mark (worker.ts:80, :1106) is sticky: every run
// downstream of a run started by inngest/function.finished, however far, sends no events about
// runs. A function told when an import finishes sends "report.requested", whose run sends
// "email.requested", whose run fails: the alert on inngest/function.failed never runs, for this
// or anything that pipeline ever starts. Nothing about the email's run is about runs.
describe("a run two sends away from a run about runs that fails", () => {
  it("still sends inngest/function.failed", async () => {
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "import", triggers: { event: "import" } }, async () => "ok"),
      flow.createFunction({ id: "after", triggers: { event: "inngest/function.finished", if: "event.data.function_id == 'test-import'" } }, async ({ step }: any) => {
        await step.sendEvent("report", { name: "report.requested", data: {} });
      }),
      flow.createFunction({ id: "report", triggers: { event: "report.requested" } }, async ({ step }: any) => {
        await step.sendEvent("email", { name: "email.requested", data: {} });
      }),
      flow.createFunction({ id: "email", triggers: { event: "email.requested" }, retries: 0 }, async () => {
        throw new Error("smtp down");
      }),
      flow.createFunction({ id: "alert", triggers: { event: "inngest/function.failed" } }, async ({ event }: any) => event.data.function_id),
    ]);
    await c.flow.send({ name: "import", data: {} });
    await c.pump(6000);
    expect([...c.runs.values()].filter((r) => r.payload.fn === "email").length).toBe(1);
    expect([...c.runs.values()].filter((r) => r.payload.fn === "alert").length).toBe(1);
  }, 60_000);
});
