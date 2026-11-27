import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns, placed } from "./harness.ts";

beforeAll(async () => {
  await LocalDurableTestRunner.setupTestEnvironment({ skipTime: false });
});
afterAll(async () => {
  await finishRuns();
  await LocalDurableTestRunner.teardownTestEnvironment();
});

// Found by the eighteenth review.
describe("a function's runs, by its ID", () => {
  it("the runs of `send` aren't the runs of `send-welcome`", async () => {
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "send", triggers: { event: "a" }, retries: 0 }, async () => null),
      flow.createFunction({ id: "send-welcome", triggers: { event: "b" }, retries: 0 }, async () => null),
    ]);
    const send = c.flow.runs.idFor("send", "e1");
    const welcome = c.flow.runs.idFor("send-welcome", "e2");
    c.history.set(send, { status: "FAILED", payload: { fn: "send" }, events: placed([]), startedAt: 1 });
    c.history.set(welcome, { status: "FAILED", payload: { fn: "send-welcome" }, events: placed([]), startedAt: 2 });
    const listed = (await c.flow.runs.list({ function: "send" })).map((r) => r.id);
    // What `sst eventflow retry --failed --function send`, or `cancel --all`, acts on.
    expect(listed).toEqual([send]);
  });
});

describe("an invoke whose data has a Date", () => {
  it("starts the run it invokes", async () => {
    const c = new Cluster((flow: any) => {
      const child = flow.createFunction({ id: "child", retries: 0 }, async ({ event }: any) => event.data.when);
      const parent = flow.createFunction({ id: "parent", triggers: { event: "go" }, retries: 0 }, async ({ step }: any) => {
        try {
          return await step.invoke("c", { function: child, data: { when: new Date(0) }, timeout: "3s" });
        } catch (error: any) {
          return `${error.name}: ${error.message}`;
        }
      });
      return [child, parent];
    });
    await c.flow.send({ name: "go" });
    await c.pump(8000);
    const result = await c.runOf("parent").result;
    // The child's run, which returns the date as JSON has it.
    expect([...c.runs.values()].some((r) => r.payload.fn === "child")).toBe(true);
    expect(result.getResult()).toBe("1970-01-01T00:00:00.000Z");
  }, 30_000);
});
