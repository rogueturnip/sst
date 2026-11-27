import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalDurableTestRunner } from "@aws/durable-execution-sdk-js-testing";
import { Cluster, finishRuns, settle } from "./harness.ts";

beforeAll(async () => {
  await LocalDurableTestRunner.setupTestEnvironment({ skipTime: true });
});
afterAll(async () => {
  await finishRuns();
  await LocalDurableTestRunner.teardownTestEnvironment();
});

/** Run `fn` with the clock `ms` ahead. */
async function later<T>(ms: number, fn: () => Promise<T>) {
  const now = Date.now;
  Date.now = () => now() + ms;
  try {
    return await fn();
  } finally {
    Date.now = now;
  }
}

const DAY = 86_400_000;

function reminders(reminded: string[]) {
  return (flow: any) => [
    flow.createFunction(
      { id: "reminder", triggers: { event: "reminder/scheduled" }, cancelOn: [{ event: "reminder/cancelled", match: "data.id" }] },
      async ({ event, step }: any) => step.run("remind", () => void reminded.push(event.data.id)),
    ),
  ];
}

// Found by the twenty-eighth review. At due time the router reads the "cancelled" mark once,
// then starts the run, and the run listens for its cancelOn only for events received after it
// registers. A cancel in between is heard by the waiter of the event still to come, which lives
// until ts + 2 minutes, and claims the mark, but nothing reads it again: the run goes on.
describe("an event of a time to come, cancelled as its run starts", () => {
  it("is cancelled by a matching event between the router's check and the run listening", async () => {
    const reminded: string[] = [];
    const c = new Cluster(reminders(reminded));
    await c.flow.send({ name: "reminder/scheduled", data: { id: "r1" }, ts: Date.now() + DAY });
    await c.drain();
    // The cancel comes as the run is being started: after the check, before it listens.
    const fetch = c.fetch.bind(c);
    let sent = false;
    c.fetch = async (service: string, path: string, init: RequestInit) => {
      if (!sent && service === "lambda" && path.startsWith("/2015-03-31/functions/worker/invocations")) {
        sent = true;
        await c.flow.send({ name: "reminder/cancelled", data: { id: "r1" } });
        const m = c.queue.pop()!;
        await c.deliver([m]);
        // A moment passes before the run listens, as a start and a cold start take.
        await settle(50);
      }
      return fetch(service, path, init);
    };
    // The clock stays on while the run starts and listens.
    await later(DAY + 2000, async () => {
      await c.drain();
      for (const run of c.runs.values()) await (run as any).result;
    });
    expect(sent).toBe(true);
    expect(reminded).toEqual([]);
  }, 60_000);
});

// The waiter of an event still to come is registered when the router gets to it, and listen()
// takes only events received from then. A cancel that SQS took after the event, but that the
// router got to first (another message group, or a backlog), is missed.
describe("an event of a time to come, cancelled just after it's sent", () => {
  it("is cancelled by a matching event sent after it, that's routed before it", async () => {
    const reminded: string[] = [];
    const c = new Cluster(reminders(reminded));
    await c.flow.send({ name: "reminder/scheduled", data: { id: "r2" }, ts: Date.now() + DAY });
    await later(1000, () => c.flow.send({ name: "reminder/cancelled", data: { id: "r2" } }));
    const [scheduled, cancelled] = c.queue.splice(0, 2);
    await later(2000, async () => {
      await c.deliver([cancelled]);
      await c.deliver([scheduled]);
    });
    await later(DAY + 2000, async () => {
      await c.drain();
      for (const run of c.runs.values()) await (run as any).result;
    });
    expect(reminded).toEqual([]);
  }, 60_000);
});

// The "cancelled" mark is keyed by the function and the event's ID, and kept 367 days, where an
// event ID is used once in 24 hours. An ID used again a few days later, as a reminder scheduled
// again under the same ID, finds the old mark, and its run is skipped.
describe("an event ID used again for an event of a time to come", () => {
  it("starts its run when an earlier event with that ID was cancelled days ago", async () => {
    const reminded: string[] = [];
    const c = new Cluster(reminders(reminded));
    await c.flow.send({ id: "reminder-7", name: "reminder/scheduled", data: { id: "r7" }, ts: Date.now() + DAY });
    await c.drain();
    await later(3_600_000, async () => {
      await c.flow.send({ name: "reminder/cancelled", data: { id: "r7" } });
      await c.drain();
    });
    await later(DAY + 2000, () => c.drain());
    expect(reminded).toEqual([]);
    // Three days on, it's scheduled again, with the same ID, for a day later.
    await later(3 * DAY, async () => {
      await c.flow.send({ id: "reminder-7", name: "reminder/scheduled", data: { id: "r7" }, ts: Date.now() + DAY });
      await c.drain();
    });
    await later(4 * DAY + 2000, async () => {
      await c.drain();
      for (const run of c.runs.values()) await (run as any).result;
    });
    expect(reminded).toEqual(["r7"]);
  }, 60_000);
});

// Not only an event of a time to come: a run's ID is made from its function and its event's ID,
// and Lambda keeps a durable execution's name while it keeps the execution. An ID used again
// after the 24 hours the docs give, with other data, gets the old run's name: the start is
// refused as already started, and no run is made.
describe("an event ID used again after 24 hours", () => {
  // Unlike Inngest, which starts a second run: AWS keeps the first run's name. It's documented,
  // and it says so in the logs.
  it("starts no second run while the first is kept, and says so", async () => {
    const seen: string[] = [];
    const warned: string[] = [];
    const warn = console.warn;
    console.warn = (line: any) => { warned.push(String(line)); };
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "welcome", triggers: { event: "user/created" } }, async ({ event, step }: any) =>
        step.run("greet", () => void seen.push(event.data.n)),
      ),
    ]);
    await c.flow.send({ id: "user-1", name: "user/created", data: { n: "first" } });
    await c.drain();
    for (const run of c.runs.values()) await (run as any).result;
    await later(3 * DAY, async () => {
      await c.flow.send({ id: "user-1", name: "user/created", data: { n: "second" } });
      await c.drain();
      for (const run of c.runs.values()) await (run as any).result;
    });
    console.warn = warn;
    expect(seen).toEqual(["first"]);
    expect(warned.some((l) => l.includes("used again after 24 hours"))).toBe(true);
  }, 60_000);
});
