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

/** Run `fn` with the clock stopped at `at`. */
async function frozen<T>(at: number, fn: () => Promise<T>) {
  const now = Date.now;
  Date.now = () => at;
  try {
    return await fn();
  } finally {
    Date.now = now;
  }
}

const DAY = 86_400_000;

function reminders(reminded: string[], more: any = {}) {
  return (flow: any) => [
    flow.createFunction(
      { id: "reminder", triggers: { event: "reminder/scheduled" }, cancelOn: [{ event: "reminder/cancelled", match: "data.id" }], ...more },
      async ({ event, step }: any) => step.run("remind", () => void reminded.push(event.data.id)),
    ),
  ];
}

// Found by the twenty-ninth review. The router's listener for an event of a time to come now
// looks back to when SQS took the event, and recent() takes events at or after that time. A
// reschedule, sent as "cancel the old one, schedule the new one" in one send(), has both taken
// in the same millisecond. When the cancel is routed first, the new reminder hears it, and
// never runs. Before, the listener looked from when it was routed, and it ran.
describe("a reminder rescheduled in one send", () => {
  it("isn't cancelled by the cancel of the old one sent just before it", async () => {
    const reminded: string[] = [];
    const c = new Cluster(reminders(reminded));
    const t = Date.now();
    await frozen(t, () =>
      c.flow.send([
        { name: "reminder/cancelled", data: { id: "r1" } },
        { name: "reminder/scheduled", data: { id: "r1" }, ts: t + DAY },
      ]),
    );
    const [cancelled, scheduled] = c.queue.splice(0, 2);
    expect(cancelled.sentAt).toBe(scheduled.sentAt);
    await later(1000, async () => {
      await c.deliver([cancelled]);
      await c.deliver([scheduled]);
    });
    await later(DAY + 2000, async () => {
      await c.drain();
      for (const run of c.runs.values()) await (run as any).result;
    });
    expect(reminded).toEqual(["r1"]);
  }, 60_000);
});

// The mark of a cancel heard for an event of a time to come lives 367 days, and the waiter that
// sets it lives until ts + 2 minutes, after the run started. Every run of the function now reads
// the mark in sst:cancelOn, which a retry or replay runs again. So a run of such an event that a
// cancel came for within 2 minutes of its start (or that failed, and then a cancel came) can't
// be retried or replayed: the new run is cancelled at once. A normal event's run replays.
describe("a replay of a run of an event of a time to come", () => {
  it("runs, though a cancel came while the first run ran", async () => {
    const reminded: string[] = [];
    let fail = true;
    const c = new Cluster((flow: any) => [
      flow.createFunction(
        { id: "reminder", triggers: { event: "reminder/scheduled" }, cancelOn: [{ event: "reminder/cancelled", match: "data.id" }], retries: 0 },
        async ({ event, step }: any) =>
          step.run("remind", () => {
            if (fail) throw new Error("mail is down");
            reminded.push(event.data.id);
          }),
      ),
    ]);
    await c.flow.send({ name: "reminder/scheduled", data: { id: "r2" }, ts: Date.now() + DAY });
    await c.drain();
    let runId = "";
    await later(DAY + 2000, async () => {
      await c.drain();
      for (const run of c.runs.values()) {
        runId = run.runId;
        await (run as any).result;
      }
      // A minute after it failed, the user cancels the reminder.
      await c.flow.send({ name: "reminder/cancelled", data: { id: "r2" } });
      await c.drain();
    });
    expect(runId).not.toBe("");
    fail = false;
    const run = c.runs.get(runId)!;
    c.history.set(runId, { status: "FAILED", payload: run.payload, events: [] });
    // A week later, the mail is back, and the failed run is replayed on purpose.
    await later(8 * DAY, async () => {
      const { id } = await c.flow.runs.replay(runId);
      await (c.run(id) as any).result;
    });
    expect(reminded).toEqual(["r2"]);
  }, 60_000);
});

// matcher() refuses an `if` that reads a path the run's event doesn't have, in any top-level
// conjunct, skipping only a conjunct that is itself `||` or `?:`. A comparison with a default
// in it, as `async.data.id == (has(event.data.ref) ? event.data.ref : event.data.id)`, is one
// conjunct of kind "==": the path in the branch that isn't taken makes the wait one that
// "can't match", and it times out though the event it waits for comes. bind() already makes
// has() false and the branch is never evaluated, so the expression itself is fine.
describe("a wait whose if has a default", () => {
  it("resumes when the event comes, though the run's event lacks the branch not taken", async () => {
    const c = new Cluster((flow: any) => [
      flow.createFunction({ id: "invoice", triggers: { event: "invoice/created" } }, async ({ step }: any) => {
        const approval = await step.waitForEvent("approval", {
          event: "invoice/approved",
          timeout: "2s",
          if: "async.data.invoiceId == (has(event.data.ref) ? event.data.ref : event.data.invoiceId)",
        });
        return approval ? approval.data.by : "timed out";
      }),
    ]);
    await c.flow.send({ name: "invoice/created", data: { invoiceId: "inv_1" } });
    await c.drain();
    const run = await (async () => {
      for (let i = 0; i < 200 && !c.runs.size; i++) await new Promise((r) => setTimeout(r, 10));
      return [...c.runs.values()][0];
    })();
    await new Promise((r) => setTimeout(r, 300));
    await c.flow.send({ name: "invoice/approved", data: { invoiceId: "inv_1", by: "bob" } });
    await c.drain();
    expect((await (run as any).result).getResult()).toBe("bob");
  }, 60_000);
});
