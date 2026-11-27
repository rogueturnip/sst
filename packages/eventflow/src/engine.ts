import { Resource } from "sst/resource";
import { awsFetch, type AwsOptions } from "./client.js";
import { heartbeatCallback, LambdaError, startExecution, stopExecution, succeedCallback } from "./lambda.js";
import { sqs } from "./aws.js";
import { hash, ulid } from "./ids.js";
import { sign } from "./sign.js";
import { DynamoStore, type Store } from "./store.js";
import type { RealtimeResource } from "./realtime.js";
import type { StandardSchemaV1 } from "./index.js";
import {
  ADMIN_MARKER,
  CONTRACT,
  ENV_RESOURCE,
  type EngineResource,
  type Message,
  type RunPayload,
  type Timer,
  type WireEvent,
} from "./types.js";

/** The largest message SQS takes, and the largest batch: 1 MiB. */
const MAX_BATCH_BYTES = 1_048_576;
/** The largest event, in bytes of JSON, that `send()` accepts. It leaves room for the message around it. */
export const MAX_EVENT_BYTES = 1_000_000;
/**
 * How long a signed message in the queue is good for: longer than the dead-letter
 * queue keeps it, so one moved back after a fix still runs.
 */
export const MESSAGE_TTL_MS = 15 * 86_400_000;
/** How long an event ID is remembered, so a second send of it is skipped. */
export const DEDUP_MS = 24 * 3_600_000;

/**
 * The parts of the engine every role uses: the resource, the store, and the calls
 * that send events, start runs, and reach the router.
 */
export class Engine {
  private _store?: Store;
  /** The app's ID, which a reference to one of its functions has. */
  appId = "";
  /** Send the events about every run that ended, also where no function has a trigger for them. */
  systemEvents = false;

  constructor(
    public readonly resource: EngineResource,
    store: Store | undefined,
    public readonly aws?: AwsOptions,
  ) {
    this._store = store;
  }

  /** The table. Only the router and the worker can use it. */
  get store(): Store {
    if (this._store) return this._store;
    if (!this.resource.tableName)
      throw new Error("Only the EventFlow's router and worker use its table. Use flow.admin to look at its state.");
    return (this._store = new DynamoStore(this.resource.tableName, this.aws));
  }

  static resolve(input: { resource?: EngineResource; aws?: AwsOptions; store?: Store }) {
    return new Engine(input.resource ?? findResource(), input.store, input.aws);
  }

  get worker() {
    const name = this.resource.workerName;
    const qualifier = this.resource.workerQualifier || process.env.AWS_LAMBDA_FUNCTION_VERSION;
    if (!name || !qualifier || qualifier === "$LATEST")
      throw new Error("Only the EventFlow's router starts runs.");
    return { name, qualifier };
  }

  /** The other EventFlows it calls functions of, by app ID. */
  apps: Record<string, EngineResource> = {};
  /** Where it publishes realtime messages, and under what. */
  realtime?: { resource: RealtimeResource; prefix: string };
  /** Runs give their sessions to the events they send. */
  propagateSessions = true;

  async sendMessages(messages: { message: Message; group?: string; delaySeconds?: number }[], queueUrl = this.resource.queueUrl) {
    // SendMessageBatch takes 10 messages and 256 KB.
    const batches: (typeof messages)[] = [];
    let batch: typeof messages = [];
    let bytes = 0;
    for (const m of messages) {
      const size = Buffer.byteLength(JSON.stringify(m.message));
      if (batch.length === 10 || (batch.length && bytes + size > MAX_BATCH_BYTES - 1024)) {
        batches.push(batch);
        batch = [];
        bytes = 0;
      }
      batch.push(m);
      bytes += size;
    }
    if (batch.length) batches.push(batch);

    for (const entries of batches) {
      const result = await sqs<{ Failed?: { Id: string; Code: string; Message?: string }[] }>(
        "SendMessageBatch",
        {
          QueueUrl: queueUrl,
          Entries: entries.map((e, i) => ({
            Id: String(i),
            MessageBody: JSON.stringify(e.message),
            ...(e.group ? { MessageGroupId: e.group } : {}),
            ...(e.delaySeconds ? { DelaySeconds: e.delaySeconds } : {}),
          })),
        },
        this.aws,
      );
      if (result.Failed?.length) {
        const failed = result.Failed.map((f) => {
          const m = entries[Number(f.Id)].message;
          return `${"event" in m ? m.event.id : m.kind}: ${f.Code}${f.Message ? ` ${f.Message}` : ""}`;
        });
        throw new SendError(`Failed to send ${failed.length} of ${entries.length} events: ${failed.join("; ")}`);
      }
    }
  }

  /** Send a timer, to come at `at`. A queue delays a message at most 15 minutes, so it may come more than once. */
  async sendTimer(timer: Timer, at: number) {
    await this.sendMessages([
      {
        message: sign({ kind: "timer" as const, timer, at, exp: Math.max(at, Date.now()) + MESSAGE_TTL_MS }),
        group: timer.tenant ?? `t-${hash(timer.fn, 10)}`,
        delaySeconds: Math.max(0, Math.min(900, Math.ceil((at - Date.now()) / 1000))),
      },
    ]);
  }

  /** Send an event about a run that ended. */
  async sendSystem(event: WireEvent) {
    await this.sendMessages([
      { message: sign({ kind: "system" as const, event, exp: Date.now() + MESSAGE_TTL_MS }), group: event.tenant },
    ]);
  }

  async sendEvents(events: WireEvent[]) {
    await this.sendMessages(events.map((event) => ({ message: { kind: "event", event }, group: event.tenant })));
  }

  /**
   * Start a run. Lambda starts one execution per name, and returns the existing one
   * when the name and payload are the same, so starting a run twice is safe. The
   * payload is signed, and the worker runs nothing that isn't.
   */
  async startRun(payload: RunPayload, worker = this.worker) {
    const signed = sign(payload);
    try {
      const started = await startExecution(worker, { name: payload.runId, payload: signed }, this.aws);
      // Kept, so a run is found by its ID in the table, and not by listing them: that's in
      // the account's 15 a second of Lambda's control plane, for every function.
      if (started.arn)
        await this.store
          .claim(arnKey(payload.runId), started.arn, ((this.resource.runTimeoutSeconds ?? 31_622_400) + 90 * 86_400) * 1000)
          .catch(() => {});
      return started;
    } catch (error) {
      if (error instanceof LambdaError) {
        const body = await error.response.text().catch(() => "");
        // The name is used by a run with another payload. The run already happened.
        if (error.response.status === 409 && body.includes("AlreadyStarted")) return undefined;
        throw new Error(`Failed to start run "${payload.runId}": ${error.response.status} ${body}`.trim(), {
          cause: error,
        });
      }
      throw error;
    }
  }

  /**
   * Resume a run's callback. Returns false when the callback is closed or unknown,
   * as when the run stopped. A throttle, a server error or a network error is tried
   * again a few times, about 1.5 seconds in all, then thrown: the callback may or may
   * not have been resumed.
   */
  async callback(callbackId: string, payload: unknown) {
    // Whether a try may have resumed it: a throttle is a no.
    let maybe = false;
    // Thrown when it wasn't resumed, for sure.
    const not = (error: unknown) => (maybe ? error : Object.assign(error instanceof Error ? error : new Error(String(error)), { notResumed: true }));
    for (let attempt = 0; ; attempt++) {
      try {
        await succeedCallback(callbackId, payload, this.aws);
        return true;
      } catch (error) {
        const status = error instanceof LambdaError ? error.response.status : undefined;
        if (status === 404) return false;
        if (status === 400) {
          // Closed, unless it's the answer that isn't taken: that isn't the callback's end.
          const body = error instanceof LambdaError ? await error.response.text().catch(() => "") : "";
          if (/InvalidParameter|Validation|TooLarge/i.test(body) && /size|length|large|exceed|bytes/i.test(body))
            throw not(new Error(`The answer to a callback wasn't taken: ${body.slice(0, 300)}`));
          return false;
        }
        const transient = status === undefined || status === 429 || status >= 500;
        if (status !== 429) maybe = true;
        if (!transient || attempt >= 4) throw status === 429 ? not(error) : error;
        await new Promise((r) => setTimeout(r, (Math.random() + 1) * 50 * 2 ** attempt));
      }
    }
  }

  /** Whether a run still waits for a callback. False once it's closed, or its run has ended. */
  async waiting(callbackId: string) {
    try {
      await heartbeatCallback(callbackId, this.aws);
      return true;
    } catch (error) {
      if (error instanceof LambdaError && [400, 404, 409].includes(error.response.status)) return false;
      throw error;
    }
  }

  /** Stop a run. Returns false when it had already ended. */
  async stopRun(arn: string) {
    try {
      await stopExecution(arn, this.aws);
      return true;
    } catch (error) {
      if (error instanceof LambdaError && [400, 404, 409].includes(error.response.status)) return false;
      throw error;
    }
  }

  /** Call the router's admin API. */
  async admin<T = unknown>(op: string, args: Record<string, unknown> = {}): Promise<T> {
    const response = await awsFetch(
      "lambda",
      `/2015-03-31/functions/${encodeURIComponent(this.resource.routerName)}/invocations`,
      { method: "POST", body: JSON.stringify({ [ADMIN_MARKER]: { op, args } }) },
      { aws: this.aws },
    );
    const text = await response.text();
    if (!response.ok)
      throw new Error(
        `The EventFlow admin API failed: ${response.status} ${text}. Is this function linked to the EventFlow's admin, with "link: [flow.admin]"?`,
      );
    if (response.headers.get("X-Amz-Function-Error")) {
      const error = JSON.parse(text || "{}");
      throw new Error(`The EventFlow admin API failed: ${error.errorMessage ?? text}`);
    }
    const result = JSON.parse(text) as { ok: true; result: T } | { ok: false; error: { name: string; message: string } };
    if (!result.ok) {
      const error = new Error(result.error.message);
      error.name = result.error.name;
      throw error;
    }
    return result.result;
  }
}

export class SendError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SendError";
  }
}

export interface SendPayload {
  name: string;
  data?: unknown;
  id?: string;
  ts?: number;
  /**
   * The tenant the event belongs to. SST sends it as the message's `MessageGroupId`,
   * so a tenant with a backlog doesn't hold up the others. Event IDs, waits, and flow
   * control without a key are kept apart per tenant. A `key` of a concurrency limit, a
   * throttle or a rate limit is shared by the tenants that have the same value, so put
   * `event.tenant` in it if it isn't meant to be. Take it from something you trust, like the
   * signed-in user: EventFlow can't check it.
   */
  tenant?: string;
  v?: string;
  /**
   * The sessions the event is in, by name, to group its runs. The events a run sends are in
   * the run's sessions too; `null` takes one, or all of them, away.
   */
  meta?: { sessions?: Record<string, string | number | null> | null };
}

// SQS's MessageGroupId: letters, digits and punctuation.
const TENANT = /^[A-Za-z0-9!"#$%&'()*+,\-./:;<=>?@[\\\]^_`{|}~]{1,128}$/;
const ID = /^[\x21-\x7e]{1,128}$/;
// eslint-disable-next-line no-control-regex
const CONTROL = /[\u0000-\u001f\u007f]/;

/** Check an event, as `send()` does and as the router does for every message. */
export function checkEvent(input: Partial<WireEvent> & Record<string, unknown>, label = "event", own = false) {
  if (!input || typeof input !== "object") throw new TypeError(`The ${label} isn't an object.`);
  const name = input.name;
  if (typeof name !== "string" || !name) throw new TypeError(`An ${label} needs a name.`);
  if (name.length > 256 || CONTROL.test(name))
    throw new TypeError(`The ${label} name "${name.slice(0, 40)}" must be 1 to 256 characters, with no control characters.`);
  // So a sender can't make up what happened to a run.
  if (name.startsWith("inngest/") && !own) throw new TypeError(`The ${label} name "${name.slice(0, 60)}" is kept for EventFlow's own, as "inngest/" is.`);
  if (input.tenant !== undefined && (typeof input.tenant !== "string" || !TENANT.test(input.tenant)))
    throw new TypeError(
      `The tenant of ${label} "${name}" must be 1 to 128 letters, digits and punctuation, as SQS allows in a MessageGroupId.`,
    );
  if (input.id !== undefined && (typeof input.id !== "string" || !ID.test(input.id)))
    throw new TypeError(`The id of ${label} "${name}" must be 1 to 128 printable characters, with no spaces.`);
  if (input.ts !== undefined && (typeof input.ts !== "number" || !Number.isFinite(input.ts)))
    throw new TypeError(`The ts of ${label} "${name}" must be a number of milliseconds.`);
  if (input.v !== undefined && (typeof input.v !== "string" || input.v.length > 64))
    throw new TypeError(`The v of ${label} "${name}" must be at most 64 characters.`);
  if (input.sessions !== undefined) {
    const entries = input.sessions && typeof input.sessions === "object" && !Array.isArray(input.sessions) ? Object.entries(input.sessions) : undefined;
    if (!entries || entries.length > 20 || entries.some(([k, v]) => !k || k.length > 128 || typeof v !== "string" || v.length > 256))
      throw new TypeError(`The sessions of ${label} "${name}" must be at most 20, with names to 128 characters and IDs to 256.`);
  }
  if ("user" in input)
    throw new TypeError(
      `The ${label} "${name}" has a "user" field. Inngest v4 removed it, and EventFlow doesn't support it. Put it in "data".`,
    );
}

/** Check a signal, as `sendSignal()` does and as the router does. */
export function checkSignal(input: { signal?: unknown; tenant?: unknown; data?: unknown }) {
  // What the callback it answers takes.
  if (Buffer.byteLength(JSON.stringify(input.data) ?? "") > 200 * 1024)
    throw new TypeError("A signal's data must be at most 200 KB. Store larger data in a bucket and send its key.");
  if (typeof input.signal !== "string" || !input.signal || input.signal.length > 256 || CONTROL.test(input.signal))
    throw new TypeError("A signal's name must be 1 to 256 characters, with no control characters.");
  if (input.tenant !== undefined && (typeof input.tenant !== "string" || !TENANT.test(input.tenant)))
    throw new TypeError("The tenant of a signal must be 1 to 128 letters, digits and punctuation, as SQS allows in a MessageGroupId.");
}

/** The sessions of an event that's sent: those of the run that sends it, and its own. */
export function mergeSessions(from: Record<string, string> | undefined, meta: SendPayload["meta"]): Record<string, string> | undefined {
  const own = meta?.sessions;
  if (own === null) return undefined;
  const merged: Record<string, string> = { ...from };
  for (const [k, v] of Object.entries(own ?? {})) {
    if (v === null) delete merged[k];
    else merged[k] = String(v);
  }
  return Object.keys(merged).length ? merged : undefined;
}

/** What a sender gives of an event, and nothing else: what's EventFlow's own isn't theirs to set. */
export function cleanEvent(event: WireEvent): WireEvent {
  return {
    id: event.id,
    name: event.name,
    data: event.data,
    ts: typeof event.ts === "number" ? event.ts : Date.now(),
    ...(typeof event.v === "string" ? { v: event.v } : {}),
    ...(event.tenant ? { tenant: event.tenant } : {}),
    ...(event.sessions ? { sessions: event.sessions } : {}),
  };
}

export function toWireEvent(input: SendPayload, now = Date.now(), from?: Record<string, string>): WireEvent {
  const sessions = mergeSessions(from, input.meta);
  checkEvent({ ...(input as any), ...(sessions ? { sessions } : {}) });
  const event: WireEvent = {
    id: input.id ?? ulid(now),
    name: input.name,
    data: input.data ?? {},
    ts: input.ts ?? now,
    ...(input.v ? { v: input.v } : {}),
    ...(input.tenant ? { tenant: input.tenant } : {}),
    ...(sessions ? { sessions } : {}),
  };
  const bytes = Buffer.byteLength(JSON.stringify(event));
  if (bytes > MAX_EVENT_BYTES)
    throw new TypeError(
      `The event "${input.name}" is ${bytes} bytes. The most is ${MAX_EVENT_BYTES}. Store large data in a bucket and send its key.`,
    );
  return event;
}

/**
 * The runtime is versioned apart from SST. One that needs something the deployed
 * component doesn't have says so at the start, not part way through a run.
 */
export function checkContract(resource: EngineResource) {
  const deployed = resource.contract ?? 0;
  if (deployed < CONTRACT)
    throw new Error(
      `This version of @sst-community/eventflow needs an EventFlow component with contract ${CONTRACT}, and the deployed one has ${deployed}. Update SST and deploy again, or use an older version of the package.`,
    );
  return resource;
}

function findResource(): EngineResource {
  const fromEnv = process.env[ENV_RESOURCE];
  if (fromEnv) return checkContract(JSON.parse(fromEnv));

  const linked = Object.entries(Resource as Record<string, any>).filter(
    ([, value]) => value && typeof value === "object" && value.type === "sst.aws.EventFlow",
  );
  if (linked.length === 1) return linked[0][1];
  if (linked.length === 0)
    throw new Error(`No EventFlow is linked to this function. Link it in sst.config.ts, like "link: [flow]".`);
  throw new Error(
    `This function is linked to ${linked.length} EventFlows (${linked
      .map(([name]) => name)
      .join(", ")}). Pass the one to use, like "new EventFlow({ id, resource: Resource.${linked[0][0]} })".`,
  );
}

/** Where `eventType().create()` keeps the event's schema. */
export const schemaOf = Symbol.for("sst.eventflow.schema");

/** The event's data, checked against its type's schema, with what the schema makes of it. */
export async function checkedData(payload: SendPayload) {
  const schema = (payload as any)?.[schemaOf] as StandardSchemaV1 | undefined;
  if (!schema) return payload.data;
  const result = await schema["~standard"].validate(payload.data);
  if (result.issues)
    throw new TypeError(
      `The event "${payload.name}" doesn't match its schema: ${result.issues
        .map((i) => `${i.path?.length ? `${i.path.join(".")}: ` : ""}${i.message}`)
        .join("; ")}`,
    );
  return result.value;
}

/** Where a run's ARN is kept, by its ID. */
export function arnKey(runId: string) {
  return hash(`run-arn\n${runId}`);
}
