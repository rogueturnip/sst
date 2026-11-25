// What goes over the wire between the senders, the router and the worker.

/**
 * An event as the engine stores and delivers it.
 */
export interface WireEvent {
  /** The event's ID. Given by the sender, or made when it's sent. */
  id: string;
  name: string;
  data: unknown;
  /** When it was sent, in ms since the epoch. */
  ts: number;
  v?: string;
  /**
   * The tenant, from the sender. It's the message's MessageGroupId. It's a label the
   * sender asserts: the engine keeps tenants apart, but can't check who they are.
   */
  tenant?: string;
  /** The sessions it's in, by name, as Inngest's. */
  sessions?: Record<string, string>;
  /** For a deferred function's event, the run that deferred it. */
  from?: { fn: string; runId: string };
  /** Its data, when that's large, is in the table. */
  blob?: { key: string; bytes: number };
}

/** A queue message. */
export type Message =
  // From a sender.
  | { kind: "event"; event: WireEvent }
  // From a sender: data for the run that waits for a signal. `id` is the message's own.
  | { kind: "signal"; signal: string; data: unknown; tenant?: string; id: string }
  // From another EventFlow: run a function that takes calls from others, and answer its
  // callback. Like an event, anyone who may send can send one.
  | { kind: "call"; fn: string; event: WireEvent; parent: string }
  // From the router or the worker, about a run that ended. Signed, and good until `exp`.
  | { kind: "system"; event: WireEvent; exp: number; sig: string }
  // From the router: a run that's held, by a throttle or a cron's jitter, to start at `at`.
  // Signed, and good until `exp`.
  | {
      kind: "start";
      fn: string;
      event: WireEvent;
      at: number;
      runId: string;
      root: string;
      parent?: string;
      received?: number;
      fired?: boolean;
      batch?: { item: string; n: number };
      external?: boolean;
      /** It was held, and its timers were sent. */
      held?: boolean;
      exp: number;
      sig: string;
    }
  // From the worker: a run another run invokes. Signed, and good until `exp`.
  | { kind: "invoke"; fn: string; event: WireEvent; parent: string; runId: string; root: string; exp: number; sig: string }
  // From the router or the worker: something to do at `at`. Signed, and good until `exp`.
  | { kind: "timer"; timer: Timer; at: number; exp: number; sig: string };

export type Timer =
  /** A debounce's window may be over. `last` is when its last event came, as this timer knows it. */
  | { type: "debounce"; fn: string; item: string; last: number; tenant?: string }
  /** A batch's timeout. */
  | { type: "batch"; fn: string; item: string; tenant?: string }
  /** A run that hasn't started by now is cancelled. */
  | { type: "start"; fn: string; runId: string; parent?: string; tenant?: string }
  /** A run that hasn't finished by now is cancelled. */
  | { type: "finish"; fn: string; runId: string; arn: string; parent?: string; tenant?: string };

/** The payload a run starts with. Signed by the router or the worker. */
export interface RunPayload {
  "sst:eventflow": 1;
  fn: string;
  runId: string;
  event: WireEvent;
  /**
   * The run this one started from. A retry or replay keeps it, and an invoked run's
   * comes from its parent's. Idempotency keys come from it.
   */
  root?: string;
  /** For a replay or a retry, the run it replays. */
  replayOf?: string;
  /** For an invoked run, the callback of the run that invoked it. */
  parent?: string;
  /**
   * For a retry, the results of the steps that succeeded in the run it retries, by
   * step ID. Those steps return their result without running again.
   */
  memo?: Record<string, unknown>;
  /** Those of them that are large, by step ID: where they are. */
  memoBlobs?: Record<string, string>;
  /** The order the function had them in, up to the first step that's run again. */
  memoOrder?: string[];

  /** When its event came: the steps of runs that came first go first. */
  at?: number;
  /** For a run with a batch, which one. `event` is its first. */
  batch?: { item: string; n: number };
  /** Another EventFlow invoked it. */
  external?: boolean;
  /** For a singleton's run, the singleton it has. */
  lock?: string;
  sig?: string;
}

/** What the engine's resource gives the SDK, from the link or the environment. */
export interface EngineResource {
  /**
   * What the component and the runtime agree on. Only the router and the worker have it.
   */
  contract?: number;
  /** The URL of the queue that events are sent to. */
  queueUrl: string;
  /** The name of the router function, which also serves the admin API. */
  routerName: string;
  /** The name of the worker function. Only the router and the worker have it. */
  workerName?: string;
  /** The worker's published version. Only the router has it. */
  workerQualifier?: string;
  /** The name of the table. Only the router and the worker have it. */
  tableName?: string;
  /** The URL of the dead-letter queue. Only the router has it. */
  deadLetterQueueUrl?: string;
  /** The longest a run can take, in seconds. */
  runTimeoutSeconds?: number;
}

/** The contract this runtime needs from the component, at least. */
export const CONTRACT = 1;
/** The newest admin API this runtime serves. `sst eventflow` reads it. */
export const ADMIN_VERSION = 1;

/** The events about a run that ended. Runs they start send none, so they don't go on forever. */
export const SYSTEM_EVENTS = ["inngest/function.finished", "inngest/function.failed", "inngest/function.cancelled"] as const;

export const RUN_MARKER = "sst:eventflow" as const;
export const TICK_MARKER = "sst:eventflow:tick" as const;
export const ADMIN_MARKER = "sst:eventflow:admin" as const;
export const ENV_RESOURCE = "SST_EVENTFLOW";
