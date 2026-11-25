import type { AwsOptions } from "./client.js";
import { createHash, randomInt } from "node:crypto";
import { AwsError, dynamo, isConditionFailure, isTransient } from "./aws.js";
import { hash, key as keyOf } from "./ids.js";
import type { WireEvent } from "./types.js";

// The engine's state, in one DynamoDB table with a `pk` and `sk`, and `expires` as
// its TTL attribute. Every item expires, so the table doesn't grow without bound.
//
// Keys are built with `key()`, which hashes their parts, so a `#` or anything else in
// an event name, a tenant or a value can't make two keys the same. The parts are also
// kept as attributes, for the admin API.
//
//   E#…    an event ID of a tenant, while it's routed, and for 24 hours after
//   A#…    what the router decided for a function and an event: start, skip or hold
//   I#…    an idempotency key of a function and a tenant, for 24 hours
//   T#…    a throttle or rate limit bucket
//   C#…    one concurrency limit, or a shard of it: its leases, and each tenant's count; and,
//          at "C#…~", the tenants that may have steps in its line
//   Q#…    the steps waiting for a lease on a limit, oldest first
//   WF#…   the fields that waits for an event of a tenant match on
//   W#…    the waits for an event, and the cancelOns
//   R#…    an event that a wait could match, for an hour
//   D#…    a debounce's window
//   B#…    a batch: the events that aren't in a closed one, and the closed ones
//   BE#…   an event of a batch
//   S#…    a singleton: the run that has it
//   F#…    how a run's attempts failed, and what its steps had returned
//   G#…    a run that waits for a signal
//   X#…    the data of a large event, or a large result, in parts
//   M#…    whether a run with a start timeout started, or was cancelled first

export interface Limit {
  /** The item's key. */
  item: string;
  limit: number;
  /** What it limits, for the admin API, like "fn:import-contacts:acme". */
  label?: string;
}

export interface Waiter {
  callbackId: string;
  runId: string;
  createdAt: number;
  expiresAt: number;
  // The wait's `if`, with the run's event bound, as JSON.
  condition?: string;
  /** "wait" for a waitForEvent, "cancel" for a cancelOn. */
  kind?: "wait" | "cancel";
  /** For a cancelOn of an invoked run, the callback of the run that invoked it. */
  parent?: string;
  /** For the admin API. */
  info?: { name: string; tenant?: string; field: string };
}

export interface LeaseWaiter {
  /** The lease the step gets when it's woken. */
  leaseId: string;
  /** Every limit the step needs. It's given them all at once. */
  limits: Limit[];
  tenant: string;
  callbackId: string;
  runId: string;
  at: number;
  expiresAt: number;
  /** Where its leases are, once it's given them. */
  placed?: string[];
}

export type Admission =
  | { decision: "start" }
  | { decision: "skip"; reason: string }
  | { decision: "hold"; at: number }
  /** It's in a debounce's window. */
  | { decision: "debounce" }
  /** It's in a batch. */
  | { decision: "batch" };

/** A debounce's window, as it's read. */
export interface Debounce {
  event: WireEvent;
  /** What the event that's kept is ordered by. */
  order: string;
  /** When the first event came, and the last. */
  opened: number;
  last: number;
  /** Its run is being started. Nothing joins the window any more. */
  closing: boolean;
}

/** An event in a batch that isn't closed. */
export interface Pending {
  order: string;
  at: number;
  bytes: number;
  /** The event's own item. */
  ref: string;
}

/** The run that has a singleton. */
export interface Lock {
  run: string;
  /** The run's execution, once it's started. */
  arn?: string;
  /** Until when a run that isn't found is taken for one that's still to start. */
  until: number;
  /** The runs it took it from, that aren't known to be stopped yet. */
  replaces?: { run: string; arn?: string }[];
}

/** How a function's attempt failed, and what its steps had returned by then. */
export interface Failure {
  attempt: number;
  error: { name: string; message: string };
  delayMs: number;
  memo: Record<string, Memo>;
}

/** A run that waits for a signal. */
export interface SignalWait {
  callbackId: string;
  runId: string;
  /** The wait's own, the same when its step is tried again. */
  owner: string;
  createdAt: number;
  expiresAt: number;
}

/** A step's result, or the error it failed with. */
export type Memo = { v: unknown } | { e: { name: string; message: string } };

export interface Store {
  /**
   * Note that an event is being routed. "duplicate" when another message with the
   * same ID was routed in the last 24 hours. The same message delivered again, or a
   * message whose routing didn't finish, is routed again.
   */
  beginEvent(item: string, delivery: string, ttlMs: number): Promise<"route" | "duplicate">;
  /** Note that an event's routing finished. */
  finishEvent(item: string, ttlMs: number): Promise<void>;
  getAdmission(item: string): Promise<Admission | undefined>;
  /** Saves the decision, unless one was saved first. Returns the one that's saved. */
  putAdmission(item: string, admission: Admission, ttlMs: number): Promise<Admission>;
  /** True the first time the key is claimed in the window. */
  claim(item: string, owner: string, ttlMs: number): Promise<boolean>;
  /** Gives up a key that's claimed, if this is its owner. */
  unclaim(item: string, owner: string): Promise<void>;
  /** The owner of a key, if it has one. */
  owner(item: string): Promise<string | undefined>;
  /** Records the owner of a key if it has none, and returns the owner it has. */
  successor(item: string, owner: string, ttlMs: number): Promise<string>;
  /** GCRA: 0 when allowed now, or how many ms until it would be. Takes the slot only when allowed. */
  take(item: string, intervalMs: number, toleranceMs: number): Promise<number>;
  /**
   * GCRA: reserves the next slot, and returns when it starts. Infinity when that's more
   * than `maxWaitMs` away: then no slot is taken.
   */
  reserve(item: string, intervalMs: number, toleranceMs: number, maxWaitMs?: number): Promise<number>;
  /**
   * Takes a lease on every limit at once, or on none: "granted" when the step holds
   * them all, including when it already did; "full" when one has no room, or steps
   * wait for one.
   */
  acquire(limits: Limit[], leaseId: string, tenant: string, leaseMs: number, first?: boolean): Promise<"granted" | "full">;
  /** Extends the leases. False when one is gone or expired. `placed` is where they were given. */
  renew(limits: Limit[], leaseId: string, leaseMs: number, claim?: boolean, placed?: string[]): Promise<boolean>;
  release(limits: Limit[], leaseId: string, tenant: string): Promise<void>;
  /**
   * Puts a step in line for every limit it needs. A step that asks again, with the
   * same time and lease, keeps its place. "held" when the step already holds them all: then
   * it goes on, and closes its callback first, so one that's giving it others at that moment
   * finds it closed, and gives them back.
   */
  enqueue(waiter: LeaseWaiter, first?: boolean): Promise<"queued" | "held">;
  /**
   * Gives free leases on a limit to steps in line: the tenant with the fewest leases,
   * then the oldest step. A step gets every lease it needs at once, or waits. `wake`
   * resumes a step, and returns false if it can't, as when its run stopped; then its
   * leases are given back. A wake that throws may not have reached the step: its
   * leases are given back and it's put back in line, in its place, for a later
   * dispatch. `joined` is the step that dispatches as it joined the line: a limit with many
   * leases only looks to that step, as each that joins looks to itself, and a step that's
   * done to the rest. Returns how many it woke.
   */
  dispatch(limit: Limit, wake: (w: LeaseWaiter) => Promise<boolean>, joined?: LeaseWaiter): Promise<number>;
  /**
   * Give back a step's leases, handing them straight to the next step in line, in one
   * write where it can, and wake it. Then hand out what else is free.
   */
  handoff(limits: Limit[], leaseId: string, tenant: string, wake: (w: LeaseWaiter) => Promise<boolean>): Promise<void>;
  waitFields(name: string, tenant: string | undefined): Promise<string[]>;
  addWaitField(name: string, tenant: string | undefined, field: string, until: number): Promise<void>;
  putWaiter(key: string, waiter: Waiter): Promise<void>;
  waiters(key: string, now: number): Promise<Waiter[]>;
  deleteWaiter(key: string, waiter: Pick<Waiter, "createdAt" | "callbackId">): Promise<void>;
  putRecent(key: string, event: WireEvent, receivedAt: number, ttlMs: number): Promise<void>;
  recent(key: string, since: number): Promise<{ event: WireEvent; receivedAt: number }[]>;
  /**
   * Puts an event in a debounce's window, in place of one that's older. "stale" when it's
   * older itself, or the window that had it has closed. "closing" when the window's run is
   * being started: finish that first, and try again.
   */
  debounceMerge(
    item: string,
    event: WireEvent,
    order: string,
    received: string,
    now: number,
    ttlMs: number,
  ): Promise<{ opened: number; last: number } | "stale" | "closing">;
  debounceRead(item: string): Promise<Debounce | undefined>;
  /** Closes the window, if the event that's kept is still this one. */
  debounceClose(item: string, order: string): Promise<boolean>;
  /** The window's run started: the next event opens a new one. */
  debounceDone(item: string, order: string): Promise<void>;
  /**
   * Adds an event to a batch that isn't closed. False when it's been in one already.
   */
  batchAdd(item: string, ref: string, order: string, event: WireEvent, at: number, ttlMs: number): Promise<boolean>;
  /** The oldest events that aren't in a closed batch. */
  batchPending(item: string, max: number): Promise<Pending[]>;
  /** Events, by their items. One that's gone is left out. */
  batchEvents(refs: string[]): Promise<WireEvent[]>;
  /**
   * Closes the next batch, with what `choose` takes of the events that are pending, or
   * returns the one that was closed and isn't done. Each event is in one batch.
   */
  batchClose(item: string, max: number, choose: (pending: Pending[]) => Pending[], ttlMs: number): Promise<{ n: number; refs: string[]; at: number } | undefined>;
  /** The batch's run started: the next one can be closed. */
  batchDone(item: string, n: number): Promise<void>;
  /** The events of a closed batch. */
  batchRead(item: string, n: number): Promise<string[]>;
  /** Takes a singleton for a run, or returns the one that has it. */
  lockTake(item: string, lock: Lock, ttlMs: number): Promise<Lock | undefined>;
  lockRead(item: string): Promise<Lock | undefined>;
  /** Notes the execution of the run that has it. */
  lockStarted(item: string, run: string, arn: string): Promise<void>;
  /** Gives it from one run to another. False when neither has it. */
  lockMove(item: string, from: string, to: Lock, ttlMs: number): Promise<boolean>;
  /** The runs it was taken from are stopped. */
  lockSettled(item: string, run: string): Promise<void>;
  lockFree(item: string, run: string): Promise<void>;
  /** Keeps a text of any length. */
  putBlob(key: string, body: string, ttlMs: number): Promise<void>;
  getBlob(key: string): Promise<string | undefined>;
  /** Keeps how a function's attempt failed. The first one kept for an attempt stays. */
  putFailure(run: string, failure: Failure, ttlMs: number): Promise<void>;
  failures(run: string): Promise<Failure[]>;
  /** The holder of a signal's name, or takes it. `replace` takes it from another. */
  signalWait(item: string, wait: SignalWait, replace: boolean | { over: string }): Promise<boolean>;
  signalRead(item: string): Promise<SignalWait | undefined>;
  signalFree(item: string, callbackId: string): Promise<void>;
  /** For the admin API: the items whose key starts with a prefix. */
  scan(prefix: string, limit: number): Promise<Record<string, any>[]>;
}

const seconds = (ms: number) => String(Math.ceil(ms / 1000));
const S = (value: string) => ({ S: value });
const N = (value: number) => ({ N: String(value) });
const pad = (n: number) => String(Math.max(0, Math.floor(n))).padStart(15, "0");
const DAY = 86_400_000;

/**
 * A map key for a tenant. A map key can't be empty, and "" is "no tenant". A long one
 * is hashed, so a limit's maps stay small.
 */
export function tenantKey(tenant: string) {
  if (tenant === "") return "\u0000";
  return tenant.length <= 24 ? tenant : `#${hash(tenant, 16)}`;
}

function waiterSort(waiter: Pick<Waiter, "createdAt" | "callbackId">) {
  // The callback is part of it, so two waits of one run that register in the same
  // millisecond don't replace each other.
  return `${pad(waiter.createdAt)}#${waiter.callbackId.slice(-24)}`;
}

/** Errors that mean "try later", not "this is broken". */
export function isRetryable(error: unknown) {
  return error instanceof AwsError && (error.status >= 500 || isTransient(error));
}

/**
 * The waiter that's woken next: the tenant with the fewest leases on the limit, then
 * the one that has waited longest.
 */
export function fairest<T extends { tenant: string; at: number }>(waiters: T[], active: Record<string, number>) {
  let best: T | undefined;
  for (const w of waiters) {
    if (!best) {
      best = w;
      continue;
    }
    const a = active[tenantKey(w.tenant)] ?? 0;
    const b = active[tenantKey(best.tenant)] ?? 0;
    if (a < b || (a === b && w.at < best.at)) best = w;
  }
  return best;
}

/** The value of a lease in the item of a condition that wasn't met. */
const leaseIn = (error: unknown, leaseId: string) => (error as AwsError).body?.Item?.leases?.M?.[leaseId]?.S;

export class DynamoStore implements Store {
  /** A test makes it shorter. */
  grantMs = GRANT_MS;

  constructor(
    private readonly table: string,
    private readonly aws?: AwsOptions,
  ) {}

  private db<T = any>(action: string, body: Record<string, unknown>, options?: { retries?: number }) {
    return dynamo<T>(action, { TableName: this.table, ...body }, this.aws, options);
  }

  async beginEvent(item: string, delivery: string, ttlMs: number) {
    try {
      await this.db("PutItem", {
        Item: {
          pk: S(item),
          sk: S("-"),
          delivery: S(delivery),
          expires: { N: seconds(Date.now() + ttlMs) },
        },
        // A new ID, the same message again, or a message whose routing didn't finish.
        ConditionExpression: "attribute_not_exists(pk) OR delivery = :d OR attribute_not_exists(routed)",
        ExpressionAttributeValues: { ":d": S(delivery) },
      });
      return "route" as const;
    } catch (error) {
      if (isConditionFailure(error)) return "duplicate" as const;
      throw error;
    }
  }

  async finishEvent(item: string, ttlMs: number) {
    await this.db("UpdateItem", {
      Key: { pk: S(item), sk: S("-") },
      UpdateExpression: "SET routed = :t, expires = :e",
      ExpressionAttributeValues: {
        ":t": { BOOL: true },
        ":e": { N: seconds(Date.now() + ttlMs) },
      },
    });
  }

  async getAdmission(item: string) {
    const result = await this.db("GetItem", {
      Key: { pk: S(item), sk: S("-") },
      ConsistentRead: true,
    });
    return result.Item?.admission ? (JSON.parse(result.Item.admission.S) as Admission) : undefined;
  }

  async putAdmission(item: string, admission: Admission, ttlMs: number) {
    try {
      await this.db("PutItem", {
        Item: {
          pk: S(item),
          sk: S("-"),
          admission: S(JSON.stringify(admission)),
          expires: { N: seconds(Date.now() + ttlMs) },
        },
        ConditionExpression: "attribute_not_exists(pk)",
      });
      return admission;
    } catch (error) {
      if (!isConditionFailure(error)) throw error;
      return (await this.getAdmission(item)) ?? admission;
    }
  }

  async claim(item: string, owner: string, ttlMs: number) {
    try {
      await this.db("PutItem", {
        Item: {
          pk: S(item),
          sk: S("-"),
          owner: S(owner),
          expires: { N: seconds(Date.now() + ttlMs) },
        },
        ConditionExpression: "attribute_not_exists(pk) OR #o = :o OR expires < :now",
        ExpressionAttributeNames: { "#o": "owner" },
        ExpressionAttributeValues: {
          ":o": S(owner),
          ":now": { N: seconds(Date.now()) },
        },
      });
      return true;
    } catch (error) {
      if (isConditionFailure(error)) return false;
      throw error;
    }
  }

  async unclaim(item: string, owner: string) {
    await this.db("DeleteItem", {
      Key: { pk: S(item), sk: S("-") },
      ConditionExpression: "#o = :o",
      ExpressionAttributeNames: { "#o": "owner" },
      ExpressionAttributeValues: { ":o": S(owner) },
    }).catch((error) => {
      if (!isConditionFailure(error)) throw error;
    });
  }

  async owner(item: string) {
    const result = await this.db("GetItem", {
      Key: { pk: S(item), sk: S("-") },
      ConsistentRead: true,
    });
    return result.Item?.owner?.S as string | undefined;
  }

  async successor(item: string, owner: string, ttlMs: number) {
    if (await this.claim(item, owner, ttlMs)) return owner;
    return (await this.owner(item)) ?? owner;
  }

  // GCRA keeps one number per bucket: the time the next start is due. It's moved on in
  // one write, so starts that come at once take their turn, and none is let through
  // because others were in the way.
  private async gcra(item: string, intervalMs: number, toleranceMs: number, reserve: boolean, maxWaitMs = Infinity): Promise<number> {
    const Key = { pk: S(item), sk: S("-") };
    // A rate limit's write isn't sent again after a lost answer: it would find the slot it
    // took, and skip the run. One that may have been made counts as made, so at worst a run
    // too many starts, and none is lost. A throttle's is: a slot too many only waits.
    const once = reserve ? undefined : { retries: 0 };
    const unknown = (error: unknown) => {
      if (reserve || (error instanceof AwsError && error.status < 500)) throw error;
      return 0;
    };
    for (;;) {
      const now = Date.now();
      // The latest the next start may be due for this one to go, or be held.
      const latest = reserve ? now + toleranceMs + maxWaitMs : now + toleranceMs;
      // Longer than a backlog of held runs, which a start can be due after.
      const expires = { N: seconds(now + 60 * DAY) };
      try {
        const bounded = Number.isFinite(latest);
        const result = await this.db("UpdateItem", {
          Key,
          UpdateExpression: "SET expires = :e ADD tat :i",
          ConditionExpression: `tat >= :now${bounded ? " AND tat <= :latest" : ""}`,
          ExpressionAttributeValues: { ":i": N(intervalMs), ":now": N(now), ":e": expires, ...(bounded ? { ":latest": N(latest) } : {}) },
          ReturnValues: "UPDATED_NEW",
          ReturnValuesOnConditionCheckFailure: "ALL_OLD",
        }, once);
        const due = Number(result.Attributes.tat.N) - intervalMs - toleranceMs;
        return reserve ? Math.max(now, due) : 0;
      } catch (error) {
        if (!isConditionFailure(error)) return unknown(error);
        const stored = (error as AwsError).body?.Item?.tat?.N;
        if (stored !== undefined && Number(stored) >= now) {
          // Over the limit, or held for longer than it may be.
          const wait = Number(stored) - toleranceMs - now;
          if (!reserve) return Math.max(1, wait);
          return Infinity;
        }
      }
      // Nothing's due: the bucket starts again from now.
      try {
        await this.db("UpdateItem", {
          Key,
          UpdateExpression: "SET tat = :tat, expires = :e",
          ConditionExpression: "attribute_not_exists(tat) OR tat < :now",
          ExpressionAttributeValues: { ":tat": N(now + intervalMs), ":now": N(now), ":e": expires },
        }, once);
        return reserve ? now : 0;
      } catch (error) {
        if (!isConditionFailure(error)) return unknown(error);
      }
    }
  }

  take(item: string, intervalMs: number, toleranceMs: number) {
    return this.gcra(item, intervalMs, toleranceMs, false);
  }

  reserve(item: string, intervalMs: number, toleranceMs: number, maxWaitMs?: number) {
    return this.gcra(item, intervalMs, toleranceMs, true, maxWaitMs);
  }

  // A limit's leases are in one item, or, for a limit over SHARD_LEASES, spread over
  // shards: an item each, whose caps add up to the limit. DynamoDB takes 1,000 write units
  // a second on an item's key, and a write costs by the item's size, so one item with
  // hundreds of leases would do only a few steps a second. A shard's item is at sort key
  // "-": `cap`; `active`, the number of leases; `leases`, each lease's expiry and tenant;
  // and `tenants`, each tenant's leases, with no zero counts kept.
  //
  // All the shards have one line, so a lease that's given back in any of them goes to the
  // step whose turn it is. The line is per tenant: a step's entry is an item under its
  // limit and tenant. An item at "w#<tenant>" beside the limit marks that the tenant may
  // have steps in line. It's put there after an entry goes in, if it isn't there, and
  // taken out when the line's found empty, which then looks at the line again. Each writes
  // first and reads second, so one of them sees the other, and a step isn't hidden.
  //
  // None of it is written in a transaction. On a busy limit, transactions on one item
  // fail each other, where plain writes take their turn. Each write is conditioned, so
  // a limit can't go over, or count a lease twice. A step takes the limits it needs
  // one at a time, always in the same order, and gives back the ones it took if the
  // next is full: it never waits holding a lease, so two steps can't each hold what
  // the other waits for.

  // Take one lease, if there's room and it isn't there yet. A write that was made, but
  // whose answer was lost, is tried again and finds its own lease: that's a lease taken.
  private async takeOne(limit: Limit, leaseId: string, tenant: string, value: string): Promise<"taken" | "held" | "full"> {
    try {
      await this.db("UpdateItem", {
        Key: { pk: S(limit.item), sk: S("-") },
        UpdateExpression:
          "SET leases.#l = :lease, expires = :exp, cap = :cap, label = if_not_exists(label, :label) ADD active :one, tenants.#t :one",
        ConditionExpression: "attribute_exists(pk) AND attribute_not_exists(leases.#l) AND active < :cap",
        ExpressionAttributeNames: { "#l": leaseId, "#t": tenantKey(tenant) },
        ExpressionAttributeValues: {
          ":lease": S(value),
          ":one": N(1),
          ":cap": N(limit.limit),
          ":exp": { N: seconds(Date.now() + DAY) },
          ":label": S(limit.label ?? limit.item),
        },
        ReturnValuesOnConditionCheckFailure: "ALL_OLD",
      });
      return "taken";
    } catch (error) {
      if (!isConditionFailure(error)) throw error;
      const there = leaseIn(error, leaseId);
      return there === value ? "taken" : there === undefined ? "full" : "held";
    }
  }

  // Take a lease on one of a limit's shards. The shards with room are tried at random, so
  // steps that ask at once spread out. `rooms` is what's known of it, from the limit as it
  // was read. An error gives back what may have been written.
  private async takeAny(limit: Limit, leaseId: string, tenant: string, value: string, rooms?: Rooms): Promise<Limit | "held" | "full"> {
    let shards = shardsOf(limit);
    if (shards.length > 1) {
      const known = rooms ?? new Map(await Promise.all(shards.map(async (s) => [s.item, await this.room(s)] as const)));
      // A few: where others are taking the last of the room, it's the line's turn.
      shards = shuffle(shards.filter((s) => (known.get(s.item) ?? 0) > 0)).slice(0, 6);
    }
    for (const shard of shards) {
      let took: "taken" | "held" | "full";
      try {
        took = await this.takeOne(shard, leaseId, tenant, value);
      } catch (error) {
        await this.releaseOne(shard.item, leaseId, tenant, value).catch(() => {});
        throw error;
      }
      if (took === "held") return "held";
      rooms?.set(shard.item, took === "taken" ? (rooms.get(shard.item) ?? 0) - 1 : 0);
      if (took === "taken") return shard;
    }
    return "full";
  }

  // Take every lease, in order, or give back the ones taken. One the step has already,
  // as from a grant in progress that may yet be undone, is made this step's own, so
  // undoing the grant doesn't give it back.
  private async takeAll(limits: Limit[], leaseId: string, tenant: string, value: string, held: Map<string, Copy>, rooms: Rooms[]) {
    const taken: Limit[] = [];
    const undo = async () => {
      for (const t of taken) await this.releaseOne(t.item, leaseId, tenant, value).catch(() => {});
    };
    for (const [i, limit] of limits.entries()) {
      const old = held.get(limit.item);
      try {
        if (old) {
          // Given back by its value if it fails, which may be after it was written.
          taken.push(old.shard);
          if (await this.setLease(old.shard.item, leaseId, old.raw, value)) continue;
          taken.pop();
        } else {
          const took = await this.takeAny(limit, leaseId, tenant, value, rooms[i]);
          if (typeof took === "object") {
            taken.push(took);
            continue;
          }
        }
      } catch (error) {
        await undo();
        throw error;
      }
      await undo();
      return undefined;
    }
    return taken;
  }

  // Replace a lease's value, if it's still the one that was read.
  private async setLease(item: string, leaseId: string, old: string, value: string) {
    try {
      await this.db("UpdateItem", {
        Key: { pk: S(item), sk: S("-") },
        UpdateExpression: "SET leases.#l = :lease, expires = :exp",
        ConditionExpression: "leases.#l = :old",
        ExpressionAttributeNames: { "#l": leaseId },
        ExpressionAttributeValues: { ":lease": S(value), ":old": S(old), ":exp": { N: seconds(Date.now() + DAY) } },
        ReturnValuesOnConditionCheckFailure: "ALL_OLD",
      });
      return true;
    } catch (error) {
      if (isConditionFailure(error)) return leaseIn(error, leaseId) === value;
      throw error;
    }
  }

  async acquire(limits: Limit[], leaseId: string, tenant: string, leaseMs: number, first = false) {
    const unique = ordered(limits);
    // A wide limit that's contended isn't asked of again and again: it's the line's turn.
    const tries = unique.some((l) => shardsOf(l).length > 1) ? 3 : 5;
    for (let attempt = 0; attempt < tries; attempt++) {
      // Whether there's room is decided as a lease is written, so what's read of it may be a
      // moment old. Whether it has its lease already isn't, where it may have asked before.
      const states = await Promise.all(unique.map((l) => this.readLimit(l, "some", !first)));
      if ((await Promise.all(states.map((s) => this.createSome(s)))).some(Boolean)) continue;
      // Already ours, as given to it while it asked again: it claims it, so a wake of
      // its old callback that fails doesn't give it back.
      if (states.every((s) => s.leases[leaseId]) && (await this.renew(unique, leaseId, leaseMs, true))) return "granted" as const;
      const now = Date.now();
      // Give back leases that expired, as when a run crashed, then look again.
      if (await this.reap(states, now)) continue;
      // A full limit has no room, and a step in line goes first: don't ask.
      if (states.some((s) => s.free <= 0)) return "full" as const;
      const lines = await Promise.all(unique.map((l, i) => this.anyInLine(l, states[i])));
      if (lines.some(Boolean)) return "full" as const;
      const held = new Map(unique.flatMap((l, i) => (states[i].copies(leaseId).length ? [[l.item, states[i].copies(leaseId)[0]] as const] : [])));
      const taken = await this.takeAll(unique, leaseId, tenant, leaseValue(now + leaseMs, tenant, "own"), held, states.map((s) => s.rooms()));
      if (taken) {
        taken.forEach((shard, i) => this.remember(unique[i], leaseId, shard));
        return "granted" as const;
      }
      // Another step took the room first.
    }
    return "full" as const;
  }

  // Whether a step waits in line. Markers are kept a while after a line empties, so look
  // at the lines of a few, and take more to mean yes.
  private async anyInLine(limit: Limit, state: LimitState) {
    const marks = Object.entries(state.waiting);
    if (!marks.length) return false;
    if (marks.reduce((n, [, m]) => n + m.lines, 0) > 16) return true;
    const heads = await Promise.all(marks.map(([t, m]) => this.lineEmpty(limit.item, t, m.lines)));
    return heads.some((empty) => !empty);
  }

  private async lineEmpty(item: string, t: string, lines: number) {
    const pages = await Promise.all(
      Array.from({ length: lines }, (_, line) =>
        this.db("Query", {
          KeyConditionExpression: "pk = :pk",
          ExpressionAttributeValues: { ":pk": S(lineKey(item, t, line)) },
          ProjectionExpression: "pk",
          ConsistentRead: true,
          Limit: 1,
        }),
      ),
    );
    return pages.every((p) => !p.Items?.length);
  }

  // The shard a lease is in, as this process last saw it, so the step that renewed it
  // gives it back without looking for it.
  private readonly places = new Map<string, Limit>();
  private remember(limit: Limit, leaseId: string, shard: Limit) {
    if (shard.item === limit.item && shardsOf(limit).length === 1) return;
    if (this.places.size > 10_000) this.places.clear();
    this.places.set(`${limit.item}|${leaseId}`, shard);
  }

  async renew(limits: Limit[], leaseId: string, leaseMs: number, claim = false, placed?: string[]) {
    const now = Date.now();
    const unique = uniqueLimits(limits);
    // Where it was given, or where this process took it, if it's known: only that's read. One
    // that's claimed as it was found looks everywhere, as it may have another.
    const known = unique.map((l) => {
      if (claim) return undefined;
      const shards = shardsOf(l);
      if (shards.length === 1 && !placed) return undefined;
      const at = placed ? placed.find((p) => p === l.item || p.startsWith(`${l.item}.`)) : this.places.get(`${l.item}|${leaseId}`)?.item;
      return at === undefined ? undefined : (shards.find((s) => s.item === at) ?? { item: at, limit: 0, label: l.label });
    });
    const states = await Promise.all(
      unique.map(async (l, i) => {
        const shard = known[i];
        if (shard) {
          const state = await this.readShards(l, [shard]);
          if (state.copies(leaseId).length) return state;
        }
        return this.readLimit(l, "none");
      }),
    );
    // Its lease on each limit. Two grants that race can leave it two, in two shards: it
    // keeps its own, or the one that lasts, and gives back the rest.
    const mine: Copy[] = [];
    const extra: Copy[] = [];
    for (const state of states) {
      const copies = state
        .copies(leaseId)
        // An expired lease may be given back at any moment: it isn't renewed.
        .filter((c) => c.lease.expires > now)
        // Of two that were given, the first: the other is of a grant that came later, finds it,
        // and is undone.
        .sort((a, b) => Number(a.lease.granted) - Number(b.lease.granted) || a.lease.expires - b.lease.expires);
      if (!copies.length) return false;
      mine.push(copies[0]);
      extra.push(...copies.slice(1));
    }
    unique.forEach((l, i) => this.remember(l, leaseId, mine[i].shard));
    const tidy = async () => {
      for (const c of extra) await this.releaseOne(c.shard.item, leaseId, c.lease.tenant, c.raw).catch(() => {});
      return true;
    };
    // A lease the step took itself, that lasts past this invocation, needs no write. One
    // it was given is made its own, so whoever gave it can't give it back any more, as
    // when the wake reached the step but its answer was lost.
    if (!claim && mine.every((c) => !c.lease.granted && c.lease.expires >= now + leaseMs)) return tidy();
    const renewed: [Limit, string, string][] = [];
    try {
      for (const { shard, lease, raw } of mine) {
        // One it was given lasts long enough to resume. From here it lasts this invocation.
        const expires = lease.granted ? now + leaseMs : Math.max(now + leaseMs, lease.expires);
        const value = leaseValue(expires, lease.tenant, "own");
        renewed.push([shard, lease.tenant, value]);
        if (await this.setLease(shard.item, leaseId, raw, value)) continue;
        renewed.pop();
        await this.giveBackAll(renewed, leaseId);
        return false;
      }
    } catch (error) {
      await this.giveBackAll(renewed, leaseId);
      throw error;
    }
    return tidy();
  }

  // A claim that lost one of its leases gives back the ones it made its own: whoever
  // granted them no longer can.
  private async giveBackAll(leases: [Limit, string, string][], leaseId: string) {
    for (const [shard, tenant, value] of leases) await this.releaseOne(shard.item, leaseId, tenant, value).catch(() => {});
  }

  async release(limits: Limit[], leaseId: string, tenant: string) {
    await Promise.all(uniqueLimits(limits).map((l) => this.releaseFrom(l, leaseId, tenant)));
  }

  // Give back a step's lease on a limit, wherever it is.
  private async releaseFrom(limit: Limit, leaseId: string, tenant: string) {
    const shards = shardsOf(limit);
    // One that isn't there may be in another's shard.
    if (shards.length === 1 && (await this.releaseOne(limit.item, leaseId, tenant))) return;
    const key = `${limit.item}|${leaseId}`;
    const known = this.places.get(key);
    this.places.delete(key);
    if (known && (await this.releaseOne(known.item, leaseId, tenant))) return;
    if (shards.length === 1 && !known) {
      const { Item } = await this.db("GetItem", { Key: { pk: S(limit.item), sk: S("-") }, ProjectionExpression: "shards", ConsistentRead: true });
      if (Number(Item?.shards?.N ?? 1) === 1) return;
    }
    const state = await this.readLimit(limit, "none");
    for (const c of state.copies(leaseId)) await this.releaseOne(c.shard.item, leaseId, tenant);
  }

  private async releaseOne(item: string, leaseId: string, tenant: string, only?: string) {
    try {
      await this.db("UpdateItem", {
        Key: { pk: S(item), sk: S("-") },
        UpdateExpression: "REMOVE leases.#l ADD active :minus, tenants.#t :minus",
        // To give back an expired lease, only the one that was read: its holder may
        // have renewed it since.
        ConditionExpression: only === undefined ? "attribute_exists(leases.#l)" : "leases.#l = :only",
        ExpressionAttributeNames: { "#l": leaseId, "#t": tenantKey(tenant) },
        ExpressionAttributeValues: { ":minus": N(-1), ...(only === undefined ? {} : { ":only": S(only) }) },
      });
    } catch (error) {
      if (isConditionFailure(error)) return false;
      throw error;
    }
    // Don't keep a tenant with no leases, so the item stays as small as its leases.
    await this.dropTenant(item, tenant);
    return true;
  }

  private async dropTenant(item: string, tenant: string) {
    await this.db("UpdateItem", {
      Key: { pk: S(item), sk: S("-") },
      UpdateExpression: "REMOVE tenants.#t",
      ConditionExpression: "tenants.#t <= :zero",
      ExpressionAttributeNames: { "#t": tenantKey(tenant) },
      ExpressionAttributeValues: { ":zero": N(0) },
    }).catch((error) => {
      if (!isConditionFailure(error)) throw error;
    });
  }

  private async reap(states: LimitState[], now: number) {
    let reaped = false;
    for (const state of states)
      for (const shard of state.shards)
        for (const [id, lease] of Object.entries(shard.leases))
          if (lease.expires <= now && (await this.releaseOne(shard.limit.item, id, lease.tenant, shard.raw[id]))) reaped = true;
    return reaped;
  }

  private entryKey(limit: Limit, waiter: Pick<LeaseWaiter, "tenant" | "at" | "leaseId">) {
    return { pk: S(lineKey(limit.item, tenantKey(waiter.tenant), lineOf(limit, waiter.leaseId))), sk: S(`${pad(waiter.at)}#${waiter.leaseId}`) };
  }

  private entry(limit: Limit, waiter: LeaseWaiter) {
    return {
      // For the admin API, to count a limit's line.
      limit: S(limit.item),
      lease: S(waiter.leaseId),
      tenant: S(waiter.tenant),
      callback: S(waiter.callbackId),
      run: S(waiter.runId),
      at: N(waiter.at),
      limits: S(JSON.stringify(ordered(waiter.limits))),
      expires: { N: seconds(waiter.expiresAt + DAY) },
    };
  }

  // Put a step that was taken out of line back in, in its place, as when it was given
  // its leases but couldn't be told. Where it's in line already, as when it asked
  // again in between, it's left as it is.
  private async requeue(waiter: LeaseWaiter) {
    for (const limit of ordered(waiter.limits)) await this.join(limit, waiter);
  }

  // Put a step in a limit's line, or say it's there already. Then mark that its tenant has
  // steps in line. One that stops in between, as when the worker crashes, is seen when the
  // step, or another of its tenant, asks again.
  private async join(limit: Limit, waiter: LeaseWaiter) {
    let joined = true;
    try {
      await this.db("PutItem", {
        Item: { ...this.entryKey(limit, waiter), ...this.entry(limit, waiter) },
        ConditionExpression: "attribute_not_exists(pk)",
      });
    } catch (error) {
      if (!isConditionFailure(error)) throw error;
      joined = false;
    }
    await this.mark(limit.item, tenantKey(waiter.tenant), linesOf(limit));
    return joined;
  }

  // Mark that a tenant may have steps in a limit's line, and in how many it's split, if it
  // isn't. A limit that was changed may have more lines in runs on another version.
  private async mark(item: string, t: string, lines: number, put = false) {
    const Key = { pk: S(marksKey(item)), sk: S(`w#${t}`) };
    const now = Date.now();
    // A mark isn't taken out within a minute of when it was put, so one that was seen, and is
    // younger, is there.
    const known = this.marks.get(`${item}|${t}`);
    if (!put && known && known.lines >= lines && now < known.at + STALE_LINE_MS - 10_000) return;
    const { Item } = await this.db("GetItem", { Key, ConsistentRead: true });
    const marked = Number(Item?.n?.N ?? 0);
    const seen = (at: number, n: number) => {
      if (this.marks.size > 5000) this.marks.clear();
      this.marks.set(`${item}|${t}`, { at, lines: n });
    };
    // Renewed now and then, so it's kept as long as its line is.
    // One that's seen often is renewed before it's looked up each time.
    const at = Number(Item?.at?.N ?? 0);
    if (!put && Item && marked >= lines && at > now - STALE_LINE_MS / 2) return seen(at, marked);
    await this.db("UpdateItem", {
      Key,
      UpdateExpression: "SET #at = :at, expires = :e, #n = if_not_exists(#n, :zero) + :more",
      // As it was read: one that another has just renewed needn't be.
      ConditionExpression: Item ? "#n = :marked AND #at = :was" : "attribute_not_exists(pk)",
      ExpressionAttributeNames: { "#at": "at", "#n": "n" },
      ExpressionAttributeValues: {
        ":at": N(now),
        ":e": { N: seconds(now + DAY) },
        ":zero": N(0),
        ":more": N(Math.max(0, lines - marked)),
        ...(Item ? { ":marked": N(marked), ":was": N(at) } : {}),
      },
    }).then(
      () => seen(now, Math.max(lines, marked)),
      (error) => {
        // Another marked it, or took the mark out: look again.
        if (isConditionFailure(error)) return this.mark(item, t, lines, put);
        throw error;
      },
    );
  }

  private readonly marks = new Map<string, Mark>();

  // Take out the mark of a tenant whose line was found empty, if it hasn't changed. Then
  // look at the line again: a step that joined in between may have seen the mark, and
  // left it.
  private async unmark(item: string, t: string, mark: Mark) {
    try {
      await this.db("DeleteItem", {
        Key: { pk: S(marksKey(item)), sk: S(`w#${t}`) },
        ConditionExpression: "#at = :at AND #n = :lines",
        ExpressionAttributeNames: { "#at": "at", "#n": "n" },
        ExpressionAttributeValues: { ":at": N(mark.at), ":lines": N(mark.lines) },
      });
    } catch (error) {
      if (isConditionFailure(error)) return;
      throw error;
    }
    if (!(await this.lineEmpty(item, t, mark.lines))) await this.mark(item, t, mark.lines, true);
  }

  // Wake a step that was given its leases. If it can't be woken, give them back: only
  // those, since a step that asked again may have found them and claimed them. One
  // that may not have been told goes back in line, and isn't tried again in this
  // dispatch, so an outage doesn't keep it busy.
  private async tell(w: LeaseWaiter, grant: Granted, wake: (w: LeaseWaiter) => Promise<boolean>, skip: Set<string>) {
    let woken: boolean | undefined;
    try {
      woken = await wake({ ...w, placed: grant.at.map((s) => s.item) });
    } catch {}
    if (woken) return true;
    if (woken === undefined) {
      skip.add(w.leaseId);
      await this.requeue(w).catch(() => {});
    }
    for (const shard of grant.at) await this.releaseOne(shard.item, w.leaseId, w.tenant, grant.value);
    return false;
  }

  async enqueue(waiter: LeaseWaiter, first = false) {
    const limits = ordered(waiter.limits);
    // A step that goes on isn't in line. An entry of it left there, whose callback it
    // won't wait for, would be given leases that no step gives back.
    const held = async () => {
      // It may have been given others in between: renewing it looks everywhere, and gives
      // those back.
      for (const l of limits) this.places.delete(`${l.item}|${waiter.leaseId}`);
      for (const l of limits) await this.leave(l, waiter);
      return "held" as const;
    };
    // One that asks for the first time likely has none, and its limits are in the table: a
    // narrow one, that's one read, says. One that was run again after a crash may have, and
    // is found as it dispatches.
    const narrow = limits.filter((l) => shardsOf(l).length === 1);
    const maybe = !first || (await Promise.all(narrow.map((l) => this.readLimit(l, "none")))).some((s) => s.leases[waiter.leaseId]);
    if (maybe) {
      const states = await Promise.all(limits.map((l) => this.readLimit(l, "none")));
      if (states.every((s) => s.leases[waiter.leaseId]) && (await this.renew(limits, waiter.leaseId, this.grantMs, true))) return held();
      await Promise.all(states.map((s) => this.createSome(s)));
    }
    const entry = this.entry(limits[0], waiter);
    for (let attempt = 0; attempt < 3; attempt++) {
      let gone = false;
      for (const limit of limits) {
        if (await this.join(limit, waiter)) continue;
        // Asking again: the step keeps its place, with its new callback.
        try {
          await this.db("UpdateItem", {
            Key: this.entryKey(limit, waiter),
            UpdateExpression: "SET callback = :cb, expires = :exp",
            ConditionExpression: "attribute_exists(pk)",
            ExpressionAttributeValues: { ":cb": entry.callback, ":exp": entry.expires },
          });
        } catch (error) {
          if (!isConditionFailure(error)) throw error;
          gone = true;
        }
      }
      // A step can be given its leases once it's in its first line. A grant takes it out
      // of its lines in the order it joined them, so if it's still in the first, it's
      // taken out of the rest too. If it isn't, an entry that came after is left over.
      if (!gone && limits.length > 1) {
        const { Item } = await this.db("GetItem", {
          Key: this.entryKey(limits[0], waiter),
          ProjectionExpression: "pk",
          ConsistentRead: true,
        });
        if (!Item) {
          gone = true;
          for (const l of limits.slice(1)) await this.leave(l, waiter);
        }
      }
      if (!gone) return "queued" as const;
      // An entry went in between: the step was given its leases, or given them and
      // they were given back, as its old callback had timed out. Then it joins again.
      const again = await Promise.all(limits.map((l) => this.readLimit(l, "none")));
      if (again.every((s) => s.leases[waiter.leaseId]) && (await this.renew(limits, waiter.leaseId, this.grantMs, true))) return held();
    }
    return "queued" as const;
  }

  async dispatch(limit: Limit, wake: (w: LeaseWaiter) => Promise<boolean>, joined?: LeaseWaiter) {
    if (joined && shardsOf(limit).length > 1) return this.dispatchOne(limit, joined, wake);
    return this.dispatchAll(limit, wake, new Set());
  }

  // Give a step that joined the line its leases, if they're free. Steps that join a wide
  // limit at once would all try to give the same oldest steps the same room.
  private async dispatchOne(limit: Limit, w: LeaseWaiter, wake: (w: LeaseWaiter) => Promise<boolean>) {
    const skip = new Set<string>();
    for (let attempt = 0; attempt < 4; attempt++) {
      const state = await this.readLimit(limit, "none");
      if (await this.reap([state], Date.now())) continue;
      if (state.leases[w.leaseId]) {
        // One it took itself, before it was run again, is only its to take: it goes on with it.
        // One that's being given to it is for who gives it to tell it of.
        if (state.copies(w.leaseId).some((c) => c.lease.granted)) return 0;
        if (!(await this.renew(w.limits, w.leaseId, this.grantMs, true))) return 0;
        for (const l of ordered(w.limits)) {
          this.places.delete(`${l.item}|${w.leaseId}`);
          await this.leave(l, w);
        }
        return (await wake(w).catch(() => false)) ? 1 : 0;
      }
      if (await this.createSome(state)) continue;
      if (state.free <= 0) return 0;
      if (!(await this.hasRoom(w.limits.filter((l) => l.item !== limit.item)))) return 0;
      const grant = await this.grant(w, [limit], { rooms: new Map([[limit.item, state.rooms()]]) });
      if (grant.granted) return (await this.tell(w, grant.granted, wake, skip)) ? 1 : 0;
      // Another is giving it its leases.
      if (grant.raced) return 0;
      // The room went to another, or it was put back: look again.
      await new Promise((r) => setTimeout(r, Math.random() * 40));
    }
    // The room keeps going to others. Leave none free, for this or another step.
    return this.dispatchAll(limit, wake, skip);
  }

  private async dispatchAll(limit: Limit, wake: (w: LeaseWaiter) => Promise<boolean>, skip: Set<string>) {
    let woken = 0;
    let again = 0;
    let races = 0;
    // Bounded by time, not by tries: a step that's gone, as when its run was stopped, is
    // taken out of line, and a step that another dispatch got to first is being woken.
    const end = Date.now() + DISPATCH_MS;
    while (woken < limit.limit + 5 && Date.now() < end) {
      const state = await this.readLimit(limit);
      if (state.missing.length && state.shards.length === state.missing.length) return woken;
      if (await this.reap([state], Date.now())) continue;
      if (await this.createSome(state)) continue;
      if (state.free <= 0) return woken;
      // One at a time, so each is given to whoever is fairest by then. A limit with shards
      // has many to give at once, as many to each tenant as keeps it fair.
      const width = shardsOf(limit).length === 1 ? 1 : Math.min(state.free, 25);
      const rooms = state.rooms();
      let undone = false;
      let raced = false;
      let given = 0;
      const give = async (c: LeaseWaiter) => {
        if (!(await this.hasRoom(c.limits.filter((l) => l.item !== limit.item)))) return;
        const grant = await this.grant(c, [limit], { rooms: new Map([[limit.item, rooms]]) });
        undone ||= grant.undone;
        raced ||= grant.raced;
        if (!grant.granted) return;
        given++;
        // A step that's gone, or asked again with a new callback, gives its leases back
        // to the next one.
        if (await this.tell(c, grant.granted, wake, skip)) woken++;
      };
      let batch: LeaseWaiter[] = [];
      for await (const c of this.candidates(limit, state, skip)) {
        batch.push(c);
        if (batch.length < width - given) continue;
        await Promise.all(batch.map(give));
        batch = [];
        if (given >= width) break;
      }
      await Promise.all(batch.map(give));
      if (given) continue;
      // Another dispatch got to a step first: there may be room, and another step.
      if (raced) {
        await new Promise((r) => setTimeout(r, 5 + Math.random() * Math.min(500, 20 * 2 ** races++)));
        continue;
      }
      // No step in line can have all it needs now. A release of the limit it's missing
      // gives it to it. One that was taken out of line, or whose lease was taken, and was
      // put back, was gone for a moment, and another dispatch may have missed it: look
      // again, a few times.
      if (!undone || ++again > 3) return woken;
      await new Promise((r) => setTimeout(r, Math.random() * 20));
    }
    return woken;
  }

  // The steps to give leases to, fairest first: each tenant's oldest step in line, the
  // tenant with the fewest leases first, then the oldest step. A step that can't have all
  // it needs, as when its own tenant's limit is full, doesn't keep the lease from the
  // next. Tenants are looked at 16 at a time, at random among those with as many leases,
  // up to MAX_TENANTS in all.
  private async *candidates(limit: Limit, state: LimitState, skip: Set<string>): AsyncGenerator<LeaseWaiter> {
    const held = (t: string) => state.tenants[t] ?? 0;
    const tenants = Object.keys(state.waiting)
      .map((t) => ({ t, tie: Math.random() }))
      .sort((a, b) => held(a.t) - held(b.t) || a.tie - b.tie)
      .slice(0, MAX_TENANTS);
    // The fairest, then the next fairest if it's given one, and so on.
    const given: Record<string, number> = Object.create(null);
    const load = (w: LeaseWaiter) => held(tenantKey(w.tenant)) + (given[w.tenant] ?? 0);
    const chunk = shardsOf(limit).length === 1 ? 16 : 3;
    for (let from = 0; from < tenants.length; from += chunk) {
      const lines = (await Promise.all(tenants.slice(from, from + chunk).map(({ t }) => this.heads(limit, state, t, skip)))).filter((l) => l.length);
      while (lines.length) {
        let best = 0;
        for (let i = 1; i < lines.length; i++) {
          const d = load(lines[i][0]) - load(lines[best][0]);
          if (d < 0 || (d === 0 && lines[i][0].at < lines[best][0].at)) best = i;
        }
        const next = lines[best].shift()!;
        if (!lines[best].length) lines.splice(best, 1);
        given[next.tenant] = (given[next.tenant] ?? 0) + 1;
        yield next;
      }
    }
  }

  // A tenant's oldest step in line that can be given a lease. Of a limit with shards, many
  // are given back at once, so it's some of the oldest, from one of its lines, in any
  // order: dispatches that all went for the one oldest step would get in each other's way.
  private async heads(limit: Limit, state: LimitState, t: string, skip: Set<string>): Promise<LeaseWaiter[]> {
    const mark = state.waiting[t];
    const wide = shardsOf(limit).length > 1;
    const wanted = wide ? 12 : 1;
    const found: LeaseWaiter[] = [];
    let empty = true;
    for (const line of shuffle(Array.from({ length: mark.lines }, (_, i) => i))) {
      let start: any;
      for (let pages = 0; pages < 10 && found.length < wanted; pages++) {
        const page = await this.db("Query", {
          KeyConditionExpression: "pk = :pk",
          ExpressionAttributeValues: { ":pk": S(lineKey(limit.item, t, line)) },
          ConsistentRead: true,
          Limit: wide ? 12 : pages ? 25 : 3,
          ExclusiveStartKey: start,
        });
        if (page.Items?.length) empty = false;
        for (const e of page.Items ?? []) {
          // A step that holds its lease here is being given it right now, or was put back in
          // line and had been told after all. One that couldn't be told it was given one
          // waits for the next dispatch.
          // One that's claimed is being given its leases by another.
          if (
            found.length >= wanted ||
            state.leases[String(e.lease.S)] ||
            skip.has(String(e.lease.S)) ||
            Number(e.claimed?.N ?? 0) > Date.now()
          )
            continue;
          found.push({
            leaseId: String(e.lease.S),
            tenant: String(e.tenant.S),
            callbackId: String(e.callback.S),
            runId: String(e.run.S),
            at: Number(e.at.N),
            // As it was put in line, so it keeps it if it's put back.
            expiresAt: Number(e.expires.N) * 1000 - DAY,
            limits: JSON.parse(e.limits.S),
          });
        }
        start = page.LastEvaluatedKey;
        if (!start) break;
      }
      if (found.length) break;
    }
    // Kept a while after its line empties, so steps that come and go don't write it each time.
    if (empty && Date.now() - mark.at > STALE_LINE_MS) await this.unmark(limit.item, t, mark).catch(() => {});
    return wide ? shuffle(found) : found;
  }

  async handoff(limits: Limit[], leaseId: string, tenant: string, wake: (w: LeaseWaiter) => Promise<boolean>) {
    const unique = uniqueLimits(limits);
    const skip = new Set<string>();
    try {
      await this.pass(unique, leaseId, tenant, wake, skip);
    } catch (error) {
      // Give back what it still holds, so an error doesn't keep it until it expires.
      for (const l of unique) await this.releaseFrom(l, leaseId, tenant).catch(() => {});
      for (const l of unique) await this.dispatchAll(l, wake, skip).catch(() => {});
      throw error;
    }
  }

  private async pass(
    unique: Limit[],
    leaseId: string,
    tenant: string,
    wake: (w: LeaseWaiter) => Promise<boolean>,
    skip: Set<string>,
  ) {
    // The limit with the most tenants in line first: that's where it's decided whose
    // turn is fair. A tenant's own limit has only its own steps in line.
    // A moment old will do: what's passed is decided as it's written.
    const states = await Promise.all(unique.map((l) => this.readLimit(l, "all", shardsOf(l).length === 1)));
    const at = new Map(
      unique.flatMap((l, i) => {
        const shard = states[i].copies(leaseId)[0]?.shard ?? this.places.get(`${l.item}|${leaseId}`);
        return shard ? [[l.item, shard] as const] : [];
      }),
    );
    const tenantsInLine = (i: number) => Object.keys(states[i].waiting).length;
    const order = unique.map((_, i) => i).sort((a, b) => tenantsInLine(b) - tenantsInLine(a) || unique[b].limit - unique[a].limit);
    for (const i of order) {
      const shard = at.get(unique[i].item);
      if (!shard) continue;
      // Over the limit, as when it was lowered: give it back.
      const there = states[i].shards.find((s) => s.limit.item === shard.item);
      if (!there) break;
      if (Object.keys(there.leases).length > there.limit.limit || states[i].held > unique[i].limit) break;
      for await (const next of this.candidates(unique[i], states[i], skip)) {
        const { granted, passed } = await this.grant(next, unique, { from: { leaseId, tenant, at } });
        if (!granted) continue;
        // Give back what wasn't passed: a limit the next step doesn't need, or one it
        // got a lease of its own on.
        const kept = unique.filter((l) => !passed?.some((p) => p.item === l.item));
        await this.release(kept, leaseId, tenant);
        for (const l of unique) this.places.delete(`${l.item}|${leaseId}`);
        if (!(await this.tell(next, granted, wake, skip))) for (const l of uniqueLimits(next.limits)) await this.dispatchAll(l, wake, skip);
        for (const l of kept) await this.dispatchAll(l, wake, skip);
        return;
      }
      break;
    }
    await this.release(unique, leaseId, tenant);
    for (const i of order) await this.dispatchAll(unique[i], wake, skip);
  }

  // Whether each limit has room, or didn't when it was looked at. Looking first saves
  // taking one lease to give it back, as when a tenant's own limit is full.
  private async hasRoom(limits: Limit[]) {
    const rooms = await Promise.all(
      uniqueLimits(limits).map(async (l) => {
        if (shardsOf(l).length > 1) return (await this.readLimit(l, "none")).free > 0;
        const { Item } = await this.db("GetItem", {
          Key: { pk: S(l.item), sk: S("-") },
          ProjectionExpression: "active, shards",
          ConsistentRead: true,
        });
        if (!Item || Number(Item.active?.N ?? 0) >= l.limit) return false;
        return Number(Item.shards?.N ?? 1) === 1 || (await this.readLimit(l, "none")).free > 0;
      }),
    );
    return rooms.every(Boolean);
  }

  private async room(shard: Limit) {
    const { Item } = await this.db("GetItem", {
      Key: { pk: S(shard.item), sk: S("-") },
      ProjectionExpression: "active",
      ConsistentRead: true,
    });
    return Item ? shard.limit - Number(Item.active?.N ?? 0) : 0;
  }

  // Give a step in line every lease it needs, or none. First its entry is claimed, which one
  // grant only can do at a time: two can't give it leases at once, and one that's late has
  // only tried to write an entry. If it can't have them all, the claim is given up, and it's
  // in its place. If it can, its entry is taken out, which only its claim's own can do, and
  // then its other lines'. So an entry is never put back: one that's gone is a step that has
  // gone on. With `from`, a step that's done gives its lease on a limit they share straight to
  // it, in one write. It returns the leases, to give back only those, and which were passed.
  // The step that's done still holds the others.
  // A worker that stops part way leaves a claim that runs out, and leases that expire.
  // `current` are the limits as the caller has them: a step's are as it joined the line,
  // and a limit that was lowered since is held to.
  private async grant(
    w: LeaseWaiter,
    current: Limit[],
    with_: { from?: { leaseId: string; tenant: string; at: Map<string, Limit> }; rooms?: Map<string, Rooms> } = {},
  ): Promise<Grant> {
    const { from, rooms } = with_;
    const value = leaseValue(Date.now() + this.grantMs, w.tenant, "granted");
    // Its entries are where it joined: in the lines of the limits as it has them.
    const joined = ordered(w.limits);
    const [first, ...rest] = joined;
    const mine = joined.map((l) => ({
      ...l,
      limit: Math.min(l.limit, current.find((c) => c.item === l.item)?.limit ?? l.limit),
    }));
    // Its entry as it was read: one that asked again with a new callback isn't given leases
    // with the old one.
    const claim = value.split("|")[1];
    const claimed = await this.claimEntry(first, w, claim).catch(() => "unknown" as const);
    if (claimed === "gone") for (const l of rest) await this.leave(l, w, w.callbackId).catch(() => {});
    if (claimed !== "claimed") return { undone: false, raced: claimed !== "unknown" };

    const passed: Limit[] = [];
    const at: Limit[] = [];
    const undo = async (): Promise<Grant> => {
      for (const shard of at) await this.releaseOne(shard.item, w.leaseId, w.tenant, value).catch(() => {});
      await this.unclaimEntry(first, w, claim).catch(() => {});
      return { undone: true, raced: false };
    };
    try {
      for (const limit of mine) {
        // The shard the step that's done has its lease in, if this step may use it.
        const theirs = from?.at.get(limit.item);
        const shard = theirs && shardsOf(limit).find((s) => s.item === theirs.item);
        if (from && shard) {
          // Given back by its value on an error, which may come after it was written.
          at.push(shard);
          if (await this.passOne(shard, from, w, value)) {
            passed.push(limit);
            continue;
          }
          at.pop();
        }
        // The step that's done no longer holds it, or it isn't one it has: take it
        // like any other.
        const took = await this.takeAny(limit, w.leaseId, w.tenant, value, rooms?.get(limit.item));
        // Full, or held by the step itself, as it asked again and found room.
        if (typeof took !== "object") return await undo();
        at.push(took);
      }
    } catch (error) {
      await undo().catch(() => {});
      throw error;
    }
    // Taken out, or gone, or it can't be known: the step is told, and where it has gone on,
    // its callback is closed. One that asked again in between is told by its new callback.
    if ((await this.finishEntry(first, w, claim).catch(() => "unknown" as const)) === "rejoined") return undo();
    for (const l of rest) await this.leave(l, w).catch(() => {});
    return { granted: { value, at }, passed, undone: false, raced: false };
  }

  private async claimEntry(limit: Limit, w: LeaseWaiter, claim: string): Promise<"claimed" | "busy" | "rejoined" | "gone"> {
    const now = Date.now();
    try {
      await this.db("UpdateItem", {
        Key: this.entryKey(limit, w),
        UpdateExpression: "SET #claim = :c, claimed = :until",
        // The same claim again, as when its answer was lost, is this one's.
        ConditionExpression: "callback = :cb AND (attribute_not_exists(#claim) OR #claim = :c OR claimed < :now)",
        ExpressionAttributeNames: { "#claim": "claim" },
        ExpressionAttributeValues: { ":c": S(claim), ":cb": S(w.callbackId), ":until": N(now + CLAIM_MS), ":now": N(now) },
        ReturnValuesOnConditionCheckFailure: "ALL_OLD",
      });
      return "claimed";
    } catch (error) {
      if (!isConditionFailure(error)) throw error;
      const item = (error as AwsError).body?.Item;
      return !item ? "gone" : item.callback?.S === w.callbackId ? "busy" : "rejoined";
    }
  }

  private async unclaimEntry(limit: Limit, w: LeaseWaiter, claim: string) {
    await this.db("UpdateItem", {
      Key: this.entryKey(limit, w),
      UpdateExpression: "REMOVE #claim, claimed",
      ConditionExpression: "#claim = :c",
      ExpressionAttributeNames: { "#claim": "claim" },
      ExpressionAttributeValues: { ":c": S(claim) },
    }).catch((error) => {
      if (!isConditionFailure(error)) throw error;
    });
  }

  private async finishEntry(limit: Limit, w: LeaseWaiter, claim: string): Promise<"done" | "rejoined" | "gone"> {
    try {
      await this.db("DeleteItem", {
        Key: this.entryKey(limit, w),
        ConditionExpression: "#claim = :c AND callback = :cb",
        ExpressionAttributeNames: { "#claim": "claim" },
        ExpressionAttributeValues: { ":c": S(claim), ":cb": S(w.callbackId) },
        ReturnValuesOnConditionCheckFailure: "ALL_OLD",
      });
      return "done";
    } catch (error) {
      if (!isConditionFailure(error)) throw error;
      const item = (error as AwsError).body?.Item;
      return item ? "rejoined" : "gone";
    }
  }

  // Take a step out of a line: with `callbackId`, if its entry still has that callback.
  private async leave(limit: Limit, waiter: Pick<LeaseWaiter, "tenant" | "at" | "leaseId">, callbackId?: string) {
    try {
      await this.db(
        "DeleteItem",
        {
          Key: this.entryKey(limit, waiter),
          ConditionExpression: `attribute_exists(pk)${callbackId === undefined ? "" : " AND callback = :cb"}`,
          ...(callbackId === undefined ? {} : { ExpressionAttributeValues: { ":cb": S(callbackId) } }),
          ReturnValuesOnConditionCheckFailure: "ALL_OLD",
        },
      );
    } catch (error) {
      if (!isConditionFailure(error)) throw error;
      return (error as AwsError).body?.Item ? ("rejoined" as const) : ("gone" as const);
    }
    return "left" as const;
  }

  // Pass a lease from a step that's done to the next, in one write.
  private async passOne(limit: Limit, from: { leaseId: string; tenant: string }, w: LeaseWaiter, value: string) {
    const t = tenantKey(w.tenant);
    const ot = tenantKey(from.tenant);
    const same = ot === t;
    try {
      await this.db("UpdateItem", {
        Key: { pk: S(limit.item), sk: S("-") },
        UpdateExpression: `REMOVE leases.#o SET leases.#l = :lease, expires = :exp${same ? "" : " ADD tenants.#t :one, tenants.#ot :minus"}`,
        ConditionExpression: "attribute_exists(leases.#o) AND attribute_not_exists(leases.#l) AND active <= :cap",
        ExpressionAttributeNames: { "#o": from.leaseId, "#l": w.leaseId, ...(same ? {} : { "#t": t, "#ot": ot }) },
        ExpressionAttributeValues: {
          ":lease": S(value),
          ":cap": N(limit.limit),
          ":exp": { N: seconds(Date.now() + DAY) },
          ...(same ? {} : { ":one": N(1), ":minus": N(-1) }),
        },
        ReturnValuesOnConditionCheckFailure: "ALL_OLD",
      });
    } catch (error) {
      if (!isConditionFailure(error)) throw error;
      if (leaseIn(error, w.leaseId) !== value) return false;
    }
    if (!same) await this.dropTenant(limit.item, from.tenant);
    return true;
  }

  // A limit's leases, from its shards, and the tenants marked as in its line: all of them,
  // or the first ones, or none.
  private async readLimit(limit: Limit, line: "all" | "some" | "none" = "all", consistent = true): Promise<LimitState> {
    const own = shardsOf(limit);
    const read = (l: Limit) => this.db("GetItem", { Key: { pk: S(l.item), sk: S("-") }, ConsistentRead: consistent });
    const [ownItems, marks] = await Promise.all([
      Promise.all(own.map(read)),
      line === "none" ? [] : this.readMarks(limit.item, line === "some"),
    ]);
    // Shards of a version with a higher limit: their leases count too, and none is given there.
    const used = Number(ownItems[0].Item?.shards?.N ?? 1);
    const others: Limit[] = [];
    for (let i = own.length; i < used; i++) others.push({ item: `${limit.item}.${i}`, limit: 0, label: limit.label });
    const items = [...ownItems, ...(await Promise.all(others.map(read)))];
    return this.stateOf(limit, own, others, items, marks);
  }

  // A limit, as it is in some of its shards.
  private async readShards(limit: Limit, shards: Limit[]) {
    const items = await Promise.all(shards.map((l) => this.db("GetItem", { Key: { pk: S(l.item), sk: S("-") }, ConsistentRead: true })));
    return this.stateOf(limit, shards, [], items, []);
  }

  private stateOf(limit: Limit, own: Limit[], others: Limit[], items: any[], marks: any[]): LimitState {
    const limits = [...own, ...others];
    const tenants: Record<string, number> = Object.create(null);
    const leases: Record<string, Lease> = Object.create(null);
    const missing: Limit[] = [];
    const shards = limits.map((l, i): ShardState => {
      const shard: ShardState = { limit: l, leases: Object.create(null), raw: Object.create(null) };
      const item = items[i].Item;
      if (!item && i < own.length) missing.push(l);
      for (const [id, value] of Object.entries<any>(item?.leases?.M ?? {})) {
        shard.raw[id] = String(value.S);
        leases[id] = shard.leases[id] = parseLease(shard.raw[id]);
      }
      for (const [t, v] of Object.entries<any>(item?.tenants?.M ?? {})) tenants[t] = (tenants[t] ?? 0) + Number(v.N);
      return shard;
    });
    const held = shards.reduce((n, s) => n + Object.keys(s.leases).length, 0);
    // What the shards have room for, and the limit, with the leases elsewhere.
    const most = Math.max(0, limit.limit - held);
    const room = (s: ShardState) =>
      missing.includes(s.limit) || others.length > 0 && most === 0 ? 0 : Math.max(0, s.limit.limit - Object.keys(s.leases).length);
    const waiting: Record<string, Mark> = Object.create(null);
    for (const i of marks) waiting[String(i.sk.S).slice(2)] = { at: Number(i.at?.N ?? 0), lines: Number(i.n?.N ?? 1) };
    return {
      limit,
      shards,
      missing,
      held,
      leases,
      tenants,
      waiting,
      free: Math.min(most, shards.reduce((n, s) => n + room(s), 0)),
      rooms: () => new Map(shards.map((s) => [s.limit.item, room(s)])),
      copies: (id) => shards.filter((s) => s.leases[id]).map((s) => ({ shard: s.limit, lease: s.leases[id], raw: s.raw[id] })),
    };
  }

  private async readMarks(item: string, some: boolean) {
    const items: any[] = [];
    let start: any;
    do {
      const page = await this.db("Query", {
        KeyConditionExpression: "pk = :pk AND begins_with(sk, :w)",
        ExpressionAttributeValues: { ":pk": S(marksKey(item)), ":w": S("w#") },
        ConsistentRead: true,
        ExclusiveStartKey: start,
        ...(some ? { Limit: 17 } : {}),
      });
      items.push(...(page.Items ?? []));
      start = some ? undefined : page.LastEvaluatedKey;
    } while (start);
    return items;
  }

  // Put some of a limit's shards in the table, where the ones it has are full. A few, at
  // random, so steps that ask at once share the work.
  private async createSome(state: LimitState) {
    if (!state.missing.length || state.free > 0) return false;
    // Others, of a version with a higher limit, may hold them all.
    if (state.held >= state.limit.limit) return false;
    const some = shuffle([...state.missing]).slice(0, 3);
    // Before a shard has a lease, the limit says it has shards, so a version with a lower
    // limit looks at them too.
    const used = Math.max(...some.map((l) => state.shards.findIndex((s) => s.limit.item === l.item))) + 1;
    if (used > 1) await this.widen(state.limit, used);
    await Promise.all(some.map((l) => this.createLimit(l)));
    return true;
  }

  // Note how many shards a limit has used.
  private async widen(limit: Limit, shards: number) {
    await this.createLimit(shardsOf(limit)[0]);
    await this.db("UpdateItem", {
      Key: { pk: S(limit.item), sk: S("-") },
      UpdateExpression: "SET shards = :n",
      ConditionExpression: "attribute_not_exists(shards) OR shards < :n",
      ExpressionAttributeValues: { ":n": N(shards) },
    }).catch((error) => {
      if (!isConditionFailure(error)) throw error;
    });
  }

  private async createLimit(limit: Limit) {
    try {
      await this.db("PutItem", {
        Item: {
          pk: S(limit.item),
          sk: S("-"),
          active: N(0),
          leases: { M: {} },
          tenants: { M: {} },
          label: S(limit.label ?? limit.item),
          cap: N(limit.limit),
          expires: { N: seconds(Date.now() + DAY) },
        },
        ConditionExpression: "attribute_not_exists(pk)",
      });
    } catch (error) {
      if (!isConditionFailure(error)) throw error;
    }
  }

  async waitFields(name: string, tenant: string | undefined) {
    const result = await this.db("Query", {
      KeyConditionExpression: "pk = :pk",
      FilterExpression: "#u > :now",
      ExpressionAttributeNames: { "#u": "until" },
      ExpressionAttributeValues: {
        ":pk": S(fieldsKey(name, tenant)),
        ":now": N(Date.now()),
      },
      ConsistentRead: true,
    });
    return (result.Items ?? []).map((item: any) => String(item.sk.S));
  }

  async addWaitField(name: string, tenant: string | undefined, field: string, until: number) {
    try {
      await this.db("PutItem", {
        Item: {
          pk: S(fieldsKey(name, tenant)),
          sk: S(field),
          until: N(until),
          expires: { N: seconds(until + 3_600_000) },
        },
        ConditionExpression: "attribute_not_exists(#u) OR #u < :until",
        ExpressionAttributeNames: { "#u": "until" },
        ExpressionAttributeValues: { ":until": N(until) },
      });
    } catch (error) {
      // A wait that lasts longer already keeps the field.
      if (!isConditionFailure(error)) throw error;
    }
  }

  async putWaiter(key: string, waiter: Waiter) {
    await this.db("PutItem", {
      Item: {
        pk: S(key),
        sk: S(waiterSort(waiter)),
        callback: S(waiter.callbackId),
        run: S(waiter.runId),
        created: N(waiter.createdAt),
        until: N(waiter.expiresAt),
        kind: S(waiter.kind ?? "wait"),
        ...(waiter.condition ? { condition: S(waiter.condition) } : {}),
        ...(waiter.parent ? { parent: S(waiter.parent) } : {}),
        ...(waiter.info ? { info: S(JSON.stringify(waiter.info)) } : {}),
        expires: { N: seconds(waiter.expiresAt + 3_600_000) },
      },
    });
  }

  async waiters(key: string, now: number) {
    const items: any[] = [];
    let start: any;
    do {
      const result = await this.db("Query", {
        KeyConditionExpression: "pk = :pk",
        FilterExpression: "#u > :now",
        ExpressionAttributeNames: { "#u": "until" },
        ExpressionAttributeValues: { ":pk": S(key), ":now": N(now) },
        ConsistentRead: true,
        ExclusiveStartKey: start,
      });
      items.push(...(result.Items ?? []));
      start = result.LastEvaluatedKey;
    } while (start);
    return items.map(
      (item): Waiter => ({
        callbackId: String(item.callback.S),
        runId: String(item.run.S),
        createdAt: Number(item.created.N),
        expiresAt: Number(item.until.N),
        condition: item.condition?.S,
        kind: item.kind?.S === "cancel" ? "cancel" : "wait",
        parent: item.parent?.S,
      }),
    );
  }

  async deleteWaiter(key: string, waiter: Pick<Waiter, "createdAt" | "callbackId">) {
    await this.db("DeleteItem", {
      Key: { pk: S(key), sk: S(waiterSort(waiter)) },
    });
  }

  async putRecent(key: string, event: WireEvent, receivedAt: number, ttlMs: number) {
    await this.db("PutItem", {
      Item: {
        pk: S(key),
        sk: S(`${pad(receivedAt)}#${event.id}`),
        event: S(JSON.stringify(event)),
        expires: { N: seconds(receivedAt + ttlMs) },
      },
    });
  }

  async recent(key: string, since: number) {
    const out: { event: WireEvent; receivedAt: number }[] = [];
    let start: any;
    do {
      const result = await this.db("Query", {
        KeyConditionExpression: "pk = :pk AND sk >= :since",
        ExpressionAttributeValues: { ":pk": S(key), ":since": S(pad(since)) },
        ConsistentRead: true,
        ExclusiveStartKey: start,
      });
      for (const item of result.Items ?? [])
        out.push({
          event: JSON.parse(item.event.S) as WireEvent,
          receivedAt: Number(String(item.sk.S).split("#")[0]),
        });
      start = result.LastEvaluatedKey;
    } while (start);
    return out;
  }

  async debounceMerge(item: string, event: WireEvent, order: string, received: string, now: number, ttlMs: number) {
    const Key = { pk: S(item), sk: S("-") };
    // The latest of the events the window had, replaced ones too: none of them, delivered
    // again once it's closed, opens another.
    const had = () =>
      this.db("UpdateItem", {
        Key,
        UpdateExpression: "SET received = :r",
        ConditionExpression: "attribute_exists(#o) AND received < :r",
        ExpressionAttributeNames: { "#o": "order" },
        ExpressionAttributeValues: { ":r": S(received) },
      }).catch((error) => {
        if (!isConditionFailure(error)) throw error;
      });
    try {
      const result = await this.db("UpdateItem", {
        Key,
        UpdateExpression:
          "SET #e = :e, #o = :o, #l = :now, opened = if_not_exists(opened, :now), received = if_not_exists(received, :r), expires = :x",
        // Newer than the one that's kept, or the first since the last window closed, and
        // not one that window had.
        ConditionExpression:
          "attribute_not_exists(closing) AND ((attribute_exists(#o) AND #o <= :o) OR (attribute_not_exists(#o) AND (attribute_not_exists(fired) OR fired < :r)))",
        ExpressionAttributeNames: { "#e": "event", "#o": "order", "#l": "last" },
        ExpressionAttributeValues: {
          ":e": S(JSON.stringify(event)),
          ":o": S(order),
          ":r": S(received),
          ":now": N(now),
          ":x": { N: seconds(now + ttlMs) },
        },
        ReturnValues: "ALL_NEW",
        ReturnValuesOnConditionCheckFailure: "ALL_OLD",
      });
      if (String(result.Attributes.received.S) < received) await had();
      return { opened: Number(result.Attributes.opened.N), last: Number(result.Attributes.last.N) };
    } catch (error) {
      if (!isConditionFailure(error)) throw error;
      if ((error as AwsError).body?.Item?.closing) return "closing" as const;
      await had();
      return "stale" as const;
    }
  }

  async debounceRead(item: string): Promise<Debounce | undefined> {
    const { Item } = await this.db("GetItem", { Key: { pk: S(item), sk: S("-") }, ConsistentRead: true });
    if (!Item?.order) return undefined;
    return {
      event: JSON.parse(Item.event.S),
      order: String(Item.order.S),
      opened: Number(Item.opened.N),
      last: Number(Item.last.N),
      closing: Boolean(Item.closing),
    };
  }

  async debounceClose(item: string, order: string) {
    try {
      await this.db("UpdateItem", {
        Key: { pk: S(item), sk: S("-") },
        UpdateExpression: "SET closing = :t",
        ConditionExpression: "#o = :o",
        ExpressionAttributeNames: { "#o": "order" },
        ExpressionAttributeValues: { ":t": { BOOL: true }, ":o": S(order) },
      });
      return true;
    } catch (error) {
      if (isConditionFailure(error)) return false;
      throw error;
    }
  }

  async debounceDone(item: string, order: string) {
    try {
      await this.db("UpdateItem", {
        Key: { pk: S(item), sk: S("-") },
        UpdateExpression: "SET fired = received REMOVE closing, #o, #e, opened, #l, received",
        ConditionExpression: "#o = :o AND attribute_exists(closing)",
        ExpressionAttributeNames: { "#o": "order", "#e": "event", "#l": "last" },
        ExpressionAttributeValues: { ":o": S(order) },
      });
    } catch (error) {
      if (!isConditionFailure(error)) throw error;
    }
  }

  // A batch's events that aren't in a closed one are in order at "q#…" under the batch, and
  // each event is an item of its own, which says once it's been in one. A closed batch is
  // its list of events at "m#<n>". At "-" is the number of the next one to close.
  async batchAdd(item: string, ref: string, order: string, event: WireEvent, at: number, ttlMs: number) {
    const body = JSON.stringify(event);
    const expires = { N: seconds(at + ttlMs) };
    try {
      await this.db("PutItem", {
        Item: { pk: S(ref), sk: S("-"), event: S(body), order: S(order), expires },
        ConditionExpression: "attribute_not_exists(pk)",
        ReturnValuesOnConditionCheckFailure: "ALL_OLD",
      });
    } catch (error) {
      if (!isConditionFailure(error)) throw error;
      const there = (error as AwsError).body?.Item;
      if (there?.batch) return false;
      // In its place, though another message brought it, as from the dead-letter queue.
      if (there?.order) {
        order = String(there.order.S);
        at = Number(order.split("#")[0]) || at;
      }
    }
    await this.db("PutItem", {
      Item: { pk: S(item), sk: S(`q#${order}`), ref: S(ref), at: N(at), bytes: N(Buffer.byteLength(body) + (event.blob?.bytes ?? 0)), expires },
    });
    return true;
  }

  async batchPending(item: string, max: number): Promise<Pending[]> {
    const items: any[] = [];
    let start: any;
    do {
      const page = await this.db("Query", {
        KeyConditionExpression: "pk = :pk AND begins_with(sk, :q)",
        ExpressionAttributeValues: { ":pk": S(item), ":q": S("q#") },
        ConsistentRead: true,
        Limit: max - items.length,
        ExclusiveStartKey: start,
      });
      items.push(...(page.Items ?? []));
      start = page.LastEvaluatedKey;
    } while (start && items.length < max);
    return items.map((i) => ({ order: String(i.sk.S).slice(2), at: Number(i.at.N), bytes: Number(i.bytes.N), ref: String(i.ref.S) }));
  }

  async batchEvents(refs: string[]) {
    const out: WireEvent[] = [];
    for (let i = 0; i < refs.length; i += 20) {
      const items = await Promise.all(
        refs.slice(i, i + 20).map((ref) => this.db("GetItem", { Key: { pk: S(ref), sk: S("-") }, ConsistentRead: true })),
      );
      for (const { Item } of items) if (Item?.event) out.push(JSON.parse(Item.event.S));
    }
    return out;
  }

  async batchClose(item: string, max: number, choose: (pending: Pending[]) => Pending[], ttlMs: number) {
    for (let attempt = 0; attempt < 5; attempt++) {
      const head = await this.db("GetItem", { Key: { pk: S(item), sk: S("-") }, ConsistentRead: true });
      const n = Number(head.Item?.next?.N ?? 0);
      const closed = await this.manifest(item, n);
      if (closed) return { n, ...closed };
      const pending = await this.batchPending(item, max);
      const chosen = choose(pending).filter((p, i, all) => all.findIndex((o) => o.ref === p.ref) === i);
      if (!chosen.length) return undefined;
      // One that was put back in line after it was in a batch, as when its message came
      // again just then, is taken out.
      const states = await Promise.all(
        chosen.map((p) => this.db("GetItem", { Key: { pk: S(p.ref), sk: S("-") }, ProjectionExpression: "#b", ExpressionAttributeNames: { "#b": "batch" }, ConsistentRead: true })),
      );
      const used = chosen.filter((_, i) => !states[i].Item || states[i].Item.batch);
      if (used.length) {
        await Promise.all(used.map((p) => this.db("DeleteItem", { Key: { pk: S(item), sk: S(`q#${p.order}`) } })));
        continue;
      }
      const refs = chosen.map((p) => p.ref);
      try {
        await this.db("PutItem", {
          Item: {
            pk: S(item),
            sk: S(`m#${pad(n)}`),
            refs: S(JSON.stringify(refs)),
            orders: S(JSON.stringify(chosen.map((p) => p.order))),
            at: N(chosen[0].at),
            expires: { N: seconds(Date.now() + ttlMs) },
          },
          ConditionExpression: "attribute_not_exists(pk)",
        });
        return { n, refs, at: chosen[0].at };
      } catch (error) {
        if (!isConditionFailure(error)) throw error;
        // Another closed it first: finish that one.
      }
    }
    throw new Error("A batch is busy.");
  }

  private async manifest(item: string, n: number): Promise<{ refs: string[]; at: number } | undefined> {
    const { Item } = await this.db("GetItem", { Key: { pk: S(item), sk: S(`m#${pad(n)}`) }, ConsistentRead: true });
    return Item ? { refs: JSON.parse(Item.refs.S), at: Number(Item.at.N) } : undefined;
  }

  async batchRead(item: string, n: number) {
    return (await this.manifest(item, n))?.refs ?? [];
  }

  async batchDone(item: string, n: number) {
    const { Item } = await this.db("GetItem", { Key: { pk: S(item), sk: S(`m#${pad(n)}`) }, ConsistentRead: true });
    if (!Item) return;
    const refs: string[] = JSON.parse(Item.refs.S);
    const orders: string[] = JSON.parse(Item.orders.S);
    // Each event says it's been in a batch before the next can be closed, so none is in two.
    for (let i = 0; i < refs.length; i += 20)
      await Promise.all(
        refs.slice(i, i + 20).map((ref) =>
          this.db("UpdateItem", {
            Key: { pk: S(ref), sk: S("-") },
            UpdateExpression: "SET #b = :b",
            ConditionExpression: "attribute_exists(pk)",
            ExpressionAttributeNames: { "#b": "batch" },
            ExpressionAttributeValues: { ":b": S(`${item}#${n}`) },
          }).catch((error) => {
            if (!isConditionFailure(error)) throw error;
          }),
        ),
      );
    for (let i = 0; i < orders.length; i += 20)
      await Promise.all(orders.slice(i, i + 20).map((order) => this.db("DeleteItem", { Key: { pk: S(item), sk: S(`q#${order}`) } })));
    try {
      await this.db("UpdateItem", {
        Key: { pk: S(item), sk: S("-") },
        UpdateExpression: "SET #n = :next, expires = :x",
        ConditionExpression: "attribute_not_exists(#n) OR #n < :next",
        ExpressionAttributeNames: { "#n": "next" },
        // The numbers of batches outlive the batches, so a number isn't used again.
        ExpressionAttributeValues: { ":next": N(n + 1), ":x": { N: seconds(Date.now() + 400 * DAY) } },
      });
    } catch (error) {
      if (!isConditionFailure(error)) throw error;
    }
  }

  async lockTake(item: string, lock: Lock, ttlMs: number) {
    try {
      await this.db("PutItem", {
        Item: { pk: S(item), sk: S("-"), ...lockItem(lock), expires: { N: seconds(Date.now() + ttlMs) } },
        ConditionExpression: "attribute_not_exists(pk)",
        ReturnValuesOnConditionCheckFailure: "ALL_OLD",
      });
      return undefined;
    } catch (error) {
      if (!isConditionFailure(error)) throw error;
      const held = lockOf((error as AwsError).body?.Item);
      if (!held) return this.lockRead(item);
      return held.run === lock.run ? undefined : held;
    }
  }

  async lockRead(item: string) {
    const { Item } = await this.db("GetItem", { Key: { pk: S(item), sk: S("-") }, ConsistentRead: true });
    return lockOf(Item);
  }

  async lockStarted(item: string, run: string, arn: string) {
    await this.ifHeld(item, run, { UpdateExpression: "SET arn = :arn", ExpressionAttributeValues: { ":arn": S(arn) } });
  }

  async lockMove(item: string, from: string, to: Lock, ttlMs: number) {
    const set = { ...lockItem(to), expires: { N: seconds(Date.now() + ttlMs) } };
    try {
      await this.db("PutItem", {
        Item: { pk: S(item), sk: S("-"), ...set },
        ConditionExpression: "#run = :held",
        ExpressionAttributeNames: { "#run": "run" },
        ExpressionAttributeValues: { ":held": S(from) },
        ReturnValuesOnConditionCheckFailure: "ALL_OLD",
      });
      return true;
    } catch (error) {
      if (!isConditionFailure(error)) throw error;
      // Moved already, as when the answer to it was lost.
      return (error as AwsError).body?.Item?.run?.S === to.run;
    }
  }

  async lockSettled(item: string, run: string) {
    await this.ifHeld(item, run, { UpdateExpression: "REMOVE replaces", ExpressionAttributeValues: {} });
  }

  private async ifHeld(item: string, run: string, update: any) {
    try {
      await this.db("UpdateItem", {
        Key: { pk: S(item), sk: S("-") },
        ...update,
        ConditionExpression: "#run = :held",
        ExpressionAttributeNames: { "#run": "run", ...(update.ExpressionAttributeNames ?? {}) },
        ExpressionAttributeValues: { ...update.ExpressionAttributeValues, ":held": S(run) },
      });
      return true;
    } catch (error) {
      if (isConditionFailure(error)) return false;
      throw error;
    }
  }

  async lockFree(item: string, run: string) {
    await this.db("DeleteItem", {
      Key: { pk: S(item), sk: S("-") },
      ConditionExpression: "#run = :run",
      ExpressionAttributeNames: { "#run": "run" },
      ExpressionAttributeValues: { ":run": S(run) },
    }).catch((error) => {
      if (!isConditionFailure(error)) throw error;
    });
  }

  // A blob is named by what's in it, so its parts are the same each time it's written. The
  // item that says how many there are goes last.
  async putBlob(key: string, body: string, ttlMs: number) {
    const expires = { N: seconds(Date.now() + ttlMs) };
    const parts = split(body);
    for (let i = 0; i < parts.length; i += 8)
      await Promise.all(
        parts.slice(i, i + 8).map((part, j) => this.db("PutItem", { Item: { pk: S(key), sk: S(`p#${pad(i + j)}`), part: S(part), expires } })),
      );
    await this.db("PutItem", { Item: { pk: S(key), sk: S("-"), count: N(parts.length), expires } });
  }

  async getBlob(key: string) {
    const { Item } = await this.db("GetItem", { Key: { pk: S(key), sk: S("-") }, ConsistentRead: true });
    if (!Item) return undefined;
    const parts = await Promise.all(
      Array.from({ length: Number(Item.count.N) }, (_, i) => this.db("GetItem", { Key: { pk: S(key), sk: S(`p#${pad(i)}`) }, ConsistentRead: true })),
    );
    if (parts.some((p) => !p.Item)) return undefined;
    return parts.map((p) => String(p.Item.part.S)).join("");
  }

  // A failure's steps can be more than an item holds: they're in parts, and the failure
  // itself goes last, so one that's there has all of them.
  async putFailure(run: string, given: Failure, ttlMs: number) {
    const failure = { ...given, error: { name: given.error.name.slice(0, 256), message: given.error.message.slice(0, 8192) } };
    const pk = S(keyOf("F", run));
    const expires = { N: seconds(Date.now() + ttlMs) };
    const body = JSON.stringify(failure.memo);
    const parts = split(body);
    const attempt = pad(failure.attempt);
    const id = randomInt(2 ** 48 - 1).toString(36);
    await Promise.all(
      parts.map((part, i) => this.db("PutItem", { Item: { pk, sk: S(`p#${attempt}#${id}#${pad(i)}`), part: S(part), expires } })),
    );
    await this.db("PutItem", {
      Item: {
        pk,
        sk: S(`f#${attempt}`),
        error: S(JSON.stringify(failure.error)),
        delay: N(failure.delayMs),
        parts: S(id),
        count: N(parts.length),
        expires,
      },
      ConditionExpression: "attribute_not_exists(pk)",
    }).catch((error) => {
      if (!isConditionFailure(error)) throw error;
    });
  }

  async failures(run: string) {
    const pk = S(keyOf("F", run));
    const items: any[] = [];
    let start: any;
    do {
      const page = await this.db("Query", {
        KeyConditionExpression: "pk = :pk AND begins_with(sk, :f)",
        ExpressionAttributeValues: { ":pk": pk, ":f": S("f#") },
        ConsistentRead: true,
        ExclusiveStartKey: start,
      });
      items.push(...(page.Items ?? []));
      start = page.LastEvaluatedKey;
    } while (start);
    const out: Failure[] = [];
    for (const i of items) {
      const attempt = String(i.sk.S).slice(2);
      const parts = await Promise.all(
        Array.from({ length: Number(i.count.N) }, (_, n) =>
          this.db("GetItem", { Key: { pk, sk: S(`p#${attempt}#${i.parts.S}#${pad(n)}`) }, ConsistentRead: true }),
        ),
      );
      out.push({
        attempt: Number(attempt),
        error: JSON.parse(i.error.S),
        delayMs: Number(i.delay.N),
        memo: JSON.parse(parts.map((p) => p.Item.part.S).join("") || "{}"),
      });
    }
    return out;
  }

  async signalWait(item: string, wait: SignalWait, replace: boolean | { over: string }) {
    try {
      await this.db("PutItem", {
        Item: {
          pk: S(item),
          sk: S("-"),
          callback: S(wait.callbackId),
          run: S(wait.runId),
          owner: S(wait.owner),
          created: N(wait.createdAt),
          until: N(wait.expiresAt),
          expires: { N: seconds(wait.expiresAt + DAY) },
        },
        ...(replace === true
          ? {}
          : {
              // Or over a wait that's known to have ended.
              ConditionExpression: `attribute_not_exists(pk) OR #o = :o OR #u < :now${replace ? " OR callback = :over" : ""}`,
              ExpressionAttributeNames: { "#o": "owner", "#u": "until" },
              ExpressionAttributeValues: { ":o": S(wait.owner), ":now": N(Date.now()), ...(replace ? { ":over": S(replace.over) } : {}) },
            }),
      });
      return true;
    } catch (error) {
      if (isConditionFailure(error)) return false;
      throw error;
    }
  }

  async signalRead(item: string): Promise<SignalWait | undefined> {
    const { Item } = await this.db("GetItem", { Key: { pk: S(item), sk: S("-") }, ConsistentRead: true });
    if (!Item || Number(Item.until.N) < Date.now()) return undefined;
    return {
      callbackId: Item.callback.S,
      runId: Item.run.S,
      owner: Item.owner.S,
      createdAt: Number(Item.created?.N ?? 0),
      expiresAt: Number(Item.until.N),
    };
  }

  async signalFree(item: string, callbackId: string) {
    await this.db("DeleteItem", {
      Key: { pk: S(item), sk: S("-") },
      ConditionExpression: "callback = :cb",
      ExpressionAttributeValues: { ":cb": S(callbackId) },
    }).catch((error) => {
      if (!isConditionFailure(error)) throw error;
    });
  }

  async scan(prefix: string, limit: number) {
    const items: Record<string, any>[] = [];
    let start: any;
    do {
      const result = await this.db("Scan", {
        FilterExpression: "begins_with(pk, :p)",
        ExpressionAttributeValues: { ":p": S(prefix) },
        ExclusiveStartKey: start,
      });
      items.push(...(result.Items ?? []).map(unmarshal));
      start = result.LastEvaluatedKey;
    } while (start && items.length < limit);
    return items.slice(0, limit);
  }
}

interface Lease {
  expires: number;
  tenant: string;
  granted: boolean;
}

interface ShardState {
  limit: Limit;
  leases: Record<string, Lease>;
  /** Each lease's stored value, to give back only the one that was read. */
  raw: Record<string, string>;
}

function lockItem(lock: Lock) {
  return {
    run: S(lock.run),
    until: N(lock.until),
    ...(lock.arn ? { arn: S(lock.arn) } : {}),
    ...(lock.replaces?.length ? { replaces: S(JSON.stringify(lock.replaces)) } : {}),
  };
}

function lockOf(item: any): Lock | undefined {
  if (!item) return undefined;
  return {
    run: String(item.run.S),
    until: Number(item.until.N),
    ...(item.arn ? { arn: String(item.arn.S) } : {}),
    ...(item.replaces ? { replaces: JSON.parse(item.replaces.S) } : {}),
  };
}

/** When a tenant was marked as in a limit's line, and in how many lines. */
interface Mark {
  at: number;
  lines: number;
}

/** A step's lease in a shard. */
interface Copy {
  shard: Limit;
  lease: Lease;
  raw: string;
}

interface LimitState {
  limit: Limit;
  /** Its shards, and those of a version with a higher limit, which have no room. */
  shards: ShardState[];
  /** How many leases it has in all. */
  held: number;
  /** The shards that aren't in the table yet. */
  missing: Limit[];
  /** Every lease, whichever shard it's in. */
  leases: Record<string, Lease>;
  tenants: Record<string, number>;
  /** The tenants marked as in line. */
  waiting: Record<string, Mark>;
  /** How many more leases it has room for, in the shards that are in the table. */
  free: number;
  /** How many more each shard has room for. */
  rooms(): Rooms;
  copies(leaseId: string): Copy[];
}

type Rooms = Map<string, number>;

/** The leases a grant gave: their value, and the shards they're in. */
interface Granted {
  value: string;
  at: Limit[];
}

interface Grant {
  granted?: Granted;
  /** The limits whose lease was passed from a step that's done. */
  passed?: Limit[];
  /** It took some, and gave them back. */
  undone: boolean;
  /** Another grant got to the step first. */
  raced: boolean;
}

/**
 * How many leases of a limit are in one item: few enough that the item is 2 KB, and a
 * write to it 2 units. Those after the first 500 have more, as each dispatch reads every
 * shard.
 */
export const SHARD_LEASES = 25;
const SMALL_SHARDS = 20;
const LARGE_SHARD = 100;

function shardSize(i: number) {
  return i < SMALL_SHARDS ? SHARD_LEASES : LARGE_SHARD;
}

/**
 * A limit's shards, whose caps add up to it: each full, but the last. A shard's cap only
 * grows with the limit, so two versions of a function, with two limits, don't hold more
 * than the larger one. The first one's item is the limit's, so a limit that has one, as
 * most do, is that item.
 */
export function shardsOf(limit: Limit): Limit[] {
  if (limit.limit <= SHARD_LEASES) return [limit];
  const shards: Limit[] = [];
  for (let i = 0, left = limit.limit; left > 0; i++) {
    const cap = Math.min(left, shardSize(i));
    shards.push({ item: i ? `${limit.item}.${i}` : limit.item, limit: cap, label: limit.label });
    left -= cap;
  }
  return shards;
}

/**
 * How many lines a tenant has on a limit. Of a limit with shards many steps join and leave
 * at once, so the line is split, and a step is in the one its lease's ID says. Each is in
 * the order its steps joined.
 */
function linesOf(limit: Limit) {
  const n = shardsOf(limit).length;
  return n === 1 ? 1 : Math.min(16, Math.ceil(n / 2));
}

function lineOf(limit: Limit, leaseId: string) {
  const n = linesOf(limit);
  return n === 1 ? 0 : createHash("sha256").update(leaseId).digest().readUInt32BE(0) % n;
}

/** A text in parts that each fit an item. */
function split(body: string) {
  const parts: string[] = [];
  for (let i = 0; i < body.length; ) {
    let end = Math.min(body.length, i + 100_000);
    // Not between the two halves of a character, which would each be lost.
    if (end < body.length && /[\uD800-\uDBFF]/.test(body[end - 1])) end--;
    parts.push(body.slice(i, end));
    i = end;
  }
  return parts;
}

function shuffle<T>(list: T[]) {
  for (let i = list.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [list[i], list[j]] = [list[j], list[i]];
  }
  return list;
}

/** How long a tenant's mark is kept once its line is empty. */
const STALE_LINE_MS = 60_000;

/** How long a step's entry is claimed by a grant: longer than one takes. */
const CLAIM_MS = 20_000;

/** How long a dispatch looks for steps to wake. */
const DISPATCH_MS = 10_000;

/** How many tenants in line a dispatch looks at, for each lease it gives. */
const MAX_TENANTS = 200;

/**
 * How long a lease given to a step in line lasts until the step, woken, makes it its own: far
 * longer than a run takes to resume. Not much longer, as whoever gave it may have stopped
 * before it woke the step, and until then it's held by no one. A step that comes later asks
 * again.
 */
export const GRANT_MS = 180_000;

/** Where a limit's marks are: apart from its leases, as both are read by every step. */
function marksKey(item: string) {
  return `${item}~`;
}

/** A line of a limit, for one tenant. */
function lineKey(item: string, tenant: string, line: number) {
  return line ? keyOf("Q", item, tenant, line) : keyOf("Q", item, tenant);
}

/** A step's limits, each once, in the order every step takes them. */
function ordered(limits: Limit[]) {
  return uniqueLimits(limits).sort((a, b) => (a.item < b.item ? -1 : a.item > b.item ? 1 : 0));
}

/**
 * A lease's value: when it expires, whether the step took it itself or was given it and
 * hasn't made it its own, and its tenant. It's unique, so a lease is given back, or taken
 * for one's own, only by whoever wrote it: two dispatches can give one step its leases in
 * the same millisecond.
 */
function leaseValue(expires: number, tenant: string, kind: "own" | "granted") {
  return `${Math.floor(expires)}|${kind === "own" ? "o" : "g"}${randomInt(2 ** 48 - 1).toString(36)}|${tenant}`;
}

export function parseLease(value: string) {
  const [expires, kind, ...tenant] = value.split("|");
  return { expires: Number(expires), granted: kind.startsWith("g"), tenant: tenant.join("|") };
}

export function uniqueLimits(limits: Limit[]) {
  const seen = new Set<string>();
  return limits.filter((l) => !seen.has(l.item) && seen.add(l.item));
}

export function fieldsKey(name: string, tenant: string | undefined) {
  return keyOf("WF", name, tenant ?? null);
}

function unmarshal(item: Record<string, any>): Record<string, any> {
  const out: Record<string, any> = Object.create(null);
  for (const [k, v] of Object.entries(item)) out[k] = value(v);
  return out;
  function value(v: any): any {
    if ("S" in v) return v.S;
    if ("N" in v) return Number(v.N);
    if ("BOOL" in v) return v.BOOL;
    if ("M" in v) return unmarshal(v.M);
    if ("L" in v) return v.L.map(value);
    if ("NULL" in v) return null;
    return v;
  }
}

export { AwsError };
