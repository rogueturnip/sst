import {
  GRANT_MS,
  tenantKey,
  uniqueLimits,
  type Admission,
  type Failure,
  type Lock,
  type Pending,
  type SignalWait,
  type LeaseWaiter,
  type Limit,
  type Store,
  type Waiter,
} from "../src/store.ts";
import type { WireEvent } from "../src/types.ts";

interface LimitItem {
  cap: number;
  label: string;
  leases: Record<string, { expires: number; tenant: string }>;
  tenants: Record<string, number>;
  /** Each tenant's line. */
  lines: Map<string, (LeaseWaiter & { seq: number })[]>;
}

// The same contract as DynamoStore, in memory, with an injectable clock: every limit
// at once or none, a line per tenant, and the fairest tenant first.
export class MemoryStore implements Store {
  now = () => Date.now();
  events = new Map<string, { delivery: string; routed: boolean; expires: number }>();
  admissions = new Map<string, Admission>();
  claims = new Map<string, { owner: string; expires: number }>();
  limits = new Map<string, LimitItem>();
  buckets = new Map<string, number>();
  fields = new Map<string, Map<string, number>>();
  waiterItems = new Map<string, Waiter[]>();
  recentItems = new Map<string, { event: WireEvent; receivedAt: number }[]>();
  /** Every acquire, for assertions: [time, items, tenant, granted]. */
  log: [number, string, string, boolean][] = [];
  private seq = 0;

  async beginEvent(item: string, delivery: string) {
    const seen = this.events.get(item);
    if (seen && seen.routed && seen.delivery !== delivery && seen.expires > this.now()) return "duplicate" as const;
    this.events.set(item, { delivery, routed: false, expires: this.now() + 86_400_000 });
    return "route" as const;
  }

  async finishEvent(item: string) {
    const seen = this.events.get(item);
    if (seen) seen.routed = true;
  }

  async getAdmission(item: string) {
    return this.admissions.get(item);
  }

  async putAdmission(item: string, admission: Admission) {
    if (!this.admissions.has(item)) this.admissions.set(item, admission);
    return this.admissions.get(item)!;
  }

  async claim(item: string, owner: string, ttlMs: number) {
    const c = this.claims.get(item);
    if (c && c.owner !== owner && c.expires > this.now()) return false;
    this.claims.set(item, { owner, expires: this.now() + ttlMs });
    return true;
  }

  async unclaim(item: string, owner: string) {
    if (this.claims.get(item)?.owner === owner) this.claims.delete(item);
  }

  async owner(item: string) {
    return this.claims.get(item)?.owner;
  }

  async successor(item: string, owner: string, ttlMs: number) {
    if (await this.claim(item, owner, ttlMs)) return owner;
    return this.claims.get(item)!.owner;
  }

  async take(item: string, intervalMs: number, toleranceMs: number) {
    const now = this.now();
    const tat = Math.max(this.buckets.get(item) ?? now, now);
    if (tat - now > toleranceMs) return tat - toleranceMs - now;
    this.buckets.set(item, tat + intervalMs);
    return 0;
  }

  async reserve(item: string, intervalMs: number, toleranceMs: number, maxWaitMs = Infinity) {
    const now = this.now();
    const tat = Math.max(this.buckets.get(item) ?? now, now);
    if (tat - toleranceMs - now > maxWaitMs) return Infinity;
    this.buckets.set(item, tat + intervalMs);
    return now + Math.max(0, tat - toleranceMs - now);
  }

  item(limit: Limit) {
    let l = this.limits.get(limit.item);
    if (!l)
      this.limits.set(limit.item, (l = { cap: limit.limit, label: limit.label ?? limit.item, leases: {}, tenants: {}, lines: new Map() }));
    return l;
  }

  active(itemKey: string, tenant?: string) {
    const l = this.limits.get(itemKey);
    if (!l) return 0;
    const now = this.now();
    return Object.values(l.leases).filter((v) => v.expires > now && (tenant === undefined || v.tenant === tenant)).length;
  }

  queued(itemKey: string) {
    const l = this.limits.get(itemKey);
    return l ? [...l.lines.values()].reduce((n, line) => n + line.length, 0) : 0;
  }

  private reap(l: LimitItem) {
    const now = this.now();
    for (const [id, v] of Object.entries(l.leases)) if (v.expires <= now) this.drop(l, id, v.tenant);
  }

  private drop(l: LimitItem, id: string, tenant: string) {
    if (!l.leases[id]) return;
    delete l.leases[id];
    const t = tenantKey(tenant);
    l.tenants[t] = (l.tenants[t] ?? 1) - 1;
    if (l.tenants[t] <= 0) delete l.tenants[t];
  }

  private give(l: LimitItem, id: string, tenant: string, ms: number) {
    l.leases[id] = { expires: this.now() + ms, tenant };
    const t = tenantKey(tenant);
    l.tenants[t] = (l.tenants[t] ?? 0) + 1;
  }

  async acquire(limits: Limit[], leaseId: string, tenant: string, leaseMs: number) {
    const items = uniqueLimits(limits).map((l) => ({ limit: l, item: this.item(l) }));
    items.forEach(({ item }) => this.reap(item));
    const held = items.every(({ item }) => item.leases[leaseId]) && (await this.renew(limits, leaseId, leaseMs, true));
    const room = items.every(
      ({ limit, item }) => Object.keys(item.leases).length < limit.limit && [...item.lines.values()].every((q) => !q.length),
    );
    const ok = held || room;
    if (ok && !held) items.forEach(({ item }) => this.give(item, leaseId, tenant, leaseMs));
    this.log.push([this.now(), items.map((i) => i.limit.item).join(","), tenant, ok]);
    return ok ? ("granted" as const) : ("full" as const);
  }

  async renew(limits: Limit[], leaseId: string, leaseMs: number, claim = false) {
    const now = this.now();
    for (const l of uniqueLimits(limits)) {
      const lease = this.item(l).leases[leaseId];
      if (!lease || lease.expires <= now) return false;
    }
    if (!claim && uniqueLimits(limits).every((l) => this.item(l).leases[leaseId].expires >= now + leaseMs)) return true;
    for (const l of uniqueLimits(limits)) this.item(l).leases[leaseId] = { ...this.item(l).leases[leaseId], expires: now + leaseMs };
    return true;
  }

  async release(limits: Limit[], leaseId: string, tenant: string) {
    for (const l of uniqueLimits(limits)) this.drop(this.item(l), leaseId, tenant);
  }

  async enqueue(waiter: LeaseWaiter) {
    const limits = uniqueLimits(waiter.limits);
    if (limits.every((l) => this.item(l).leases[waiter.leaseId]) && (await this.renew(limits, waiter.leaseId, GRANT_MS, true)))
      return "held" as const;
    for (const l of limits) {
      const lines = this.item(l).lines;
      const line = lines.get(waiter.tenant) ?? [];
      lines.set(waiter.tenant, line);
      const same = line.find((w) => w.at === waiter.at && w.leaseId === waiter.leaseId);
      if (same) same.callbackId = waiter.callbackId;
      else {
        line.push({ ...waiter, limits, seq: this.seq++ });
        line.sort((a, b) => a.at - b.at || a.seq - b.seq);
      }
    }
    return "queued" as const;
  }

  async dispatch(limit: Limit, wake: (w: LeaseWaiter) => Promise<boolean>, _joined?: LeaseWaiter, just?: string, skip = new Set<string>()) {
    const l = this.item(limit);
    let woken = 0;
    for (;;) {
      this.reap(l);
      if (Object.keys(l.leases).length >= limit.limit) return woken;
      // The oldest step of each tenant in line, fewest leases first, then oldest.
      const candidates = [...l.lines.values()]
        .map((line) => line.find((w) => !skip.has(w.leaseId)))
        .filter((w): w is NonNullable<typeof w> => !!w)
        // A tenant that just gave one back counts it, as DynamoStore picks the next step
        // before the lease is given back.
        .sort(
          (a, b) =>
            (l.tenants[tenantKey(a.tenant)] ?? 0) + (a.tenant === just ? 1 : 0) - (l.tenants[tenantKey(b.tenant)] ?? 0) - (b.tenant === just ? 1 : 0) ||
            a.at - b.at ||
            a.seq - b.seq,
        );
      const next = candidates.find((c) => this.canGrant(c));
      if (!next) return woken;
      const given = new Map<LimitItem, { expires: number; tenant: string }>();
      for (const other of uniqueLimits(next.limits)) {
        const item = this.item(other);
        const line = item.lines.get(next.tenant)!;
        line.splice(line.findIndex((w) => w.leaseId === next.leaseId && w.at === next.at), 1);
        this.give(item, next.leaseId, next.tenant, GRANT_MS);
        given.set(item, item.leases[next.leaseId]);
      }
      let told: boolean | undefined;
      try {
        told = await wake(next);
      } catch {}
      if (told) {
        woken++;
        continue;
      }
      // One that may not have been told goes back in line, in its place, unless it
      // asked again in between, and waits for the next dispatch.
      if (told === undefined) {
        skip.add(next.leaseId);
        for (const other of uniqueLimits(next.limits)) {
          const line = this.item(other).lines.get(next.tenant) ?? [];
          this.item(other).lines.set(next.tenant, line);
          if (!line.some((w) => w.leaseId === next.leaseId && w.at === next.at)) {
            line.push(next);
            line.sort((a, b) => a.at - b.at || a.seq - b.seq);
          }
        }
      }
      // Only the leases this gave: a step that renewed them holds them as its own.
      for (const [item, lease] of given) if (item.leases[next.leaseId] === lease) this.drop(item, next.leaseId, next.tenant);
    }
  }

  async handoff(limits: Limit[], leaseId: string, tenant: string, wake: (w: LeaseWaiter) => Promise<boolean>) {
    await this.release(limits, leaseId, tenant);
    // The limit with the most tenants in line first, as DynamoStore does.
    const inLine = (l: Limit) => [...this.item(l).lines.values()].filter((line) => line.length).length;
    const skip = new Set<string>();
    for (const l of uniqueLimits(limits).sort((a, b) => inLine(b) - inLine(a) || b.limit - a.limit))
      await this.dispatch(l, wake, undefined, tenant, skip);
  }

  private canGrant(w: LeaseWaiter) {
    return uniqueLimits(w.limits).every((other) => {
      const item = this.item(other);
      this.reap(item);
      return Object.keys(item.leases).length < other.limit && !item.leases[w.leaseId];
    });
  }

  private fieldKey(name: string, tenant: string | undefined) {
    return JSON.stringify([name, tenant ?? null]);
  }

  async waitFields(name: string, tenant: string | undefined) {
    const now = this.now();
    return [...(this.fields.get(this.fieldKey(name, tenant)) ?? new Map()).entries()].filter(([, u]) => u > now).map(([f]) => f);
  }

  async addWaitField(name: string, tenant: string | undefined, field: string, until: number) {
    const k = this.fieldKey(name, tenant);
    const m = this.fields.get(k) ?? new Map<string, number>();
    m.set(field, Math.max(until, m.get(field) ?? 0));
    this.fields.set(k, m);
  }

  async putWaiter(key: string, waiter: Waiter) {
    const list = (this.waiterItems.get(key) ?? []).filter((w) => !(w.createdAt === waiter.createdAt && w.callbackId === waiter.callbackId));
    list.push(waiter);
    this.waiterItems.set(key, list);
  }

  async waiters(key: string, now: number) {
    return (this.waiterItems.get(key) ?? []).filter((w) => w.expiresAt > now);
  }

  async deleteWaiter(key: string, waiter: Pick<Waiter, "createdAt" | "callbackId">) {
    this.waiterItems.set(
      key,
      (this.waiterItems.get(key) ?? []).filter((w) => !(w.createdAt === waiter.createdAt && w.callbackId === waiter.callbackId)),
    );
  }

  async putRecent(key: string, event: WireEvent, receivedAt: number) {
    const list = this.recentItems.get(key) ?? [];
    list.push({ event, receivedAt });
    this.recentItems.set(key, list);
  }

  async recent(key: string, since: number) {
    return (this.recentItems.get(key) ?? []).filter((r) => r.receivedAt >= since);
  }

  windows = new Map<string, { event?: WireEvent; order?: string; opened?: number; last?: number; received?: string; closing?: boolean; fired?: string }>();
  async debounceMerge(item: string, event: WireEvent, order: string, received: string, now: number) {
    const w = this.windows.get(item) ?? {};
    this.windows.set(item, w);
    if (w.closing) return "closing" as const;
    const open = w.order !== undefined;
    if (open && (w.received ?? "") < received) w.received = received;
    if (open ? w.order! > order : w.fired !== undefined && w.fired >= received) return "stale" as const;
    Object.assign(w, { event, order, received: w.received ?? received, last: now, opened: w.opened ?? now });
    return { opened: w.opened!, last: now };
  }
  async debounceRead(item: string) {
    const w = this.windows.get(item);
    return w?.order === undefined ? undefined : { event: w.event!, order: w.order, opened: w.opened!, last: w.last!, closing: !!w.closing };
  }
  async debounceClose(item: string, order: string) {
    const w = this.windows.get(item);
    if (w?.order !== order) return false;
    w.closing = true;
    return true;
  }
  async debounceDone(item: string, order: string) {
    const w = this.windows.get(item);
    if (w?.order === order && w.closing) this.windows.set(item, { fired: w.received });
  }

  batches = new Map<string, { next: number; pending: Pending[]; closed: Map<number, { refs: string[]; orders: string[]; at: number }> }>();
  batchItems = new Map<string, { event: WireEvent; order: string; batch?: string }>();
  private batch(item: string) {
    let b = this.batches.get(item);
    if (!b) this.batches.set(item, (b = { next: 0, pending: [], closed: new Map() }));
    return b;
  }
  async batchAdd(item: string, ref: string, order: string, event: WireEvent, at: number) {
    if (this.batchItems.get(ref)?.batch) return false;
    const there = this.batchItems.get(ref);
    if (there) {
      order = there.order;
      at = Number(order.split("#")[0]) || at;
    } else this.batchItems.set(ref, { event, order });
    const b = this.batch(item);
    b.pending = [...b.pending.filter((p) => p.order !== order), { order, at, bytes: JSON.stringify(event).length + (event.blob?.bytes ?? 0), ref }].sort((x, y) => (x.order < y.order ? -1 : 1));
    return true;
  }
  async batchPending(item: string, max: number) {
    return this.batch(item).pending.slice(0, max);
  }
  async batchEvents(refs: string[]) {
    return refs.flatMap((r) => (this.batchItems.has(r) ? [this.batchItems.get(r)!.event] : []));
  }
  async batchClose(item: string, max: number, choose: (p: Pending[]) => Pending[]) {
    const b = this.batch(item);
    const open = b.closed.get(b.next);
    if (open) return { n: b.next, refs: open.refs, at: open.at };
    b.pending = b.pending.filter((p) => !this.batchItems.get(p.ref)?.batch);
    const chosen = choose(b.pending.slice(0, max));
    if (!chosen.length) return undefined;
    b.closed.set(b.next, { refs: chosen.map((p) => p.ref), orders: chosen.map((p) => p.order), at: chosen[0].at });
    return { n: b.next, refs: chosen.map((p) => p.ref), at: chosen[0].at };
  }
  async batchRead(item: string, n: number) {
    return this.batch(item).closed.get(n)?.refs ?? [];
  }
  async batchDone(item: string, n: number) {
    const b = this.batch(item);
    const c = b.closed.get(n);
    if (!c) return;
    for (const r of c.refs) this.batchItems.get(r)!.batch = `${item}#${n}`;
    b.pending = b.pending.filter((p) => !c.orders.includes(p.order));
    b.next = Math.max(b.next, n + 1);
  }

  locks = new Map<string, Lock>();
  async lockTake(item: string, lock: Lock) {
    const held = this.locks.get(item);
    if (held && held.run !== lock.run) return { ...held };
    if (!held) this.locks.set(item, { ...lock });
    return undefined;
  }
  async lockRead(item: string) {
    return this.locks.get(item);
  }
  async lockStarted(item: string, run: string, arn: string) {
    const held = this.locks.get(item);
    if (held?.run === run) held.arn = arn;
  }
  async lockSettled(item: string, run: string) {
    const held = this.locks.get(item);
    if (held?.run === run) delete held.replaces;
  }
  async lockMove(item: string, from: string, to: Lock) {
    if (this.locks.get(item)?.run === to.run && from !== to.run) return true;
    if (this.locks.get(item)?.run !== from) return false;
    this.locks.set(item, { ...to });
    return true;
  }
  async lockFree(item: string, run: string) {
    if (this.locks.get(item)?.run === run) this.locks.delete(item);
  }

  blobs = new Map<string, string>();
  async putBlob(key: string, body: string) {
    this.blobs.set(key, body);
  }
  async getBlob(key: string) {
    return this.blobs.get(key);
  }

  failed = new Map<string, Failure[]>();
  async putFailure(run: string, failure: Failure) {
    const list = this.failed.get(run) ?? [];
    if (!list.some((f) => f.attempt === failure.attempt)) list.push(JSON.parse(JSON.stringify(failure)));
    this.failed.set(run, list);
  }
  async failures(run: string) {
    return this.failed.get(run) ?? [];
  }

  signals = new Map<string, SignalWait>();
  async signalWait(item: string, wait: SignalWait, replace: boolean | { over: string }) {
    const held = this.signals.get(item);
    if (held && replace !== true && !(replace && held.callbackId === replace.over) && held.owner !== wait.owner && held.expiresAt >= this.now()) return false;
    this.signals.set(item, wait);
    return true;
  }
  async signalRead(item: string) {
    const w = this.signals.get(item);
    return w && w.expiresAt >= this.now() ? w : undefined;
  }
  async signalFree(item: string, callbackId: string) {
    if (this.signals.get(item)?.callbackId === callbackId) this.signals.delete(item);
  }

  async scan(prefix: string) {
    if (prefix === "C#")
      return [...this.limits.entries()].flatMap(([pk, l]) => [{ pk, sk: "w#", n: this.queued(pk) }, {
        pk,
        sk: "-",
        label: l.label,
        cap: l.cap,
        tenants: { ...l.tenants },
        leases: Object.fromEntries(Object.entries(l.leases).map(([id, v]) => [id, `${v.expires}|o|${v.tenant}`])),
      }]);
    if (prefix === "W#")
      return [...this.waiterItems.entries()].flatMap(([pk, list]) =>
        list.map((w) => ({
          pk,
          run: w.runId,
          kind: w.kind ?? "wait",
          created: w.createdAt,
          until: w.expiresAt,
          info: w.info ? JSON.stringify(w.info) : undefined,
        })),
      );
    return [];
  }
}
