import { AsyncLocalStorage } from "async_hooks";
import type { Grace } from "./invocation.js";

// Which step call led to which, in a function. A branch of one, as in
// `Promise.all([a(), b()])`, is an async function, and it keeps its async context: a step
// that's called puts its own there, and the next one called after it, in that branch, finds
// it. One that isn't found may have led to it still, as from a function that's called and
// awaited: it only ever says yes.

interface Link {
  owner: object;
  parent?: Link;
  depth: number;
  /** What the function has left of the time after it. */
  grace?: Grace;
  /** The step's name. */
  name?: string;
}

const links = new AsyncLocalStorage<Link>();

/** Note a step's call, and what led to it. It's called as the step is. */
export function link(owner: object): Link {
  const current = links.getStore();
  const parent = current?.owner === owner ? current : undefined;
  const next = { owner, parent, depth: (parent?.depth ?? 0) + 1 };
  links.enterWith(next);
  return next;
}

/** Whether one call led to another. */
export function led(to: Link, from: Link) {
  let l: Link | undefined = to;
  while (l && l.depth > from.depth) l = l.parent;
  return l === from;
}

/** The last step call before here. */
export function lastLink(owner: object): Link | undefined {
  const current = links.getStore();
  return current?.owner === owner ? current : undefined;
}

/** Run a function's attempt, with chains of its own. */
export function unlinked<T>(fn: () => T): T {
  return links.run(undefined as any, fn);
}

export type { Link };
