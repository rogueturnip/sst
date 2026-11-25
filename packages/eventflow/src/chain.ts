import { AsyncLocalStorage } from "async_hooks";

// Which step call led to which, in a function. A branch of one, as in
// `Promise.all([a(), b()])`, is an async function, and it keeps its async context: a step
// that's called puts its own there, and the next one called after it, in that branch, finds
// it. So a call's place in the chains is the same each time the function is replayed,
// whatever finishes first.
//
// A step ID that's used more than once is numbered in the order it's used. That's the same
// each time only where one use led to the other: in a loop, or a list that's started at once.
// In two branches that run at once it's whichever comes first, so then it's refused.

interface Link {
  owner: object;
  parent?: Link;
  depth: number;
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

/** Run a function's attempt, with chains of its own. */
export function unlinked<T>(fn: () => T): T {
  return links.run(undefined as any, fn);
}

export type { Link };
