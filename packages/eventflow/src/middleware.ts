import type { MiddlewareClass } from "./definition.js";
import { hash } from "./ids.js";

// Inngest's middleware, with the hooks that mean the same here. Each request makes its own
// instances, the client's first, and the first wraps the rest.

/** What a hook is told about a step. */
export interface StepInfo {
  hashedId: string;
  input?: unknown[];
  /**
   * Whether the step's result is a saved one. It isn't known before the step runs, where
   * it's `undefined`: a hook around the handler only runs for one that isn't.
   */
  memoized: boolean | undefined;
  options: { id: string; name?: string };
  stepType: string;
}

/**
 * The base class of a middleware, as Inngest's `Middleware.BaseMiddleware`.
 *
 * @example
 * ```ts
 * class Tracing extends Middleware.BaseMiddleware {
 *   readonly id = "tracing";
 *   async wrapStepHandler({ stepInfo, next }) {
 *     return tracer.startActiveSpan(stepInfo.options.id, () => next());
 *   }
 * }
 *
 * new EventFlow({ id: "app", middleware: [Tracing] });
 * ```
 */
export abstract class BaseMiddleware {
  abstract readonly id: string;
  readonly client: any;
  constructor({ client }: { client: any }) {
    this.client = client;
  }
}

export interface BaseMiddleware {
  onMemoizationEnd?(arg: { ctx: any; fn: any }): void | Promise<void>;
  onRunStart?(arg: { ctx: any; fn: any }): void | Promise<void>;
  onRunComplete?(arg: { ctx: any; fn: any; output: unknown }): void | Promise<void>;
  onRunError?(arg: { ctx: any; fn: any; error: Error; isFinalAttempt: boolean }): void | Promise<void>;
  onStepStart?(arg: { ctx: any; fn: any; stepInfo: StepInfo }): void | Promise<void>;
  onStepComplete?(arg: { ctx: any; fn: any; stepInfo: StepInfo; output: unknown }): void | Promise<void>;
  onStepError?(arg: { ctx: any; fn: any; stepInfo: StepInfo; error: Error; isFinalAttempt: boolean }): void | Promise<void>;
  transformFunctionInput?(arg: { ctx: any; fn: any; steps: Record<string, unknown> }): any;
  transformSendEvent?(arg: { events: any[]; fn: any | null }): any;
  transformStepInput?(arg: { fn: any; stepInfo: Pick<StepInfo, "hashedId" | "memoized" | "stepType">; stepOptions: { id: string; name?: string }; input: unknown[] }): any;
  wrapFunctionHandler?(arg: { ctx: any; fn: any; next: () => Promise<unknown> }): Promise<unknown>;
  wrapSendEvent?(arg: { events: any[]; fn: any | null; next: () => Promise<any> }): Promise<any>;
  wrapStep?(arg: { ctx: any; fn: any; stepInfo: StepInfo; next: () => Promise<unknown> }): Promise<unknown>;
  wrapStepHandler?(arg: { ctx: any; fn: any; stepInfo: StepInfo; next: () => Promise<unknown> }): Promise<unknown>;
}

const UNSUPPORTED = ["wrapRequest"];

/** Check the classes, and tell them they're used. */
export function registerMiddleware(classes: MiddlewareClass[], client: unknown, fn: unknown) {
  for (const c of classes) {
    for (const hook of UNSUPPORTED)
      if (typeof c.prototype?.[hook] === "function")
        throw new TypeError(`The middleware "${c.name}" has ${hook}(), which EventFlow doesn't have: there's no request.`);
    if (typeof (c as any).functions === "function")
      throw new TypeError(`The middleware "${c.name}" has functions(), which EventFlow doesn't have. Pass them to serve().`);
    (c as any).onRegister?.({ client, fn });
  }
}

/** The hooks of a request's middleware. */
export class Hooks {
  private readonly list: BaseMiddleware[];
  constructor(
    classes: MiddlewareClass[],
    client: unknown,
    private readonly fn: unknown,
  ) {
    this.list = classes.map((c) => new c({ client }));
  }

  get any() {
    return this.list.length > 0;
  }

  has(hook: keyof BaseMiddleware) {
    return this.list.some((m) => typeof m[hook] === "function");
  }

  async tell(hook: "onMemoizationEnd" | "onRunStart" | "onRunComplete" | "onRunError" | "onStepStart" | "onStepComplete" | "onStepError", arg: any) {
    for (const m of this.list) await (m[hook] as any)?.({ fn: this.fn, ...arg });
  }

  async transform(hook: "transformFunctionInput" | "transformSendEvent" | "transformStepInput", arg: any) {
    for (const m of this.list) {
      if (!m[hook]) continue;
      const out = await (m[hook] as any)({ fn: this.fn, ...arg });
      if (out) arg = { ...arg, ...out };
    }
    return arg;
  }

  wrap(hook: "wrapFunctionHandler" | "wrapSendEvent" | "wrapStep" | "wrapStepHandler", arg: any, next: () => Promise<any>): Promise<any> {
    return this.list.reduceRight<() => Promise<any>>(
      (inner, m) => (m[hook] ? () => (m[hook] as any)({ fn: this.fn, ...arg, next: inner }) : inner),
      next,
    )();
  }

  info(id: string, name: string | undefined, stepType: string, input?: unknown[], memoized?: boolean): StepInfo {
    return { hashedId: hash(id), input, memoized, options: { id, ...(name ? { name } : {}) }, stepType };
  }
}
