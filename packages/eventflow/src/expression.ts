// The part of Inngest's CEL expressions that EventFlow evaluates:
//
//   event.data.customerId                         a path, for keys
//   event.data.region + "-" + event.data.plan     a key of two
//   event.data.plan == "pro" && !event.data.test  comparisons, &&, ||, !, ( )
//   event.data.amount * 2 > 100 ? "a" : "b"       + - * / %, and ? :
//   event.data.plan in ["pro", "team"]            in a list, or a key in an object
//   has(event.data.coupon)                        has(), size(), string(), int(), double()
//   event.data.email.endsWith("@example.com")     contains, startsWith, endsWith, size,
//                                                 lowerAscii, upperAscii, trim, split, join
//   event.data.items.exists(i, i.price > 100)     exists, all, exists_one, filter, map
//
// Anything else throws a TypeError when it's parsed, instead of being read some other
// way. `matches()` isn't here: JavaScript's regular expressions can take forever on some
// text, and the text is the sender's.

export type Node =
  | { kind: "path"; path: string[] }
  | { kind: "literal"; value: unknown }
  /** A path of the run's event that it doesn't have, once it's bound. */
  | { kind: "missing"; path: string[] }
  | { kind: "list"; items: Node[] }
  | { kind: "unary"; op: "!" | "-"; of: Node }
  | { kind: "binary"; op: BinaryOp; left: Node; right: Node }
  | { kind: "ternary"; if: Node; then: Node; else: Node }
  | { kind: "get"; of: Node; key: Node }
  | { kind: "call"; name: string; args: Node[] }
  | { kind: "macro"; name: string; of: Node; as: string; body: Node };

type BinaryOp = "||" | "&&" | "==" | "!=" | "<" | "<=" | ">" | ">=" | "in" | "+" | "-" | "*" | "/" | "%";

export interface Expression {
  source: string;
  node: Node;
}

const TOKEN =
  /\s*(?:(&&|\|\||==|!=|<=|>=|[<>!+\-*/%?:,.()[\]])|("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')|(\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)|([A-Za-z_$][\w$]*))\s*/y;

const MAX_LENGTH = 8 * 1024;
const MAX_DEPTH = 64;
/** The most elements a macro, or a size, works on: an event can be large. */
const MAX_WORK = 100_000;

const PRECEDENCE: Record<string, number> = {
  "||": 1,
  "&&": 2,
  "==": 3,
  "!=": 3,
  "<": 3,
  "<=": 3,
  ">": 3,
  ">=": 3,
  in: 3,
  "+": 4,
  "-": 4,
  "*": 5,
  "/": 5,
  "%": 5,
};

const FUNCTIONS: Record<string, number> = { has: 1, size: 1, string: 1, int: 1, double: 1, bool: 1 };
const METHODS: Record<string, number> = {
  contains: 1,
  startsWith: 1,
  endsWith: 1,
  size: 0,
  lowerAscii: 0,
  upperAscii: 0,
  trim: 0,
  split: 1,
  join: 1,
};
const MACROS = new Set(["exists", "all", "exists_one", "filter", "map"]);

interface Token {
  type: "op" | "string" | "number" | "ident";
  text: string;
}

export function parseExpression(source: string, label: string): Expression {
  if (typeof source !== "string" || !source.trim())
    throw new TypeError(`The ${label} must be a non-empty expression.`);
  if (source.length > MAX_LENGTH) throw new TypeError(`The ${label} is over ${MAX_LENGTH} characters.`);
  const fail = (near: string): never => {
    throw new TypeError(
      `The ${label} "${source}" isn't supported near "${near}". EventFlow evaluates paths like "event.data.id", literals, lists, comparisons, &&, ||, !, arithmetic, ?:, "in", has(), and the string and list functions in the guide.`,
    );
  };

  const tokens: Token[] = [];
  TOKEN.lastIndex = 0;
  while (TOKEN.lastIndex < source.length) {
    const start = TOKEN.lastIndex;
    const m = TOKEN.exec(source);
    if (!m) {
      fail(source.slice(start).trim());
      break;
    }
    if (m[1]) tokens.push({ type: "op", text: m[1] });
    else if (m[2]) tokens.push({ type: "string", text: m[2] });
    else if (m[3]) tokens.push({ type: "number", text: m[3] });
    else if (m[4]) tokens.push({ type: "ident", text: m[4] });
    else fail(source.slice(start).trim());
  }

  let i = 0;
  let depth = 0;
  const peek = (text: string) => tokens[i]?.type === "op" && tokens[i].text === text;
  const take = (text: string) => (peek(text) ? (i++, true) : false);
  const expect = (text: string) => {
    if (!take(text)) fail(tokens[i]?.text ?? "the end");
  };
  const deeper = <T>(fn: () => T): T => {
    if (++depth > MAX_DEPTH) throw new TypeError(`The ${label} is nested too deep.`);
    try {
      return fn();
    } finally {
      depth--;
    }
  };

  const args = (): Node[] => {
    const list: Node[] = [];
    if (take(")")) return list;
    do list.push(expression());
    while (take(","));
    expect(")");
    return list;
  };

  const primary = (): Node => {
    const t = tokens[i++];
    if (!t) return fail("the end");
    if (t.type === "string") return { kind: "literal", value: unquote(t.text) };
    if (t.type === "number") return { kind: "literal", value: Number(t.text) };
    if (t.type === "op") {
      if (t.text === "(") {
        const inner = expression();
        expect(")");
        return inner;
      }
      if (t.text === "[") {
        const items: Node[] = [];
        if (!take("]")) {
          do items.push(expression());
          while (take(","));
          expect("]");
        }
        return { kind: "list", items };
      }
      return fail(t.text);
    }
    if (t.text === "true") return { kind: "literal", value: true };
    if (t.text === "false") return { kind: "literal", value: false };
    if (t.text === "null") return { kind: "literal", value: null };
    if (t.text === "in") return fail("in");
    if (take("(")) {
      if (!Object.hasOwn(FUNCTIONS, t.text)) fail(`${t.text}(`);
      const list = args();
      if (list.length !== FUNCTIONS[t.text]) fail(`${t.text}(`);
      if (t.text === "has" && list[0].kind !== "path") fail("has(");
      return { kind: "call", name: t.text, args: list };
    }
    return { kind: "path", path: [t.text] };
  };

  const postfix = (): Node => {
    let node = primary();
    for (;;) {
      if (take(".")) {
        const name = tokens[i++];
        if (name?.type !== "ident") return fail(name?.text ?? "the end");
        if (take("(")) {
          if (MACROS.has(name.text)) {
            const as = tokens[i++];
            if (as?.type !== "ident" || as.text === "event" || as.text === "async") return fail(as?.text ?? "the end");
            expect(",");
            const body = expression();
            expect(")");
            node = { kind: "macro", name: name.text, of: node, as: as.text, body };
            continue;
          }
          if (!Object.hasOwn(METHODS, name.text)) fail(`.${name.text}(`);
          const list = args();
          if (list.length !== METHODS[name.text]) fail(`.${name.text}(`);
          node = { kind: "call", name: `.${name.text}`, args: [node, ...list] };
        } else if (node.kind === "path") node = { kind: "path", path: [...node.path, name.text] };
        else node = { kind: "get", of: node, key: { kind: "literal", value: name.text } };
      } else if (take("[")) {
        const key = expression();
        expect("]");
        if (node.kind === "path" && key.kind === "literal" && (typeof key.value === "string" || Number.isInteger(key.value)))
          node = { kind: "path", path: [...node.path, String(key.value)] };
        else node = { kind: "get", of: node, key };
      } else return node;
    }
  };

  const unary = (): Node =>
    deeper(() => {
      if (take("!")) return { kind: "unary", op: "!", of: unary() };
      if (take("-")) {
        const of = unary();
        if (of.kind === "literal" && typeof of.value === "number") return { kind: "literal", value: -of.value };
        return { kind: "unary", op: "-", of };
      }
      return postfix();
    });

  const binary = (min: number): Node => {
    let left = unary();
    for (;;) {
      const t = tokens[i];
      const op = t && (t.type === "op" || (t.type === "ident" && t.text === "in")) ? t.text : "";
      const precedence = Object.hasOwn(PRECEDENCE, op) ? PRECEDENCE[op] : 0;
      if (!precedence || precedence < min) return left;
      i++;
      left = { kind: "binary", op: op as BinaryOp, left, right: binary(precedence + 1) };
    }
  };

  const expression = (): Node =>
    deeper(() => {
      const test = binary(1);
      if (!take("?")) return test;
      const then = expression();
      expect(":");
      return { kind: "ternary", if: test, then, else: expression() };
    });

  const node = expression();
  if (i < tokens.length) fail(tokens[i].text);
  return { source, node };
}

function unquote(text: string) {
  const body = text.slice(1, -1);
  return body.replace(/\\(.)/g, (_, c: string) => (c === "n" ? "\n" : c === "t" ? "\t" : c === "r" ? "\r" : c));
}

export function getPath(scope: unknown, path: string[]): unknown {
  let value: any = scope;
  for (const key of path) {
    if (value === null || value === undefined || typeof value !== "object") return undefined;
    // Only the value's own properties, so a path can't reach a prototype, like
    // `__proto__` or `constructor`.
    if (!Object.prototype.hasOwnProperty.call(value, key)) return undefined;
    value = value[key];
  }
  return value;
}

class EvaluationError extends Error {}

/** Whether the expression is true. A value that isn't a boolean counts as JavaScript counts it. */
export function evaluate(expr: Expression, scope: Record<string, unknown>): boolean {
  return Boolean(evaluateValue(expr, scope));
}

/** The expression's value. Throws where it has none, as `1 + "a"`. */
export function evaluateValue(expr: Expression, scope: Record<string, unknown>): unknown {
  return value(expr.node, scope, { work: 0 });
}

function value(node: Node, scope: Record<string, unknown>, budget: { work: number }): unknown {
  switch (node.kind) {
    case "literal":
      return node.value;
    case "missing":
      throw new EvaluationError(`The run's event has no "${node.path.join(".")}".`);
    case "path":
      return getPath(scope, node.path);
    case "list":
      return node.items.map((n) => value(n, scope, budget));
    case "get": {
      const of = value(node.of, scope, budget);
      const key = value(node.key, scope, budget);
      if (typeof key !== "string" && typeof key !== "number") return undefined;
      return getPath(of, [String(key)]);
    }
    case "unary": {
      const of = value(node.of, scope, budget);
      return node.op === "!" ? !of : -number(of);
    }
    case "ternary":
      return value(value(node.if, scope, budget) ? node.then : node.else, scope, budget);
    case "binary":
      return binaryValue(node, scope, budget);
    case "macro":
      return macro(node, scope, budget);
    case "call":
      return call(node, scope, budget);
    default:
      throw new EvaluationError("Unknown expression.");
  }
}

function binaryValue(node: Extract<Node, { kind: "binary" }>, scope: Record<string, unknown>, budget: { work: number }): unknown {
  if (node.op === "&&" || node.op === "||") {
    // As in CEL, the side that decides it does, if the other has no value.
    const decides = node.op === "||";
    let left: unknown;
    try {
      left = value(node.left, scope, budget);
    } catch (error) {
      if (Boolean(value(node.right, scope, budget)) === decides) return decides;
      throw error;
    }
    if (Boolean(left) === decides) return decides;
    return Boolean(value(node.right, scope, budget));
  }
  const left = value(node.left, scope, budget);
  const right = value(node.right, scope, budget);
  switch (node.op) {
    case "==":
      return equal(left, right);
    case "!=":
      return !equal(left, right);
    // Order only compares numbers with numbers, and strings with strings, so a
    // string or a list in an event can't pass a numeric check like `amount > 100`.
    case "<":
      return comparable(left, right) && (left as any) < (right as any);
    case "<=":
      return comparable(left, right) && (left as any) <= (right as any);
    case ">":
      return comparable(left, right) && (left as any) > (right as any);
    case ">=":
      return comparable(left, right) && (left as any) >= (right as any);
    case "in":
      if (Array.isArray(right)) {
        spend(budget, right.length);
        return right.some((v) => equal(v, left));
      }
      if (right && typeof right === "object") return typeof left === "string" && Object.prototype.hasOwnProperty.call(right, left);
      throw new EvaluationError('"in" needs a list or an object.');
    case "+":
      if (typeof left === "string" && typeof right === "string") return left + right;
      if (Array.isArray(left) && Array.isArray(right)) {
        spend(budget, left.length + right.length);
        return [...left, ...right];
      }
      return number(left) + number(right);
    case "-":
      return number(left) - number(right);
    case "*":
      return number(left) * number(right);
    case "/":
    case "%": {
      const by = number(right);
      if (by === 0) throw new EvaluationError("Division by zero.");
      return node.op === "/" ? number(left) / by : number(left) % by;
    }
  }
}

function macro(node: Extract<Node, { kind: "macro" }>, scope: Record<string, unknown>, budget: { work: number }): unknown {
  const of = value(node.of, scope, budget);
  if (!Array.isArray(of)) throw new EvaluationError(`.${node.name}() needs a list.`);
  spend(budget, of.length);
  const each = (item: unknown) => value(node.body, Object.assign(Object.create(null), scope, { [node.as]: item }), budget);
  switch (node.name) {
    case "exists":
      return of.some((v) => Boolean(each(v)));
    case "all":
      return of.every((v) => Boolean(each(v)));
    case "exists_one":
      return of.filter((v) => Boolean(each(v))).length === 1;
    case "filter":
      return of.filter((v) => Boolean(each(v)));
    default:
      return of.map(each);
  }
}

function call(node: Extract<Node, { kind: "call" }>, scope: Record<string, unknown>, budget: { work: number }): unknown {
  if (node.name === "has") return node.args[0].kind === "path" && getPath(scope, node.args[0].path) !== undefined;
  const [a, b] = node.args.map((n) => value(n, scope, budget));
  switch (node.name) {
    case "size":
    case ".size":
      if (typeof a === "string") return [...a].length;
      if (Array.isArray(a)) return a.length;
      if (a && typeof a === "object") return Object.keys(a).length;
      throw new EvaluationError("size() needs a string, a list or an object.");
    case "string":
      if (typeof a === "string") return a;
      if (typeof a === "number" || typeof a === "boolean") return String(a);
      throw new EvaluationError("string() needs a string, a number or a boolean.");
    case "int":
    case "double": {
      const n = typeof a === "string" && a.trim() ? Number(a) : a;
      if (typeof n !== "number" || !Number.isFinite(n)) throw new EvaluationError(`${node.name}() needs a number.`);
      return node.name === "int" ? Math.trunc(n) : n;
    }
    case "bool":
      if (a === true || a === "true") return true;
      if (a === false || a === "false") return false;
      throw new EvaluationError("bool() needs a boolean.");
    case ".contains":
      return text(a).includes(text(b));
    case ".startsWith":
      return text(a).startsWith(text(b));
    case ".endsWith":
      return text(a).endsWith(text(b));
    case ".lowerAscii":
      return text(a).replace(/[A-Z]/g, (c) => c.toLowerCase());
    case ".upperAscii":
      return text(a).replace(/[a-z]/g, (c) => c.toUpperCase());
    case ".trim":
      return text(a).trim();
    case ".split": {
      const parts = text(a).split(text(b));
      spend(budget, parts.length);
      return parts;
    }
    case ".join":
      if (!Array.isArray(a)) throw new EvaluationError(".join() needs a list.");
      spend(budget, a.length);
      return a.map(text).join(text(b));
    default:
      throw new EvaluationError(`Unknown function ${node.name}().`);
  }
}

function spend(budget: { work: number }, n: number) {
  budget.work += n;
  if (budget.work > MAX_WORK) throw new EvaluationError("The expression does too much work.");
}

function number(v: unknown): number {
  if (typeof v !== "number" || !Number.isFinite(v)) throw new EvaluationError("Not a number.");
  return v;
}

function text(v: unknown): string {
  if (typeof v !== "string") throw new EvaluationError("Not a string.");
  return v;
}

function comparable(a: unknown, b: unknown) {
  return (
    (typeof a === "number" && typeof b === "number" && Number.isFinite(a) && Number.isFinite(b)) ||
    (typeof a === "string" && typeof b === "string")
  );
}

function equal(a: unknown, b: unknown) {
  if (a === b) return true;
  if (typeof a === "object" && typeof b === "object" && a && b) return JSON.stringify(a) === JSON.stringify(b);
  return false;
}

/** The parts of an expression that all have to be true: `a && b && c`. */
export function conjuncts(node: Node): Node[] {
  return node.kind === "binary" && node.op === "&&" ? [...conjuncts(node.left), ...conjuncts(node.right)] : [node];
}

/** Every path the expression reads, outside has(). */
export function paths(node: Node): string[][] {
  switch (node.kind) {
    case "path":
      return [node.path];
    case "list":
      return node.items.flatMap(paths);
    case "unary":
      return paths(node.of);
    case "binary":
      return [...paths(node.left), ...paths(node.right)];
    case "ternary":
      return [...paths(node.if), ...paths(node.then), ...paths(node.else)];
    case "get":
      return [...paths(node.of), ...paths(node.key)];
    case "call":
      return node.name === "has" ? [] : node.args.flatMap(paths);
    case "macro":
      return [...paths(node.of), ...paths(node.body)];
    default:
      return [];
  }
}

/**
 * Replace every path under `root` with its value in `scope`, so the expression only
 * refers to the other roots. A wait uses it to fix the values from the run's event
 * when the wait starts. A path that has no value there doesn't become null, which would
 * equal a null in any event: what reads it has no value either.
 */
export function bind(expr: Expression, root: string, scope: unknown): Expression {
  const sub = (n: Node): Node => {
    switch (n.kind) {
      case "path": {
        if (n.path[0] !== root) return n;
        const v = getPath(scope, n.path.slice(1));
        return v === undefined ? { kind: "missing", path: n.path } : { kind: "literal", value: v };
      }
      case "list":
        return { ...n, items: n.items.map(sub) };
      case "unary":
        return { ...n, of: sub(n.of) };
      case "binary":
        return { ...n, left: sub(n.left), right: sub(n.right) };
      case "ternary":
        return { ...n, if: sub(n.if), then: sub(n.then), else: sub(n.else) };
      case "get":
        return { ...n, of: sub(n.of), key: sub(n.key) };
      case "call": {
        const arg = n.args[0];
        if (n.name === "has" && arg.kind === "path" && arg.path[0] === root)
          return { kind: "literal", value: getPath(scope, arg.path.slice(1)) !== undefined };
        return { ...n, args: n.args.map(sub) };
      }
      case "macro":
        return { ...n, of: sub(n.of), body: sub(n.body) };
      default:
        return n;
    }
  };
  return { source: expr.source, node: sub(expr.node) };
}

/**
 * A key: an expression like "event.data.customerId", or a function of the event. Returns
 * the key as a string, or undefined when the event has no value there.
 */
export type KeyInput = string | ((event: any) => unknown);

export function compileKey(
  input: KeyInput,
  label: string,
): (scope: Record<string, unknown>) => string | undefined {
  if (typeof input === "function") {
    return (scope) => stringify(input(scope.event));
  }
  const expr = parseExpression(input, label);
  return (scope: Record<string, unknown>) => {
    try {
      return stringify(evaluateValue(expr, scope));
    } catch (error) {
      if (error instanceof EvaluationError) return undefined;
      throw error;
    }
  };
}

function stringify(value: unknown): string | undefined {
  if (value === undefined || value === null || value === "") return undefined;
  return typeof value === "string" ? value : JSON.stringify(value);
}
