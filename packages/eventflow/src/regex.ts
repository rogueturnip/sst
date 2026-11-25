// Regular expressions for `matches()`, as CEL has them: RE2's, which take time in proportion
// to the text, whatever the pattern. JavaScript's can take forever on text that's made for
// it, and the text is a sender's.
//
// Literals, ".", classes, \d \w \s and their opposites, \b, ^, $, groups, "|", and the
// repetitions * + ? {n,m}, also lazy. No backreferences or lookaround, which RE2 hasn't either.

type Test = (c: number) => boolean;
type Inst =
  | { op: "char"; test: Test }
  | { op: "split"; a: number; b: number }
  | { op: "jump"; to: number }
  | { op: "assert"; kind: "^" | "$" | "b" | "B" }
  | { op: "match" };

type Ast =
  | { t: "empty" }
  | { t: "char"; test: Test }
  | { t: "assert"; kind: "^" | "$" | "b" | "B" }
  | { t: "cat"; items: Ast[] }
  | { t: "alt"; items: Ast[] }
  | { t: "repeat"; of: Ast; min: number; max: number };

const MAX_PROGRAM = 2000;
const MAX_REPEAT = 200;

const digit: Test = (c) => c >= 48 && c <= 57;
const word: Test = (c) => digit(c) || (c >= 65 && c <= 90) || (c >= 97 && c <= 122) || c === 95;
const space: Test = (c) => c === 32 || (c >= 9 && c <= 13);
const not = (t: Test): Test => (c) => !t(c);
const CLASSES: Record<string, Test> = { d: digit, D: not(digit), w: word, W: not(word), s: space, S: not(space) };
const ESCAPES: Record<string, number> = { n: 10, t: 9, r: 13, f: 12, v: 11 };

export class Regex {
  private readonly program: Inst[] = [];

  constructor(readonly source: string) {
    const parser = new Parser(source);
    this.emit(parser.parse());
    this.program.push({ op: "match" });
  }

  private emit(a: Ast) {
    const p = this.program;
    if (p.length > MAX_PROGRAM) throw new TypeError("The pattern is too large.");
    switch (a.t) {
      case "empty":
        return;
      case "char":
        p.push({ op: "char", test: a.test });
        return;
      case "assert":
        p.push({ op: "assert", kind: a.kind });
        return;
      case "cat":
        for (const i of a.items) this.emit(i);
        return;
      case "alt": {
        const ends: { op: "jump"; to: number }[] = [];
        a.items.forEach((item, i) => {
          if (i === a.items.length - 1) return this.emit(item);
          const split = { op: "split" as const, a: p.length + 1, b: 0 };
          p.push(split);
          this.emit(item);
          const end = { op: "jump" as const, to: 0 };
          p.push(end);
          ends.push(end);
          split.b = p.length;
        });
        for (const end of ends) end.to = p.length;
        return;
      }
      case "repeat": {
        // Nothing, however often, is nothing.
        const before = p.length;
        this.emit(a.of);
        if (p.length === before) return;
        p.length = before;
        for (let i = 0; i < a.min; i++) this.emit(a.of);
        if (a.max === Infinity) {
          const split = { op: "split" as const, a: p.length + 1, b: 0 };
          const at = p.length;
          p.push(split);
          this.emit(a.of);
          p.push({ op: "jump", to: at });
          split.b = p.length;
          return;
        }
        const splits: { op: "split"; a: number; b: number }[] = [];
        for (let i = a.min; i < a.max; i++) {
          const split = { op: "split" as const, a: p.length + 1, b: 0 };
          p.push(split);
          splits.push(split);
          this.emit(a.of);
        }
        for (const s of splits) s.b = p.length;
      }
    }
  }

  /** Whether it matches anywhere in the text. `spend` is told of each character's work. */
  test(text: string, spend: (n: number) => void = () => {}): boolean {
    const chars = Array.from(text, (c) => c.codePointAt(0)!);
    const p = this.program;
    let matched = false;
    let current = new Set<number>();
    const add = (set: Set<number>, pc: number, at: number) => {
      const stack = [pc];
      while (stack.length) {
        const i = stack.pop()!;
        if (set.has(i)) continue;
        set.add(i);
        const inst = p[i];
        if (inst.op === "jump") stack.push(inst.to);
        else if (inst.op === "split") stack.push(inst.b, inst.a);
        else if (inst.op === "match") matched = true;
        else if (inst.op === "assert") {
          const before = at > 0 && word(chars[at - 1]);
          const after = at < chars.length && word(chars[at]);
          const holds =
            inst.kind === "^" ? at === 0 : inst.kind === "$" ? at === chars.length : (before !== after) === (inst.kind === "b");
          if (holds) stack.push(i + 1);
        }
      }
    };
    for (let at = 0; at <= chars.length; at++) {
      add(current, 0, at);
      if (matched) return true;
      if (at === chars.length) return false;
      spend(1 + (current.size >> 3));
      const next = new Set<number>();
      for (const i of current) {
        const inst = p[i];
        if (inst.op === "char" && inst.test(chars[at])) add(next, i + 1, at + 1);
      }
      if (matched) return true;
      current = next;
    }
    return false;
  }
}

class Parser {
  private i = 0;
  private depth = 0;
  private readonly chars: string[];
  constructor(private readonly source: string) {
    if (source.length > 1000) throw new TypeError("The pattern is over 1,000 characters.");
    this.chars = Array.from(source);
  }

  private fail(): never {
    throw new TypeError(`The pattern "${this.source}" isn't supported near "${this.chars.slice(this.i).join("").slice(0, 20) || "the end"}".`);
  }

  parse(): Ast {
    const ast = this.alt();
    if (this.i < this.chars.length) this.fail();
    return ast;
  }

  private alt(): Ast {
    if (++this.depth > 50) throw new TypeError("The pattern is nested too deep.");
    const items = [this.cat()];
    while (this.chars[this.i] === "|") {
      this.i++;
      items.push(this.cat());
    }
    this.depth--;
    return items.length === 1 ? items[0] : { t: "alt", items };
  }

  private cat(): Ast {
    const items: Ast[] = [];
    while (this.i < this.chars.length && this.chars[this.i] !== "|" && this.chars[this.i] !== ")") items.push(this.repeat());
    return items.length ? { t: "cat", items } : { t: "empty" };
  }

  private repeat(): Ast {
    let of = this.atom();
    for (;;) {
      const c = this.chars[this.i];
      let min: number, max: number;
      if (c === "*") [min, max] = [0, Infinity];
      else if (c === "+") [min, max] = [1, Infinity];
      else if (c === "?") [min, max] = [0, 1];
      else if (c === "{") {
        const m = /^\{(\d+)(,(\d*))?\}/.exec(this.chars.slice(this.i, this.i + 12).join(""));
        if (!m) return of;
        min = Number(m[1]);
        max = m[2] === undefined ? min : m[3] === "" ? Infinity : Number(m[3]);
        if (min > max || min > MAX_REPEAT || (max !== Infinity && max > MAX_REPEAT)) this.fail();
        this.i += m[0].length - 1;
      } else return of;
      this.i++;
      if (of.t === "assert" || of.t === "repeat") this.fail();
      // Lazy or not, it's the same whether there's a match.
      if (this.chars[this.i] === "?") this.i++;
      of = { t: "repeat", of, min, max };
      if (["*", "+", "?"].includes(this.chars[this.i])) this.fail();
    }
  }

  private atom(): Ast {
    const c = this.chars[this.i++];
    if (c === "(") {
      if (this.chars[this.i] === "?") {
        if (this.chars[this.i + 1] !== ":") this.fail();
        this.i += 2;
      }
      const inner = this.alt();
      if (this.chars[this.i++] !== ")") this.fail();
      return inner.t === "repeat" || inner.t === "assert" ? { t: "cat", items: [inner] } : inner;
    }
    if (c === "[") return { t: "char", test: this.set() };
    if (c === ".") return { t: "char", test: (x) => x !== 10 };
    if (c === "^" || c === "$") return { t: "assert", kind: c };
    if (c === "\\") {
      const e = this.chars[this.i++];
      if (e === undefined) this.fail();
      if (e === "b" || e === "B") return { t: "assert", kind: e };
      return { t: "char", test: this.escaped(e) };
    }
    if (c === "*" || c === "+" || c === "?") {
      this.i--;
      this.fail();
    }
    const code = c.codePointAt(0)!;
    return { t: "char", test: (x) => x === code };
  }

  private escaped(e: string): Test {
    if (Object.hasOwn(CLASSES, e)) return CLASSES[e];
    if (Object.hasOwn(ESCAPES, e)) return ((code) => (x: number) => x === code)(ESCAPES[e]);
    if (/[A-Za-z0-9]/.test(e)) {
      this.i--;
      this.fail();
    }
    const code = e.codePointAt(0)!;
    return (x) => x === code;
  }

  private set(): Test {
    const negated = this.chars[this.i] === "^";
    if (negated) this.i++;
    const tests: Test[] = [];
    for (let first = true; ; first = false) {
      const c = this.chars[this.i++];
      if (c === undefined) this.fail();
      if (c === "]" && !first) break;
      let from: number;
      if (c === "\\") {
        const e = this.chars[this.i++];
        if (e === undefined) this.fail();
        if (Object.hasOwn(CLASSES, e)) {
          tests.push(CLASSES[e]);
          continue;
        }
        if (Object.hasOwn(ESCAPES, e)) from = ESCAPES[e];
        else if (/[A-Za-z0-9]/.test(e)) return this.fail();
        else from = e.codePointAt(0)!;
      } else from = c.codePointAt(0)!;
      let to = from;
      if (this.chars[this.i] === "-" && this.chars[this.i + 1] !== undefined && this.chars[this.i + 1] !== "]") {
        this.i++;
        let end = this.chars[this.i++];
        if (end === "\\") {
          end = this.chars[this.i++];
          if (end === undefined || /[A-Za-z0-9]/.test(end)) this.fail();
        }
        to = end.codePointAt(0)!;
        if (to < from) this.fail();
      }
      tests.push((x) => x >= from && x <= to);
    }
    return negated ? (x) => !tests.some((t) => t(x)) : (x) => tests.some((t) => t(x));
  }
}

const cache = new Map<string, Regex>();

/** A pattern, parsed once. */
export function regex(source: string) {
  let r = cache.get(source);
  if (!r) {
    r = new Regex(source);
    if (cache.size > 200) cache.clear();
    cache.set(source, r);
  }
  return r;
}
