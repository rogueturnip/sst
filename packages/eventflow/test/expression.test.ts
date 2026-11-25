import { describe, expect, it } from "bun:test";
import { regex } from "../src/regex.ts";
import { bind, compileKey, evaluate, evaluateValue, parseExpression } from "../src/expression.ts";

const event = {
  name: "order/placed",
  data: {
    id: "o1", amount: 120, plan: "pro", test: false, email: "Ada@Example.com", empty: "", nothing: null,
    tags: ["a", "b"], items: [{ price: 5, sku: "x" }, { price: 150, sku: "y" }], "odd-key": 7, nested: { list: [1, 2, 3] },
  },
};
const value = (source: string, scope: any = { event }) => evaluateValue(parseExpression(source, "test"), scope);
const is = (source: string, scope: any = { event }) => evaluate(parseExpression(source, "test"), scope);

describe("expressions", () => {
  it("evaluates what Inngest's keys and ifs use", () => {
    const cases: [string, unknown][] = [
      ["event.data.id", "o1"],
      ["event.data.items[1].sku", "y"],
      ['event.data["odd-key"] + 1', 8],
      ['event.data.plan + "-" + event.data.id', "pro-o1"],
      ['event.data.plan == "pro" && !event.data.test', true],
      ['event.data.plan == "free" || event.data.amount > 100', true],
      ["!(event.data.amount > 100)", false],
      ['event.data.plan in ["pro", "team"]', true],
      ['"amount" in event.data', true],
      ["event.data.amount * 2 - 40 / 4 % 3", 239],
      ["-event.data.amount", -120],
      ["1 + 2 * 3 == 7", true],
      ["(1 + 2) * 3", 9],
      ['event.data.amount >= 100 ? "big" : "small"', "big"],
      ["has(event.data.id) && !has(event.data.coupon)", true],
      ["size(event.data.tags)", 2],
      ["event.data.tags.size() + size(event.data.plan)", 5],
      ['event.data.email.lowerAscii().endsWith("@example.com")', true],
      ['event.data.email.startsWith("Ada") && event.data.email.contains("@")', true],
      ["event.data.items.exists(i, i.price > 100)", true],
      ["event.data.items.all(i, i.price > 100)", false],
      ["event.data.items.exists_one(i, i.price > 100)", true],
      ["event.data.items.filter(i, i.price > 100).map(i, i.sku)", ["y"]],
      ["event.data.items.map(i, i.price * event.data.amount)", [600, 18000]],
      ['event.data.tags.join("-")', "a-b"],
      ['string(event.data.amount) + "!"', "120!"],
      ['int("42") + int(1.9)', 43],
      ['"it\\"s" == \'it"s\'', true],
      ["event.data.nothing == null", true],
      ["1.5e2", 150],
    ];
    for (const [source, expected] of cases) expect([source, value(source)]).toEqual([source, expected]);
  });

  it("follows precedence: ! before *, before +, before ==, before &&, before ||", () => {
    expect(value("true || false && false")).toBe(true);
    expect(value("(true || false) && false")).toBe(false);
    expect(value("1 + 1 == 2 && 2 < 3")).toBe(true);
    expect(value("false ? 1 : true ? 2 : 3")).toBe(2);
    expect(value("10 - 3 - 2")).toBe(5);
  });

  it("refuses what it doesn't know, when it's parsed", () => {
    for (const bad of [
      "event.data.id.matches(event.data.id)", "event.data.id.matches('(')", "event.data.id.matches('(a)\\\\1')", "event.data.id.matches('(?=a)')", "process.exit()", "event.data.id = 1", "event.data.", "&& a", "a &&", "a ? b", "[1, 2", "(a",
      "a b", "foo(1)", "event.data.x.constructor()", "has(1)", "event.data.items.map(event, 1)", "a..b", "'open", "a ; b", "${a}", "a => b", "in", "x.size(1)",
    ])
      expect(() => parseExpression(bad, "test")).toThrow(TypeError);
    expect(() => parseExpression("(".repeat(500) + "1" + ")".repeat(500), "test")).toThrow("nested too deep");
    expect(() => parseExpression("a".repeat(9000), "test")).toThrow("characters");
  });

  it("can't reach a prototype, through a path, a key, or a macro's variable", () => {
    for (const source of [
      "event.__proto__", "event.constructor", 'event["__proto__"]', 'event.data["const" + "ructor"]', 'event.data.tags["length"]',
      "event.data.items.map(i, i.__proto__)", '"__proto__" in event.data', "toString", "event.data.id.length",
    ]) {
      const v = value(source);
      expect([source, [undefined, false].includes(v as any) || (Array.isArray(v) && v.every((x) => x === undefined)) || source.includes("length")]).toEqual([source, true]);
    }
    expect(value("event.data.items.map(__proto__, 1)")).toEqual([1, 1]);
    expect(({} as any).polluted).toBeUndefined();
  });

  it("doesn't read a string or a list as a number", () => {
    for (const amount of ["1000", [1000], { valueOf: 1000 }, null, true]) {
      expect(is("event.data.amount > 100", { event: { data: { amount } } })).toBe(false);
      expect(() => value("event.data.amount + 1", { event: { data: { amount } } })).toThrow();
    }
  });

  it("bounds the work an event can make it do", () => {
    const big = { event: { data: { list: Array.from({ length: 2000 }, (_, i) => i) } } };
    const started = Date.now();
    expect(() => is("event.data.list.all(a, event.data.list.all(b, a + b >= 0))", big)).toThrow("too much work");
    expect(Date.now() - started).toBeLessThan(2000);
  });

  it("has the side that decides it decide && and ||, when the other has no value", () => {
    expect(value("1 / 0 > 1 || true")).toBe(true);
    expect(value("1 / 0 > 1 && false")).toBe(false);
    expect(() => value("1 / 0 > 1 || false")).toThrow();
  });

  it("binds the run's values, and never matches on one it doesn't have", () => {
    const of = (source: string) => bind(parseExpression(source, "test"), "event", event);
    const match = (source: string, async: unknown) => {
      try {
        return evaluate(JSON.parse(JSON.stringify(of(source))), { async });
      } catch {
        return false;
      }
    };
    expect(match("async.data.id == event.data.id", { data: { id: "o1" } })).toBe(true);
    expect(match("async.data.id == event.data.id", { data: { id: "o2" } })).toBe(false);
    // Not as null, which a sender could send.
    expect(match("async.data.user == event.data.user", { data: { user: null } })).toBe(false);
    expect(match("async.data.user == event.data.user", { data: {} })).toBe(false);
    expect(match("event.data.user == async.data.user || async.data.admin", { data: { admin: true } })).toBe(true);
    expect(match("has(event.data.coupon) ? async.data.a : async.data.b", { data: { a: true, b: false } })).toBe(false);
    expect(match("event.data.items.exists(i, i.sku == async.data.sku)", { data: { sku: "y" } })).toBe(true);
  });

  it("makes keys of any expression, and none where it has no value", () => {
    const key = (source: string) => compileKey(source, "test")({ event });
    expect(key('event.data.plan + ":" + event.data.id')).toBe("pro:o1");
    expect(key("event.data.amount")).toBe("120");
    expect(key("event.data.tags")).toBe('["a","b"]');
    for (const none of ["event.data.missing", "event.data.empty", "event.data.nothing", 'event.data.missing + "x"']) expect(key(none)).toBeUndefined();
  });
});

describe("matches()", () => {
  it("agrees with JavaScript's on patterns both have", () => {
    const patterns = ["abc", "^abc$", "a.c", "[a-c]+", "[^a-c]", "\\d{2,3}", "^\\w+@\\w+\\.com$", "(foo|bar)baz", "a(b|c)*d", "x?y+z*", "^$", "\\bcat\\b", "[\\w.-]+", "a{0,2}b", "(?:ab)+$", "(a|ab)(c|bcd)", "\\s", "[]a]", "é+", "😀", "^[A-Z]{3}-\\d+$", "a|", "(a*)(b+)?$", "[\\d-]+x", "\\.", "a+?b"];
    const texts = ["", "abc", "xabcx", "aXc", "cat", "concat", "the cat.", "foobaz", "barbaz", "ad", "abcbd", "y", "xyyz", "12", "1234", "ada@example.com", "ab", "abab", "abcd", "b", "]", "éé", "x😀", "ABC-12", "a b", "1-2x", ".", "aab", "\n"];
    for (const p of patterns)
      for (const t of texts) {
        const [js, mine] = [new RegExp(p === "[]a]" ? "[\\]a]" : p, "u".replace("u", /[\]a]|\[\\d-/.test(p) ? "" : "u")).test(t), regex(p).test(t)];
        expect([p, t, mine]).toEqual([p, t, js]);
      }
  });

  it("takes time in proportion to the text, whatever the pattern", () => {
    const started = Date.now();
    for (const p of ["(a+)+$", "(a|a)*$", "(a|aa)+$", "([a-z]+)*\\d", "(.*a){12}"]) regex(p).test("a".repeat(20_000) + "!");
    expect(Date.now() - started).toBeLessThan(3000);
    const event = { event: { data: { t: "a".repeat(300_000) + "!" } } };
    expect(() => is('event.data.t.matches("(a+)+(a+)+(a+)+b{1,100}$")', event)).toThrow("too much work");
  });

  it("is an expression's", () => {
    expect(is('event.data.email.lowerAscii().matches("^[a-z]+@example[.]com$")')).toBe(true);
    expect(() => value("event.data.amount.matches('1')")).toThrow();
  });
});
