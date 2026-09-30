import test from "node:test";
import assert from "node:assert/strict";
import { mergeCodeParameters, parameterExpression, parseParameterExpression, parameterError } from "../src/state/parameters.ts";

test("code reload preserves values and manual rows while deduplicating names", () => {
  const rows = [{ name: "bizdate", value: "$[yyyymmdd-1]", source: "MANUAL" as const }, { name: "region", value: "杭州", source: "MANUAL" as const }];
  assert.deepEqual(mergeCodeParameters(rows, ["bizdate", "next", "next"]), [
    { ...rows[0], source: "CODE" }, rows[1], { name: "next", value: "", source: "CODE" },
  ]);
  assert.equal(rows[0].source, "MANUAL");
});
test("visual and expression modes round-trip expressions and provenance", () => {
  const rows = [{ name: "bizdate", value: "$[yyyymmdd-1]", source: "CODE" as const }, { name: "region", value: "杭州", source: "MANUAL" as const }];
  assert.deepEqual(parseParameterExpression(parameterExpression(rows), rows), rows);
  assert.deepEqual(parseParameterExpression("", rows), []);
});
test("rejects duplicated names, missing values and whitespace around equals", () => {
  for (const value of ["a=1 a=2", "a=", "a =1", "a= 1", "a=b=c", "1bad=ok"]) assert.throws(() => parseParameterExpression(value, []));
  assert.match(parameterError([{ name: "x", value: "two words", source: "MANUAL" }]), /空白/);
});
