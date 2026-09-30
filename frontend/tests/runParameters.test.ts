import test from "node:test";
import assert from "node:assert/strict";
import { api } from "../src/api.ts";
import type { RunParameterPreparation, StudioObject } from "../src/types.ts";
import {
  effectiveRunParameters,
  initialDebugParameters,
  restoreRunParameterDefault,
  runParameterError,
  runParameterValueError,
  setDebugParameter,
  shouldOpenRunParameters,
} from "../src/state/runParameters.ts";

const preparation = (overrides: Partial<RunParameterPreparation> = {}): RunParameterPreparation => ({
  parameters: [
    { name: "bizdate", value: "20260929", defaultValue: "20260929", source: "SCHEDULE" },
    { name: "region", value: "remembered", defaultValue: "default", source: "DEBUG" },
    { name: "missing", source: "MISSING" },
  ],
  debugParameters: { region: "remembered" },
  missingParameters: ["missing"],
  ...overrides,
});

test("ordinary runs prompt only for missing parameters while custom runs always prompt", () => {
  assert.equal(shouldOpenRunParameters("NORMAL", preparation()), true);
  const ready = preparation({ missingParameters: [] });
  assert.equal(shouldOpenRunParameters("NORMAL", ready), false);
  assert.equal(shouldOpenRunParameters("CUSTOM", ready), true);
  const empty = preparation({ parameters: [], debugParameters: {}, missingParameters: [] });
  assert.equal(shouldOpenRunParameters("NORMAL", empty), false);
  assert.equal(shouldOpenRunParameters("CUSTOM", empty), true);
  assert.equal(runParameterError(empty, {}), "");
});

test("unchanged schedule defaults never become remembered debug overrides", () => {
  const prepared = preparation();
  const initial = initialDebugParameters(prepared);
  assert.deepEqual(initial, { region: "remembered" });
  assert.notEqual(initial, prepared.debugParameters);
  const edited = setDebugParameter(initial, "missing", " north = 1 ");
  assert.deepEqual(edited, { region: "remembered", missing: " north = 1 " });
  assert.equal(Object.hasOwn(edited, "bizdate"), false);
  assert.deepEqual(prepared.debugParameters, { region: "remembered" });
  assert.equal(prepared.parameters[2].value, undefined);
  assert.equal(runParameterError(prepared, edited), "");
});

test("restoring an override resolves the current schedule default rather than its old remembered value", () => {
  const prepared = preparation();
  const restored = restoreRunParameterDefault(prepared.debugParameters, "region");
  assert.deepEqual(restored, {});
  assert.deepEqual(prepared.debugParameters, { region: "remembered" });
  const rows = effectiveRunParameters(prepared, restored);
  assert.equal(rows[1].value, "default");
  assert.equal(rows[1].source, "SCHEDULE");
  assert.equal(rows[2].source, "MISSING");
  const withoutDefault = preparation({ parameters: [{ name: "region", value: "remembered", source: "DEBUG" }], missingParameters: [] });
  assert.equal(effectiveRunParameters(withoutDefault, {})[0].value, undefined);
  assert.match(runParameterError(withoutDefault, {}), /region.*请填写/);
});

test("restoring all defaults clears the replacement profile and reevaluates unresolved parameters", () => {
  const prepared = preparation();
  assert.equal(effectiveRunParameters(prepared, {})[1].value, "default");
  assert.match(runParameterError(prepared, {}), /missing.*请填写/);
  assert.deepEqual(prepared.debugParameters, { region: "remembered" });
});

test("debug constants retain spaces, equals, quotes and Unicode while rejecting missing or oversized values", () => {
  for (const value of ["with spaces", "a=b", " O'Reilly;中文💫 ", "x".repeat(4096)]) assert.equal(runParameterValueError(value), "");
  assert.match(runParameterValueError(undefined), /填写/);
  assert.match(runParameterValueError(""), /填写/);
  assert.match(runParameterValueError("x".repeat(4097)), /4096/);
});

test("parameter names matching object prototype properties remain explicit values", () => {
  const prepared = preparation({ parameters: [{ name: "__proto__", source: "MISSING" }, { name: "constructor", source: "MISSING" }], debugParameters: {}, missingParameters: ["__proto__", "constructor"] });
  assert.match(runParameterError(prepared, {}), /__proto__/);
  const edited = setDebugParameter(setDebugParameter({}, "__proto__", "safe"), "constructor", "literal");
  assert.equal(Object.hasOwn(edited, "__proto__"), true);
  assert.equal(runParameterError(prepared, edited), "");
  assert.deepEqual(effectiveRunParameters(prepared, edited).map(row => row.value), ["safe", "literal"]);
  assert.equal(Object.getPrototypeOf(edited), Object.prototype);
});

test("run submission distinguishes an omitted remembered profile from explicit replacement and clearing", async context => {
  const payloads: Record<string, unknown>[] = [];
  context.mock.method(globalThis, "fetch", async (_url: unknown, input?: RequestInit) => {
    payloads.push(JSON.parse(input?.body as string));
    return new Response(JSON.stringify({ id: "accepted" }), { status: 200 });
  });
  await api.run("node", false, "MANUAL", 4);
  await api.run("node", false, "MANUAL", 4, undefined, undefined, { region: " a=b " });
  await api.run("node", false, "MANUAL", 4, undefined, undefined, {});
  assert.equal(Object.hasOwn(payloads[0], "debugParameters"), false);
  assert.equal(payloads[0].expectedVersion, 4);
  assert.deepEqual(payloads[1].debugParameters, { region: " a=b " });
  assert.deepEqual(payloads[2].debugParameters, {});
  assert.equal(Object.hasOwn(payloads[1], "scheduleParameters"), false);
});

test("preparing parameters uses the captured draft without changing its schedule configuration", async context => {
  const object: StudioObject = { id: "captured", workspaceId: "workspace", parentId: null, kind: "NODE", nodeType: "MySQL", name: "captured", description: "", content: "SELECT '${missing}'", config: { run: { provider: "MYSQL" }, schedule: { parameters: [] } }, tags: [], favorite: false, deleted: false, version: 3, owner: "local", updatedAt: "now" };
  let payload: { object: StudioObject } | undefined;
  context.mock.method(globalThis, "fetch", async (url: unknown, input?: RequestInit) => {
    assert.equal(url, "/api/v1/runs/parameters/prepare");
    assert.equal(input?.method, "POST");
    payload = JSON.parse(input?.body as string);
    return new Response(JSON.stringify(preparation()), { status: 200 });
  });
  const result = await api.prepareRunParameters(object);
  assert.deepEqual(payload, { object });
  assert.deepEqual(object.config.schedule.parameters, []);
  assert.equal(Object.hasOwn(object.config, "debugParameters"), false);
  assert.deepEqual(result, preparation());
});
