import { test } from "node:test";
import assert from "node:assert/strict";
import { api } from "../src/api.ts";

test("unified instance operations retain kind, pagination, date range and attempt identity", async () => {
  const calls: { url: string; body?: string; method?: string }[] = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (input, init) => {
    calls.push({ url: String(input), body: init?.body as string | undefined, method: init?.method });
    return new Response(JSON.stringify({ items: [], total: 0 }), { status: 200 });
  };
  try {
    await api.schedulingInstances("warehouse space", { page: "3", kind: "TASK", scheduleId: "hourly", businessDateFrom: "2026-09-28", businessDateTo: "2026-10-01", status: "BLOCKED" });
    const url = new URL(calls[0].url, "http://localhost");
    assert.equal(url.pathname, "/api/v1/scheduling/instances");
    assert.equal(url.searchParams.get("workspaceId"), "warehouse space");
    assert.equal(url.searchParams.get("page"), "3");
    assert.equal(url.searchParams.get("businessDateTo"), "2026-10-01");
    await api.rerunSchedulingInstance("WORKFLOW", "period-1", "ATTEMPT", "old-run");
    assert.equal(calls[1].url, "/api/v1/scheduling/instances/WORKFLOW/period-1/rerun");
    assert.deepEqual(JSON.parse(calls[1].body!), { mode: "ORIGINAL", attemptRunId: "old-run" });
    await api.setSchedulingEnabled("TASK", "plan-1", false, 4);
    assert.deepEqual(JSON.parse(calls[2].body!), { enabled: false, expectedVersion: 4 });
  } finally { globalThis.fetch = originalFetch; }
});

test("backfill submission sends the preview token and reuses the request identifier", async () => {
  const bodies: unknown[] = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (_input, init) => {
    bodies.push(JSON.parse(init!.body as string));
    return new Response(JSON.stringify({ batchKey: "batch", instances: [], total: 0 }), { status: 200 });
  };
  try {
    const input = { workspaceId: "dw", kind: "TASK" as const, scheduleId: "hourly", startDate: "2026-09-28", endDate: "2026-10-01", startTime: "02:00", endTime: "04:00", includeDownstream: true, previewToken: "signed-preview", requestId: "same-request" };
    await api.submitBackfill(input);
    await api.submitBackfill(input);
    assert.deepEqual(bodies, [input, input]);
  } finally { globalThis.fetch = originalFetch; }
});
