import { test } from "node:test";
import assert from "node:assert/strict";
import { activeInstanceStatuses, dependencySummary, parametersDiffer, validBackfillRange } from "../src/state/schedulingOperations.ts";

test("release parameter comparison ignores row order and extraction source but detects changed values", () => {
  const released = [{ name: "region", value: "杭州", source: "MANUAL" as const }, { name: "date", value: "$bizdate", source: "CODE" as const }];
  assert.equal(parametersDiffer([...released].reverse().map(row => ({ ...row, source: "MANUAL" })), released), false);
  assert.equal(parametersDiffer([{ ...released[0], value: "上海" }, released[1]], released), true);
  assert.equal(parametersDiffer(released.slice(1), released), true);
});

test("same-day dependencies retain every period and report expected count for a compact preview", () => {
  const slots = [{ taskId: "hourly", alias: "ods", name: "小时源", matchMode: "ALL_DAY" as const, scheduledAt: "2026-10-01T00:00:00Z", expectedCount: 24 }, { taskId: "hourly", alias: "ods", matchMode: "ALL_DAY" as const, scheduledAt: "2026-10-01T01:00:00Z" }, { taskId: "daily", alias: "dwd", scheduledAt: "2026-10-01T02:00:00Z" }];
  const result = dependencySummary(slots);
  assert.equal(result.length, 2);
  assert.equal(result[0].count, 24);
  assert.equal(result[0].slots.length, 2);
  assert.equal(result[1].mode, "LATEST");
  assert.equal(result[1].count, 1);
});

test("waiting and retry instances are active even before a run exists", () => {
  for (const status of ["PENDING", "WAITING_DEPENDENCY", "WAITING_RESOURCE", "RETRY_WAIT", "RUNNING"]) assert.equal(activeInstanceStatuses.has(status), true);
  for (const status of ["FAILED", "BLOCKED", "SKIPPED", "SUCCESS", "CANCELLED"]) assert.equal(activeInstanceStatuses.has(status), false);
});

test("backfill rejects reversed business dates and daily root-period windows", () => {
  assert.equal(validBackfillRange("2026-09-28", "2026-10-01", "00:00", "23:59"), "");
  assert.match(validBackfillRange("", "2026-10-01", "", ""), /请选择/);
  assert.match(validBackfillRange("2026-10-02", "2026-10-01", "00:00", "23:59"), /结束业务日期/);
  assert.match(validBackfillRange("2026-10-01", "2026-10-01", "03:00", "02:00"), /结束期次/);
});
