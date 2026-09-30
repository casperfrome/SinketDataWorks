import test from "node:test";
import assert from "node:assert/strict";
import { applySyncChange, defaultPartitionAssignments, parameterText, schemaLabel, sourceTableReset, syncParameterText, targetTableReset } from "../src/state/sync.ts";
import { nodeTypes, nodeGroups, defaultContent, getNodeType } from "../src/data/nodeTypes.ts";
import type { DataSource, StudioObject, SyncMetadata } from "../src/types.ts";

const object = (config: StudioObject["config"]): StudioObject => ({ id: "sync", workspaceId: "w", parentId: null, kind: "NODE", nodeType: "数据集成", name: "sync", content: "", description: "", config, tags: [], favorite: false, deleted: false, version: 1, owner: "local", updatedAt: "now" });

test("node catalog exposes only supported nodes and keeps legacy editors readable", () => {
  assert.deepEqual(nodeTypes.map(node => node.type), ["数据集成", "MySQL", "Doris"]);
  assert.deepEqual(nodeGroups, ["数据集成", "数据库"]);
  assert.equal(getNodeType("离线同步").editor, "form");
  assert.equal(getNodeType("周期工作流").editor, "workflow");
  assert.match(defaultContent("Doris"), /SELECT 1/);
  assert.doesNotMatch(defaultContent("Doris"), /模拟/);
});

test("schema label distinguishes connection identities without hiding the database", () => {
  assert.equal(schemaLabel({ database: "test_ods", type: "DORIS", name: "订单仓库" } as DataSource), "test_ods · Doris / 订单仓库");
});

test("parameter collection covers data, source partition and target value expressions", () => {
  const config = { where: "status = '${status}'", sourcePartitionFilter: { partitions: ["p20260929"], where: "ds = '${read_day}'" }, targetPartitionAssignments: [{ target: "ds", mode: "value" as const, value: "${write_day}" }, { target: "region", mode: "column" as const, source: "region_id" }] };
  assert.equal(syncParameterText(config), "status = '${status}'\nds = '${read_day}'\n'${write_day}'");
  assert.equal(parameterText(object({ run: { provider: "SYNC" }, sync: config })), syncParameterText(config));
  assert.equal(parameterText({ ...object({ run: { provider: "DORIS" } }), content: "SELECT '${day}'" }), "SELECT '${day}'");
});

test("partition parameter scanning quotes literal text and escapes SQL delimiters", () => {
  const values = ["O'Reilly", "foo:bar", "C:\\orders\\${day}", "a'; -- ${region}", "", ":bizdate-extra", ":unknown"];
  const code = syncParameterText({ where: "id > :build_id", sourcePartitionFilter: { where: "ds = '${read_day}'" }, targetPartitionAssignments: values.map((value, index) => ({ target: `key${index}`, mode: "value", value })) });
  assert.equal(code, "id > :build_id\nds = '${read_day}'\n'O''Reilly'\n'foo:bar'\n'C:\\\\orders\\\\${day}'\n'a''; -- ${region}'\n''\n':bizdate-extra'\n':unknown'");
});

test("only complete built-in partition values remain unquoted SQL tokens", () => {
  const tokens = [":bizdate", ":source_cutoff", ":build_id", ":upstream_orders_build_id", ":upstream_a1_2_build_id"];
  assert.equal(syncParameterText({ targetPartitionAssignments: tokens.map(value => ({ target: "ds", mode: "value", value })) }), tokens.join("\n"));
  assert.equal(syncParameterText({ targetPartitionAssignments: [{ target: "ds", mode: "value", value: ":bizdate\n" }] }), "':bizdate\n'");
});

test("DATE partition defaults add a business-date parameter without replacing user values", () => {
  const metadata = { partition: { type: "RANGE", automatic: true, columns: [{ name: "ds", type: "DATEV2" }, { name: "region", type: "VARCHAR(20)" }], partitions: [] } } as unknown as SyncMetadata;
  const assignments = defaultPartitionAssignments(metadata);
  assert.deepEqual(assignments, [{ target: "ds", mode: "value", value: "${bizdate}" }]);
  const initial = object({ schedule: { parameters: [{ name: "region", value: "CN", source: "MANUAL" }], parameterExpressionDraft: "region=CN" }, sync: { where: "id > 0" } });
  const config = applySyncChange(initial, { targetPartitionAssignments: assignments }).config!;
  assert.deepEqual(config.schedule.parameters.map((p: { name: string; value: string }) => [p.name, p.value]), [["region", "CN"], ["bizdate", "$bizdate"]]);
  assert.equal(config.schedule.parameterExpressionDraft, "region=CN bizdate=$bizdate");
  assert.equal(initial.config.schedule.parameters.length, 1);
  const configured = object({ schedule: { parameters: [{ name: "bizdate", value: "$[yyyymmdd-2]", source: "MANUAL" }] } });
  assert.equal(applySyncChange(configured, { targetPartitionAssignments: assignments }).config!.schedule.parameters[0].value, "$[yyyymmdd-2]");
});

test("changing source table drops dependent fields and preserves fixed target partition values", () => {
  const patch = sourceTableReset({ columns: ["ordered_at"], sourcePartitionFilter: { where: "ds = '${bizdate}'" }, targetPartitionAssignments: [{ target: "ds", mode: "column", source: "ordered_at" }, { target: "region", mode: "value", value: "CN" }] });
  assert.deepEqual(patch.columns, []);
  assert.deepEqual(patch.mapping, []);
  assert.equal(patch.sourcePartitionFilter, undefined);
  assert.equal(patch.where, undefined);
  assert.deepEqual(patch.targetPartitionAssignments, [{ target: "region", mode: "value", value: "CN" }]);
  assert.deepEqual(targetTableReset(), { mapping: [], keyColumns: [], targetPartitionAssignments: [], targetPartitions: [] });
});
