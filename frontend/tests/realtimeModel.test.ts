import assert from "node:assert/strict";
import test from "node:test";
import { createBinding, createRelease, createTask, mysqlTypeToFlink, validateBinding, validateKafkaSource, validateTask } from "../src/realtime/model.ts";
import type { KafkaDatasource, RealtimeDatasource } from "../src/realtime/types.ts";

const kafka: KafkaDatasource = { id: "kafka", workspaceId: "workspace", type: "KAFKA", name: "Kafka", bootstrapServers: "broker-1:9092,[::1]:9092", securityProtocol: "PLAINTEXT", saslMechanism: "PLAIN", username: "" };
const sources: RealtimeDatasource[] = [kafka, { id: "mysql", workspaceId: "workspace", type: "MYSQL", name: "MySQL", host: "127.0.0.1", port: 3306, database: "business", username: "reader", passwordSet: true }, { id: "doris", workspaceId: "workspace", type: "DORIS", name: "Doris", host: "127.0.0.1", port: 9030, database: "warehouse", username: "writer", passwordSet: true, options: { feHttpUrls: ["http://127.0.0.1:8030"] } }];
const configuredTask = () => {
  const task = createTask("workspace", "订单实时开发", null, true);
  task.bindings[0] = { ...task.bindings[0], datasourceId: "kafka", topic: "orders", consumerGroup: "flink-orders" };
  task.bindings[1] = { ...task.bindings[1], datasourceId: "doris", physicalTable: "orders", labelPrefix: "orders-job" };
  return task;
};

test("task defaults and configured example validate without inventing real datasource IDs", () => {
  const blank = createTask("workspace", "New task");
  assert.equal(blank.runtime.checkpointSeconds, 60);
  assert.equal(blank.bindings.length, 0);
  assert.equal(validateTask(configuredTask(), sources).length, 0);
  const errors = validateTask(blank, sources);
  assert.ok(errors.some(error => error.includes("SQL")));
});

test("Kafka validates broker endpoints, optional SASL user, and disallows URL credentials", () => {
  assert.deepEqual(validateKafkaSource(kafka), []);
  for (const bootstrapServers of ["broker", "broker:0", "broker:65536", "broker:9092,", "http://user:secret@broker:9092"]) assert.ok(validateKafkaSource({ ...kafka, bootstrapServers }).length > 0);
  assert.ok(validateKafkaSource({ ...kafka, securityProtocol: "SASL_SSL" }).some(error => error.includes("用户名")));
  assert.deepEqual(validateKafkaSource({ ...kafka, securityProtocol: "SASL_SSL", username: "user" }), []);
});

test("connector matching, duplicate fields, and nullable primary keys are rejected", () => {
  const binding = configuredTask().bindings[0];
  assert.ok(validateBinding({ ...binding, datasourceId: "mysql" }, sources).some(error => error.includes("类型不匹配")));
  const invalid = { ...binding, fields: [...binding.fields, { ...binding.fields[0], id: "duplicate", name: "ORDER_ID", nullable: true }] };
  const errors = validateBinding(invalid, sources);
  assert.ok(errors.some(error => error.includes("重复")));
  assert.ok(errors.some(error => error.includes("不能允许空值")));
  assert.ok(validateBinding({ ...binding, fields: [{ ...binding.fields[0], type: "INT); DROP TABLE x" }] }, sources).some(error => error.includes("类型无效")));
});

test("Kafka upsert requires keys and a supported key/value format", () => {
  const binding = { ...createBinding("SINK", "KAFKA"), datasourceId: "kafka", topic: "orders", writeMode: "upsert" as const };
  assert.deepEqual(validateBinding(binding, sources), []);
  assert.ok(validateBinding({ ...binding, fields: binding.fields.map(field => ({ ...field, primaryKey: false })) }, sources).some(error => error.includes("需要主键")));
  assert.ok(validateBinding({ ...binding, format: "csv" }, sources).some(error => error.includes("JSON")));
});

test("CDC validates key policy, server ID range capacity and timezone", () => {
  const task = configuredTask();
  task.bindings[0] = { ...createBinding("SOURCE", "MYSQL_CDC"), datasourceId: "mysql", physicalTable: "orders", serverId: "5400-5401" };
  task.runtime.parallelism = 3;
  assert.ok(validateTask(task, sources).some(error => error.includes("来源并行度")));
  task.bindings[0].serverId = "5400-5402";
  assert.deepEqual(validateTask(task, sources), []);
  assert.ok(validateBinding({ ...task.bindings[0], serverId: "5402-5400" }, sources).some(error => error.includes("递增范围")));
  assert.deepEqual(validateBinding({ ...task.bindings[0], serverId: "" }, sources), []);
  assert.ok(validateBinding({ ...task.bindings[0], serverId: "2147483648" }, sources).some(error => error.includes("2147483647")));
  assert.ok(validateBinding({ ...task.bindings[0], timezone: "Fake/Timezone" }, sources).some(error => error.includes("时区")));
  assert.ok(validateBinding({ ...task.bindings[0], fields: task.bindings[0].fields.map(field => ({ ...field, primaryKey: false })) }, sources).some(error => error.includes("chunk key")));
});

test("Doris HTTP FE endpoints and delete model must remain compatible", () => {
  const binding = configuredTask().bindings[1];
  assert.deepEqual(validateBinding({ ...binding, feHttpUrls: "localhost:8030,http://other:8030" }, sources), []);
  assert.ok(validateBinding({ ...binding, feHttpUrls: "http://user:secret@localhost:8030" }, sources).some(error => error.includes("FE HTTP")));
  assert.ok(validateBinding({ ...binding, dorisModel: "DUPLICATE" }, sources).some(error => error.includes("Unique")));
  assert.ok(validateBinding({ ...binding, fields: binding.fields.map(field => ({ ...field, primaryKey: false })) }, sources).some(error => error.includes("主键字段")));
});

test("Watermarks require a source timestamp of supported precision and nonnegative delay", () => {
  const binding = configuredTask().bindings[0];
  assert.deepEqual(validateBinding({ ...binding, eventTimeField: "event_time", watermarkSeconds: 5 }, sources), []);
  assert.ok(validateBinding({ ...binding, eventTimeField: "amount" }, sources).some(error => error.includes("TIMESTAMP")));
  assert.ok(validateBinding({ ...binding, eventTimeField: "event_time", watermarkSeconds: -1 }, sources).some(error => error.includes("非负整数")));
});

test("MySQL type conversion retains unsigned range, decimal precision and timestamp semantics", () => {
  assert.equal(mysqlTypeToFlink("bigint(20) unsigned"), "DECIMAL(20,0)");
  assert.equal(mysqlTypeToFlink("int unsigned"), "BIGINT");
  assert.equal(mysqlTypeToFlink("decimal(18, 2)"), "DECIMAL(18,2)");
  assert.equal(mysqlTypeToFlink("decimal(65,2)"), undefined);
  assert.equal(mysqlTypeToFlink("datetime(6)"), "TIMESTAMP(6)");
  assert.equal(mysqlTypeToFlink("timestamp(3)"), "TIMESTAMP_LTZ(3)");
  assert.equal(mysqlTypeToFlink("varchar(100)"), "STRING");
  assert.equal(mysqlTypeToFlink("geometry"), undefined);
});

test("publish deep-copies SQL, connector fields and runtime and versions are task-specific", () => {
  const task = configuredTask();
  const other = createRelease({ ...task, id: "other-task" }, []);
  other.releaseNo = 88;
  const first = createRelease(task, [other]);
  const second = createRelease(task, [other, first], "发布说明");
  task.sql = "-- newer draft";
  task.bindings[0].fields[0].name = "changed";
  task.runtime.parallelism = 9;
  assert.equal(first.releaseNo, 1);
  assert.equal(second.releaseNo, 2);
  assert.equal(second.note, "发布说明");
  assert.notEqual(first.snapshot.sql, task.sql);
  assert.equal(first.snapshot.bindings[0].fields[0].name, "order_id");
  assert.equal(first.snapshot.runtime.parallelism, 1);
});


test("handwritten SQL can validate locally without configuring bindings", () => {
  const task = { ...createTask("workspace", "SQL only"), sql: "SELECT 1;" };
  assert.deepEqual(validateTask(task, []), []);
});
