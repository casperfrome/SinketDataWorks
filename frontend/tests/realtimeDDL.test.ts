import assert from "node:assert/strict";
import test from "node:test";
import { applyManagedDDL, findManagedDDL, generateDDL } from "../src/realtime/ddl.ts";
import { createBinding } from "../src/realtime/model.ts";
import type { DataSource } from "../src/types.ts";
import type { KafkaDatasource } from "../src/realtime/types.ts";

const kafka: KafkaDatasource = { id: "kafka", workspaceId: "workspace", type: "KAFKA", name: "Kafka", bootstrapServers: "broker:9092", securityProtocol: "SASL_SSL", saslMechanism: "PLAIN", username: "reader" };
const mysql: DataSource = { id: "mysql", workspaceId: "workspace", type: "MYSQL", name: "MySQL", host: "::1", port: 3306, database: "db.prod", username: "reader", passwordSet: true };
const doris: DataSource = { id: "doris", workspaceId: "workspace", type: "DORIS", name: "Doris", host: "sql-host", port: 9030, database: "warehouse", username: "writer", passwordSet: true, options: { feHttpUrls: ["http://fe-1:8030", "http://fe-2:8030/"] } };

test("Kafka source DDL includes offset and watermark while omitting passwords and JAAS secrets", () => {
  const binding = { ...createBinding("SOURCE"), datasourceId: "kafka", topic: "orders", consumerGroup: "flink-orders", eventTimeField: "event_time" };
  const ddl = generateDDL(binding, { ...kafka, password: "secret-must-not-appear" } as KafkaDatasource);
  assert.match(ddl, /'connector' = 'kafka'/);
  assert.match(ddl, /'properties.bootstrap.servers' = 'broker:9092'/);
  assert.match(ddl, /'properties.group.id' = 'flink-orders'/);
  assert.match(ddl, /WATERMARK FOR `event_time`/);
  assert.match(ddl, /当前不能直接执行/);
  assert.doesNotMatch(ddl, /secret-must-not-appear|'password'\s*=|'properties.sasl.jaas.config'\s*=/);
});

test("append Kafka has no primary-key constraint while upsert uses key/value formats", () => {
  const binding = { ...createBinding("SINK", "KAFKA"), datasourceId: "kafka", topic: "result" };
  const append = generateDDL(binding, kafka);
  assert.doesNotMatch(append, /PRIMARY KEY/);
  assert.doesNotMatch(append, /properties.group.id|scan.startup.mode/);
  const upsert = generateDDL({ ...binding, writeMode: "upsert" }, kafka);
  assert.match(upsert, /'connector' = 'upsert-kafka'/);
  assert.match(upsert, /PRIMARY KEY \(`order_id`\) NOT ENFORCED/);
  assert.match(upsert, /'key.format' = 'json'/);
  assert.match(upsert, /'value.format' = 'json'/);
});

test("CDC exact physical names escape regex metacharacters and preserve source reference", () => {
  const binding = { ...createBinding("SOURCE", "MYSQL_CDC"), datasourceId: "mysql", physicalTable: "orders$archive", serverId: "5400-5404" };
  const ddl = generateDDL(binding, mysql);
  assert.ok(ddl.includes("'database-name' = 'db\\.prod'"));
  assert.ok(ddl.includes("'table-name' = 'orders\\$archive'"));
  assert.match(ddl, /'server-id' = '5400-5404'/);
  assert.match(ddl, /PRIMARY KEY \(`order_id`\) NOT ENFORCED/);
  assert.match(ddl, /数据源引用：mysql/);
  assert.doesNotMatch(ddl, /'password'\s*=/);
});

test("JDBC sink URL handles IPv6 and only upsert emits an enforced-by-storage key hint", () => {
  const binding = { ...createBinding("SINK", "MYSQL_JDBC"), datasourceId: "mysql", physicalTable: "orders" };
  assert.match(generateDDL(binding, mysql), /jdbc:mysql:\/\/\[::1\]:3306\/db.prod/);
  assert.doesNotMatch(generateDDL(binding, mysql), /PRIMARY KEY/);
  assert.match(generateDDL({ ...binding, writeMode: "upsert" }, mysql), /PRIMARY KEY/);
});

test("Doris derives FE HTTP endpoints and never uses SQL port as fenodes", () => {
  const binding = { ...createBinding("SINK", "DORIS"), datasourceId: "doris", physicalTable: "orders", labelPrefix: "orders-label" };
  const ddl = generateDDL(binding, doris);
  assert.match(ddl, /'fenodes' = 'fe-1:8030,fe-2:8030'/);
  assert.match(ddl, /'table.identifier' = 'warehouse.orders'/);
  assert.match(ddl, /'sink.label-prefix' = 'orders-label'/);
  assert.doesNotMatch(ddl, /sql-host:9030/);
  assert.match(generateDDL({ ...binding, feHttpUrls: "localhost:8040" }, doris), /'fenodes' = 'localhost:8040'/);
});

test("identifier and SQL literal escaping preserve user names without creating statements", () => {
  const binding = { ...createBinding("SINK", "MYSQL_JDBC"), datasourceId: "mysql", tableName: "a`b", physicalTable: "orders'archive", writeMode: "upsert" as const };
  const ddl = generateDDL(binding, mysql);
  assert.match(ddl, /CREATE TABLE `a``b`/);
  assert.match(ddl, /'table-name' = 'orders''archive'/);
  assert.match(generateDDL(binding, { ...mysql, id: "unrelated", username: "do-not-use" }), /<主机>/);
  assert.doesNotMatch(generateDDL(binding, { ...mysql, id: "unrelated", username: "do-not-use" }), /do-not-use/);
});

test("first insertion precedes business SQL and repeated updates only replace matching block", () => {
  const manual = "-- 手工业务代码\nINSERT INTO target_orders SELECT * FROM source_orders;\n";
  const sourceDDL = "CREATE TABLE source_orders (id BIGINT);";
  const sinkDDL = "CREATE TABLE target_orders (id BIGINT);";
  const first = applyManagedDDL(manual, "source", sourceDDL);
  assert.ok(first.indexOf("CREATE TABLE") < first.indexOf("INSERT INTO"));
  assert.ok(first.endsWith(manual));
  const both = applyManagedDDL(first, "sink", sinkDDL);
  const changed = applyManagedDDL(both, "source", "-- 手工修改受管理块\nCREATE TABLE source_orders (id STRING);");
  assert.equal(findManagedDDL(changed, "sink"), sinkDDL);
  assert.equal(findManagedDDL(changed, "source"), "-- 手工修改受管理块\nCREATE TABLE source_orders (id STRING);");
  assert.ok(changed.endsWith(manual));
  assert.equal(findManagedDDL(changed, "absent"), undefined);
  assert.equal((changed.match(/@realtime-binding:source:begin/g) || []).length, 1);
});

test("incomplete, nested and duplicate generated markers never swallow manual SQL", () => {
  const incomplete = "-- @realtime-binding:a:begin\nSELECT 1;";
  assert.throws(() => applyManagedDDL(incomplete, "a", "replacement"), /不完整/);
  const duplicate = applyManagedDDL("", "a", "SELECT 1;") + applyManagedDDL("", "a", "SELECT 2;");
  assert.throws(() => applyManagedDDL(duplicate, "a", "replacement"), /重复/);
  const nested = "-- @realtime-binding:a:begin\n-- @realtime-binding:b:begin\nSELECT 1;\n-- @realtime-binding:a:end";
  assert.throws(() => applyManagedDDL(nested, "a", "replacement"), /嵌套/);
  assert.ok(incomplete.includes("SELECT 1;"));
});
