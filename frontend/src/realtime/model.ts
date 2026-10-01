import type { KafkaDatasource, RealtimeBinding, RealtimeDatasource, RealtimeField, RealtimeRelease, RealtimeTask } from "./types.ts";

export const uid = (prefix = "rt") => `${prefix}-${globalThis.crypto?.randomUUID?.() || `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`}`;
const timestamp = (now = Date.now()) => new Date(now).toISOString();
const field = (name: string, type: string, primaryKey = false): RealtimeField => ({ id: uid("field"), name, type, primaryKey, nullable: !primaryKey });

export function createBinding(role: RealtimeBinding["role"], connector?: RealtimeBinding["connector"]): RealtimeBinding {
  return {
    id: uid("binding"), role, connector: connector || (role === "SOURCE" ? "KAFKA" : "DORIS"),
    datasourceId: "", tableName: role === "SOURCE" ? "source_orders" : "target_orders", physicalTable: "", topic: "",
    fields: [field("order_id", "BIGINT", true), field("amount", "DECIMAL(18,2)"), field("event_time", "TIMESTAMP(3)")],
    format: "json", consumerGroup: "", startupMode: "group-offsets", writeMode: "append", cdcStartupMode: "initial",
    serverId: "", timezone: "Asia/Shanghai", eventTimeField: "", watermarkSeconds: 5,
    dorisModel: "UNIQUE", syncDeletes: true, labelPrefix: "", feHttpUrls: "",
  };
}

export function createTask(workspaceId: string, name: string, folderId: string | null = null, example = false): RealtimeTask {
  const task: RealtimeTask = {
    id: uid("realtime-task"), workspaceId, folderId, name: name.trim(), description: "", sql: "-- 编写 Flink SQL，或从连接配置生成 CREATE TABLE 预览。\n",
    bindings: [], runtime: { parallelism: 1, checkpointSeconds: 60, restartAttempts: 3, restartDelaySeconds: 10 }, updatedAt: timestamp(),
  };
  if (example) {
    task.description = "Kafka 订单事件写入 Doris 的配置示例";
    task.bindings = [createBinding("SOURCE"), createBinding("SINK")];
    task.sql = "-- 示例：配置 Source / Sink 后生成连接 DDL。\nEXECUTE STATEMENT SET\nBEGIN\n    INSERT INTO target_orders\n    SELECT order_id, amount, event_time FROM source_orders;\n    -- 多 Sink 时，在此追加写入其他逻辑目标表的 INSERT INTO。\nEND;\n";
  }
  return task;
}

export function validateKafkaSource(source: KafkaDatasource): string[] {
  const errors: string[] = [];
  if (!source.name.trim()) errors.push("请输入 Kafka 数据源名称");
  if (source.name.length > 100) errors.push("数据源名称不能超过 100 个字符");
  const brokers = source.bootstrapServers.split(",").map(value => value.trim());
  if (!brokers.length || brokers.some(value => !/^(?:[a-zA-Z0-9_.-]+|\[[0-9a-fA-F:]+\]):\d+$/.test(value) || Number(value.slice(value.lastIndexOf(":") + 1)) < 1 || Number(value.slice(value.lastIndexOf(":") + 1)) > 65535)) errors.push("Bootstrap Servers 须为 host:port，多地址用逗号分隔");
  if (!["PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL"].includes(source.securityProtocol)) errors.push("请选择有效的安全协议");
  if (source.securityProtocol !== "PLAINTEXT") {
    if (!source.username.trim()) errors.push("SASL 认证需要用户名");
    if (!["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"].includes(source.saslMechanism)) errors.push("请选择有效的 SASL 认证机制");
  }
  return errors;
}

export function mysqlTypeToFlink(type: string): string | undefined {
  const normalized = type.trim().toLowerCase();
  const unsigned = /\bunsigned\b/.test(normalized);
  const base = normalized.replace(/\s+unsigned\b|\s+zerofill\b/g, "");
  if (/^(?:tinyint\(1\)|boolean|bool)$/.test(base)) return "BOOLEAN";
  if (/^tinyint(?:\(\d+\))?$/.test(base)) return unsigned ? "SMALLINT" : "TINYINT";
  if (/^smallint(?:\(\d+\))?$/.test(base)) return unsigned ? "INT" : "SMALLINT";
  if (/^(?:mediumint|int|integer)(?:\(\d+\))?$/.test(base)) return unsigned ? "BIGINT" : "INT";
  if (/^bigint(?:\(\d+\))?$/.test(base)) return unsigned ? "DECIMAL(20,0)" : "BIGINT";
  const decimal = base.match(/^(?:decimal|numeric)\((\d+),\s*(\d+)\)$/);
  if (decimal && Number(decimal[1]) <= 38) return `DECIMAL(${decimal[1]},${decimal[2]})`;
  if (/^float(?:\(.*\))?$/.test(base)) return "FLOAT";
  if (/^(?:double|real)(?:\(.*\))?$/.test(base)) return "DOUBLE";
  if (/^(?:char|varchar)\(\d+\)$|^(?:tinytext|text|mediumtext|longtext|json|enum\(.*\)|set\(.*\))$/.test(base)) return "STRING";
  if (base === "date") return "DATE";
  const time = base.match(/^(datetime|timestamp|time)(?:\((\d)\))?$/);
  if (time) return `${time[1] === "timestamp" ? "TIMESTAMP_LTZ" : time[1] === "time" ? "TIME" : "TIMESTAMP"}(${time[2] || "0"})`;
  if (/^(?:binary|varbinary)\(\d+\)$|^(?:tinyblob|blob|mediumblob|longblob)$/.test(base)) return "BYTES";
  return undefined;
}

const validType = (type: string) => {
  const value = type.trim().toUpperCase().replace(/\s+/g, "");
  if (/^(?:STRING|BOOLEAN|TINYINT|SMALLINT|INT|INTEGER|BIGINT|FLOAT|DOUBLE|DATE|BYTES)$/.test(value)) return true;
  if (/^(?:TIME|TIMESTAMP|TIMESTAMP_LTZ)(?:\([0-9]\))?$/.test(value)) return true;
  const sized = value.match(/^(?:CHAR|VARCHAR|BINARY|VARBINARY)\((\d+)\)$/);
  if (sized) return Number(sized[1]) > 0 && Number(sized[1]) <= 2147483647;
  const decimal = value.match(/^(?:DECIMAL|NUMERIC)\((\d+),(\d+)\)$/);
  return !!decimal && Number(decimal[1]) >= 1 && Number(decimal[1]) <= 38 && Number(decimal[2]) <= Number(decimal[1]);
};

export function validateBinding(binding: RealtimeBinding, sources: RealtimeDatasource[]): string[] {
  const errors: string[] = [];
  const source = sources.find(item => item.id === binding.datasourceId);
  if (!source) errors.push("请选择当前工作空间内有效的数据源");
  const expected = binding.connector === "KAFKA" ? "KAFKA" : binding.connector === "DORIS" ? "DORIS" : "MYSQL";
  if (source && source.type !== expected) errors.push("连接器与数据源类型不匹配");
  if (binding.role === "SOURCE" && !["KAFKA", "MYSQL_CDC"].includes(binding.connector)) errors.push("来源仅支持 Kafka 或 MySQL CDC");
  if (binding.role === "SINK" && !["KAFKA", "MYSQL_JDBC", "DORIS"].includes(binding.connector)) errors.push("目标仅支持 Kafka、MySQL JDBC 或 Doris");
  if (!binding.tableName.trim()) errors.push("请输入 Flink 表名");
  if (!binding.fields.length) errors.push("至少配置一个字段");
  const names = new Set<string>();
  for (const column of binding.fields) {
    if (!column.name.trim()) errors.push("字段名称不能为空");
    if (names.has(column.name.trim().toLowerCase())) errors.push(`字段 ${column.name} 重复`);
    names.add(column.name.trim().toLowerCase());
    if (!validType(column.type)) errors.push(`字段 ${column.name || "未命名"} 的 Flink 类型无效或暂未支持`);
    if (column.primaryKey && column.nullable) errors.push(`主键字段 ${column.name} 不能允许空值`);
  }
  const primaryKeys = binding.fields.filter(column => column.primaryKey);
  if (binding.connector === "KAFKA") {
    if (!binding.topic.trim() || !/^[a-zA-Z0-9._-]+$/.test(binding.topic) || binding.topic === "." || binding.topic === ".." || binding.topic.length > 249) errors.push("请输入有效的单个 Kafka Topic 名称");
    if (source?.type === "KAFKA") errors.push(...validateKafkaSource(source));
    if (binding.role === "SOURCE" && !binding.consumerGroup.trim()) errors.push("请输入 Kafka 消费组");
    if (binding.role === "SINK" && binding.writeMode === "upsert") {
      if (!primaryKeys.length) errors.push("Upsert Kafka 需要主键");
      if (binding.format !== "json") errors.push("当前 Upsert Kafka 配置器仅支持 JSON 键与值格式");
    }
  } else if (!binding.physicalTable.trim()) errors.push("请选择或输入物理表名");
  if (binding.connector === "MYSQL_CDC") {
    if (!primaryKeys.length) errors.push("当前 MySQL CDC 配置器需要主键；无主键表需后续配置 chunk key");
    if (binding.serverId && !/^\d+(?:-\d+)?$/.test(binding.serverId)) errors.push("请输入有效的 MySQL CDC Server ID 或范围，留空自动分配");
    else if (binding.serverId) {
      const ids = binding.serverId.split("-").map(Number);
      if (ids.some(id => id < 1 || id > 2147483647) || (ids.length === 2 && ids[1] < ids[0])) errors.push("Server ID 须为 1–2147483647 的整数或递增范围");
    }
    try { new Intl.DateTimeFormat("zh-CN", { timeZone: binding.timezone }); } catch { errors.push("请输入有效的数据库时区"); }
  }
  if (binding.connector === "MYSQL_JDBC" && binding.writeMode === "upsert" && !primaryKeys.length) errors.push("MySQL JDBC Upsert 需要与目标表一致的主键或唯一键");
  if (binding.connector === "DORIS") {
    const urls = binding.feHttpUrls.trim() ? binding.feHttpUrls.split(",").map(item => item.trim()) : source?.type === "DORIS" ? source.options?.feHttpUrls || [] : [];
    if (!urls.length || urls.some(value => { try { const url = new URL(value.includes("://") ? value : `http://${value}`); return url.protocol !== "http:" || !url.hostname || !!url.username || !!url.password || !!url.search || !!url.hash || !["", "/"].includes(url.pathname); } catch { return true; } })) errors.push("Doris 需要有效的 FE HTTP 地址；不能使用 SQL 端口");
    if (!binding.labelPrefix.trim()) errors.push("请输入独立的 Doris Label Prefix");
    if (binding.syncDeletes && binding.dorisModel !== "UNIQUE") errors.push("Doris 同步删除仅支持 Unique 表模型");
    if (binding.syncDeletes && !primaryKeys.length) errors.push("Doris 同步删除需要配置目标表的主键字段");
  }
  if (binding.eventTimeField) {
    const eventTime = binding.fields.find(column => column.name === binding.eventTimeField);
    if (binding.role !== "SOURCE") errors.push("Watermark 仅配置在来源表");
    if (!eventTime || !/^TIMESTAMP(?:_LTZ)?\(3\)$/i.test(eventTime.type.replace(/\s+/g, ""))) errors.push("Watermark 字段必须是 TIMESTAMP(3) 或 TIMESTAMP_LTZ(3)");
    if (!Number.isInteger(binding.watermarkSeconds) || binding.watermarkSeconds < 0) errors.push("Watermark 延迟须为非负整数秒");
  }
  return [...new Set(errors)];
}

export function validateTask(task: RealtimeTask, sources: RealtimeDatasource[]): string[] {
  const errors: string[] = [];
  if (!task.name.trim()) errors.push("任务名称不能为空");
  if (!task.sql.trim() || !task.sql.replace(/--[^\n]*|\/\*[\s\S]*?\*\//g, "").trim()) errors.push("请输入 Flink SQL");
  const tableNames = new Set<string>();
  for (const binding of task.bindings) {
    errors.push(...validateBinding(binding, sources).map(message => `${binding.tableName || binding.role}：${message}`));
    const name = binding.tableName.trim().toLowerCase();
    if (tableNames.has(name)) errors.push(`Flink 表名 ${binding.tableName} 重复`);
    tableNames.add(name);
    if (binding.connector === "MYSQL_CDC" && /^\d+(?:-\d+)?$/.test(binding.serverId)) {
      const [start, end = start] = binding.serverId.split("-").map(Number);
      if (end - start + 1 < task.runtime.parallelism) errors.push(`${binding.tableName}：Server ID 范围不足以覆盖来源并行度`);
    }
  }
  if (!Number.isInteger(task.runtime.parallelism) || task.runtime.parallelism < 1 || task.runtime.parallelism > 256) errors.push("并行度须为 1–256 的整数");
  if (!Number.isInteger(task.runtime.checkpointSeconds) || task.runtime.checkpointSeconds < 1) errors.push("Checkpoint 周期须为正整数秒");
  if (!Number.isInteger(task.runtime.restartAttempts) || task.runtime.restartAttempts < 0) errors.push("重启次数须为非负整数");
  if (!Number.isInteger(task.runtime.restartDelaySeconds) || task.runtime.restartDelaySeconds < 1) errors.push("重启间隔须为正整数秒");
  return errors;
}

export function createRelease(task: RealtimeTask, existingReleases: RealtimeRelease[], note = ""): RealtimeRelease {
  return { id: uid("release"), taskId: task.id, releaseNo: Math.max(0, ...existingReleases.filter(item => item.taskId === task.id).map(item => item.releaseNo)) + 1, createdAt: timestamp(), note, snapshot: structuredClone(task) };
}
