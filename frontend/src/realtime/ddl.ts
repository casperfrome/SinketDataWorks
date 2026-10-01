import type { RealtimeBinding, RealtimeDatasource } from "./types.ts";
export { validateBinding } from "./model.ts";

const identifier = (value: string) => `\`${value.replace(/`/g, "``")}\``;
const literal = (value: string) => `'${value.replace(/'/g, "''")}'`;
const regexLiteral = (value: string) => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const markerId = (id: string) => encodeURIComponent(id);
const markers = (id: string) => [`-- @realtime-binding:${markerId(id)}:begin`, `-- @realtime-binding:${markerId(id)}:end`];

export function generateDDL(binding: RealtimeBinding, sourceOrSources?: RealtimeDatasource | RealtimeDatasource[], taskId = "task"): string {
  const source = Array.isArray(sourceOrSources) ? sourceOrSources.find(item => item.id === binding.datasourceId) : sourceOrSources;
  const sourceMatches = source?.id === binding.datasourceId;
  const options: [string, string][] = [];
  const comments = ["-- 结构预览：凭据由后端按数据源 ID 注入，当前不能直接执行。", `-- 数据源引用：${binding.datasourceId.replace(/[\r\n]/g, " ") || "尚未选择"}`];
  const keyFields = binding.fields.filter(field => field.primaryKey);
  const columns = binding.fields.map(field => `  ${identifier(field.name)} ${field.type.trim().toUpperCase()}${field.primaryKey || !field.nullable ? " NOT NULL" : ""}`);
  const upsertKafka = binding.connector === "KAFKA" && binding.role === "SINK" && binding.writeMode === "upsert";
  const needsPrimaryKey = binding.connector === "MYSQL_CDC" || upsertKafka || binding.connector === "MYSQL_JDBC" && binding.writeMode === "upsert";
  if (keyFields.length && needsPrimaryKey) columns.push(`  PRIMARY KEY (${keyFields.map(field => identifier(field.name)).join(", ")}) NOT ENFORCED`);
  if (binding.role === "SOURCE" && binding.eventTimeField) columns.push(`  WATERMARK FOR ${identifier(binding.eventTimeField)} AS ${identifier(binding.eventTimeField)} - INTERVAL ${literal(String(binding.watermarkSeconds))} SECOND`);
  if (binding.connector === "KAFKA") {
    options.push(["connector", upsertKafka ? "upsert-kafka" : "kafka"], ["topic", binding.topic], ["properties.bootstrap.servers", sourceMatches && source?.type === "KAFKA" ? source.bootstrapServers : "<Bootstrap Servers>"]);
    if (upsertKafka) options.push(["key.format", binding.format], ["value.format", binding.format]);
    else options.push(["format", binding.format]);
    if (binding.role === "SOURCE") options.push(["properties.group.id", binding.consumerGroup], ["scan.startup.mode", binding.startupMode]);
    if (sourceMatches && source?.type === "KAFKA" && source.securityProtocol !== "PLAINTEXT") {
      options.push(["properties.security.protocol", source.securityProtocol], ["properties.sasl.mechanism", source.saslMechanism]);
      comments.push("-- properties.sasl.jaas.config 待凭据服务注入；预览不包含会话密码。");
    }
  } else if (binding.connector === "MYSQL_CDC") {
    const database = sourceMatches && source?.type === "MYSQL" ? source.database : "<数据库>";
    options.push(["connector", "mysql-cdc"], ["hostname", sourceMatches && source?.type === "MYSQL" ? source.host : "<主机>"], ["port", String(sourceMatches && source?.type === "MYSQL" ? source.port : 3306)], ["database-name", regexLiteral(database)], ["table-name", regexLiteral(binding.physicalTable)], ["scan.startup.mode", binding.cdcStartupMode], ["server-time-zone", binding.timezone]);
    if (binding.serverId) options.push(["server-id", binding.serverId]);
    if (sourceMatches && source?.type === "MYSQL") options.push(["username", source.username]);
    comments.push("-- password 已省略；数据库需具备 CDC 读取权限与 Binlog 配置。");
  } else if (binding.connector === "MYSQL_JDBC") {
    const host = sourceMatches && source?.type === "MYSQL" ? source.host : "<主机>";
    const port = sourceMatches && source?.type === "MYSQL" ? source.port : 3306;
    const database = sourceMatches && source?.type === "MYSQL" ? source.database : "<数据库>";
    options.push(["connector", "jdbc"], ["url", `jdbc:mysql://${host.includes(":") && !host.startsWith("[") ? `[${host}]` : host}:${port}/${database}`], ["table-name", binding.physicalTable]);
    if (sourceMatches && source?.type === "MYSQL") options.push(["username", source.username]);
    comments.push("-- password 已省略；username / password 在运行时配套注入。");
  } else {
    const configured = binding.feHttpUrls.trim() ? binding.feHttpUrls.split(",").map(value => value.trim()) : sourceMatches && source?.type === "DORIS" ? source.options?.feHttpUrls || [] : [];
    const fenodes = configured.map(value => { try { return new URL(value.includes("://") ? value : `http://${value}`).host; } catch { return "<FE HTTP 地址>"; } }).join(",") || "<FE HTTP 地址>";
    const database = sourceMatches && source?.type === "DORIS" ? source.database : "<数据库>";
    options.push(["connector", "doris"], ["fenodes", fenodes], ["table.identifier", `${database}.${binding.physicalTable}`], ["sink.label-prefix", binding.labelPrefix || `${taskId.replace(/[^a-zA-Z0-9_-]/g, "_")}_${binding.id.replace(/[^a-zA-Z0-9_-]/g, "_")}`], ["sink.properties.format", "json"], ["sink.properties.read_json_by_line", "true"], ["sink.enable-delete", String(binding.syncDeletes)]);
    if (sourceMatches && source?.type === "DORIS") options.push(["username", source.username]);
    comments.push("-- password 已省略；FE 使用 HTTP 地址，同步删除要求 Doris Unique 模型。");
  }
  return `${comments.join("\n")}\nCREATE TABLE ${identifier(binding.tableName)} (\n${columns.join(",\n")}\n) WITH (\n${options.map(([key, value]) => `  ${literal(key)} = ${literal(value)}`).join(",\n")}\n);`;
}

function range(sql: string, bindingId: string): [number, number] | undefined {
  const [begin, end] = markers(bindingId);
  const lines = sql.split(/(?<=\n)/);
  let offset = 0;
  let start: number | undefined;
  let found: [number, number] | undefined;
  for (const line of lines) {
    const normalized = line.replace(/\r?\n$/, "").trim();
    if (normalized === begin) {
      if (start !== undefined || found) throw new Error("此连接的 DDL 标记重复，请手动修复后再更新");
      start = offset;
    } else if (normalized === end) {
      if (start === undefined || found) throw new Error("此连接的 DDL 标记不完整，请手动修复后再更新");
      found = [start, offset + line.replace(/\r?\n$/, "").length];
      start = undefined;
    } else if (start !== undefined && /^-- @realtime-binding:.*:(?:begin|end)$/.test(normalized)) {
      throw new Error("连接 DDL 标记存在嵌套，请手动修复后再更新");
    }
    offset += line.length;
  }
  if (start !== undefined) throw new Error("此连接的 DDL 标记不完整，请手动修复后再更新");
  return found;
}
export function findManagedDDL(sql: string, bindingId: string): string | undefined {
  const found = range(sql, bindingId);
  if (!found) return undefined;
  const block = sql.slice(found[0], found[1]);
  const firstNewline = block.indexOf("\n");
  const lastNewline = block.lastIndexOf("\n");
  return block.slice(firstNewline + 1, lastNewline).replace(/\r$/, "");
}
export function applyManagedDDL(sql: string, bindingId: string, ddl: string): string {
  const [begin, end] = markers(bindingId);
  const block = `${begin}\n${ddl.trim()}\n${end}`;
  const found = range(sql, bindingId);
  if (found) return `${sql.slice(0, found[0])}${block}${sql.slice(found[1])}`;
  return `${block}\n${sql ? `\n${sql}` : ""}`;
}
