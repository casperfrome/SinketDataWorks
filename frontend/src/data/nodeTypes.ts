export interface NodeField {
  key: string;
  label: string;
  kind?: "text" | "textarea" | "select" | "number" | "switch";
  options?: string[];
  placeholder?: string;
  required?: boolean;
  default?: string | number | boolean;
}
export interface NodeType {
  type: string;
  label: string;
  group: string;
  editor: "code" | "notebook" | "workflow" | "form";
  language: string;
  color: string;
  fields?: NodeField[];
}
const sql = (group: string, names: string[], color = "#4389ec"): NodeType[] =>
  names.map((type) => ({
    type,
    label: type,
    group,
    editor: "code",
    language: "sql",
    color,
  }));
const code = (
  group: string,
  type: string,
  language: string,
  color = "#36a6ba",
): NodeType => ({ type, label: type, group, editor: "code", language, color });
const form = (
  group: string,
  type: string,
  fields: NodeField[],
  color = "#a08beb",
): NodeType => ({
  type,
  label: type,
  group,
  editor: "form",
  language: "json",
  color,
  fields,
});
const connection: NodeField = {
  key: "datasource",
  label: "数据源",
  placeholder: "填写数据源名称，例如 local_mysql",
  required: true,
};
const sparkFields: NodeField[] = [
  {
    key: "resource",
    label: "程序资源",
    required: true,
    placeholder: "资源管理中已上传的 JAR / Python 文件",
  },
  { key: "mainClass", label: "主类", placeholder: "com.example.Main" },
  {
    key: "arguments",
    label: "程序参数",
    kind: "textarea",
    placeholder: "--date ${bizdate}",
  },
  {
    key: "executorMemory",
    label: "Executor 内存（GB）",
    kind: "number",
    default: 2,
  },
  { key: "executorCount", label: "Executor 数量", kind: "number", default: 2 },
  {
    key: "sparkConfig",
    label: "Spark 配置",
    kind: "textarea",
    placeholder: "spark.sql.shuffle.partitions=200",
  },
];
const syncFields: NodeField[] = [
  {
    key: "sourceType",
    label: "来源类型",
    kind: "select",
    options: ["MySQL", "MaxCompute", "Hologres", "PostgreSQL", "Hive", "OSS"],
    default: "MySQL",
  },
  { key: "sourceDatasource", label: "来源数据源", required: true },
  { key: "sourceTable", label: "来源表", required: true },
  {
    key: "targetType",
    label: "目标类型",
    kind: "select",
    options: ["MaxCompute", "MySQL", "Hologres", "PostgreSQL", "Hive", "OSS"],
    default: "MaxCompute",
  },
  { key: "targetDatasource", label: "目标数据源", required: true },
  { key: "targetTable", label: "目标表", required: true },
  {
    key: "columnMapping",
    label: "字段映射",
    kind: "textarea",
    placeholder: "id → id\nname → name\ncreated_at → created_at",
  },
  {
    key: "writeMode",
    label: "写入模式",
    kind: "select",
    options: ["append", "overwrite", "upsert"],
    default: "append",
  },
  { key: "concurrency", label: "并发数", kind: "number", default: 1 },
];
const templates: NodeType[] = [
  ...sql("MaxCompute", ["ODPS SQL", "ODPS Script", "ODPS DDL"]),
  code("MaxCompute", "PyODPS 3", "python"),
  code("MaxCompute", "PyODPS 2", "python"),
  form("MaxCompute", "ODPS MR", [
    { key: "resource", label: "JAR 资源", required: true },
    { key: "mainClass", label: "主类", required: true },
    { key: "arguments", label: "运行参数", kind: "textarea" },
  ]),
  form("MaxCompute", "MaxCompute Spark", sparkFields),
  ...sql("Hologres", ["Hologres SQL", "Hologres SQL Script"]),
  code("Flink", "Flink SQL", "sql"),
  form("Flink", "Flink JAR", [
    ...sparkFields.slice(0, 3),
    { key: "parallelism", label: "并行度", kind: "number", default: 1 },
    {
      key: "checkpointInterval",
      label: "Checkpoint 间隔（秒）",
      kind: "number",
      default: 60,
    },
  ]),
  code("Flink", "Flink Python", "python"),
  ...sql("EMR", ["EMR Hive", "EMR Spark SQL", "EMR Presto", "EMR Impala"]),
  form("EMR", "EMR Spark", sparkFields),
  form("EMR", "EMR MR", [...sparkFields.slice(0, 3)]),
  code("EMR", "EMR Shell", "shell"),
  code("EMR", "EMR Spark Streaming", "python"),
  ...sql("CDH", ["CDH Hive", "CDH Spark SQL", "CDH Impala"]),
  form("CDH", "CDH Spark", sparkFields),
  form("CDH", "CDH MR", sparkFields.slice(0, 3)),
  code("CDH", "CDH Shell", "shell"),
  ...sql("ClickHouse", ["ClickHouse SQL"]),
  ...sql("AnalyticDB", ["AnalyticDB for PostgreSQL", "AnalyticDB for MySQL"]),
  ...sql("Lindorm", ["Lindorm SQL"]),
  form("Serverless Spark", "Serverless Spark", sparkFields),
  ...sql("Serverless Spark", ["Serverless Spark SQL"]),
  ...sql("Serverless StarRocks", ["StarRocks SQL"]),
  form("数据质量", "数据质量检查", [
    connection,
    { key: "table", label: "校验表", required: true },
    {
      key: "rule",
      label: "校验规则",
      kind: "select",
      options: ["表行数大于 0", "字段非空", "字段唯一性", "自定义 SQL"],
      default: "表行数大于 0",
    },
    { key: "column", label: "校验字段" },
    { key: "threshold", label: "告警阈值", kind: "number", default: 0 },
    { key: "sql", label: "校验 SQL", kind: "textarea" },
  ]),
  ...sql("数据库", [
    "MySQL",
    "PostgreSQL",
    "SQL Server",
    "Oracle",
    "DB2",
    "OceanBase",
    "PolarDB",
    "TiDB",
    "DM",
    "KingbaseES",
  ]),
  form("逻辑节点", "虚拟节点", [
    {
      key: "purpose",
      label: "节点说明",
      kind: "textarea",
      placeholder: "用于组织工作流、汇聚上游依赖",
    },
  ]),
  form("逻辑节点", "分支节点", [
    {
      key: "expression",
      label: "分支表达式",
      required: true,
      placeholder: '${status} == "success"',
    },
    { key: "trueBranch", label: "满足条件的下游节点" },
    { key: "falseBranch", label: "不满足条件的下游节点" },
  ]),
  form("逻辑节点", "归并节点", [
    {
      key: "mergePolicy",
      label: "归并策略",
      kind: "select",
      options: ["任一分支成功", "全部分支完成"],
      default: "任一分支成功",
    },
  ]),
  form("逻辑节点", "赋值节点", [
    { key: "variable", label: "变量名称", required: true },
    { key: "value", label: "变量值", kind: "textarea", required: true },
  ]),
  form("逻辑节点", "循环节点", [
    {
      key: "items",
      label: "循环集合",
      kind: "textarea",
      placeholder: '["20260925", "20260926", "20260927"]',
    },
    { key: "variable", label: "循环变量", default: "item" },
    { key: "parallelism", label: "并发数", kind: "number", default: 1 },
  ]),
  ...sql("多云", ["BigQuery", "Snowflake", "Redshift", "Databricks SQL"]),
  form("多云", "AWS EMR", sparkFields),
  form("多云", "Databricks", sparkFields),
  code("通用", "Shell", "shell"),
  code("通用", "Python", "python"),
  code("通用", "SQL", "sql"),
  form("通用", "HTTP", [
    {
      key: "url",
      label: "请求 URL",
      required: true,
      placeholder: "https://example.com/api",
    },
    {
      key: "method",
      label: "请求方法",
      kind: "select",
      options: ["GET", "POST", "PUT", "PATCH", "DELETE"],
      default: "GET",
    },
    {
      key: "headers",
      label: "请求头（JSON）",
      kind: "textarea",
      placeholder: '{"Content-Type":"application/json"}',
    },
    { key: "body", label: "请求体", kind: "textarea" },
    { key: "timeout", label: "超时时间（秒）", kind: "number", default: 30 },
  ]),
  form("通用", "离线同步", syncFields),
  form("通用", "实时同步", syncFields),
  form("通用", "数据集成", syncFields),
  form("通用", "等待节点", [
    {
      key: "waitSeconds",
      label: "等待时长（秒）",
      kind: "number",
      default: 60,
    },
  ]),
  form("算法", "PAI Designer", [
    { key: "pipelineId", label: "工作流 ID", required: true },
    { key: "parameters", label: "工作流参数（JSON）", kind: "textarea" },
  ]),
  code("算法", "PAI Python", "python"),
  form("算法", "PAI DLC", [
    { key: "image", label: "运行镜像", required: true },
    { key: "entrypoint", label: "启动命令", kind: "textarea", required: true },
    { key: "gpuCount", label: "GPU 数量", kind: "number", default: 1 },
    { key: "dataset", label: "训练数据集" },
  ]),
  form("算法", "PAI EAS", [
    { key: "serviceName", label: "服务名称", required: true },
    { key: "input", label: "预测输入", kind: "textarea" },
  ]),
  form("Kubernetes", "Kubernetes", [
    { key: "namespace", label: "命名空间", default: "default", required: true },
    { key: "image", label: "容器镜像", required: true },
    { key: "command", label: "运行命令", kind: "textarea" },
    { key: "cpu", label: "CPU 核数", kind: "number", default: 1 },
    { key: "memory", label: "内存（GB）", kind: "number", default: 2 },
  ]),
  code("Kubernetes", "Kubernetes YAML", "yaml"),
  form("大模型", "大模型推理", [
    {
      key: "model",
      label: "模型",
      kind: "select",
      options: [
        "Qwen（本地模拟）",
        "DeepSeek（本地模拟）",
        "自定义模型（本地模拟）",
      ],
      default: "Qwen（本地模拟）",
    },
    { key: "systemPrompt", label: "系统提示词", kind: "textarea" },
    { key: "prompt", label: "用户提示词", kind: "textarea", required: true },
    { key: "temperature", label: "Temperature", kind: "number", default: 0.7 },
    {
      key: "outputFormat",
      label: "输出格式",
      kind: "select",
      options: ["text", "json"],
      default: "text",
    },
  ]),
  form("大模型", "向量化", [
    {
      key: "model",
      label: "Embedding 模型",
      default: "text-embedding-v3（本地模拟）",
    },
    { key: "input", label: "输入文本", kind: "textarea", required: true },
    { key: "dimension", label: "向量维度", kind: "number", default: 1024 },
  ]),
  {
    type: "Notebook",
    label: "Notebook",
    group: "个人开发",
    editor: "notebook",
    language: "python",
    color: "#ea994c",
  },
  ...["周期工作流", "触发式工作流", "手动工作流"].map((type) => ({
    type,
    label: type,
    group: "工作流",
    editor: "workflow" as const,
    language: "json",
    color: "#3c92d8",
  })),
];
const aliases: Record<string, string> = {
  "MaxCompute SQL": "ODPS SQL",
  "MaxCompute Script": "ODPS Script",
  "MaxCompute MR": "ODPS MR",
  "元数据映射至 Hologres": "数据集成",
  "数据同步至 Hologres": "数据集成",
  "数据同步至 MaxCompute": "数据集成",
  "Flink SQL Batch": "Flink SQL",
  "Flink JAR Batch": "Flink JAR",
  "Flink Python Batch": "Flink Python",
  "EMR PySpark": "Python",
  "EMR Trino": "EMR Presto",
  "EMR Kyuubi": "EMR Spark SQL",
  "CDH Presto": "EMR Presto",
  "ADB for PostgreSQL": "AnalyticDB for PostgreSQL",
  "ADB for MySQL": "AnalyticDB for MySQL",
  "ADB Spark": "Serverless Spark",
  "ADB Spark SQL": "Serverless Spark SQL",
  "Lindorm Spark": "Serverless Spark",
  "Lindorm Spark SQL": "Serverless Spark SQL",
  "Lindorm OLAP": "Lindorm SQL",
  "Lindorm Ray": "Python",
  "Lindorm Flink Batch": "Flink JAR",
  "Serverless Spark Batch": "Serverless Spark",
  "Serverless PySpark": "Python",
  "Serverless Ray": "Python",
  "Serverless Kyuubi": "Serverless Spark SQL",
  "Serverless StarRocks SQL": "StarRocks SQL",
  质量监控: "数据质量检查",
  DRDS: "MySQL",
  "PolarDB MySQL": "MySQL",
  "PolarDB PostgreSQL": "PostgreSQL",
  Doris: "MySQL",
  MariaDB: "MySQL",
  SelectDB: "MySQL",
  Saphana: "SQL",
  Vertica: "SQL",
  "GBase 8a": "SQL",
  变量赋值: "赋值节点",
  "Azure Databricks SQL": "Databricks SQL",
  "AWS Redshift SQL": "Redshift",
  "AWS StarRocks SQL": "StarRocks SQL",
  "Google Cloud BigQuery SQL": "BigQuery",
  "Python 节点": "Python",
  "Shell 节点": "Shell",
  "HTTP 节点": "HTTP",
  "HTTP 触发器": "HTTP",
  参数节点: "赋值节点",
  Check节点: "数据质量检查",
  "for-each 节点": "循环节点",
  "do-while 节点": "循环节点",
  "PAI Flow": "PAI Designer",
  "Kubernetes Spark": "Kubernetes",
  大模型节点: "大模型推理",
};
const specialTemplates: Record<string, NodeType> = {
  SQL组件节点: form("MaxCompute", "SQL组件节点", [
    {
      key: "componentId",
      label: "SQL 组件",
      required: true,
      placeholder: "组件管理中的组件名称或 ID",
    },
    {
      key: "parameters",
      label: "组件参数（JSON）",
      kind: "textarea",
      placeholder: '{"bizdate": "${bizdate}"}',
    },
  ]),
  "元数据映射至 Hologres": form("MaxCompute", "元数据映射至 Hologres", [
    { key: "sourceProject", label: "MaxCompute 项目", required: true },
    { key: "sourceTable", label: "源表", required: true },
    { key: "targetDatasource", label: "Hologres 数据源", required: true },
    { key: "targetSchema", label: "目标 Schema", default: "public" },
    { key: "targetTable", label: "目标表", required: true },
    {
      key: "mappingMode",
      label: "映射模式",
      kind: "select",
      options: ["外表映射", "同步表结构"],
      default: "外表映射",
    },
  ]),
  数据对比: form("数据质量", "数据对比", [
    { key: "sourceDatasource", label: "来源数据源", required: true },
    { key: "sourceTable", label: "来源表", required: true },
    { key: "targetDatasource", label: "目标数据源", required: true },
    { key: "targetTable", label: "目标表", required: true },
    {
      key: "compareMode",
      label: "对比方式",
      kind: "select",
      options: ["表行数对比", "字段汇总对比", "主键明细对比"],
      default: "表行数对比",
    },
    { key: "keyColumns", label: "对比主键", placeholder: "多个字段用逗号分隔" },
    { key: "tolerance", label: "允许误差（%）", kind: "number", default: 0 },
  ]),
  SUB_PROCESS: form("逻辑节点", "SUB_PROCESS", [
    { key: "workflowId", label: "子工作流 ID", required: true },
    { key: "parameters", label: "传入参数（JSON）", kind: "textarea" },
    {
      key: "waitForCompletion",
      label: "等待子工作流完成",
      kind: "switch",
      default: true,
    },
  ]),
  依赖检查节点: form("通用", "依赖检查节点", [
    {
      key: "dependencyType",
      label: "依赖类型",
      kind: "select",
      options: ["开发节点", "数据分区", "文件"],
      default: "开发节点",
    },
    { key: "dependency", label: "依赖对象", required: true },
    { key: "timeout", label: "等待超时（分钟）", kind: "number", default: 60 },
  ]),
  函数计算: form("通用", "函数计算", [
    { key: "serviceName", label: "服务名称", required: true },
    { key: "functionName", label: "函数名称", required: true },
    { key: "event", label: "事件参数（JSON）", kind: "textarea" },
    {
      key: "invocationType",
      label: "调用方式",
      kind: "select",
      options: ["同步调用", "异步调用"],
      default: "同步调用",
    },
  ]),
  SSH: form("通用", "SSH", [
    { key: "host", label: "主机地址", required: true },
    { key: "port", label: "端口", kind: "number", default: 22 },
    { key: "username", label: "用户名", default: "root" },
    { key: "command", label: "远程命令", kind: "textarea", required: true },
  ]),
  数据推送: form("通用", "数据推送", [
    { key: "sourceDatasource", label: "来源数据源", required: true },
    { key: "sourceTable", label: "来源表", required: true },
    { key: "targetUrl", label: "推送 URL", required: true },
    { key: "batchSize", label: "每批条数", kind: "number", default: 100 },
    { key: "mapping", label: "推送字段映射（JSON）", kind: "textarea" },
  ]),
  "do-while 节点": form("通用", "do-while 节点", [
    {
      key: "condition",
      label: "继续循环条件",
      required: true,
      placeholder: "${counter} < 10",
    },
    {
      key: "maxIterations",
      label: "最大循环次数",
      kind: "number",
      default: 10,
    },
    { key: "interval", label: "循环间隔（秒）", kind: "number", default: 0 },
  ]),
  "Kubernetes Spark": form("Kubernetes", "Kubernetes Spark", [
    { key: "namespace", label: "命名空间", default: "default" },
    ...sparkFields,
    { key: "image", label: "Spark 容器镜像", required: true },
  ]),
};
const catalog: Record<string, string[]> = {
  数据集成: ["数据集成"],
  数据库: ["MySQL", "Doris"],
};
export const nodeTypes: NodeType[] = Object.entries(catalog).flatMap(
  ([group, names]) =>
    names.map((type) => {
      const template = specialTemplates[type] ??
        templates.find((n) => n.type === (aliases[type] ?? type)) ?? {
          type,
          label: type,
          group,
          editor: "code" as const,
          language: "sql",
          color: "#4389ec",
        };
      return { ...template, type, label: type, group };
    }),
);
export const nodeGroups = [...new Set(nodeTypes.map((n) => n.group))];
const legacyTypes: Record<string, string> = {
  MAXCOMPUTE_SQL: "MaxCompute SQL",
  MAXCOMPUTE_SCRIPT: "MaxCompute Script",
  PYTHON: "Python 节点",
  SHELL: "Shell 节点",
  CYCLE_WORKFLOW: "周期工作流",
  MANUAL_WORKFLOW: "手动工作流",
  TRIGGER_WORKFLOW: "触发式工作流",
  NOTEBOOK: "Notebook",
  SQL_TEMPLATE: "MaxCompute SQL",
};
export function getNodeType(type: string): NodeType {
  const canonical = legacyTypes[type.toUpperCase()] ?? type;
  if (canonical === "UDF")
    return {
      type,
      label: "UDF 函数",
      group: "函数",
      editor: "code",
      language: "java",
      color: "#e3a463",
    };
  return (
    nodeTypes.find((n) => n.type.toLowerCase() === canonical.toLowerCase()) ??
    templates.find((n) => n.type.toLowerCase() === canonical.toLowerCase()) ?? {
      type,
      label: type || "SQL",
      group: "通用",
      editor: "code",
      language: /python|pyodps|pyspark|ray/i.test(type)
        ? "python"
        : /shell/i.test(type)
          ? "shell"
          : /java|jar/i.test(type)
            ? "java"
            : "sql",
      color: "#4389ec",
    }
  );
}
export function defaultContent(type: string): string {
  if (["MySQL", "Doris"].includes(type)) return `-- ${type} SQL 执行\nSELECT 1 AS result;\n`;
  const node = getNodeType(type);
  if (node.editor !== "code") return "";
  if (node.language === "sql")
    return `-- ${type} 节点\n-- 业务日期参数：\${bizdate}\n-- 本地模拟运行不会执行此 SQL\n\nSELECT\n    user_id,\n    order_amount,\n    created_at\nFROM ods_orders\nWHERE ds = '\${bizdate}';\n`;
  if (node.language === "python")
    return '# Python 开发节点\n# 本地模拟运行不会执行此代码\n\ndef main():\n    print("Hello SinketDataWorks")\n\nif __name__ == "__main__":\n    main()\n';
  if (node.language === "shell")
    return '#!/bin/bash\n# 本地模拟运行不会执行此脚本\necho "业务日期: ${bizdate}"\n';
  if (node.language === "yaml")
    return "apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: dataworks-job\nspec:\n  template:\n    spec:\n      containers:\n        - name: task\n          image: python:3.12\n      restartPolicy: Never\n";
  return "";
}
