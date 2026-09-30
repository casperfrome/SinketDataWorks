/// <reference types="vite/client" />
import { useState } from "react";
import Editor, { loader } from "@monaco-editor/react";
import * as monaco from "monaco-editor";
import EditorWorker from "monaco-editor/esm/vs/editor/editor.worker?worker";
import JsonWorker from "monaco-editor/esm/vs/language/json/json.worker?worker";
import CssWorker from "monaco-editor/esm/vs/language/css/css.worker?worker";
import HtmlWorker from "monaco-editor/esm/vs/language/html/html.worker?worker";
import TypeScriptWorker from "monaco-editor/esm/vs/language/typescript/ts.worker?worker";
import {
  Alert,
  Button,
  Descriptions,
  Empty,
  Form,
  Input,
  InputNumber,
  Select,
  Switch,
  Table,
  Tabs,
  Tag,
} from "antd";
import {
  PlusOutlined,
  DeleteOutlined,
  DatabaseOutlined,
  FileTextOutlined,
} from "@ant-design/icons";
import type { StudioObject } from "../types";
import { getNodeType } from "../data/nodeTypes";
import SyncEditor from "./SyncEditor";
import WorkflowEditor from "./WorkflowEditor";
import NotebookEditor from "./NotebookEditor";
import "../editor.css";

(self as typeof self & { MonacoEnvironment: unknown }).MonacoEnvironment = {
  getWorker: (_moduleId: string, label: string) => {
    if (label === "json") return new JsonWorker();
    if (["css", "scss", "less"].includes(label)) return new CssWorker();
    if (["html", "handlebars", "razor"].includes(label))
      return new HtmlWorker();
    if (["typescript", "javascript"].includes(label))
      return new TypeScriptWorker();
    return new EditorWorker();
  },
};
loader.config({ monaco });
monaco.editor.defineTheme("dataworks-dark", {
  base: "vs-dark",
  inherit: true,
  rules: [
    { token: "comment", foreground: "6F9872" },
    { token: "keyword", foreground: "569CD6" },
    { token: "string", foreground: "CE9178" },
    { token: "number", foreground: "B5CEA8" },
  ],
  colors: {
    "editor.background": "#171b1e",
    "editor.foreground": "#c4c9cd",
    "editorLineNumber.foreground": "#626971",
    "editorLineNumber.activeForeground": "#b9c4ce",
    "editor.lineHighlightBackground": "#1d2227",
    "editor.selectionBackground": "#254c6d",
    "editorCursor.foreground": "#a6b2bf",
    "editorIndentGuide.background1": "#2c3238",
    "editorGutter.background": "#171b1e",
  },
});

export interface StudioEditorProps {
  object: StudioObject;
  onChange: (patch: Partial<StudioObject>) => void;
  objects: StudioObject[];
  onOpen: (id: string) => void;
  theme: "dark" | "light";
  fontSize: number;
  wordWrap: boolean;
}
export default function StudioEditor(props: StudioEditorProps) {
  const { object, onChange, theme, fontSize, wordWrap } = props;
  const type = getNodeType(object.nodeType);
  if(object.config.run?.provider === "SYNC" || object.nodeType === "离线同步")return <SyncEditor object={object} onChange={onChange}/>;
  if (object.kind === "WORKFLOW" || type.editor === "workflow")
    return (
      <WorkflowEditor
        object={object}
        onChange={onChange}
        objects={props.objects}
        onOpen={props.onOpen}
        theme={theme}
      />
    );
  if (object.kind === "NOTEBOOK" || type.editor === "notebook")
    return (
      <NotebookEditor
        key={object.id}
        object={object}
        onChange={onChange}
        theme={theme}
        fontSize={fontSize}
      />
    );
  if (object.kind === "TABLE")
    return (
      <TableEditor
        key={object.id}
        object={object}
        onChange={onChange}
        theme={theme}
        fontSize={fontSize}
      />
    );
  if (object.kind === "ENVIRONMENT")
    return <EnvironmentEditor object={object} onChange={onChange} />;
  if (type.editor === "form" && object.kind !== "RESOURCE")
    return <NodeConfiguration object={object} onChange={onChange} />;
  const language =
    object.kind === "RESOURCE" ? resourceLanguage(object.name) : type.language;
  return (
    <div className="studio-code-editor">
      {object.kind === "RESOURCE" && (
        <div className="resource-info">
          <FileTextOutlined />
          <span>{object.name}</span>
          <Tag>项目资源</Tag>
          <span className="muted">
            {new Blob([object.content ?? ""]).size.toLocaleString()} 字节
          </span>
          <span className="muted">
            修改后点击保存，内容将保留至本地资源目录
          </span>
        </div>
      )}
      <Editor
        path={`${object.id}.${language}`}
        language={language}
        theme={theme === "dark" ? "dataworks-dark" : "vs"}
        value={object.content ?? ""}
        onChange={(v) => {
          if ((v ?? "") !== object.content) onChange({ content: v ?? "" });
        }}
        loading={<div className="editor-loading">正在加载编辑器…</div>}
        options={{
          fontSize,
          fontFamily: 'Consolas, "Cascadia Code", monospace',
          lineNumbers: "on",
          minimap: { enabled: false },
          scrollBeyondLastLine: false,
          wordWrap: wordWrap ? "on" : "off",
          automaticLayout: true,
          padding: { top: 14, bottom: 20 },
          tabSize: 4,
          insertSpaces: true,
          renderLineHighlight: "all",
          smoothScrolling: true,
          folding: true,
          glyphMargin: false,
          bracketPairColorization: { enabled: true },
          find: { addExtraSpaceOnTop: false },
          quickSuggestions: true,
        }}
      />
      <div className="code-language-bar">
        <span>{language.toUpperCase()}</span>
        <span>UTF-8</span>
        <span>空格: 4</span>
        <span>Ctrl + F 查找 · Ctrl + H 替换</span>
      </div>
    </div>
  );
}
function resourceLanguage(name: string) {
  const ext = name.split(".").pop()?.toLowerCase();
  return (
    (
      {
        sql: "sql",
        py: "python",
        sh: "shell",
        js: "javascript",
        ts: "typescript",
        json: "json",
        xml: "xml",
        html: "html",
        css: "css",
        scss: "scss",
        less: "less",
        yaml: "yaml",
        yml: "yaml",
        java: "java",
        txt: "plaintext",
        csv: "plaintext",
      } as Record<string, string>
    )[ext ?? ""] ?? "plaintext"
  );
}
function NodeConfiguration({
  object,
  onChange,
}: {
  object: StudioObject;
  onChange: StudioEditorProps["onChange"];
}) {
  const type = getNodeType(object.nodeType),
    config = object.config ?? {};
  const update = (key: string, value: unknown) =>
    onChange({ config: { ...config, [key]: value } });
  return (
    <div className="node-configuration">
      <div className="configuration-title">
        <span className="node-type-emblem" style={{ color: type.color }}>
          ▧
        </span>
        <div>
          <h3>{type.label}</h3>
          <p>{type.group} / 节点配置</p>
        </div>
        <Tag color="blue">本地模拟</Tag>
      </div>
      <Alert
        showIcon
        type="info"
        title="运行使用本地模拟。配置会真实保存，外部计算服务不会被调用。"
      />
      <Form layout="vertical" className="node-config-form">
        {type.fields?.map((field) => (
          <Form.Item
            key={field.key}
            label={field.label}
            required={field.required}
          >
            {field.kind === "select" ? (
              <Select
                value={config[field.key] ?? field.default}
                options={field.options?.map((value) => ({
                  value,
                  label: value,
                }))}
                onChange={(v) => update(field.key, v)}
                placeholder={`请选择${field.label}`}
                allowClear
              />
            ) : field.kind === "number" ? (
              <InputNumber
                value={config[field.key] ?? field.default}
                min={0}
                onChange={(v) => update(field.key, v)}
                style={{ width: "100%" }}
              />
            ) : field.kind === "switch" ? (
              <Switch
                checked={config[field.key] ?? field.default ?? false}
                onChange={(v) => update(field.key, v)}
              />
            ) : field.kind === "textarea" ? (
              <Input.TextArea
                autoSize={{ minRows: 3, maxRows: 12 }}
                value={config[field.key] ?? field.default ?? ""}
                placeholder={field.placeholder}
                onChange={(e) => update(field.key, e.target.value)}
                className="monospace"
              />
            ) : (
              <Input
                value={config[field.key] ?? field.default ?? ""}
                placeholder={field.placeholder ?? `请输入${field.label}`}
                onChange={(e) => update(field.key, e.target.value)}
              />
            )}
          </Form.Item>
        ))}
      </Form>
      {type.fields?.some((f) => f.key === "columnMapping") && (
        <div className="configuration-note">
          字段映射每行一组，使用「来源字段 →
          目标字段」。支持在节点调度配置中设置业务日期参数。
        </div>
      )}
    </div>
  );
}
interface ColumnDefinition {
  name: string;
  type: string;
  comment?: string;
  nullable?: boolean;
  partition?: boolean;
  primaryKey?: boolean;
}
function TableEditor({
  object,
  onChange,
  theme,
  fontSize,
}: {
  object: StudioObject;
  onChange: StudioEditorProps["onChange"];
  theme: "dark" | "light";
  fontSize: number;
}) {
  const [active, setActive] = useState("schema");
  const columns: ColumnDefinition[] = object.config.columns ?? [];
  const update = (next: ColumnDefinition[]) =>
    onChange({ config: { ...object.config, columns: next } });
  const patch = (index: number, key: keyof ColumnDefinition, value: unknown) =>
    update(columns.map((c, i) => (i === index ? { ...c, [key]: value } : c)));
  const ddl = `CREATE TABLE ${object.name} (\n${columns
    .filter((c) => !c.partition)
    .map(
      (c) =>
        `    ${c.name} ${c.type}${c.nullable === false ? " NOT NULL" : ""}${c.comment ? ` COMMENT '${c.comment.replaceAll("'", "''")}'` : ""}`,
    )
    .join(",\n")}\n)${
    columns.some((c) => c.partition)
      ? `\nPARTITIONED BY (${columns
          .filter((c) => c.partition)
          .map((c) => `${c.name} ${c.type}`)
          .join(", ")})`
      : ""
  };`;
  const dataColumns = columns.map((c) => ({
    title: c.name,
    dataIndex: c.name,
    key: c.name,
    render: (v: unknown) =>
      v === null ? <span className="muted">NULL</span> : String(v ?? ""),
  }));
  const sample = (
    (object.config.sampleRows ?? []) as (Record<string, unknown> | unknown[])[]
  ).map((row) =>
    Array.isArray(row)
      ? Object.fromEntries(
          columns.map((column, index) => [column.name, row[index]]),
        )
      : row,
  );
  return (
    <div className="table-definition">
      <div className="table-heading">
        <DatabaseOutlined />
        <div>
          <strong>{object.name}</strong>
          <div className="muted">
            {object.config.datasource ?? "演示数据源"} · {columns.length} 个字段
          </div>
        </div>
        <Tag>本地元数据</Tag>
      </div>
      <Tabs
        activeKey={active}
        onChange={setActive}
        items={[
          {
            key: "schema",
            label: "表结构",
            children: (
              <>
                <div className="table-action">
                  <Button
                    size="small"
                    icon={<PlusOutlined />}
                    onClick={() =>
                      update([
                        ...columns,
                        {
                          name: `column_${columns.length + 1}`,
                          type: "STRING",
                          nullable: true,
                          comment: "",
                        },
                      ])
                    }
                  >
                    添加字段
                  </Button>
                  <span className="muted">双击输入框可编辑字段定义</span>
                </div>
                <Table
                  size="small"
                  pagination={false}
                  rowKey={(_c, index) => String(index)}
                  dataSource={columns}
                  locale={{
                    emptyText: <Empty description="尚无字段，请添加表字段" />,
                  }}
                  scroll={{ x: 650 }}
                  columns={[
                    {
                      title: "字段名",
                      dataIndex: "name",
                      width: 180,
                      render: (v, _r, i) => (
                        <Input
                          size="small"
                          value={v}
                          onChange={(e) => patch(i, "name", e.target.value)}
                          aria-label={`字段名 ${i + 1}`}
                        />
                      ),
                    },
                    {
                      title: "类型",
                      dataIndex: "type",
                      width: 135,
                      render: (v, _r, i) => (
                        <Select
                          size="small"
                          value={v}
                          style={{ width: "100%" }}
                          options={[
                            "STRING",
                            "BIGINT",
                            "INT",
                            "DOUBLE",
                            "DECIMAL",
                            "BOOLEAN",
                            "DATETIME",
                            "TIMESTAMP",
                            "DATE",
                            "ARRAY<STRING>",
                            "MAP<STRING,STRING>",
                          ].map((value) => ({ value, label: value }))}
                          onChange={(v) => patch(i, "type", v)}
                        />
                      ),
                    },
                    {
                      title: "描述",
                      dataIndex: "comment",
                      render: (v, _r, i) => (
                        <Input
                          size="small"
                          value={v}
                          onChange={(e) => patch(i, "comment", e.target.value)}
                          aria-label={`字段描述 ${i + 1}`}
                        />
                      ),
                    },
                    {
                      title: "允许空",
                      dataIndex: "nullable",
                      width: 74,
                      render: (v, _r, i) => (
                        <Switch
                          size="small"
                          checked={v !== false}
                          onChange={(v) => patch(i, "nullable", v)}
                        />
                      ),
                    },
                    {
                      title: "分区",
                      dataIndex: "partition",
                      width: 62,
                      render: (v, _r, i) => (
                        <Switch
                          size="small"
                          checked={!!v}
                          onChange={(v) => patch(i, "partition", v)}
                        />
                      ),
                    },
                    {
                      title: "操作",
                      width: 48,
                      render: (_v, _r, i) => (
                        <Button
                          size="small"
                          type="text"
                          danger
                          icon={<DeleteOutlined />}
                          aria-label={`删除字段 ${i + 1}`}
                          onClick={() =>
                            update(columns.filter((_, j) => j !== i))
                          }
                        />
                      ),
                    },
                  ]}
                />
              </>
            ),
          },
          {
            key: "data",
            label: "示例数据",
            children: (
              <>
                <Alert
                  type="info"
                  showIcon
                  title="以下为本地演示数据，不会查询云端或元数据库中的业务表。"
                />
                <Table
                  style={{ marginTop: 12 }}
                  size="small"
                  columns={dataColumns}
                  dataSource={sample.map((row, i) => ({ ...row, key: i }))}
                  scroll={{ x: "max-content" }}
                  pagination={{ pageSize: 20 }}
                  locale={{ emptyText: "此表尚未配置示例数据" }}
                />
              </>
            ),
          },
          {
            key: "ddl",
            label: "DDL",
            children: (
              <div className="ddl-editor">
                <Editor
                  language="sql"
                  value={ddl}
                  theme={theme === "dark" ? "dataworks-dark" : "vs"}
                  options={{
                    readOnly: true,
                    fontSize,
                    minimap: { enabled: false },
                    automaticLayout: true,
                    scrollBeyondLastLine: false,
                  }}
                />
              </div>
            ),
          },
          {
            key: "properties",
            label: "表属性",
            children: (
              <Form layout="vertical" style={{ maxWidth: 600 }}>
                <Form.Item label="数据源">
                  <Input
                    value={object.config.datasource ?? ""}
                    onChange={(e) =>
                      onChange({
                        config: {
                          ...object.config,
                          datasource: e.target.value,
                        },
                      })
                    }
                  />
                </Form.Item>
                <Form.Item label="表描述">
                  <Input.TextArea
                    value={object.description}
                    onChange={(e) => onChange({ description: e.target.value })}
                  />
                </Form.Item>
                <Descriptions
                  size="small"
                  column={1}
                  items={[
                    { key: "owner", label: "责任人", children: object.owner },
                    { key: "version", label: "版本", children: object.version },
                    {
                      key: "updated",
                      label: "最后修改",
                      children: object.updatedAt,
                    },
                  ]}
                />
              </Form>
            ),
          },
        ]}
      />
    </div>
  );
}
function EnvironmentEditor({
  object,
  onChange,
}: {
  object: StudioObject;
  onChange: StudioEditorProps["onChange"];
}) {
  return (
    <div className="node-configuration">
      <div className="configuration-title">
        <h3>个人开发环境</h3>
        <Tag color="blue">本地模拟</Tag>
      </div>
      <Form layout="vertical">
        <Form.Item label="镜像">
          <Select
            value={
              object.config.image ?? object.config.runtime ?? "Python 3.12"
            }
            options={["Python 3.12", "PyODPS 3", "Spark 3.5", "自定义镜像"].map(
              (v) => ({ label: v, value: v }),
            )}
            onChange={(image) =>
              onChange({ config: { ...object.config, image } })
            }
          />
        </Form.Item>
        <Form.Item label="CPU 核数">
          <InputNumber
            min={1}
            value={Number.parseFloat(String(object.config.cpu ?? 2)) || 2}
            onChange={(cpu) => onChange({ config: { ...object.config, cpu } })}
          />
        </Form.Item>
        <Form.Item label="内存（GB）">
          <InputNumber
            min={1}
            value={Number.parseFloat(String(object.config.memory ?? 4)) || 4}
            onChange={(memory) =>
              onChange({ config: { ...object.config, memory } })
            }
          />
        </Form.Item>
        <Form.Item label="环境变量">
          <Input.TextArea
            rows={6}
            placeholder="KEY=value，每行一项"
            value={object.content}
            onChange={(e) => onChange({ content: e.target.value })}
          />
        </Form.Item>
      </Form>
    </div>
  );
}
