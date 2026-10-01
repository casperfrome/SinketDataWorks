import { useEffect, useRef, useState } from "react";
import {
  App, Alert, AutoComplete, Button, Collapse, Descriptions, Empty, Form, Input,
  InputNumber, Modal, Select, Space, Switch, Table, Tag, Tooltip,
} from "antd";
import { ArrowDownToLine, Braces, CodeXml, Database, History, Plus, RefreshCw, Settings2, Trash2, Waves } from "lucide-react";
import { api } from "../api";
import { createBinding, mysqlTypeToFlink, uid } from "./model";
import { generateDDL, validateBinding } from "./ddl";
import type { RealtimeBinding, RealtimeDatasource, RealtimeField, RealtimeRelease, RealtimeTask } from "./types";

export interface BindingEditorProps {
  task: RealtimeTask;
  sources: RealtimeDatasource[];
  onChange: (task: RealtimeTask) => void;
  onInsertDDL: (binding: RealtimeBinding) => void;
  onManageDatasources: () => void;
  section: "SOURCE" | "SINK" | "runtime" | "versions";
  releases: RealtimeRelease[];
}

const connectorLabels: Record<RealtimeBinding["connector"], string> = {
  KAFKA: "Kafka", MYSQL_CDC: "MySQL CDC", MYSQL_JDBC: "MySQL JDBC", DORIS: "Doris",
};
const fieldTypes = ["STRING", "TINYINT", "SMALLINT", "INT", "BIGINT", "BOOLEAN", "FLOAT", "DOUBLE", "DECIMAL(18, 2)", "DATE", "TIME(0)", "TIMESTAMP(3)", "TIMESTAMP_LTZ(3)", "BYTES"];
const stamp = (value: string) => new Date(value).toLocaleString("zh-CN", { hour12: false });
const errorText = (error: unknown) => error instanceof Error ? error.message : "元数据读取失败，请重试";
const isTimestamp = (type: string) => /^TIMESTAMP(?:_LTZ)?\s*\(\s*3\s*\)$/i.test(type.trim());

function acceptsSource(binding: RealtimeBinding, source: RealtimeDatasource) {
  if (binding.connector === "KAFKA") return source.type === "KAFKA";
  if (binding.connector === "DORIS") return source.type === "DORIS";
  return source.type === "MYSQL";
}

export default function BindingEditor(props: BindingEditorProps) {
  const { task, section, releases, onChange } = props;
  const [expanded, setExpanded] = useState<string[]>([]);
  useEffect(() => { setExpanded(task.bindings.filter(binding => binding.role === section).map(binding => binding.id)); }, [task.id, section]);

  if (section === "runtime") return <RuntimeEditor task={task} onChange={onChange} />;
  if (section === "versions") return <ReleaseViewer task={task} releases={releases} />;
  const bindings = task.bindings.filter(binding => binding.role === section);
  const source = section === "SOURCE";
  const add = () => {
    const binding = createBinding(section, source ? "KAFKA" : "DORIS");
    onChange({ ...task, bindings: [...task.bindings, binding] });
    setExpanded(current => [...current, binding.id]);
  };
  return <div className="rt-binding-editor">
    <div className="rt-panel-section-heading">
      <div><span className="rt-eyebrow">{source ? "输入数据" : "输出数据"}</span><h3>{source ? "Source 配置" : "Sink 配置"}</h3></div>
      <Tooltip title={source ? "添加 Source" : "添加 Sink"}><Button size="small" icon={<Plus size={14} />} onClick={add}>添加</Button></Tooltip>
    </div>
    <p className="rt-panel-help">{source ? "定义输入流及字段结构，配置完成后插入 DDL。" : "定义目标表及写入方式，使用逻辑表名编写 INSERT INTO。"}</p>
    {!bindings.length ? <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={source ? "尚未添加 Source" : "尚未添加 Sink"}><Button size="small" type="primary" onClick={add}>添加{source ? " Source" : " Sink"}</Button></Empty> : <Collapse
      className="rt-binding-collapse"
      activeKey={expanded}
      onChange={keys => setExpanded(Array.isArray(keys) ? keys.map(String) : [String(keys)])}
      items={bindings.map(binding => ({
        key: binding.id,
        label: <span className="rt-binding-title">{source ? <Waves size={14} /> : <Database size={14} />}<strong>{binding.tableName || (source ? "未命名 Source" : "未命名 Sink")}</strong><Tag>{connectorLabels[binding.connector]}</Tag></span>,
        children: <BindingCard key={`${task.id}:${binding.id}`} {...props} binding={binding} />,
      }))}
    />}
  </div>;
}

function BindingCard({ binding, task, sources, onChange, onInsertDDL, onManageDatasources }: BindingEditorProps & { binding: RealtimeBinding }) {
  const { message, modal } = App.useApp();
  const [schemaOpen, setSchemaOpen] = useState(false);
  const [ddlOpen, setDDLOpen] = useState(false);
  const [tables, setTables] = useState<{ name: string; type: string; comment: string }[]>([]);
  const [topics, setTopics] = useState<string[]>([]);
  const [tableLoading, setTableLoading] = useState(false);
  const [columnLoading, setColumnLoading] = useState(false);
  const [metadataError, setMetadataError] = useState("");
  const [metadataRevision, setMetadataRevision] = useState(0);
  const requestRevision = useRef(0);
  const live = useRef({ task, binding, onChange });
  live.current = { task, binding, onChange };
  const source = sources.find(item => item.id === binding.datasourceId);
  const choices = sources.filter(item => acceptsSource(binding, item));
  const issues = validateBinding(binding, sources);
  const isKafka = binding.connector === "KAFKA";
  const isCDC = binding.connector === "MYSQL_CDC";
  const isDoris = binding.connector === "DORIS";
  const isSource = binding.role === "SOURCE";

  useEffect(() => {
    let alive = true;
    ++requestRevision.current;
    setColumnLoading(false);
    setTables([]); setTopics([]);
    setMetadataError("");
    if (isKafka && source?.type === "KAFKA") {
      setTableLoading(true);
      void api.datasourceTopics(source.id).then(items => { if (alive) setTopics(items.map(item => item.name)); }).catch(reason => { if (alive) setMetadataError(errorText(reason)); }).finally(() => { if (alive) setTableLoading(false); });
      return () => { alive = false; ++requestRevision.current; };
    }
    if (!source || source.type === "KAFKA" || !acceptsSource(binding, source)) {
      setTableLoading(false);
      return () => { alive = false; ++requestRevision.current; };
    }
    setTableLoading(true);
    void api.sourceTables(source.id).then(items => { if (alive) setTables(items); }).catch(error => { if (alive) setMetadataError(errorText(error)); }).finally(() => { if (alive) setTableLoading(false); });
    return () => { alive = false; ++requestRevision.current; };
  }, [task.id, binding.id, binding.connector, binding.datasourceId, metadataRevision]);

  useEffect(() => { ++requestRevision.current; setColumnLoading(false); }, [binding.physicalTable]);

  const patch = (next: Partial<RealtimeBinding>) => {
    const current = live.current;
    current.onChange({ ...current.task, bindings: current.task.bindings.map(item => item.id === current.binding.id ? { ...item, ...next } : item) });
  };
  const updateFields = (fields: RealtimeField[]) => patch({ fields, eventTimeField: fields.some(field => field.name === live.current.binding.eventTimeField && isTimestamp(field.type)) ? live.current.binding.eventTimeField : "" });
  const changeConnector = (connector: RealtimeBinding["connector"]) => {
    const fresh = createBinding(binding.role, connector);
    patch({ ...fresh, id: binding.id, tableName: binding.tableName, fields: binding.fields });
  };
  const remove = () => modal.confirm({
    title: `删除 ${binding.tableName || (isSource ? "Source" : "Sink")}`,
    content: "配置将从当前草稿中删除，SQL 中已插入的 DDL 需要手动调整。",
    okText: "删除配置", okButtonProps: { danger: true },
    onOk: () => { const current = live.current; current.onChange({ ...current.task, bindings: current.task.bindings.filter(item => item.id !== current.binding.id) }); },
  });
  const importFields = async () => {
    if (!source || source.type === "KAFKA" || !binding.physicalTable.trim()) return;
    const revision = ++requestRevision.current;
    const identity = { taskId: task.id, bindingId: binding.id, sourceId: source.id, table: binding.physicalTable };
    const stillCurrent = () => revision === requestRevision.current && live.current.task.id === identity.taskId && live.current.binding.id === identity.bindingId && live.current.binding.datasourceId === identity.sourceId && live.current.binding.physicalTable === identity.table;
    setColumnLoading(true);
    setMetadataError("");
    try {
      const columns = await api.sourceColumns(source.id, binding.physicalTable);
      if (!stillCurrent()) return;
      if (!columns.length) { message.warning("该表未返回字段，请手动添加字段。"); return; }
      const unknown = columns.filter(column => !mysqlTypeToFlink(column.type));
      const fields: RealtimeField[] = columns.map(column => ({ id: uid("field"), name: column.name, type: mysqlTypeToFlink(column.type) || column.type.toUpperCase(), nullable: column.columnKey !== "PRI" && column.nullable.toUpperCase() !== "NO", primaryKey: column.columnKey === "PRI" }));
      const apply = () => {
        if (!stillCurrent()) { message.info("数据源或表已变更，请重新导入字段。"); return; }
        updateFields(fields);
        setSchemaOpen(true);
        if (unknown.length) message.warning(`${unknown.length} 个字段的类型需手动确认。`);
        else message.success(`已导入 ${fields.length} 个字段`);
      };
      if (live.current.binding.fields.length) modal.confirm({ title: "导入字段结构", content: <><p>使用「{identity.table}」的 {fields.length} 个字段替换当前字段配置。</p>{unknown.length > 0 && <p>以下字段类型需手动确认：{unknown.map(column => `${column.name} (${column.type})`).join("、")}。</p>}<p>此操作只修改当前草稿。</p></>, okText: "替换字段", onOk: apply });
      else apply();
    } catch (error) { if (stillCurrent()) setMetadataError(errorText(error)); }
    finally { if (stillCurrent()) setColumnLoading(false); }
  };
  let ddl = "";
  try { ddl = generateDDL(binding, sources, task.id); } catch (error) { ddl = `-- ${errorText(error)}`; }

  return <div className="rt-binding-card">
    <Form layout="vertical" size="small" className="rt-binding-form">
      <div className="rt-form-pair">
        <Form.Item label="连接器"><Select aria-label={`${binding.role} 连接器`} value={binding.connector} onChange={changeConnector} options={(isSource ? ["KAFKA", "MYSQL_CDC"] : ["KAFKA", "MYSQL_JDBC", "DORIS"]).map(value => ({ value, label: connectorLabels[value as RealtimeBinding["connector"]] }))} /></Form.Item>
        <Form.Item label="逻辑表名" required><Input aria-label={`${binding.role} 逻辑表名`} value={binding.tableName} onChange={event => patch({ tableName: event.target.value })} placeholder={isSource ? "orders_source" : "orders_sink"} /></Form.Item>
      </div>
      <Form.Item label={<span className="rt-form-label-action">数据源<Button type="link" size="small" onClick={onManageDatasources}>管理数据源</Button></span>} required>
        <Select aria-label={`${binding.role} 数据源`} value={binding.datasourceId || undefined} onChange={datasourceId => {
          const nextSource = sources.find(item => item.id === datasourceId);
          patch({ datasourceId, physicalTable: "", feHttpUrls: nextSource?.type === "DORIS" ? nextSource.options?.feHttpUrls?.join(",") || "" : "" });
        }} placeholder={`选择 ${isKafka ? "Kafka" : isDoris ? "Doris" : "MySQL"} 数据源`} options={choices.map(item => ({ value: item.id, label: item.type === "KAFKA" ? item.name : `${item.name} / ${item.database}` }))} showSearch optionFilterProp="label" notFoundContent={<Button type="link" size="small" onClick={onManageDatasources}>添加数据源</Button>} />
      </Form.Item>
      {isKafka ? <>
        <Form.Item label={<span className="rt-form-label-action">Topic<Button type="link" size="small" loading={tableLoading} disabled={!source} onClick={() => setMetadataRevision(value => value + 1)}>刷新 Topics</Button></span>} required><AutoComplete aria-label={`${binding.role} Topic`} value={binding.topic} options={topics.map(value => ({value}))} onChange={topic => patch({topic})} placeholder="选择或输入 Topic" filterOption={(input,option) => String(option?.value||"").toLowerCase().includes(input.toLowerCase())} /></Form.Item>
        {isSource && <>
          <Form.Item label="Consumer Group" required><Input value={binding.consumerGroup} onChange={event => patch({ consumerGroup: event.target.value })} placeholder="flink_orders_dev" aria-label="Consumer Group" /></Form.Item>
          <Form.Item label="起始位点"><Select value={binding.startupMode} onChange={startupMode => patch({ startupMode })} options={[{ value: "group-offsets", label: "消费组位点（group-offsets）" }, { value: "earliest-offset", label: "最早位点（earliest-offset）" }, { value: "latest-offset", label: "最新位点（latest-offset）" }]} /></Form.Item>
        </>}
        {!isSource && <Form.Item label="写入方式"><Select value={binding.writeMode} onChange={writeMode => patch({ writeMode, format: writeMode === "upsert" ? "json" : binding.format })} options={[{ value: "append", label: "追加（Kafka）" }, { value: "upsert", label: "更新 / 删除（Upsert Kafka）" }]} /></Form.Item>}
        <Form.Item label="消息格式"><Select value={binding.format} onChange={format => patch({ format })} options={[{ value: "json", label: "JSON" }, { value: "csv", label: "CSV", disabled: !isSource && binding.writeMode === "upsert" }]} /></Form.Item>
        {!isSource && binding.writeMode === "upsert" && <Alert type="info" showIcon title="Upsert Kafka 使用主键及 JSON 消息格式。" />}
      </> : <>
        <Form.Item label={<span className="rt-form-label-action">物理表名<Button type="link" size="small" loading={tableLoading} icon={<RefreshCw size={12} />} onClick={() => setMetadataRevision(value => value + 1)} disabled={!source}>刷新表</Button></span>} required>
          <AutoComplete aria-label={`${binding.role} 物理表名`} value={binding.physicalTable} onChange={physicalTable => patch({ physicalTable })} placeholder="选择或填写物理表名" options={tables.map(table => ({ value: table.name, label: table.comment ? `${table.name} · ${table.comment}` : table.name }))} filterOption={(input, option) => String(option?.value || "").toLowerCase().includes(input.toLowerCase())} />
        </Form.Item>
        {isCDC && <>
          <Form.Item label="启动模式"><Select value={binding.cdcStartupMode} onChange={cdcStartupMode => patch({ cdcStartupMode })} options={[{ value: "initial", label: "全量快照 + 增量（initial）" }, { value: "latest-offset", label: "仅最新增量（latest-offset）" }]} /></Form.Item>
          <div className="rt-form-pair"><Form.Item label="Server ID"><Input aria-label="CDC Server ID" value={binding.serverId} onChange={event => patch({ serverId: event.target.value })} placeholder="留空自动分配；如 5400-5404" /></Form.Item><Form.Item label="服务器时区"><Input value={binding.timezone} onChange={event => patch({ timezone: event.target.value })} placeholder="Asia/Shanghai" aria-label="CDC 服务器时区" /></Form.Item></div>
          <p className="rt-panel-help">MySQL 需开启 Binlog；Server ID 支持 1–2147483647 的整数或递增范围，留空由服务端自动分配。手动配置应与其他 CDC 任务区分。</p>
        </>}
        {binding.connector === "MYSQL_JDBC" && <Form.Item label="写入方式"><Select value={binding.writeMode} onChange={writeMode => patch({ writeMode })} options={[{ value: "append", label: "追加写入" }, { value: "upsert", label: "按主键更新（Upsert）" }]} /></Form.Item>}
        {isDoris && <>
          <Form.Item label="FE HTTP 地址" required extra="使用完整 HTTP 地址，多个地址以逗号分隔。"><Input.TextArea rows={2} value={binding.feHttpUrls} onChange={event => patch({ feHttpUrls: event.target.value })} placeholder="http://127.0.0.1:8030" aria-label="Doris FE HTTP 地址" /></Form.Item>
          <Form.Item label="目标表模型"><Select value={binding.dorisModel} onChange={dorisModel => patch({ dorisModel, syncDeletes: dorisModel === "UNIQUE" ? binding.syncDeletes : false })} options={[{ value: "DUPLICATE", label: "Duplicate Key · 追加" }, { value: "UNIQUE", label: "Unique Key · 主键更新" }, { value: "AGGREGATE", label: "Aggregate Key · 聚合" }]} /></Form.Item>
          <Form.Item label="同步删除" extra={binding.dorisModel === "UNIQUE" ? "需要 Unique Key 表与主键字段，目标表能力需部署时确认。" : "同步删除仅支持 Unique Key 表。"}><Switch checked={binding.syncDeletes} disabled={binding.dorisModel !== "UNIQUE"} onChange={syncDeletes => patch({ syncDeletes })} aria-label="Doris 同步删除" /></Form.Item>
          <Form.Item label="Stream Load Label 前缀" required><Input value={binding.labelPrefix} onChange={event => patch({ labelPrefix: event.target.value })} placeholder="flink_orders" aria-label="Doris Label 前缀" /></Form.Item>
        </>}
      </>}
      {metadataError && <Alert type="warning" showIcon title="元数据读取失败" description={metadataError} closable onClose={() => setMetadataError("")} />}
      <div className="rt-schema-summary">
        <div><Braces size={14} /><strong>字段结构</strong><span>{binding.fields.length} 个字段 · {binding.fields.filter(field => field.primaryKey).length} 个主键</span></div>
        <Space size={6} wrap><Button size="small" onClick={() => setSchemaOpen(true)} icon={<Settings2 size={13} />}>编辑字段</Button>{!isKafka && <Button size="small" icon={<ArrowDownToLine size={13} />} loading={columnLoading} disabled={!source || !binding.physicalTable.trim()} onClick={() => void importFields()}>导入字段</Button>}</Space>
        {binding.fields.length > 0 && <div className="rt-field-chips">{binding.fields.slice(0, 4).map(field => <Tag key={field.id}>{field.name || "未命名"}{field.primaryKey && " · PK"}</Tag>)}{binding.fields.length > 4 && <span>+{binding.fields.length - 4}</span>}</div>}
      </div>
      {isSource && <>
        <Form.Item label="事件时间字段" extra="选择 TIMESTAMP(3) 或 TIMESTAMP_LTZ(3) 字段，为事件时间窗口配置 Watermark。"><Select aria-label="事件时间字段" allowClear value={binding.eventTimeField || undefined} onChange={eventTimeField => patch({ eventTimeField: eventTimeField || "" })} placeholder="不配置事件时间" options={binding.fields.filter(field => isTimestamp(field.type)).map(field => ({ value: field.name, label: `${field.name} · ${field.type}` }))} /></Form.Item>
        {binding.eventTimeField && <Form.Item label="Watermark 延迟（秒）"><InputNumber min={0} max={86400} precision={0} value={binding.watermarkSeconds} onChange={value => { if (value !== null) patch({ watermarkSeconds: value }); }} aria-label="Watermark 延迟秒数" /></Form.Item>}
      </>}
    </Form>
    <div className="rt-binding-actions"><Button size="small" icon={<CodeXml size={13} />} onClick={() => setDDLOpen(true)}>预览 DDL</Button><Tooltip title={issues.length ? "请先完善配置" : "插入当前 SQL，已有 SQL 保留"}><Button size="small" type="primary" disabled={issues.length > 0} onClick={() => onInsertDDL(binding)}>插入 DDL</Button></Tooltip><Tooltip title="删除配置"><Button size="small" type="text" danger icon={<Trash2 size={14} />} aria-label={`删除 ${binding.tableName || binding.role}`} onClick={remove} /></Tooltip></div>
    {issues.length > 0 && <Alert className="rt-binding-validation" type="warning" title="配置待完善" description={<ul>{issues.map((issue, index) => <li key={`${index}:${issue}`}>{issue}</li>)}</ul>} />}
    <Modal title={`${binding.tableName || binding.role} · 字段结构`} open={schemaOpen} width={760} onCancel={() => setSchemaOpen(false)} footer={<Button type="primary" onClick={() => setSchemaOpen(false)}>完成</Button>}>
      <div className="rt-schema-toolbar"><p>字段修改保留在任务草稿中。主键字段不可为空。</p><Button size="small" icon={<Plus size={14} />} onClick={() => updateFields([...live.current.binding.fields, { id: uid("field"), name: `field_${live.current.binding.fields.length + 1}`, type: "STRING", nullable: true, primaryKey: false }])}>添加字段</Button></div>
      <Table<RealtimeField> size="small" rowKey="id" pagination={false} scroll={{ x: 630, y: 390 }} dataSource={binding.fields} locale={{ emptyText: "添加字段，或从数据源导入已有表结构。" }} columns={[
        { title: "字段名", dataIndex: "name", width: 180, render: (value: string, field) => <Input aria-label={`字段 ${field.id} 名称`} size="small" value={value} onChange={event => updateFields(live.current.binding.fields.map(item => item.id === field.id ? { ...item, name: event.target.value } : item))} /> },
        { title: "Flink 类型", dataIndex: "type", width: 210, render: (value: string, field) => <AutoComplete aria-label={`字段 ${field.name || field.id} 类型`} size="small" className="rt-full-width" value={value} options={fieldTypes.map(type => ({ value: type }))} onChange={type => updateFields(live.current.binding.fields.map(item => item.id === field.id ? { ...item, type } : item))} filterOption={(input, option) => String(option?.value || "").toLowerCase().includes(input.toLowerCase())} /> },
        { title: "允许空", width: 76, render: (_, field) => <Switch size="small" aria-label={`字段 ${field.name || field.id} 允许空`} checked={field.nullable} disabled={field.primaryKey} onChange={nullable => updateFields(live.current.binding.fields.map(item => item.id === field.id ? { ...item, nullable } : item))} /> },
        { title: "主键", width: 64, render: (_, field) => <Switch size="small" aria-label={`字段 ${field.name || field.id} 主键`} checked={field.primaryKey} onChange={primaryKey => updateFields(live.current.binding.fields.map(item => item.id === field.id ? { ...item, primaryKey, nullable: primaryKey ? false : item.nullable } : item))} /> },
        { title: "操作", width: 55, render: (_, field) => <Button type="text" size="small" danger icon={<Trash2 size={14} />} aria-label={`删除字段 ${field.name || field.id}`} onClick={() => updateFields(live.current.binding.fields.filter(item => item.id !== field.id))} /> },
      ]} />
    </Modal>
    <Modal title={`${binding.tableName || binding.role} · DDL 预览`} open={ddlOpen} width={850} onCancel={() => setDDLOpen(false)} footer={<Space><Button onClick={() => setDDLOpen(false)}>关闭</Button><Button type="primary" disabled={issues.length > 0} onClick={() => { onInsertDDL(binding); setDDLOpen(false); }}>插入当前 SQL</Button></Space>}>
      <p className="rt-panel-help">连接信息引用数据源，执行时由服务器注入凭据。修改配置后重新插入 DDL；手改受管块会在 SQL 校验时提示冲突。</p>
      <pre className="rt-code-preview">{ddl}</pre>
    </Modal>
  </div>;
}

export function RuntimeEditor({ task, onChange }: Pick<BindingEditorProps, "task" | "onChange">) {
  const patch = (next: Partial<RealtimeTask["runtime"]>) => onChange({ ...task, runtime: { ...task.runtime, ...next } });
  return <div className="rt-runtime-editor">
    <div className="rt-panel-section-heading"><div><span className="rt-eyebrow">持续运行</span><h3>运行配置</h3></div><Settings2 size={17} /></div>
    <p className="rt-panel-help">配置实时作业的并行、Checkpoint 与重启策略。发布版本将保存这些配置。</p>
    <Form layout="vertical" size="small">
      <Form.Item label="默认并行度" required extra="同一算子的并发实例数。"><InputNumber aria-label="默认并行度" min={1} max={256} precision={0} value={task.runtime.parallelism} onChange={value => { if (value !== null) patch({ parallelism: value }); }} /></Form.Item>
      <Form.Item label="Checkpoint 间隔（秒）" required extra="周期性保存一致性状态。"><InputNumber aria-label="Checkpoint 间隔秒数" min={1} max={86400} precision={0} value={task.runtime.checkpointSeconds} onChange={value => { if (value !== null) patch({ checkpointSeconds: value }); }} /></Form.Item>
      <Form.Item label="失败重启次数" required extra="设置为 0 表示不自动重启。"><InputNumber aria-label="失败重启次数" min={0} max={100} precision={0} value={task.runtime.restartAttempts} onChange={value => { if (value !== null) patch({ restartAttempts: value }); }} /></Form.Item>
      <Form.Item label="重启等待（秒）" required><InputNumber aria-label="重启等待秒数" min={1} max={86400} precision={0} value={task.runtime.restartDelaySeconds} onChange={value => { if (value !== null) patch({ restartDelaySeconds: value }); }} /></Form.Item>
    </Form>
    <div className="rt-runtime-note"><Waves size={15} /><div><strong>独立实时运维</strong><p>发布后前往实时运维启动作业、查看 Checkpoint 或创建 Savepoint。</p></div></div>
    <Alert type="info" showIcon title="真实 Flink 运行配置" description="发布后配置进入服务器版本快照；Checkpoint、重启策略和并行度在 Flink 作业中执行。" />
  </div>;
}

export function ReleaseViewer({ task, releases }: Pick<BindingEditorProps, "task" | "releases">) {
  const [selectedId, setSelectedId] = useState("");
  const candidates = releases.filter(release => release.taskId === task.id).sort((left, right) => right.releaseNo - left.releaseNo);
  const selected = candidates.find(release => release.id === selectedId) || candidates[0];
  const [sqlOpen, setSQLOpen] = useState(false);
  useEffect(() => { setSelectedId(""); setSQLOpen(false); }, [task.id]);
  return <div className="rt-release-viewer">
    <div className="rt-panel-section-heading"><div><span className="rt-eyebrow">发布快照</span><h3>版本记录</h3></div><History size={17} /></div>
    <p className="rt-panel-help">每个版本保存独立的 SQL、Source / Sink 和运行配置。草稿修改不会影响已发布版本。</p>
    {!selected ? <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="尚未发布版本，完成配置后点击发布。" /> : <>
      {selected.importWarning && <Alert type="warning" showIcon title="旧版本需要重新发布" description={selected.importWarning} />}
      <Select className="rt-full-width" aria-label="选择发布版本" value={selected.id} onChange={setSelectedId} options={candidates.map(release => ({ value: release.id, label: `R${release.releaseNo} · ${stamp(release.createdAt)}` }))} />
      <Descriptions className="rt-release-description" column={1} size="small" items={[
        { key: "version", label: "版本", children: <Tag color="blue">R{selected.releaseNo}</Tag> },
        { key: "created", label: "发布时间", children: stamp(selected.createdAt) },
        { key: "note", label: "发布说明", children: selected.note || "未填写" },
        { key: "parallelism", label: "并行度", children: selected.snapshot.runtime.parallelism },
        { key: "checkpoint", label: "Checkpoint", children: `${selected.snapshot.runtime.checkpointSeconds} 秒` },
        { key: "restart", label: "重启策略", children: `${selected.snapshot.runtime.restartAttempts} 次 / 等待 ${selected.snapshot.runtime.restartDelaySeconds} 秒` },
      ]} />
      <h4 className="rt-panel-subheading">Source / Sink 快照</h4>
      <div className="rt-release-bindings">{selected.snapshot.bindings.map(binding => <div key={binding.id}><span>{binding.role === "SOURCE" ? <Waves size={14} /> : <Database size={14} />}<strong>{binding.tableName || "未命名"}</strong></span><Tag>{connectorLabels[binding.connector]}</Tag><small>{binding.topic || binding.physicalTable || "未配置目标"} · {binding.fields.length} 个字段</small></div>)}</div>
      <Button className="rt-full-width" icon={<CodeXml size={14} />} onClick={() => setSQLOpen(true)}>查看 SQL 快照</Button>
      <Modal title={`${selected.snapshot.name} · R${selected.releaseNo} SQL 快照`} open={sqlOpen} width={900} onCancel={() => setSQLOpen(false)} footer={<Button onClick={() => setSQLOpen(false)}>关闭</Button>}><pre className="rt-code-preview">{selected.snapshot.sql || "-- 此版本没有 SQL"}</pre></Modal>
    </>}
  </div>;
}
