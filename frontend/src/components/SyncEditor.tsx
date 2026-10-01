import { useEffect, useRef, useState } from "react";
import { Alert, App, Button, Collapse, Form, Input, InputNumber, Select, Space, Table, Tag, Tooltip } from "antd";
import { ArrowRight, Database, SlidersHorizontal } from "lucide-react";
import { api } from "../api";
import type { DataSource, StudioObject, SyncConfig, SyncMetadata, SyncPartitionAssignment } from "../types";
import { applySyncChange, defaultPartitionAssignments, schemaLabel, sourceTableReset, targetTableReset } from "../state/sync";

const isDate = (type: string) => /^DATE(?:V2)?(?:\b|$)/i.test(type);
const fieldOptions = (columns: { name: string; type: string }[]) => columns.map(column => ({ value: column.name, label: `${column.name} · ${column.type}` }));
const partitionOptions = (metadata?: SyncMetadata) => (metadata?.partition?.partitions || []).map(partition => ({ value: partition.name, label: `${partition.name} · ${partition.range || "分区"}` }));

export default function SyncEditor({ object, onChange }: { object: StudioObject; onChange: (patch: Partial<StudioObject>) => void }) {
  const { message } = App.useApp();
  const c: SyncConfig = object.config.sync || {};
  const latest = useRef({ object, onChange }); latest.current = { object, onChange };
  const [sources, setSources] = useState<DataSource[]>([]);
  const [sourceTables, setSourceTables] = useState<{ name: string }[]>([]), [targetTables, setTargetTables] = useState<{ name: string }[]>([]);
  const [source, setSource] = useState<SyncMetadata>(), [target, setTarget] = useState<SyncMetadata>();
  const [sourceLoading, setSourceLoading] = useState(false), [targetLoading, setTargetLoading] = useState(false);
  const [error, setError] = useState(""), [busy, setBusy] = useState(false);
  const change = (patch: Partial<SyncConfig>) => onChange(applySyncChange(object, patch));

  useEffect(() => {
    let alive = true;
    void api.datasources(object.workspaceId).then(data => { if (alive) setSources(data.filter((s): s is DataSource => s.type !== "KAFKA")); }).catch(e => { if (alive) setError(e.message); });
    return () => { alive = false; };
  }, [object.workspaceId]);
  useEffect(() => {
    let alive = true; setSourceTables([]); setSource(undefined); setError("");
    if (c.sourceDataSourceId) void api.sourceTables(c.sourceDataSourceId).then(data => { if (alive) setSourceTables(data); }).catch(e => { if (alive) setError(e.message); });
    return () => { alive = false; };
  }, [c.sourceDataSourceId]);
  useEffect(() => {
    let alive = true; setTargetTables([]); setTarget(undefined); setError("");
    if (c.targetDataSourceId) void api.sourceTables(c.targetDataSourceId).then(data => { if (alive) setTargetTables(data); }).catch(e => { if (alive) setError(e.message); });
    return () => { alive = false; };
  }, [c.targetDataSourceId]);
  useEffect(() => {
    let alive = true; setSource(undefined); setError(""); setSourceLoading(!!(c.sourceDataSourceId && c.sourceTable));
    if (c.sourceDataSourceId && c.sourceTable) void api.syncMetadata(c.sourceDataSourceId, c.sourceTable).then(data => { if (alive) setSource(data); }).catch(e => { if (alive) setError(e.message); }).finally(() => { if (alive) setSourceLoading(false); });
    return () => { alive = false; };
  }, [object.id, c.sourceDataSourceId, c.sourceTable]);
  useEffect(() => {
    let alive = true; setTarget(undefined); setError(""); setTargetLoading(!!(c.targetDataSourceId && c.targetTable));
    if (c.targetDataSourceId && c.targetTable) void api.syncMetadata(c.targetDataSourceId, c.targetTable).then(data => {
      if (!alive) return;
      setTarget(data);
      const current = latest.current, sync: SyncConfig = current.object.config.sync || {};
      const defaults = defaultPartitionAssignments(data);
      if (!sync.targetPartitionAssignments?.length && defaults.length) current.onChange(applySyncChange(current.object, { targetPartitionAssignments: defaults, mapping: (sync.mapping || []).filter(mapping => !defaults.some(assignment => assignment.target === mapping.target)) }));
    }).catch(e => { if (alive) setError(e.message); }).finally(() => { if (alive) setTargetLoading(false); });
    return () => { alive = false; };
  }, [object.id, c.targetDataSourceId, c.targetTable]);

  const sourceType = sources.find(s => s.id === c.sourceDataSourceId)?.type || "MYSQL";
  const targetType = sources.find(s => s.id === c.targetDataSourceId)?.type;
  const sourceColumns = source?.columns || [];
  const selectedColumns = c.columns?.length ? sourceColumns.filter(column => c.columns!.includes(column.name)) : sourceColumns;
  const assignments = c.targetPartitionAssignments || [];
  const assignedTargets = new Set(assignments.map(assignment => assignment.target));
  const targetColumns = (target?.columns || []).filter(column => !assignedTargets.has(column.name));
  const automaticMapping = selectedColumns.filter(column => targetColumns.some(targetColumn => targetColumn.name === column.name)).map(column => ({ source: column.name, target: column.name }));
  const mapping = c.mapping?.length ? c.mapping.filter(row => !assignedTargets.has(row.target)) : automaticMapping;
  const missingTargets = targetColumns.filter(column => !mapping.some(row => row.target === column.name));
  const sourcePartitioned = sourceType === "DORIS" && source?.partition?.type !== "NONE" && !!source?.partition;
  const targetPartitioned = targetType === "DORIS" && target?.partition?.type !== "NONE" && !!target?.partition;
  const overwriteNeedsPartitions = targetPartitioned && assignments.some(assignment => assignment.mode === "column") && !c.targetPartitions?.length;
  const duplicateModel = /DUPLICATE/i.test(target?.model || "");
  const assignPartition = (targetName: string, patch: Partial<SyncPartitionAssignment>) => {
    const previous = assignments.find(assignment => assignment.target === targetName) || { target: targetName, mode: "value" as const, value: "" };
    const next = { ...previous, ...patch };
    change({ targetPartitionAssignments: [...assignments.filter(assignment => assignment.target !== targetName), next], mapping: mapping.filter(row => row.target !== targetName) });
  };
  const tableOptions = (tables: { name: string }[]) => tables.map(table => ({ value: table.name, label: table.name }));

  return <div className="node-configuration sync-editor">
    <div className="configuration-title"><div><h3>数据集成</h3><p>配置数据库之间的批量同步、分区与字段映射</p></div><Tag color="green">MySQL ↔ Doris</Tag></div>
    {error && <Alert type="error" showIcon title={error} closable onClose={() => setError("")} />}
    <Form layout="vertical">
      <div className="sync-endpoints">
        <section className="sync-endpoint">
          <div className="sync-section-heading"><Database size={16} /><h4>来源</h4><span>读取数据</span></div>
          <Form.Item label="来源数据库（Schema）" required><Select aria-label="来源数据源" showSearch optionFilterProp="label" placeholder="选择 MySQL 或 Doris Schema" value={c.sourceDataSourceId} options={sources.map(s => ({ value: s.id, label: schemaLabel(s) }))} onChange={sourceDataSourceId => change({ sourceDataSourceId, sourceTable: undefined, columns: [], mapping: [], where: undefined, sourcePartitionFilter: undefined, targetDataSourceId: undefined, targetTable: undefined, keyColumns: [], targetPartitionAssignments: [], targetPartitions: [] })} /></Form.Item>
          <Form.Item label="来源表" required><Select aria-label="来源表" showSearch optionFilterProp="label" disabled={!c.sourceDataSourceId} loading={sourceLoading} placeholder="选择要读取的表" value={c.sourceTable} options={tableOptions(sourceTables)} onChange={sourceTable => change({ ...sourceTableReset(c), sourceTable })} /></Form.Item>
          {sourcePartitioned && <div className="sync-partition-panel">
            <div className="sync-partition-heading"><strong>来源分区</strong><Tag>{source.partition.automatic ? "自动分区" : source.partition.type}</Tag></div>
            <Form.Item label="分区筛选" extra={source.partition.partitions.length ? "留空读取所有分区；可与分区字段条件、数据过滤组合。" : "当前没有已有分区，可通过下方分区字段条件筛选。"}><Select aria-label="来源分区" mode="multiple" showSearch optionFilterProp="label" placeholder="选择已有分区" value={c.sourcePartitionFilter?.partitions || []} options={partitionOptions(source)} onChange={partitions => change({ sourcePartitionFilter: { ...c.sourcePartitionFilter, partitions } })} /></Form.Item>
            <Form.Item label="分区字段条件" extra={`分区字段：${source.partition.columns.map(column => `${column.name} (${column.type})`).join("、")}。支持调度参数。`}><Input.TextArea aria-label="来源分区条件" className="monospace" autoSize={{ minRows: 2, maxRows: 5 }} placeholder="ds = '${bizdate}'" value={c.sourcePartitionFilter?.where || ""} onChange={e => change({ sourcePartitionFilter: { ...c.sourcePartitionFilter, where: e.target.value } })} /></Form.Item>
          </div>}
          <Form.Item label="数据过滤（WHERE）" extra="留空读取全部记录。参数在右侧调度配置中定义。"><Input.TextArea aria-label="同步筛选条件" className="monospace" autoSize={{ minRows: 2, maxRows: 5 }} placeholder="例如 status = 'PAID' AND ordered_at >= '${bizdate}'" value={c.where || ""} onChange={e => change({ where: e.target.value })} /></Form.Item>
          <Form.Item label="读取字段" extra="留空读取全部字段。"><Select aria-label="读取字段" mode="multiple" disabled={!source} placeholder="全部字段" value={c.columns || []} options={fieldOptions(sourceColumns)} onChange={columns => change({ columns, mapping: [], targetPartitionAssignments: assignments.filter(assignment => assignment.mode !== "column" || !columns.length || columns.includes(assignment.source || "")) })} /></Form.Item>
        </section>
        <div className="sync-flow-arrow" aria-hidden="true"><ArrowRight size={19} /></div>
        <section className="sync-endpoint">
          <div className="sync-section-heading"><Database size={16} /><h4>目标</h4><span>写入数据</span></div>
          <Form.Item label="目标数据库（Schema）" required><Select aria-label="目标数据源" showSearch optionFilterProp="label" disabled={!c.sourceDataSourceId} placeholder="选择另一种数据库的 Schema" value={c.targetDataSourceId} options={sources.filter(s => (s.type || "MYSQL") !== sourceType).map(s => ({ value: s.id, label: schemaLabel(s) }))} onChange={targetDataSourceId => change({ ...targetTableReset(), targetDataSourceId, targetTable: undefined })} /></Form.Item>
          <Form.Item label="目标表" required extra={target && `表模型：${target.model}`}><Select aria-label="目标表" showSearch optionFilterProp="label" disabled={!c.targetDataSourceId} loading={targetLoading} placeholder="选择已有目标表" value={c.targetTable} options={tableOptions(targetTables)} onChange={targetTable => change({ ...targetTableReset(), targetTable })} /></Form.Item>
          {targetPartitioned && <div className="sync-partition-panel">
            <div className="sync-partition-heading"><strong>写入分区</strong><Tag>{target.partition.automatic ? "自动分区" : target.partition.type}</Tag></div>
            {target.partition.columns.map(column => {
              const assignment = assignments.find(item => item.target === column.name);
              return <div className="sync-partition-assignment" key={column.name}>
                <div className="sync-partition-column"><code>{column.name}</code><span>{column.type}</span></div>
                <Form.Item label="分区值来源" required><Select aria-label={`分区 ${column.name} 值来源`} value={assignment?.mode || "value"} options={[{ value: "value", label: "固定值 / 调度参数" }, { value: "column", label: "来源字段" }]} onChange={mode => assignPartition(column.name, mode === "value" ? { mode, value: isDate(column.type) ? "${bizdate}" : "", source: undefined } : { mode, source: undefined, value: undefined })} /></Form.Item>
                {assignment?.mode === "column" ? <Form.Item label="来源字段" required extra={isDate(column.type) ? "DATE 分区取所选字段的日期，例如 ordered_at → ds。" : "使用每条记录的字段值路由到分区。"}><Select aria-label={`分区 ${column.name} 来源字段`} showSearch optionFilterProp="label" placeholder="选择字段" value={assignment.source} options={fieldOptions(selectedColumns)} onChange={source => assignPartition(column.name, { source })} /></Form.Item> : <Form.Item label="分区值" required extra={isDate(column.type) ? "支持 YYYY-MM-DD 或 ${bizdate}。默认 bizdate 使用业务日期。" : "支持固定值或 ${参数名}。"}><Input aria-label={`分区 ${column.name} 值`} className="monospace" placeholder={isDate(column.type) ? "${bizdate}" : "输入分区值"} value={assignment?.value || ""} onChange={e => assignPartition(column.name, { mode: "value", value: e.target.value })} /></Form.Item>}
              </div>;
            })}
            <Form.Item label="限制写入的已有分区" extra={target.partition.automatic ? "可选。留空按分区值自动路由，缺失的日期分区在写入时创建。" : "可选。留空按分区值路由；记录必须落在已有分区范围内。"}><Select aria-label="目标分区" mode="multiple" showSearch optionFilterProp="label" placeholder="按分区值路由" value={c.targetPartitions || []} options={partitionOptions(target)} onChange={targetPartitions => change({ targetPartitions })} /></Form.Item>
          </div>}
          <Form.Item label="写入方式"><Select aria-label="写入方式" value={c.writeMode || "append"} options={[{ value: "append", label: "追加" }, { value: "upsert", label: duplicateModel ? "主键更新（需 Unique Key 表）" : "主键更新", disabled: duplicateModel }, { value: "overwrite", label: targetPartitioned ? "覆盖目标分区" : "清空后覆盖", disabled: overwriteNeedsPartitions }]} onChange={writeMode => change({ writeMode })} /></Form.Item>
          {c.writeMode === "upsert" && target?.model === "INNODB" && <Form.Item label="主键 / 唯一索引" required><Select aria-label="更新主键" value={JSON.stringify(c.keyColumns || [])} options={target.uniqueKeys.map(key => ({ value: JSON.stringify(key), label: key.join(", ") }))} onChange={value => change({ keyColumns: JSON.parse(value) })} /></Form.Item>}
        </section>
      </div>
      {overwriteNeedsPartitions && <Alert type="info" showIcon title="来源字段路由支持追加；覆盖时须选择已有目标分区" description="选择上方“限制写入的已有分区”后可启用覆盖，或将分区值来源改为固定值 / 调度参数。" />}
      {c.writeMode === "overwrite" && !overwriteNeedsPartitions && <Alert type="warning" showIcon title={targetPartitioned ? "仅清空本次写入的目标分区，再分批导入" : "预检通过后清空目标表，再分批导入"} description={targetPartitioned ? "已有其他分区保留；尚不存在的自动分区跳过清空。停止或失败会保留已提交的数据。" : "空来源会清空目标；停止或失败不撤回清空和已提交的数据。"} />}
      <section className="sync-mapping-section">
        <div className="sync-section-heading"><h4>字段映射</h4><span>{mapping.length} 个字段{assignments.length ? ` · ${assignments.length} 个分区字段` : ""}</span><Space><Button size="small" disabled={!source || !target} onClick={() => change({ mapping: automaticMapping })}>按同名匹配</Button><Button size="small" disabled={!source || !target} onClick={() => change({ mapping: [...mapping, { source: "", target: "" }] })}>添加映射</Button></Space></div>
        <p className="sync-mapping-help">同名字段默认匹配。分区字段由上方配置生成，映射中可直接查看。</p>
        <Table size="small" rowKey="index" pagination={false} scroll={{ x: 560 }} locale={{ emptyText: "选择来源表和目标表后配置字段映射" }} dataSource={mapping.map((row, index) => ({ ...row, index }))} columns={[
          { title: "来源字段", width: "41%", render: (_, row) => <Select aria-label={`来源字段 ${row.index + 1}`} style={{ width: "100%" }} showSearch optionFilterProp="label" placeholder="选择来源字段" value={row.source || undefined} options={fieldOptions(selectedColumns)} onChange={source => change({ mapping: mapping.map((item, index) => index === row.index ? { ...item, source } : item) })} /> },
          { title: "目标字段", width: "41%", render: (_, row) => <Select aria-label={`目标字段 ${row.index + 1}`} style={{ width: "100%" }} showSearch optionFilterProp="label" placeholder="选择目标字段" value={row.target || undefined} options={fieldOptions(targetColumns)} onChange={target => change({ mapping: mapping.map((item, index) => index === row.index ? { ...item, target } : item) })} /> },
          { title: "操作", width: 70, render: (_, row) => <Button type="link" onClick={() => change({ mapping: mapping.filter((_, index) => index !== row.index) })}>删除</Button> }
        ]} />
        {!!assignments.length && <div className="sync-derived-mappings">{assignments.map(assignment => <div key={assignment.target}><Tag color="blue">分区映射</Tag><code>{assignment.mode === "value" ? assignment.value || "待配置" : assignment.source || "待选字段"}</code><ArrowRight size={14} /><code>{assignment.target}</code><span>{(target?.partition?.columns || []).find(column => column.name === assignment.target)?.type}{assignment.mode === "column" && isDate((target?.partition?.columns || []).find(column => column.name === assignment.target)?.type || "") ? " · 取日期" : ""}</span></div>)}</div>}
        {!!missingTargets.length && <div className="sync-unmapped"><span>未映射目标字段</span>{missingTargets.map(column => <Tooltip key={column.name} title={`${column.type} · ${column.nullable === "YES" ? "可为空" : "不可为空；请确认默认值或补充映射"}`}><Tag>{column.name}</Tag></Tooltip>)}</div>}
      </section>
      <Collapse className="sync-advanced" ghost items={[{ key: "advanced", label: <span><SlidersHorizontal size={14} />运行配置<span className="panel-muted">每批 {c.batchRows || 10000} 行 · 并行度 {c.parallelism || 1}</span></span>, children: <div className="sync-options"><Form.Item label="每批行数"><InputNumber aria-label="每批行数" min={1} max={100000} value={c.batchRows || 10000} onChange={value => change({ batchRows: value || 10000 })} /></Form.Item><Form.Item label="写入并行度"><InputNumber aria-label="写入并行度" min={1} max={16} value={c.parallelism || 1} onChange={value => change({ parallelism: value || 1 })} /></Form.Item><Form.Item label="超时（秒）"><InputNumber aria-label="同步超时" min={1} max={86400} value={c.timeoutSeconds || 3600} onChange={value => change({ timeoutSeconds: value || 3600 })} /></Form.Item></div> }]} />
      <div className="sync-validation"><Button loading={busy} disabled={!c.sourceTable || !c.targetTable || sourceLoading || targetLoading || (c.writeMode === "overwrite" && overwriteNeedsPartitions)} onClick={async () => { setBusy(true); setError(""); try { const result = await api.validateSync(object); message.success(result.message); } catch (e) { setError((e as Error).message); } finally { setBusy(false); } }}>校验连接、分区与字段映射</Button><span>校验通过后保存并运行</span></div>
    </Form>
  </div>;
}
