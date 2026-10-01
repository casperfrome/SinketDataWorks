import { useEffect, useRef, useState } from "react";
import { Alert, AutoComplete, Button, Input, InputNumber, Modal, Radio, Space, Table, Tooltip } from "antd";
import { DownOutlined } from "@ant-design/icons";
import { CircleHelp, Code2, Plus, RefreshCw } from "lucide-react";
import { api } from "../api";
import type { ParameterPreview, ScheduleConfig, ScheduleParameter, StudioObject } from "../types";
import { mergeCodeParameters, parameterError, parameterExpression, parseParameterExpression } from "../state/parameters";
import { defaultSchedule, yesterday } from "../state/schedules";
import BusinessDateInput from "./BusinessDateInput";
import { parameterText } from "../state/sync";
import { parametersDiffer } from "../state/schedulingOperations";
import "../scheduling.css";

export interface ParameterEditorProps {
  object: StudioObject;
  onChange: (patch: Partial<StudioObject>) => void;
  config?: ScheduleConfig;
  releasedObject?: StudioObject;
  releaseNo?: number;
}
const suggestions = ["$bizdate", "$cyctime", "$[yyyymmdd-1]", "${yyyymmdd}", "$[yyyymmddhh24miss]", "$[add_months(yyyymmdd,-1)]"];
const parameterValueOptions = suggestions.map(value => ({ value }));
const emptyParameters: ScheduleParameter[] = [];

export default function ScheduleParameterEditor({ object, onChange, config, releasedObject, releaseNo }: ParameterEditorProps) {
  const schedule = object.config.schedule || {};
  const rows: ScheduleParameter[] = schedule.parameters || emptyParameters;
  const latest = useRef({object, rows, onChange}); latest.current = {object, rows, onChange};
  const [expressionMode, setExpressionMode] = useState(schedule.parameterExpressionDraft !== undefined);
  const [text, setText] = useState<string>(schedule.parameterExpressionDraft ?? parameterExpression(rows));
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [previewOpen, setPreviewOpen] = useState(false);
  const [day, setDay] = useState(yesterday);
  const [count, setCount] = useState(5);
  const [preview, setPreview] = useState<ParameterPreview[]>([]);
  const [previewError, setPreviewError] = useState("");
  const [previewMode, setPreviewMode] = useState("release");
  const previewSequence = useRef(0);
  const releasedRows: ScheduleParameter[] = releasedObject?.config.schedule?.parameters || [];
  const previewRows = previewMode === "release" && releasedObject ? releasedRows : rows;
  useEffect(() => { previewSequence.current++; setPreview([]); setPreviewError(""); }, [releasedObject, releaseNo, previewMode, rows, object.content, config]);
  useEffect(() => {
    setText(schedule.parameterExpressionDraft ?? parameterExpression(rows));
    setExpressionMode(schedule.parameterExpressionDraft !== undefined);
    setError(""); setPreview([]);
  }, [object.id, object.version]);
  const timeConfig = { ...defaultSchedule, ...schedule, ...config };
  const patch = (next: ScheduleParameter[], invalidText?: string) => {
    onChange({ config: { ...object.config, schedule: { ...schedule, parameters: next, parameterExpressionDraft: invalidText } } });
    setPreview([]); setError("");
  };
  const changeRow = (index: number, values: Partial<ScheduleParameter>) => patch(rows.map((row, i) => i === index ? { ...row, ...values } : row));
  const loadCode = async () => {
    setBusy(true); setError("");
    try {
      const names = await api.extractParameters(parameterText(object));
      const current = latest.current;
      if (current.object.id !== object.id) return;
      current.onChange({config:{...current.object.config,schedule:{...current.object.config.schedule,parameters:mergeCodeParameters(current.rows,names),parameterExpressionDraft:undefined}}});
      setPreview([]);
    }
    catch (e) { setError((e as Error).message); }
    finally { setBusy(false); }
  };
  const changeExpression = (value: string) => {
    setText(value);
    try { patch(parseParameterExpression(value, rows)); }
    catch (e) { patch(rows, value); setError((e as Error).message); }
  };
  const toggleMode = () => {
    if (expressionMode) {
      try { patch(parseParameterExpression(text, rows)); setExpressionMode(false); }
      catch (e) { setError((e as Error).message); }
    } else {
      const invalid = parameterError(rows); if (invalid) { setError(invalid); return; }
      setText(parameterExpression(rows)); setExpressionMode(true); setError("");
    }
  };
  const loadPreview = async () => {
    const sequence = ++previewSequence.current;
    setBusy(true); setPreviewError(""); setPreview([]);
    try {
      const useRelease = previewMode === "release" && !!releasedObject;
      const values = useRelease ? releasedRows : expressionMode ? parseParameterExpression(text, rows) : rows;
      const invalid = parameterError(values); if (invalid) throw new Error(invalid);
      const result = await api.previewParameters({ ...timeConfig, parameters: values, code: parameterText(useRelease ? releasedObject! : object), businessDate: day, count });
      if (sequence === previewSequence.current) setPreview(result);
    } catch (e) { if (sequence === previewSequence.current) setPreviewError((e as Error).message); }
    finally { setBusy(false); }
  };
  return <div className="schedule-parameters">
    {releasedObject && <Alert type={parametersDiffer(rows,releasedRows) ? "warning" : "info"} showIcon title={parametersDiffer(rows,releasedRows) ? `开发参数与 R${releaseNo} 不一致，保存参数后需重新发布并应用` : `调度执行 R${releaseNo} 的参数快照`} className="spaced-form" />}
    <div className="parameter-heading"><h3>调度参数</h3><Tooltip title="查看调度参数格式"><a href="https://help.aliyun.com/zh/dataworks/user-guide/best-practices-of-configuring-scheduling-parameters" target="_blank" rel="noreferrer" aria-label="调度参数帮助"><CircleHelp size={13} /></a></Tooltip><span /></div>
    <div className="parameter-actions">
      {!expressionMode && <><Button type="text" size="small" icon={<Plus size={14} />} onClick={() => patch([...rows, { name: "", value: "", source: "MANUAL" }])}>新增参数</Button><Button type="text" size="small" loading={busy} icon={<RefreshCw size={13} />} onClick={() => void loadCode()}>加载代码中的参数</Button></>}
      <Button type="text" size="small" className="parameter-mode" icon={<Code2 size={14} />} disabled={busy} onClick={toggleMode}>{expressionMode ? "可视化定义" : "用表达式定义"}</Button>
    </div>
    {expressionMode ? <Input.TextArea aria-label="调度参数表达式" className="code-input" value={text} rows={4} onChange={e => changeExpression(e.target.value)} placeholder="bizdate=$[yyyymmdd-1] region=杭州" /> :
      <div className="parameter-table-wrap"><table className="parameter-table"><thead><tr><th>参数名</th><th>参数值</th><th>来源</th><th>操作</th></tr></thead><tbody>
        {rows.map((row, i) => <tr key={i}><td><Input size="small" aria-label={`参数名 ${i + 1}`} value={row.name} onChange={e => changeRow(i, { name: e.target.value })} maxLength={64} /></td><td>
          <AutoComplete
            size="small"
            className="parameter-value-select"
            aria-label={`参数值 ${i + 1}`}
            value={row.value}
            options={parameterValueOptions}
            allowClear
            suffixIcon={<DownOutlined />}
            popupMatchSelectWidth={false}
            classNames={{ popup: { root: "parameter-value-popup" } }}
            onChange={value => changeRow(i, { value })}
          />
        </td><td>{row.source === "CODE" ? "代码解析" : "手动添加"}</td><td><Button type="link" size="small" onClick={() => patch(rows.filter((_, index) => i !== index))} aria-label={`删除参数 ${i + 1}`}>删除</Button></td></tr>)}
        {!rows.length && <tr><td colSpan={4} className="parameter-empty">暂无参数</td></tr>}
      </tbody></table></div>}
    {(error || parameterError(rows)) && <p className="parameter-error" role="alert">{error || parameterError(rows)}</p>}
    <Button type="primary" size="small" className="parameter-preview-button" onClick={() => { setPreviewOpen(true); void loadPreview(); }}>调度参数预览</Button>
    <Modal title="调度参数预览" open={previewOpen} width={780} onCancel={() => setPreviewOpen(false)} footer={<Button onClick={() => setPreviewOpen(false)}>关闭</Button>}>
      {releasedObject && <Radio.Group disabled={busy} value={previewMode} onChange={event=>setPreviewMode(event.target.value)} options={[{value:"release",label:`执行版本 R${releaseNo}`},{value:"development",label:"当前开发参数"}]} />}
      <Space wrap className="parameter-preview-controls"><label>业务日期</label><BusinessDateInput label="参数预览业务日期" value={day} onChange={value => { setDay(value); setPreview([]); }} /><label>实例数</label><InputNumber aria-label="参数预览实例数" min={1} max={20} value={count} onChange={value => { setCount(value || 5); setPreview([]); }} /><Button loading={busy} onClick={() => void loadPreview()}>预览</Button></Space>
      <p className="panel-muted">时区：{timeConfig.timezone}</p>
      {previewError && <p className="parameter-error" role="alert">{previewError}</p>}
      <Table size="small" loading={busy} rowKey="scheduledAt" pagination={false} dataSource={preview} scroll={{ x: "max-content" }} columns={[
        { title: "计划时间", dataIndex: "scheduledAt", render: value => new Date(value).toLocaleString("zh-CN", { timeZone: timeConfig.timezone, hour12: false }) },
        { title: "业务日期", dataIndex: "businessDate" },
        ...previewRows.filter((row, index) => previewRows.findIndex(other => other.name === row.name) === index).map(row => ({ title: row.name, key: row.name, render: (_: unknown, item: ParameterPreview) => item.values[row.name] ?? "—" })),
      ]} />
    </Modal>
  </div>;
}
