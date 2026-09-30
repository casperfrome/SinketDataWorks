import ScheduleParameterEditor from "./ScheduleParameterEditor";
import BusinessDateInput from "./BusinessDateInput";
import { useEffect, useRef, useState } from "react";
import { Alert, App, Button, Empty, Form, Input, InputNumber, Radio, Select, Space, Spin, Switch, Table, Tag } from "antd";
import { api } from "../api";
import type { StudioObject, Run, ScheduleConfig, SchedulePreview, WorkflowRelease, WorkflowSchedule } from "../types";
import { cycleCron, cronFields, defaultSchedule, yesterday, type CycleFields } from "../state/schedules";
import "../scheduling.css";

const sections = ["调度参数", "调度策略", "调度时间", "调度依赖"];
const initialFields: CycleFields = { cycle: "DAY", interval: 1, time: "02:00", weekdays: ["MON"], monthdays: [1] };
export default function ScheduleEditor({ object, onObjectChange, onSaveObject, onSaved, onRun }: { object: StudioObject; onObjectChange: (patch: Partial<StudioObject>) => void; onSaveObject: () => Promise<boolean>; onSaved?: () => void; onRun?: (run: Run) => void }) {
  const workspaceId=object.workspaceId, workflowId=object.id;
  const { message } = App.useApp();
  const [config, setConfig] = useState<ScheduleConfig>({ ...defaultSchedule });
  const [fields, setFields] = useState<CycleFields>({ ...initialFields });
  const [saved, setSaved] = useState<WorkflowSchedule | null>(null), [releases, setReleases] = useState<WorkflowRelease[]>([]), [releaseId, setReleaseId] = useState("");
  const [preview, setPreview] = useState<SchedulePreview[]>([]), [error, setError] = useState(""), [loading, setLoading] = useState(true), [busy, setBusy] = useState(false), [revision, setRevision] = useState(0);
  const [day, setDay] = useState(yesterday), [section, setSection] = useState(0);
  const refs = useRef<Record<number, HTMLElement | null>>({});
  useEffect(() => {
    let alive = true; setLoading(true); setError(""); setPreview([]);
    void Promise.all([api.schedules(workspaceId, workflowId), api.workflowReleases(workspaceId, workflowId)]).then(([plans, versions]) => {
      if (!alive) return; const existing = plans[0] || null; setSaved(existing); setReleases(versions); setReleaseId(existing?.releaseId || versions[0]?.id || "");
      const localTime=Object.fromEntries(Object.keys(defaultSchedule).filter(k=>object.config.schedule?.[k]!==undefined).map(k=>[k,object.config.schedule[k]]));const initial={...defaultSchedule,...existing,...localTime};setConfig(initial);setFields(cronFields(initial.cron));
    }).catch(e => { if (alive) setError(e.message); }).finally(() => { if (alive) setLoading(false); });
    return () => { alive = false; };
  }, [workspaceId, workflowId, revision]);
  const patch = (value: Partial<ScheduleConfig>) => { setConfig(c => ({ ...c, ...value })); onObjectChange({config:{...object.config,schedule:{...object.config.schedule,...Object.fromEntries(Object.keys(defaultSchedule).map(k=>[k,config[k as keyof ScheduleConfig]])),...value}}}); setPreview([]); };
  const changeCycle = (value: Partial<CycleFields>) => {
    const next = { ...fields, ...value }; setFields(next); setError("");
    if (next.cycle === "CUSTOM") { patch({ cycle: "CUSTOM" }); return; }
    try { patch({ cycle: next.cycle, cron: cycleCron(next) }); } catch (e) { setError((e as Error).message); }
  };
  const act = async (action: () => Promise<void>) => { setBusy(true); setError(""); try { await action(); } catch (e) { setError((e as Error).message); } finally { setBusy(false); } };
  const validatedConfig = () => fields.cycle === "CUSTOM" ? config : {...config, cron:cycleCron(fields)};
  const save = () => act(async () => {
    if (!await onSaveObject()) return;
    const config = validatedConfig();
    const upcoming = await api.previewSchedule(config); setPreview(upcoming);
    const result = await api.saveSchedule(workflowId, { ...config, releaseId, expectedVersion: saved?.version }, saved?.id);
    setSaved(result); message.success(result.enabled ? "调度配置已应用，等待下一个执行时点" : "调度配置已保存，当前暂停"); onSaved?.();
  });
  if (loading) return <div className="schedule-loading"><Spin /></div>;
  return <div className="schedule-editor">
    <div className="schedule-context"><span>运行已发布版本</span><Tag color={saved?.enabled ? "green" : "default"}>{saved?.enabled ? "已启用" : "未启用"}</Tag><Button size="small" onClick={() => setRevision(r => r + 1)} disabled={busy}>重新加载</Button></div>
    <nav className="schedule-section-tabs" aria-label="调度配置分组">{sections.map((label, index) => <button key={label} type="button" className={section === index ? "selected" : ""} onClick={() => { setSection(index); refs.current[index]?.scrollIntoView({ behavior: "smooth", block: "start" }); }}>{label}</button>)}</nav>
    {error && <Alert type="error" showIcon title={error} />}
    {!releases.length && <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="先发布此工作流，再选择执行版本并配置调度" />}
    <Form layout="horizontal" labelCol={{ flex: "112px" }} wrapperCol={{ flex: 1 }} size="small" colon={false}>
        <Form.Item label="执行版本" required><Select aria-label="调度执行版本" value={releaseId || undefined} placeholder="选择已发布版本" options={releases.map(r => ({ value: r.id, label: `R${r.releaseNo} · ${r.note || r.name}` }))} onChange={setReleaseId} /></Form.Item>
      <section ref={el => { refs.current[0] = el; }} className="schedule-section"><ScheduleParameterEditor object={object} onChange={onObjectChange} config={config} /></section>
      <section ref={el => { refs.current[1] = el; }} className="schedule-section"><h3>调度策略</h3>
        <Form.Item label="调度类型"><Radio.Group value={config.enabled} onChange={e => patch({ enabled: e.target.value })}><Radio value={true}>正常调度</Radio><Radio value={false}>暂停调度</Radio></Radio.Group></Form.Item>
        <Form.Item label="失败自动重跑"><Switch aria-label="失败自动重跑" checked={config.retries > 0} onChange={v => patch({ retries: v ? 1 : 0 })} /></Form.Item>
        {config.retries > 0 && <><Form.Item label="重跑次数"><InputNumber min={1} max={10} value={config.retries} onChange={n => patch({ retries: n || 1 })} /></Form.Item><Form.Item label="重跑间隔（分钟）"><InputNumber min={1} max={30} value={config.retryIntervalSeconds / 60} onChange={n => patch({ retryIntervalSeconds: (n || 1) * 60 })} /></Form.Item></>}
        <p className="panel-muted">临时连接、死锁或资源不足可重跑；超时、代码错误、停止和服务重启不自动重跑。重跑沿用原日期和数据截止时间。</p>
        <div className="schedule-policy"><span>同库落表并行数</span><strong>1</strong><span>上一轮未完成</span><strong>跳过并记录</strong><span>停机期间漏跑</span><strong>跳过，可手动重算</strong></div>
      </section>
      <section ref={el => { refs.current[2] = el; }} className="schedule-section"><h3>调度时间</h3>
        <Form.Item label="业务日期" extra="相对于计划执行日期的天数。-1 表示处理前一天。"><InputNumber aria-label="业务日期偏移" value={config.businessDateOffset} min={-365} max={0} onChange={n => patch({ businessDateOffset: n ?? -1 })} /></Form.Item>
        <Form.Item label="调度时区"><Select showSearch aria-label="调度时区" value={config.timezone} options={["Asia/Shanghai", "UTC", "Asia/Tokyo", "Europe/London", "America/New_York"].map(value => ({ value, label: value }))} onChange={timezone => patch({ timezone })} /></Form.Item>
        <Form.Item label="调度周期"><Select aria-label="调度周期" value={fields.cycle} options={[{ value: "MINUTE", label: "分钟" }, { value: "HOUR", label: "小时" }, { value: "DAY", label: "日" }, { value: "WEEK", label: "周" }, { value: "MONTH", label: "月" }, { value: "CUSTOM", label: "高级 Cron" }]} onChange={cycle => changeCycle({ cycle, interval: 1 })} /></Form.Item>
        {["MINUTE", "HOUR"].includes(fields.cycle) && <Form.Item label="间隔" extra={fields.cycle === "MINUTE" ? "从每小时第 0 分钟开始，按间隔取执行时点。" : "从每日第 0 小时开始，按间隔取执行时点。"}><InputNumber aria-label="调度间隔" value={fields.interval} min={1} max={fields.cycle === "MINUTE" ? 59 : 23} onChange={n => changeCycle({ interval: n || 1 })} /></Form.Item>}
        {!["MINUTE", "CUSTOM"].includes(fields.cycle) && <Form.Item label={fields.cycle === "HOUR" ? "分钟位置" : "调度时间"}><Input type="time" aria-label="调度时间" value={fields.time} onChange={e => changeCycle({ time: e.target.value })} /></Form.Item>}
        {fields.cycle === "WEEK" && <Form.Item label="执行日"><Select mode="multiple" value={fields.weekdays} options={["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"].map((value, i) => ({ value, label: `周${"一二三四五六日"[i]}` }))} onChange={weekdays => changeCycle({ weekdays })} /></Form.Item>}
        {fields.cycle === "MONTH" && <Form.Item label="执行日" extra="某月不存在的日期自动跳过。"><Select mode="multiple" value={fields.monthdays} options={Array.from({ length: 31 }, (_, i) => ({ value: i + 1, label: `${i + 1} 日` }))} onChange={monthdays => changeCycle({ monthdays })} /></Form.Item>}
        <Form.Item label="生效日期"><BusinessDateInput label="调度生效日期" value={config.startDate} onChange={startDate => patch({startDate})} /></Form.Item>
        <Form.Item label="结束日期" extra="留空表示永久生效。"><BusinessDateInput label="调度结束日期" min={config.startDate} value={config.endDate} onChange={endDate => patch({endDate})} /></Form.Item>
        <Form.Item label="Cron 表达式" extra="秒 分 时 日 月 周；秒固定为 0。"><Input aria-label="Cron 表达式" className="code-input" value={config.cron} readOnly={fields.cycle !== "CUSTOM"} onChange={e => patch({ cron: e.target.value })} /></Form.Item>
        <Space wrap><Button disabled={busy} onClick={() => { setFields({ ...initialFields, cycle: "MINUTE" }); patch({ cycle: "MINUTE", cron: "0 * * * * *" }); }}>每分钟演示</Button><Button loading={busy} onClick={() => void act(async () => setPreview(await api.previewSchedule(validatedConfig())))}>预览执行时间</Button></Space>
        {!!preview.length && <Table className="schedule-preview" size="small" rowKey="scheduledAt" pagination={false} dataSource={preview} columns={[{ title: "未来执行时间", dataIndex: "scheduledAt", render: (v: string) => new Date(v).toLocaleString("zh-CN", { timeZone: config.timezone, hour12: false }) }, { title: "业务日期", dataIndex: "businessDate" }]} />}
      </section>
      <section ref={el => { refs.current[3] = el; }} className="schedule-section"><h3>调度依赖</h3><p>内部节点按工作流画布连线执行。库存示例流程：</p><div className="inventory-pipeline"><span>ODS 源表</span><b>→</b><span>DWD 日账</span><b>→</b><span>DWS 仓汇总</span><b>→</b><span>ADS 宽表</span></div><p className="panel-muted">全部成功后更新正式结果；任一步失败，保留上次成功数据。此版本仅支持工作流内部依赖。</p></section>
    </Form>
    <div className="schedule-save"><Button type="primary" loading={busy} disabled={!releaseId} onClick={() => void save()}>应用到调度</Button><span className="panel-muted">{saved?.nextFireAt ? `下次：${new Date(saved.nextFireAt).toLocaleString("zh-CN", { timeZone: config.timezone, hour12: false })}` : "启用后从下一个时点执行"}</span></div>
    <section className="schedule-manual"><h3>指定日期重新计算</h3><Space wrap><BusinessDateInput label="重新计算业务日期" value={day} onChange={setDay} /><Button disabled={!releaseId || !day} loading={busy} onClick={() => void act(async () => { const run = await api.runRelease(releaseId, day); message.success("已提交指定日期运行，使用当前数据截止时间"); onRun?.(run); })}>运行所选版本</Button></Space></section>
  </div>;
}
