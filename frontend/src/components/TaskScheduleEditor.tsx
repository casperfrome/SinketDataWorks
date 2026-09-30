import ScheduleParameterEditor from "./ScheduleParameterEditor";
import BusinessDateInput from "./BusinessDateInput";
import { useEffect, useRef, useState } from "react";
import { Alert, App, Button, Empty, Form, Input, InputNumber, Radio, Select, Space, Spin, Switch, Table, Tag } from "antd";
import { api } from "../api";
import type { Run, ScheduleConfig, TaskPreview, TaskRelease, TaskSchedule, TaskScheduleDraft, TaskDependency, StudioObject } from "../types";
import { cycleCron, cronFields, defaultSchedule, yesterday, reasonNames, type CycleFields } from "../state/schedules";
import "../scheduling.css";

const sections = ["调度参数", "调度策略", "调度时间", "调度依赖"];
const initialFields: CycleFields = { cycle: "DAY", interval: 1, time: "02:00", weekdays: ["MON"], monthdays: [1] };
export interface TaskScheduleEditorProps {
  onObjectChange: (patch: Partial<StudioObject>) => void; onSaveObject: () => Promise<boolean>;
  object: StudioObject; objects: StudioObject[]; draft?: TaskScheduleDraft;
  onDraftChange?: (draft?: TaskScheduleDraft) => void;
  onSaved?: () => void; onRun?: (run: Run) => void;
  onPublish: () => Promise<TaskRelease | undefined>;
}
export default function TaskScheduleEditor({ object, objects, onObjectChange, onSaveObject, draft, onDraftChange, onSaved, onRun, onPublish }: TaskScheduleEditorProps) {
  const workspaceId=object.workspaceId, taskId=object.id;
  const [dependencies,setDependencies]=useState<TaskDependency[]>([]);
  const [plans,setPlans]=useState<TaskSchedule[]>([]);
  const baseline=useRef(""); const draftCallback=useRef(onDraftChange);draftCallback.current=onDraftChange;
  const toInput=(c:ScheduleConfig,r:string,d:TaskDependency[],v?:number)=>({ ...Object.fromEntries(Object.keys(defaultSchedule).map(k=>[k,c[k as keyof ScheduleConfig]])) as unknown as ScheduleConfig,cron:cronFields(c.cron).cycle === "CUSTOM" ? c.cron : cycleCron(cronFields(c.cron)),taskId,releaseId:r,dependencies:d,expectedVersion:v });
  const { message } = App.useApp();
  const [config, setConfig] = useState<ScheduleConfig>({ ...defaultSchedule });
  const [fields, setFields] = useState<CycleFields>({ ...initialFields });
  const [saved, setSaved] = useState<TaskSchedule | null>(null), [releases, setReleases] = useState<TaskRelease[]>([]), [releaseId, setReleaseId] = useState("");
  const [preview, setPreview] = useState<TaskPreview[]>([]), [error, setError] = useState(""), [loading, setLoading] = useState(true), [busy, setBusy] = useState(false), [revision, setRevision] = useState(0);
  const [day, setDay] = useState(yesterday), [section, setSection] = useState(0);
  const refs = useRef<Record<number, HTMLElement | null>>({});
  useEffect(() => {
    let alive = true; setLoading(true); setError(""); setPreview([]);
    void Promise.all([api.taskSchedules(workspaceId), api.taskReleases(taskId)]).then(([all, versions]) => {
      if (!alive) return; const existing=all.find(p=>p.taskId===taskId)||null;setPlans(all);setSaved(existing);setReleases(versions);
      const base=toInput(existing||defaultSchedule,existing?.releaseId||"",existing?.dependencies||[],existing?.version);
      baseline.current=JSON.stringify(base);
      const localTime=Object.fromEntries(Object.keys(defaultSchedule).filter(k=>object.config.schedule?.[k]!==undefined).map(k=>[k,object.config.schedule[k]]));const initial=draft?.input||{...base,...localTime};setReleaseId(initial.releaseId);setDependencies(initial.dependencies);setConfig(initial);setFields(cronFields(initial.cron));
    }).catch(e=>{if(alive)setError(e.message);}).finally(()=>{if(alive)setLoading(false);});
    return ()=>{alive=false;};
  },[workspaceId,taskId,revision]);
  useEffect(()=>{if(loading||!baseline.current)return;const input=toInput(config,releaseId,dependencies,saved?.version);draftCallback.current?.(JSON.stringify(input)===baseline.current?undefined:{input,id:saved?.id});},[config,releaseId,dependencies,saved,loading]);
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
    const upcoming = await api.previewTaskSchedule(toInput(config,releaseId,dependencies,saved?.version)); setPreview(upcoming);
    const result = await api.saveTaskSchedule(toInput(config,releaseId,dependencies,saved?.version), saved?.id);
    baseline.current=JSON.stringify(toInput(result,result.releaseId,result.dependencies,result.version));setConfig(result);setSaved(result);draftCallback.current?.(undefined); message.success(result.enabled ? "调度配置已应用，等待下一个执行时点" : "调度配置已保存，当前暂停"); onSaved?.();
  });
  if (loading) return <div className="schedule-loading"><Spin /></div>;
  return <div className="schedule-editor">
    <div className="schedule-context"><div><strong>{object.name}</strong><div className="panel-muted">本任务 · 独立调度与提交</div></div><Tag color={saved?.enabled ? "green" : "default"}>{saved?.enabled ? "已启用" : "未启用"}</Tag><Button size="small" onClick={() => {if(draft){message.warning("请先保存或关闭并丢弃未保存配置");return;}setRevision(r => r + 1);}} disabled={busy}>重新加载</Button></div>
    <nav className="schedule-section-tabs" aria-label="调度配置分组">{sections.map((label, index) => <button key={label} type="button" className={section === index ? "selected" : ""} onClick={() => { setSection(index); refs.current[index]?.scrollIntoView({ behavior: "smooth", block: "start" }); }}>{label}</button>)}</nav>
    {error && <Alert type="error" showIcon title={error} />}
    {object.config.taskSqlMigration && <Alert type="warning" title={object.config.taskSqlMigration.message} description={<details><summary>查看独立任务 SQL 参考</summary><pre className="migration-sql">{object.config.taskSqlMigration.suggestedSql}</pre></details>} />}
    <Space className="task-release-actions"><Button disabled={busy} onClick={()=>void act(async()=>{const r=await onPublish();if(!r)return;setReleases(await api.taskReleases(taskId));setReleaseId(r.id);message.success(`已发布任务 R${r.releaseNo}，保存配置后应用`);})}>发布当前任务</Button><Button onClick={()=>void act(async()=>setReleases(await api.taskReleases(taskId)))}>刷新版本</Button></Space>
    <Form disabled={busy} layout="horizontal" labelCol={{ flex: "112px" }} wrapperCol={{ flex: 1 }} size="small" colon={false}>
        <Form.Item label="执行版本"><Select aria-label="调度执行版本" value={releaseId || undefined} placeholder="选择已发布版本" options={releases.map(r => ({ value: r.id, label: `R${r.releaseNo} · ${r.note || r.name}` }))} onChange={setReleaseId} /></Form.Item>
      <section ref={el => { refs.current[0] = el; }} className="schedule-section"><ScheduleParameterEditor object={object} onChange={onObjectChange} config={config} /></section>
      <section ref={el => { refs.current[1] = el; }} className="schedule-section"><h3>调度策略</h3>
        <Form.Item label="调度类型"><Radio.Group value={config.enabled} onChange={e => patch({ enabled: e.target.value })}><Radio value={true}>正常调度</Radio><Radio value={false}>暂停调度</Radio></Radio.Group></Form.Item>
        <Form.Item label="失败自动重跑"><Switch aria-label="失败自动重跑" checked={config.retries > 0} onChange={v => patch({ retries: v ? 1 : 0 })} /></Form.Item>
        {config.retries > 0 && <><Form.Item label="重跑次数"><InputNumber min={1} max={10} value={config.retries} onChange={n => patch({ retries: n || 1 })} /></Form.Item><Form.Item label="重跑间隔（分钟）"><InputNumber min={1} max={30} value={config.retryIntervalSeconds / 60} onChange={n => patch({ retryIntervalSeconds: (n || 1) * 60 })} /></Form.Item></>}
        <p className="panel-muted">临时连接、死锁或资源不足可重跑；超时、代码错误、停止和服务重启不自动重跑。重跑沿用原日期和数据截止时间。</p>
        <div className="schedule-policy"><span>同库落表并行数</span><strong>1</strong><span>本任务上一轮未完成</span><strong>跳过并记录</strong><span>停机期间漏跑</span><strong>跳过，可手动重算</strong></div>
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
        <Space wrap><Button disabled={busy} onClick={() => { setFields({ ...initialFields, cycle: "MINUTE" }); patch({ cycle: "MINUTE", cron: "0 * * * * *" }); }}>每分钟演示</Button><Button loading={busy} onClick={() => void act(async () => setPreview(await api.previewTaskSchedule(toInput(validatedConfig(),releaseId,dependencies,saved?.version))))}>预览执行时间</Button></Space>
        {!!preview.length && <Table className="schedule-preview" size="small" rowKey="scheduledAt" pagination={false} dataSource={preview} columns={[{ title: "未来执行时间", dataIndex: "scheduledAt", render: (v: string) => new Date(v).toLocaleString("zh-CN", { timeZone: config.timezone, hour12: false }) }, { title: "业务日期", dataIndex: "businessDate" }, {title:"对应上游",render:(_,r)=><div>{r.dependencies.length?r.dependencies.map(d=><div key={d.taskId}><b>{d.name}</b><div>{d.scheduledAt?new Date(d.scheduledAt).toLocaleString("zh-CN",{hour12:false}):reasonNames[d.reason||""]||"无匹配"}{d.warning&&" · 已暂停"}</div></div>):"无上游"}</div>}]} />}
      </section>
      <section ref={el=>{refs.current[3]=el;}} className="schedule-section"><h3>调度依赖</h3>
        <p>等待所有上游对应期次成功，再运行当前任务。</p>
        <Select<string> aria-label="添加上游任务" showSearch optionFilterProp="label" value={undefined} placeholder="搜索并添加上游任务" style={{width:"100%"}} options={objects.filter(o=>o.kind==="NODE"&&["MYSQL","SYNC"].includes(o.config.run?.provider)&&o.id!==taskId&&!dependencies.some(d=>d.taskId===o.id)).map(o=>({value:o.id,label:o.name}))} onChange={id=>{const o=objects.find(x=>x.id===id)!;const used=new Set(dependencies.map(d=>d.alias));let alias=o.name.split("_")[0].replace(/[^A-Za-z0-9_]/g,"")||"source";if(!/^[A-Za-z]/.test(alias))alias="source";const base=alias;let i=2;while(used.has(alias))alias=base+i++;setDependencies([...dependencies,{taskId:id,name:o.name,alias}]);setPreview([]);}} />
        {dependencies.map((d,index)=><div className="task-dependency-row" key={d.taskId}><div className="task-dependency-heading"><strong>{objects.find(o=>o.id===d.taskId)?.name||d.name||d.taskId}</strong><Button type="text" size="small" danger onClick={()=>{setDependencies(dependencies.filter(x=>x.taskId!==d.taskId));setPreview([]);}}>移除</Button></div><p className="panel-muted">{plans.find(p=>p.taskId===d.taskId)?.cron||"尚未配置调度"}</p><Form.Item label="参数别名"><Input aria-label={`上游参数别名 ${index+1}`} value={d.alias} maxLength={32} onChange={e=>{setDependencies(dependencies.map(x=>x.taskId===d.taskId?{...x,alias:e.target.value}:x));setPreview([]);}} /></Form.Item><code>:upstream_{d.alias}_build_id</code></div>)}
        {!dependencies.length&&<Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="无上游依赖，到达计划时间即可运行" />}
        <p className="panel-muted">匹配同一业务日期、截至本任务计划时间最近一期的上游。上游失败、漏跑或取消会阻断本期；不会改用更早的成功结果。可配置不同执行频率。</p>
      </section>
    </Form>
    <div className="schedule-save"><Button type="primary" loading={busy} disabled={config.enabled && !releaseId} onClick={() => void save()}>{config.enabled ? "应用到调度" : "保存配置"}</Button><span className="panel-muted">{saved?.nextFireAt ? `下次：${new Date(saved.nextFireAt).toLocaleString("zh-CN", { timeZone: config.timezone, hour12: false })}` : "启用后从下一个时点执行"}</span></div>
    <section className="schedule-manual"><h3>指定日期重新计算</h3><Space wrap><BusinessDateInput label="重新计算业务日期" value={day} onChange={setDay} /><Button disabled={!releaseId || !day} loading={busy} onClick={() => void act(async () => { const run = await api.runTaskRelease(releaseId, day); message.success("已提交指定日期运行，使用当前数据截止时间"); onRun?.(run); })}>运行所选版本</Button></Space></section>
  </div>;
}
