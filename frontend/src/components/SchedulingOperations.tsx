import { useEffect, useRef, useState } from "react";
import { Alert, App, Button, Checkbox, Descriptions, Drawer, Empty, Input, Modal, Select, Space, Table, Tabs, Tag } from "antd";
import { api } from "../api";
import type { BackfillInput, BackfillPreview, DependencySlot, SchedulingInstance, SchedulingKind, SchedulingTask } from "../types";
import { yesterday, triggerNames, reasonNames, slotStatusNames } from "../state/schedules";
import { statusNames } from "../state/workflows";
import { activeInstanceStatuses, dependencySummary, instanceSourceNames, taskStateNames, validBackfillRange } from "../state/schedulingOperations";
import BusinessDateInput from "./BusinessDateInput";
import RunDetails from "./RunDetails";
import RunParameters from "./RunParameters";
import "../scheduling.css";

const statusColor = (status: string) => status === "SUCCESS" ? "green" : ["FAILED", "BLOCKED"].includes(status) ? "error" : activeInstanceStatuses.has(status) ? "processing" : "default";
const timeLabel = (value?: string, timezone = "Asia/Shanghai") => value ? new Date(value).toLocaleString("zh-CN", { timeZone: timezone, hour12: false }) : "—";
const statusOptions = Object.entries(triggerNames).map(([value, label]) => ({ value, label }));
const kindOptions = [{ value: "TASK", label: "独立任务" }, { value: "WORKFLOW", label: "工作流" }];
const statusLabel = (status = "") => triggerNames[status] || slotStatusNames[status] || (statusNames as Record<string,string>)[status] || status || "待满足";

function DependencyDetails({ slots = [], timezone, onInspect }: { slots?: DependencySlot[]; timezone?: string; onInspect?: (id: string) => void }) {
  if (!slots.length) return <p className="panel-muted">无上游依赖。</p>;
  return <div className="operations-dependencies">{dependencySummary(slots).map(group => <section key={group.key}>
    <strong>{group.name}</strong><Tag>{group.mode === "ALL_DAY" ? "同日全部期次" : "计划时间前最近一期"}</Tag><span>已成功 {group.slots.filter(slot=>slot.status==="SUCCESS").length} / {group.count} 期</span>
    <Table<DependencySlot> size="small" rowKey={(slot,index)=>`${slot.triggerId||slot.scheduledAt||"missing"}:${index}`} pagination={group.slots.length>20?{pageSize:20,showSizeChanger:false}:false} dataSource={[...group.slots].sort((a,b)=>Number(a.status==="SUCCESS")-Number(b.status==="SUCCESS")||Number(!a.reason)-Number(!b.reason))} columns={[
      {title:"上游计划时间",render:(_,slot)=>timeLabel(slot.scheduledAt,slot.timezone||timezone)},
      {title:"状态",render:(_,slot)=><Tag color={statusColor(slot.status||"")}>{statusLabel(slot.status)}</Tag>},
      {title:"说明",render:(_,slot)=>reasonNames[slot.reason||""]||slot.reason||(slot.warning?"上游已暂停":"—")},
      ...(onInspect?[{title:"操作",render:(_:unknown,slot:DependencySlot)=><Button type="link" size="small" disabled={!slot.triggerId} onClick={()=>onInspect(slot.triggerId!)}>查看上游</Button>}]:[]),
    ]} />
  </section>)}</div>;
}

export interface SchedulingSelection { kind: SchedulingKind; scheduleId: string }
export default function SchedulingOperations({ workspaceId, selection, onConfigure }: { workspaceId: string; selection?: SchedulingSelection; onConfigure: (objectId: string) => void }) {
  const { message } = App.useApp();
  const [tab, setTab] = useState("tasks"), [search, setSearch] = useState(""), [kind, setKind] = useState("");
  const [state, setState] = useState(""), [instanceStatus, setInstanceStatus] = useState(""), [scheduleId, setScheduleId] = useState("");
  const [dateFrom, setDateFrom] = useState(""), [dateTo, setDateTo] = useState(""), [source, setSource] = useState(""), [batchKey, setBatchKey] = useState("");
  const [tasks, setTasks] = useState<SchedulingTask[]>([]), [instances, setInstances] = useState<SchedulingInstance[]>([]);
  const [taskTotal, setTaskTotal] = useState(0), [instanceTotal, setInstanceTotal] = useState(0), [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true), [busy, setBusy] = useState(false), [error, setError] = useState(""), [revision, setRevision] = useState(0);
  const [selected, setSelected] = useState<{ kind: SchedulingKind; id: string }>(), [detail, setDetail] = useState<SchedulingInstance>();
  const [attemptRunId, setAttemptRunId] = useState<string>(), [runTab, setRunTab] = useState("logs");
  const [backfillTask, setBackfillTask] = useState<SchedulingTask>(), [preview, setPreview] = useState<BackfillPreview>();
  const [startDate, setStartDate] = useState(yesterday), [endDate, setEndDate] = useState(yesterday);
  const [startTime, setStartTime] = useState("00:00"), [endTime, setEndTime] = useState("23:59"), [downstream, setDownstream] = useState(false);
  const [backfillError, setBackfillError] = useState("");
  const requestId = useRef("");
  const refresh = () => setRevision(value => value + 1);
  const resetPage = (change: () => void) => { change(); setPage(1); };
  useEffect(() => { setPage(1); setSearch(""); setKind(""); setScheduleId(""); setBatchKey(""); setSelected(undefined); setBackfillTask(undefined); }, [workspaceId]);
  useEffect(() => { if (selection) { setTab("instances"); setKind(selection.kind); setScheduleId(selection.scheduleId); setPage(1); setBatchKey(""); } }, [selection]);

  useEffect(() => {
    let alive = true; let requestPending = false;
    setLoading(true); setError("");
    const load = async () => {
      if (requestPending || document.visibilityState !== "visible") return;
      requestPending = true;
      try {
        if (tab === "tasks") {
          const result = await api.schedulingTasks(workspaceId, { page: String(page), search, status: state, kind });
          if (alive) { setTasks(result.items); setTaskTotal(result.total); }
        } else {
          const result = await api.schedulingInstances(workspaceId, { page: String(page), search, status: instanceStatus, kind, scheduleId, businessDateFrom: dateFrom, businessDateTo: dateTo, source, batchKey });
          if (alive) { setInstances(result.items); setInstanceTotal(result.total); }
        }
        if (alive) setError("");
      } catch (e) { if (alive) setError((e as Error).message); }
      finally { requestPending = false; if (alive) setLoading(false); }
    };
    void load();
    const timer = window.setInterval(() => void load(), 2000);
    const visible = () => { if (document.visibilityState === "visible") void load(); };
    document.addEventListener("visibilitychange", visible);
    return () => { alive = false; clearInterval(timer); document.removeEventListener("visibilitychange", visible); };
  }, [workspaceId, tab, page, search, state, instanceStatus, kind, scheduleId, dateFrom, dateTo, source, batchKey, revision]);

  useEffect(() => {
    if (!selected) { setDetail(undefined); return; }
    let alive = true; let pending = false;
    setDetail(undefined); setAttemptRunId(undefined);
    const load = async () => {
      if (pending || document.visibilityState !== "visible") return;
      pending = true;
      try { const result = await api.schedulingInstance(selected.kind, selected.id); if (alive) setDetail(result); }
      catch (e) { if (alive) setError((e as Error).message); }
      finally { pending = false; }
    };
    void load(); const timer = window.setInterval(() => void load(), 2000);
    const visible = () => { if (document.visibilityState === "visible") void load(); };
    document.addEventListener("visibilitychange", visible);
    return () => { alive = false; clearInterval(timer); document.removeEventListener("visibilitychange", visible); };
  }, [selected, revision]);

  const act = async (action: () => Promise<void>) => { setBusy(true); try { await action(); refresh(); } catch (e) { message.error((e as Error).message); } finally { setBusy(false); } };
  const viewInstances = (task: SchedulingTask) => { setTab("instances"); setKind(task.kind); setScheduleId(task.scheduleId || ""); setPage(1); setBatchKey(""); };
  const rerun = (instance: SchedulingInstance, mode: "ORIGINAL" | "LATEST" | "ATTEMPT", runId?: string) => void act(async () => {
    await api.rerunSchedulingInstance(instance.kind, instance.id, mode, runId);
    message.success(mode === "LATEST" ? "已按最新发布版本重跑此期次" : "已按原版本和输入重跑此期次");
  });
  const openBackfill = (task: SchedulingTask) => { setBackfillTask(task); setPreview(undefined); setBackfillError(""); setStartDate(yesterday()); setEndDate(yesterday()); setStartTime("00:00"); setEndTime("23:59"); setDownstream(false); requestId.current = crypto.randomUUID(); };
  const changeBackfill = (change: () => void) => { change(); setPreview(undefined); setBackfillError(""); requestId.current = crypto.randomUUID(); };
  const backfillInput = (): BackfillInput => ({ workspaceId, kind: backfillTask!.kind, scheduleId: backfillTask!.scheduleId!, startDate, endDate, startTime, endTime, includeDownstream: downstream });
  const previewBackfill = async () => {
    const invalid = validBackfillRange(startDate, endDate, startTime, endTime); if (invalid) { setBackfillError(invalid); return; }
    setBusy(true); setBackfillError(""); setPreview(undefined);
    try { setPreview(await api.previewBackfill(backfillInput())); } catch (e) { setBackfillError((e as Error).message); } finally { setBusy(false); }
  };
  const submitBackfill = async () => {
    if (!preview) return;
    setBusy(true); setBackfillError("");
    try {
      const result = await api.submitBackfill({ ...backfillInput(), previewToken: preview.token, requestId: requestId.current });
      setBackfillTask(undefined); setTab("instances"); setPage(1); setKind(""); setScheduleId(""); setInstanceStatus(""); setSearch(""); setDateFrom(""); setDateTo(""); setSource("BACKFILL"); setBatchKey(result.batchKey);
      message.success(`已提交 ${result.total} 个补数实例`); refresh();
    } catch (e) { setBackfillError((e as Error).message); } finally { setBusy(false); }
  };

  return <div className="scheduling-operations">
    <div className="operations-heading"><div><h2>调度运维</h2><p>查看任务计划，按业务日期追踪每一期执行。</p></div><Button onClick={refresh}>刷新</Button></div>
    {error && <Alert type="error" showIcon title={error} />}
    <Tabs activeKey={tab} onChange={value => resetPage(() => { setTab(value); setSearch(""); })} items={[{ key: "tasks", label: "任务" }, { key: "instances", label: "实例" }]} />
    <Space wrap className="operations-filters">
      <Input.Search aria-label="搜索调度任务" placeholder="搜索任务名称" value={search} onChange={event => resetPage(() => setSearch(event.target.value))} allowClear style={{ width: 220 }} />
      <Select aria-label="调度类型筛选" placeholder="全部类型" value={kind || undefined} allowClear options={kindOptions} onChange={value => resetPage(() => setKind(value || ""))} style={{ width: 130 }} />
      {tab === "tasks" ? <Select aria-label="任务状态筛选" placeholder="全部任务状态" value={state || undefined} allowClear options={Object.entries(taskStateNames).map(([value, label]) => ({ value, label }))} onChange={value => resetPage(() => setState(value || ""))} style={{ width: 150 }} /> : <>
        <Select aria-label="实例状态筛选" placeholder="全部实例状态" value={instanceStatus || undefined} allowClear options={statusOptions} onChange={value => resetPage(() => setInstanceStatus(value || ""))} style={{ width: 145 }} />
        <BusinessDateInput label="实例开始业务日期" value={dateFrom} onChange={value => resetPage(() => setDateFrom(value))} /><span>至</span><BusinessDateInput label="实例结束业务日期" value={dateTo} min={dateFrom} onChange={value => resetPage(() => setDateTo(value))} />
        <Select aria-label="实例来源筛选" placeholder="全部来源" value={source || undefined} allowClear options={Object.entries(instanceSourceNames).map(([value, label]) => ({ value, label }))} onChange={value => resetPage(() => setSource(value || ""))} style={{ width: 120 }} />
        {scheduleId && <Tag closable onClose={() => resetPage(() => setScheduleId(""))}>当前任务</Tag>}
        {batchKey && <Tag closable onClose={() => resetPage(() => setBatchKey(""))}>本批补数</Tag>}
      </>}
      <span className="operations-live">每 2 秒更新</span>
    </Space>
    {tab === "tasks" ? <Table<SchedulingTask> size="small" rowKey={row => `${row.kind}:${row.objectId}`} loading={loading} dataSource={tasks} scroll={{ x: 1000 }} pagination={{ current: page, pageSize: 20, total: taskTotal, showSizeChanger: false, onChange: setPage }} locale={{ emptyText: <Empty description="暂无可调度任务，在离线数据开发中创建 SQL、同步任务或工作流。" /> }} columns={[
      { title: "任务", dataIndex: "name", render: (name, task) => <Button type="link" onClick={() => onConfigure(task.objectId)}>{name}</Button> },
      { title: "类型", render: (_, task) => task.kind === "WORKFLOW" ? "工作流" : task.nodeType || "独立任务" },
      { title: "计划状态", render: (_, task) => <Tag color={task.enabled ? "green" : "default"}>{taskStateNames[task.status] || task.status}</Tag> },
      { title: "执行版本", render: (_, task) => <><span>{task.releaseNo ? `R${task.releaseNo}` : "—"}</span>{task.latestReleaseNo && task.latestReleaseNo !== task.releaseNo && <div className="operations-note">新版本 R{task.latestReleaseNo} 未应用</div>}</> },
      { title: "调度时间", render: (_, task) => <><code>{task.cron || "未配置"}</code><div className="operations-note">{task.timezone}</div></> },
      { title: "下次执行", render: (_, task) => timeLabel(task.nextFireAt || undefined, task.timezone) },
      { title: "最近一期", render: (_, task) => <><Tag color={statusColor(task.latestStatus || "")}>{triggerNames[task.latestStatus || ""] || "—"}</Tag>{task.latestReason && <div className="operations-note">{reasonNames[task.latestReason] || task.latestReason}</div>}</> },
      { title: "操作", fixed: "right", render: (_, task) => <Space wrap size={0}><Button size="small" type="link" disabled={!task.scheduleId} onClick={() => viewInstances(task)}>实例</Button><Button size="small" type="link" disabled={busy || !task.scheduleId || task.version === undefined} onClick={() => void act(async () => { await api.setSchedulingEnabled(task.kind, task.scheduleId!, !task.enabled, task.version!); message.success(task.enabled ? "已暂停新期次和待执行周期实例；正在运行的任务继续执行" : "已启用调度"); })}>{task.enabled ? "暂停" : "启用"}</Button><Button size="small" type="link" disabled={!task.scheduleId || !task.releaseId} onClick={() => openBackfill(task)}>补数</Button></Space> },
    ]} /> : <Table<SchedulingInstance> size="small" rowKey={row => `${row.kind}:${row.id}`} loading={loading} dataSource={instances} scroll={{ x: 1020 }} pagination={{ current: page, pageSize: 20, total: instanceTotal, showSizeChanger: false, onChange: setPage }} locale={{ emptyText: <Empty description="当前筛选条件没有调度实例。启用计划后会生成期次，也可从任务页补数。" /> }} columns={[
      { title: "任务", dataIndex: "name", render: (name, instance) => <Button type="link" onClick={() => { setSelected({ kind: instance.kind, id: instance.id }); setRunTab("logs"); }}>{name}</Button> },
      { title: "业务日期", dataIndex: "businessDate" }, { title: "计划时间", render: (_, instance) => timeLabel(instance.scheduledAt, instance.timezone) },
      { title: "来源", render: (_, instance) => instanceSourceNames[instance.source || "SCHEDULED"] || instance.source },
      { title: "状态", render: (_, instance) => <Tag color={statusColor(instance.status)}>{triggerNames[instance.status] || instance.status}</Tag> },
      { title: "说明", render: (_, instance) => reasonNames[instance.reason || ""] || instance.reason || "—" },
      { title: "版本 / 尝试次数", render: (_, instance) => `R${instance.releaseNo || "—"} / ${instance.attempts?.length || 0}` },
      { title: "操作", fixed: "right", render: (_, instance) => <Space size={0}><Button type="link" size="small" onClick={() => setSelected({ kind: instance.kind, id: instance.id })}>详情</Button><Button type="link" size="small" disabled={busy || activeInstanceStatuses.has(instance.status)} onClick={() => rerun(instance, "LATEST")}>使用最新发布版本重跑</Button></Space> },
    ]} />}
    <Drawer title={detail ? `${detail.name} · ${detail.businessDate}` : "实例详情"} open={!!selected} onClose={() => setSelected(undefined)} size={900} destroyOnHidden>
      {detail && detail.id===selected?.id && detail.kind===selected?.kind ? <>
        <Space wrap><Tag color={statusColor(detail.status)}>{triggerNames[detail.status] || detail.status}</Tag><Tag>{detail.kind === "TASK" ? "独立任务" : "工作流"}</Tag><Tag>{instanceSourceNames[detail.source || "SCHEDULED"]}</Tag>
          <Button disabled={busy || activeInstanceStatuses.has(detail.status)} onClick={() => rerun(detail, "LATEST")}>使用最新发布版本重跑</Button><Button disabled={busy || activeInstanceStatuses.has(detail.status)} onClick={() => rerun(detail, "ORIGINAL")}>原输入复现</Button>
          {(activeInstanceStatuses.has(detail.status)||detail.status==="PAUSED"||detail.status==="BLOCKED") && <Button danger disabled={busy} onClick={() => void act(async () => { await api.stopSchedulingInstance(detail.kind, detail.id); message.success("已提交停止请求"); })}>停止本实例</Button>}
        </Space>
        <p className="panel-muted">使用最新发布版本重跑时，采用当前最新发布代码和对应上游；原输入复现沿用原代码、数据截止时间和已固定输入。</p>
        <Descriptions size="small" column={2} className="operations-instance-context" items={[
          { key: "id", label: "实例 ID", children: detail.id }, { key: "release", label: "执行版本", children: `R${detail.releaseNo || "—"}` },
          { key: "attempts", label: "尝试次数", children: detail.attempts?.length || 0 },
          { key: "date", label: "业务日期", children: detail.businessDate }, { key: "scheduled", label: "计划时间", children: timeLabel(detail.scheduledAt, detail.timezone) },
          { key: "cutoff", label: "数据截止时间", children: timeLabel(detail.sourceCutoffAt, detail.timezone) }, { key: "reason", label: "说明", children: reasonNames[detail.reason || ""] || detail.reason || "—" },
          ...(detail.nextRetryAt ? [{ key: "retry", label: "下次重试", children: timeLabel(detail.nextRetryAt, detail.timezone) }] : []),
          ...(detail.batchKey ? [{ key: "batch", label: "补数批次", children: detail.batchKey }] : []),
        ]} />
        <h4>对应上游期次</h4><DependencyDetails slots={detail.dependencySlots} timezone={detail.timezone} onInspect={id=>setSelected({kind:"TASK",id})} />
        <h4>尝试记录</h4><Table size="small" rowKey="runId" pagination={false} dataSource={detail.attempts || []} locale={{ emptyText: "本实例尚未提交运行，可从上游依赖和说明中查看原因。" }} columns={[
          { title: "尝试", dataIndex: "attempt" }, { title: "版本", render:(_,attempt)=>attempt.releaseNo?`R${attempt.releaseNo}`:"—" }, { title: "提交时间", render: (_, attempt) => timeLabel(attempt.createdAt, detail.timezone) }, { title: "数据截止时间", render:(_,attempt)=>timeLabel(attempt.sourceCutoffAt,detail.timezone) }, { title: "状态", render: (_, attempt) => statusLabel(attempt.status) },
          { title: "操作", render: (_, attempt) => <Space><Button type="link" onClick={() => setAttemptRunId(attempt.runId)}>日志与结果</Button><Button type="link" disabled={busy || activeInstanceStatuses.has(detail.status)} onClick={() => rerun(detail, "ATTEMPT", attempt.runId)}>按此尝试重跑</Button></Space> },
        ]} />
        {(attemptRunId || detail.runId) && <>{attemptRunId&&<Tag>当前查看尝试 {detail.attempts.find(attempt=>attempt.runId===attemptRunId)?.attempt||"—"}</Tag>}<Tabs activeKey={runTab} onChange={setRunTab} items={[{ key: "logs", label: "运行日志" }, { key: "result", label: detail.kind === "WORKFLOW" ? "节点与结果" : "运行结果" }, { key: "details", label: "运行详情" }]} /><RunDetails id={attemptRunId || detail.runId!} tab={runTab} /></>}
      </> : <Empty description="正在加载实例…" />}
    </Drawer>
    <Modal title={backfillTask ? `${backfillTask.name} · 补数` : "补数"} open={!!backfillTask} onCancel={() => { if (!busy) setBackfillTask(undefined); }} width={1000} footer={<Space><Button disabled={busy} onClick={() => setBackfillTask(undefined)}>取消</Button><Button loading={busy} onClick={() => void previewBackfill()}>预览期次</Button><Button type="primary" disabled={!preview || !preview.total || busy} onClick={() => void submitBackfill()}>提交 {preview?.total || ""} 个实例</Button></Space>} destroyOnHidden>
      <p>按业务日期和根任务的计划时间选择期次。预览固定执行版本、参数和依赖，提交后在实例页追踪。</p>
      <Space wrap className="operations-filters"><label>业务日期</label><BusinessDateInput label="补数开始业务日期" disabled={busy} value={startDate} onChange={value => changeBackfill(() => setStartDate(value))} /><span>至</span><BusinessDateInput label="补数结束业务日期" disabled={busy} value={endDate} min={startDate} onChange={value => changeBackfill(() => setEndDate(value))} /><label>每日根任务期次</label><Input type="time" aria-label="补数开始期次时间" disabled={busy} value={startTime} onChange={event => changeBackfill(() => setStartTime(event.target.value))} style={{ width: 115 }} /><span>至</span><Input type="time" aria-label="补数结束期次时间" disabled={busy} value={endTime} onChange={event => changeBackfill(() => setEndTime(event.target.value))} style={{ width: 115 }} /><Checkbox disabled={busy || backfillTask?.kind === "WORKFLOW"} checked={downstream} onChange={event => changeBackfill(() => setDownstream(event.target.checked))}>包含受影响下游</Checkbox></Space>
      {backfillError && <Alert type="error" showIcon title={backfillError} />}
      {preview?.warnings?.map((warning, index) => <Alert key={index} type="warning" showIcon title={warning} className="spaced-form" />)}
      {preview && <><p>共 {preview.total} 个期次。展开行可核对本次参数和对应上游。</p><Table size="small" rowKey={(row, index) => `${row.kind}:${row.scheduleId}:${row.scheduledAt}:${index}`} dataSource={preview.items} pagination={{ pageSize: 8 }} scroll={{ x: 760 }} expandable={{ expandedRowRender: row => <><RunParameters parameters={row.parameters} title="预览实际参数" /><DependencyDetails slots={row.dependencySlots} timezone={row.timezone || backfillTask?.timezone} />{!!row.missingUpstreams?.length && <Alert type="warning" title={`有 ${row.missingUpstreams.length} 个上游期次尚未满足，提交后将等待上游。`} />}</> }} columns={[
        { title: "任务", dataIndex: "name" }, { title: "业务日期", dataIndex: "businessDate" }, { title: "计划时间", render: (_, row) => <>{timeLabel(row.scheduledAt, row.timezone || backfillTask?.timezone)}<div className="operations-note">{row.timezone || backfillTask?.timezone}</div></> }, { title: "版本", render: (_, row) => `R${row.releaseNo || "—"}` }, { title: "说明", render: (_, row) => reasonNames[row.reason || ""] || row.reason || (row.existingTriggerId ? "已有期次，将按补数规则处理" : "新期次") },
      ]} /></>}
    </Modal>
  </div>;
}
