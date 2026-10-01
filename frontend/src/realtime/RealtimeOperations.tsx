import { useState } from "react";
import { Alert, App, Button, Descriptions, Empty, Input, Modal, Select, Space, Table, Tabs, Tag } from "antd";
import { Activity, ArrowUpRight, Clock3, Database, Gauge, Play, RotateCcw, Save, Square, TriangleAlert } from "lucide-react";
import type { RealtimeDatasource, RealtimeJob, RealtimeRelease, RealtimeState } from "./types";
import { createJob, jobMetrics, transitionJob, validateTask } from "./model";

export const jobStatusNames: Record<string, string> = { STARTING: "启动中", RUNNING: "运行中", STOPPING: "停止中", STOPPED: "已停止", RESTARTING: "重启中", FAILED: "失败" };
const statusColors: Record<string, string> = { STARTING: "processing", RUNNING: "success", STOPPING: "warning", STOPPED: "default", RESTARTING: "processing", FAILED: "error" };
export const isJobActive = (job?: RealtimeJob) => !!job && ["STARTING", "RUNNING", "RESTARTING", "STOPPING"].includes(job.status);
const time = (value?: string) => value ? new Date(value).toLocaleString("zh-CN", { hour12: false }) : "—";
const duration = (seconds: number) => Math.floor(seconds / 3600) + "h " + Math.floor(seconds % 3600 / 60) + "m " + Math.floor(seconds % 60) + "s";

interface Props {
  state: RealtimeState; now: number; sources: RealtimeDatasource[];
  update: (fn: (state: RealtimeState) => RealtimeState) => boolean; onOpen: (id: string) => void;
  commit: (fn: (state: RealtimeState) => RealtimeState) => boolean;
}

export default function RealtimeOperations({ state, update, commit, now, sources, onOpen }: Props) {
  const { message, modal } = App.useApp();
  const [search, setSearch] = useState(""), [status, setStatus] = useState("all");
  const [versions, setVersions] = useState<Record<string, string>>({});
  const [restore, setRestore] = useState<{ taskId: string; releaseId: string }>();
  const [restorePath, setRestorePath] = useState("");
  const rows = state.tasks.map(task => ({
    task, job: state.jobs.filter(job => job.taskId === task.id).at(-1),
    releases: state.releases.filter(release => release.taskId === task.id).sort((a, b) => b.releaseNo - a.releaseNo),
  })).filter(row => row.task.name.toLowerCase().includes(search.toLowerCase()) && (status === "all" || (row.job?.status || "UNDEPLOYED") === status));
  const selectedJob = state.jobs.find(job => job.id === state.selectedJobId);
  const selectedRelease = state.releases.find(release => release.id === selectedJob?.releaseId);
  const metrics = selectedJob && selectedRelease ? jobMetrics(selectedJob, selectedRelease, now) : undefined;

  const selectJob = (id: string) => update(current => ({ ...current, selectedJobId: id }));
  const operate = (job: RealtimeJob, action: "STOP" | "RESTART" | "FAIL" | "SAVEPOINT") => {
    try {
      const success = commit(current => ({ ...current, selectedJobId: job.id, jobs: current.jobs.map(item => item.id === job.id ? transitionJob(item, action) : item) }));
      if (success) message.success({ STOP: "模拟停止已提交", RESTART: "模拟重启已提交", FAIL: "已注入模拟故障", SAVEPOINT: "模拟 Savepoint 已生成" }[action]);
    } catch (error) { message.error(error instanceof Error ? error.message : "操作失败"); }
  };
  const start = (taskId: string, releaseId: string, restoredFrom?: string) => {
    const release = state.releases.find(item => item.id === releaseId && item.taskId === taskId);
    if (!release) { message.warning("请先发布任务并选择版本"); return; }
    const savepoint = restoredFrom ? state.jobs.flatMap(job => job.savepoints).find(item => item.path === restoredFrom && item.releaseId === releaseId) : undefined;
    if (restoredFrom && !savepoint) { message.warning("请选择属于当前发布版本的 Savepoint"); return; }
    const errors = validateTask(release.snapshot, sources);
    if (errors.length) { modal.error({ title: "发布版本的配置需要修正", content: <ul>{errors.map((error, index) => <li key={index}>{error}</li>)}</ul> }); return; }
    let newJob: RealtimeJob | undefined;
    const success = commit(current => {
      if (isJobActive(current.jobs.filter(item => item.taskId === taskId).at(-1))) return current;
      newJob = createJob(release, savepoint);
      return { ...current, jobs: [...current.jobs, newJob], selectedJobId: newJob.id };
    });
    if (success && newJob) { setRestore(undefined); message.success(restoredFrom ? "已从模拟 Savepoint 提交启动" : "模拟作业已提交启动"); }
  };
  const releaseOptions = (releases: RealtimeRelease[]) => releases.map(release => ({ value: release.id, label: "R" + release.releaseNo + " · " + time(release.createdAt) }));

  return <div className="rt-operations">
    <div className="rt-operations-heading">
      <div><span className="rt-eyebrow">STREAMING OPERATIONS</span><h2>实时运维</h2><p>按发布版本管理常驻流作业，查看运行与状态恢复记录。</p></div>
      <Tag color="blue">模拟运行</Tag>
    </div>
    <div className="rt-operations-filter"><Input.Search aria-label="搜索实时作业" placeholder="搜索任务名称" value={search} onChange={event => setSearch(event.target.value)} allowClear />
      <Select aria-label="实时作业状态" value={status} onChange={setStatus} options={[{ value: "all", label: "全部状态" }, { value: "UNDEPLOYED", label: "未启动" }, ...Object.entries(jobStatusNames).map(([value, label]) => ({ value, label }))]} />
    </div>
    <Table size="small" rowKey={row => row.task.id} dataSource={rows} scroll={{ x: 970 }} pagination={{ pageSize: 8 }} locale={{ emptyText: <Empty description="暂无实时任务"><Button onClick={() => onOpen("")}>前往开发创建任务</Button></Empty> }}
      columns={[
        { title: "任务", width: 210, render: (_, row) => <button className="rt-name-button" onClick={() => row.job ? selectJob(row.job.id) : onOpen(row.task.id)}><Activity size={15} /><span>{row.task.name}<small>{row.task.bindings.filter(binding => binding.role === "SOURCE").length} Source · {row.task.bindings.filter(binding => binding.role === "SINK").length} Sink</small></span></button> },
        { title: "状态", width: 96, render: (_, row) => <Tag color={statusColors[row.job?.status || ""]}>{row.job ? jobStatusNames[row.job.status] : "未启动"}</Tag> },
        { title: "启动版本", width: 230, render: (_, row) => <Select aria-label={row.task.name + " 启动版本"} size="small" disabled={isJobActive(row.job)} value={versions[row.task.id] || row.releases[0]?.id} placeholder="先发布任务" style={{ width: "100%" }} options={releaseOptions(row.releases)} onChange={id => setVersions(current => ({ ...current, [row.task.id]: id }))} /> },
        { title: "运行版本", width: 86, render: (_, row) => { const release = state.releases.find(item => item.id === row.job?.releaseId); return release ? "R" + release.releaseNo : "—"; } },
        { title: "开始时间", width: 168, render: (_, row) => time(row.job?.startedAt) },
        { title: "操作", width: 220, render: (_, row) => <Space size={3} wrap>
          <Button type="link" size="small" disabled={isJobActive(row.job) || !row.releases.length} onClick={() => start(row.task.id, versions[row.task.id] || row.releases[0]?.id)}>启动</Button>
          <Button type="link" size="small" disabled={!row.job || !["STARTING", "RUNNING", "RESTARTING"].includes(row.job.status)} onClick={() => row.job && operate(row.job, "STOP")}>停止</Button>
          <Button type="link" size="small" disabled={!row.job || !["RUNNING", "STOPPED", "FAILED"].includes(row.job.status)} onClick={() => row.job && operate(row.job, "RESTART")}>重启</Button>
          <Button type="link" size="small" disabled={isJobActive(row.job) || !state.jobs.some(item => item.taskId === row.task.id && item.savepoints.length)} onClick={() => {
            const points = state.jobs.filter(item => item.taskId === row.task.id).flatMap(item => item.savepoints);
            const preferred = versions[row.task.id] || row.releases[0]?.id;
            setRestore({ taskId: row.task.id, releaseId: points.some(point => point.releaseId === preferred) ? preferred : points.at(-1)!.releaseId }); setRestorePath("");
          }}>恢复启动</Button>
          <Button type="link" size="small" onClick={() => onOpen(row.task.id)}>开发 <ArrowUpRight size={12} /></Button>
        </Space> },
      ]} />
    {selectedJob && selectedRelease && metrics ? <section className="rt-job-detail">
      <div className="rt-detail-heading"><div><h3>{selectedRelease.snapshot.name}</h3><Tag color={statusColors[selectedJob.status]}>{jobStatusNames[selectedJob.status]}</Tag><Tag>R{selectedRelease.releaseNo}</Tag><Tag color="blue">模拟数据</Tag></div>
        <Space wrap>
          <Button size="small" icon={<Save size={13} />} disabled={selectedJob.status !== "RUNNING"} onClick={() => operate(selectedJob, "SAVEPOINT")}>生成 Savepoint</Button>
          <Button size="small" danger icon={<TriangleAlert size={13} />} disabled={selectedJob.status !== "RUNNING"} onClick={() => modal.confirm({ title: "注入模拟故障", content: "此操作将使当前模拟作业进入失败状态，可通过重启或 Savepoint 恢复。", okText: "注入故障", onOk: () => operate(selectedJob, "FAIL") })}>模拟故障</Button>
        </Space>
      </div>
      <div className="rt-metrics">
        {[{ label: "输入吞吐", value: metrics.inputRate.toLocaleString(), unit: "条 / 秒", icon: Database }, { label: "输出吞吐", value: metrics.outputRate.toLocaleString(), unit: "条 / 秒", icon: Activity }, { label: "处理延迟", value: metrics.latencyMs.toLocaleString(), unit: "ms", icon: Gauge }, { label: "运行时长", value: duration(metrics.uptimeSeconds), unit: "", icon: Clock3 }].map(item => <div key={item.label} className="rt-metric"><span><item.icon size={14} />{item.label}</span><strong>{item.value}<small>{item.unit}</small></strong></div>)}
      </div>
      <Tabs items={[
        { key: "overview", label: "作业详情", children: <>
          <Descriptions size="small" column={{ xs: 1, sm: 2, lg: 3 }} items={[
            { key: "id", label: "模拟作业 ID", children: selectedJob.id }, { key: "version", label: "发布版本", children: "R" + selectedRelease.releaseNo },
            { key: "parallelism", label: "并行度", children: selectedRelease.snapshot.runtime.parallelism }, { key: "checkpoint", label: "Checkpoint 间隔", children: selectedRelease.snapshot.runtime.checkpointSeconds + " 秒" },
            { key: "restart", label: "重启策略", children: selectedRelease.snapshot.runtime.restartAttempts + " 次 / " + selectedRelease.snapshot.runtime.restartDelaySeconds + " 秒" }, { key: "restore", label: "恢复起点", children: selectedJob.restoredFrom || "全新启动" },
          ]} />
          <div className="rt-bindings-overview">{(["SOURCE", "SINK"] as const).map(role => <div key={role}><h4>{role === "SOURCE" ? "Source 输入" : "Sink 输出"}</h4>{selectedRelease.snapshot.bindings.filter(binding => binding.role === role).map(binding => <div key={binding.id}><Tag>{binding.connector}</Tag><code>{binding.tableName}</code><span>{binding.topic || binding.physicalTable}</span></div>)}</div>)}</div>
        </> },
        { key: "logs", label: "运行日志", children: <div className="rt-log-list">{selectedJob.logs.map(log => <div key={log.id}><time>{time(log.at)}</time><span>{log.message}</span></div>)}</div> },
        { key: "checkpoints", label: "Checkpoint · " + metrics.checkpointCount, children: <><p className="rt-muted">按运行时间与配置间隔生成的模拟检查点。</p><Table size="small" rowKey="id" dataSource={metrics.checkpoints} pagination={{ pageSize: 5 }} columns={[{ title: "ID", dataIndex: "id", render: id => "#" + id }, { title: "完成时间", dataIndex: "at", render: time }, { title: "耗时", dataIndex: "durationMs", render: value => value + " ms" }, { title: "状态", dataIndex: "status", render: () => <Tag color="success">模拟完成</Tag> }]} /></> },
        { key: "savepoints", label: "Savepoint · " + selectedJob.savepoints.length, children: <Table size="small" rowKey="id" dataSource={selectedJob.savepoints} pagination={{ pageSize: 5 }} locale={{ emptyText: "运行时可生成用于恢复启动的模拟 Savepoint" }} columns={[{ title: "创建时间", dataIndex: "createdAt", render: time }, { title: "模拟路径", dataIndex: "path", render: path => <code>{path}</code> }]} /> },
        { key: "history", label: "操作历史", children: <Table size="small" rowKey="id" dataSource={state.jobs.filter(job => job.taskId === selectedJob.taskId).flatMap(job => job.logs).slice().reverse()} pagination={{ pageSize: 8 }} columns={[{ title: "时间", dataIndex: "at", render: time, width: 190 }, { title: "操作", dataIndex: "message" }]} /> },
      ]} />
    </section> : <div className="rt-detail-empty"><Gauge size={28} /><p>选择一个作业，查看指标、日志与恢复记录</p></div>}
    <Modal title="从模拟 Savepoint 恢复启动" open={!!restore} onCancel={() => setRestore(undefined)} okText="恢复启动" okButtonProps={{ disabled: !restorePath }} onOk={() => restore && start(restore.taskId, restore.releaseId, restorePath)}>
      <Alert type="info" showIcon title="Savepoint 与恢复过程均为前端模拟" />
      <div className="rt-form-row"><label>恢复版本</label><Select aria-label="恢复发布版本" value={restore?.releaseId} options={releaseOptions(state.releases.filter(release => release.taskId === restore?.taskId).sort((a, b) => b.releaseNo - a.releaseNo)).map(option => ({ ...option, disabled: !state.jobs.filter(job => job.taskId === restore?.taskId).flatMap(job => job.savepoints).some(point => point.releaseId === option.value) }))} onChange={id => { setRestorePath(""); setRestore(current => current && ({ ...current, releaseId: id })); }} /></div>
      <div className="rt-form-row"><label>Savepoint</label><Select aria-label="恢复 Savepoint" value={restorePath || undefined} placeholder="选择属于当前版本的保存点" notFoundContent="此版本暂无 Savepoint" options={state.jobs.filter(job => job.taskId === restore?.taskId).flatMap(job => job.savepoints).filter(savepoint => savepoint.releaseId === restore?.releaseId).map(savepoint => ({ value: savepoint.path, label: time(savepoint.createdAt) + " · " + savepoint.path }))} onChange={setRestorePath} /></div>
    </Modal>
  </div>;
}
