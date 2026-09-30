import { useEffect, useState } from "react";
import { Alert, Button, Descriptions, Empty, Select, Space, Spin, Table, Tag } from "antd";
import { api } from "../api";
import type { QueryResult, Run } from "../types";
import SyncRunDetails from "./SyncRunDetails";
import WorkflowRunView from "./WorkflowRunView";
import RunParameters from "./RunParameters";
import { isPending, runLabel, sourceLabel } from "../state/workflows";

const statementStatuses = { SKIPPED: "未执行", RUNNING: "执行中", SUCCESS: "成功", FAILED: "失败", CANCELLED: "已停止", UNKNOWN: "提交结果未知" };
const statementKinds = { QUERY: "查询", UPDATE: "数据写入", DDL: "表结构操作" };

export default function RunDetails({ id, tab = "logs" }: { id: string; tab?: string }) {
  const [run, setRun] = useState<Run | null>(null);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [page, setPage] = useState(1);
  const [statementIndex, setStatementIndex] = useState<number>();
  const [error, setError] = useState("");
  const [revision, setRevision] = useState(0);
  useEffect(() => { setPage(1); setStatementIndex(undefined); setResult(null); }, [id]);
  useEffect(() => {
    let alive = true;
    let timer: ReturnType<typeof setTimeout>;
    setRun(null); setError("");
    const load = async () => {
      try {
        const detail = await api.runDetail(id);
        if (!alive) return;
        setRun(detail); setError("");
        if (isPending(detail)) timer = setTimeout(load, 1000);
      } catch (e) { if (alive) setError((e as Error).message); }
    };
    void load();
    return () => { alive = false; clearTimeout(timer); };
  }, [id, revision]);
  useEffect(() => {
    let alive = true;
    if (tab !== "result" || !run || ["WORKFLOW","SYNC"].includes(run.provider||"") || run.id !== id || isPending(run)) return;
    setResult(null); setError("");
    void api.runResults(id, page, statementIndex).then(data => { if (alive) setResult(data); }).catch(e => { if (alive) setError(e.message); });
    return () => { alive = false; };
  }, [id, page, statementIndex, tab, run?.id, run?.status, revision]);
  if (error) return <Alert type="error" title={error} action={<Button onClick={() => setRevision(r => r + 1)}>重试</Button>} />;
  if (!run || run.id !== id) return <Spin />;
  if (run.provider === "WORKFLOW") return <WorkflowRunView run={run} tab={tab} refresh={() => setRevision(r => r + 1)} renderChild={(childId, childTab) => <RunDetails id={childId} tab={childTab} />} />;
  if(run.provider === "SYNC")return <SyncRunDetails run={run} refresh={()=>setRevision(r=>r+1)}/>;
  const pending = isPending(run);
  if (tab === "logs") return <div className="query-detail">
    <Space><Tag color={run.simulation ? "blue" : "green"}>{runLabel(run)}</Tag>
      <span>{run.elapsedMs !== undefined ? `耗时 ${run.elapsedMs} ms` : pending ? "执行中…" : ""}</span>
      {pending && <Button size="small" danger onClick={async () => { try { await api.stop(run.parentRunId || id); setRevision(r => r + 1); } catch(e) { setError((e as Error).message); } }}>{run.parentRunId ? "停止所属工作流" : "停止任务"}</Button>}
    </Space>
    {run.businessDate&&<Space wrap><Tag>业务日期 {run.businessDate}</Tag>{run.releaseNo&&<Tag>任务 R{run.releaseNo}</Tag>}{run.buildId&&<Tag>批次 {run.buildId}</Tag>}{run.publicationStatus&&<Tag color={run.publicationStatus==="PUBLISHED"?"green":"default"}>{run.publicationStatus==="PUBLISHED"?"正式结果已发布":run.publicationStatus==="RECOVERING"?"核实提交中":run.publicationStatus==="NOT_PUBLISHED"?"未发布":"生成结果中"}</Tag>}</Space>}
    {!!run.upstreamRuns?.length&&<div className="task-trigger-dependencies"><h4>本次使用的上游</h4>{run.upstreamRuns.map(u=><div key={u.runId}><strong>{u.name||u.taskId}</strong><code>{u.buildId||u.runId}</code></div>)}</div>}
    <RunParameters parameters={run.scheduleParameters} />
    <pre className="detail-code run-log">{run.logs?.join("\n") || "等待日志…"}</pre>
    {run.errorCode && <Tag color="error">{run.errorCode}</Tag>}
  </div>;
  if (tab === "result") {
    if (pending) return <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="执行结束后显示结果" />;
    if (!result) return <Spin />;
    return <div className="query-detail">
      {!!result.statements?.length && <Space wrap>
        <Select aria-label="SQL 语句结果" style={{ minWidth: 280 }} value={statementIndex ?? result.statementIndex} onChange={index => { setStatementIndex(index); setPage(1); }}
          options={result.statements.map(item => ({ value: item.statementIndex, label: `SQL ${item.statementIndex} · ${statementKinds[item.kind]} · ${statementStatuses[item.status]}` }))} />
        <span>耗时 {result.elapsedMs ?? 0} ms</span>
        {result.commitStatus === "COMMITTED" && <Tag color="green">已提交</Tag>}
      </Space>}
      {result.status && result.status !== "SUCCESS" && <Alert showIcon type={result.status === "SKIPPED" ? "info" : "warning"} title={result.message || statementStatuses[result.status]} description={result.errorCode} />}
      {result.status === "SUCCESS" && result.kind !== "QUERY" && <Alert type="success" showIcon title={result.kind === "DDL" ? "表结构操作已完成" : `执行成功，影响 ${result.affectedRows ?? 0} 行`} />}
      {run.executionMode === "MATERIALIZE" && <Alert type="info" title={`本任务写入 ${run.writtenRows ?? 0} 行；${run.publicationStatus==="PUBLISHED"?"正式结果已发布":run.publicationStatus==="STAGED"?"暂存成功，等待旧版工作流提交":"正式结果尚未发布"}。此处仅预览前 1,000 行。`}/>}
      {result.truncated && <Alert type="warning" showIcon title="结果已截断：每条查询最多 1,000 行，整次运行共用 5 MiB，单元格最多 64 KiB。预览截断不影响后续 SQL 执行。" />}
      {result.columns.length ? <Table size="small" scroll={{ x: "max-content" }} rowKey="index"
        pagination={{ current: page, pageSize: 100, total: result.total, showSizeChanger: false, onChange: setPage, showTotal: total => `已保存 ${total} 行` }}
        columns={result.columns.map((label, i) => ({ title: label, key: String(i), dataIndex: String(i), render: (value: unknown) => value === null ? <span className="panel-muted">NULL</span> : <span className="query-cell">{String(value)}</span> }))}
        dataSource={result.rows.map((row, index) => ({ index, ...Object.fromEntries(row.map((value, i) => [String(i), value])) }))}
      /> : (!result.kind || result.kind === "QUERY") && <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="本次查询没有保存的结果" />}
    </div>;
  }
  return <div className="query-detail">
    <Descriptions size="small" column={2} items={[
      { key: "id", label: "运行 ID", children: run.id },
      { key: "executionSource", label: "执行来源", children: sourceLabel(run) },
      { key: "source", label: "数据源", children: run.dataSource?.name || "本地模拟" },
      { key: "version", label: "执行版本", children: `V${run.objectVersion || run.snapshot?.version || "—"}` },
      { key: "time", label: "耗时", children: run.elapsedMs !== undefined ? `${run.elapsedMs} ms` : "—" },
      { key: "submitted", label: "提交时间", children: new Date(run.createdAt).toLocaleString("zh-CN") },
      { key: "status", label: "状态", children: run.status },
    ]} />
    <RunParameters parameters={run.scheduleParameters} />
    <h4>执行时的代码快照</h4><pre className="detail-code">{run.snapshot?.content || "无代码内容"}</pre>
  </div>;
}
