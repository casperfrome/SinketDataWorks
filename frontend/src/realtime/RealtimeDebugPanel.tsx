import { useCallback, useEffect, useRef, useState } from "react";
import { Alert, Button, Input, Space, Table, Tabs, Tag } from "antd";
import { X } from "lucide-react";
import { api } from "../api";
import type { RealtimePreview, RealtimeTask } from "./types";

export function previewRows(result: RealtimePreview) {
  return (result.rows || []).map((row, index) => ({ index, kind: Array.isArray(row) ? result.rowKinds?.[index] || "+I" : ({ INSERT: "+I", UPDATE_BEFORE: "-U", UPDATE_AFTER: "+U", DELETE: "-D" }[row.kind || ""] || row.kind || "+I"), fields: Array.isArray(row) ? row : row.fields }));
}
export default function RealtimeDebugPanel({ workspaceId, task, onClose }: { workspaceId: string; task: RealtimeTask; onClose: () => void }) {
  const firstSource = task.bindings.find(binding => binding.role === "SOURCE");
  const [query, setQuery] = useState(() => /^\s*(SELECT|WITH|VALUES)\b/i.test(task.sql) ? task.sql : firstSource ? `SELECT * FROM \`${firstSource.tableName.replaceAll("`", "``")}\`` : "");
  const [tab, setTab] = useState("diagnostics"), [busy, setBusy] = useState("");
  const [diagnostic, setDiagnostic] = useState(""), [plan, setPlan] = useState("");
  const [error, setError] = useState(""), [preview, setPreview] = useState<RealtimePreview>();
  const [previewId, setPreviewId] = useState("");
  const [logs, setLogs] = useState<{ at: string; message: string }[]>([]);
  const lastPreviewState = useRef("");
  const appendLog = useCallback((message: string) => {
    if (mounted.current) setLogs(current => [...current, { at: new Date().toISOString(), message }].slice(-100));
  }, []);
  const mounted = useRef(true), livePreview = useRef("");
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; if (livePreview.current) void api.realtimeCancelPreview(workspaceId,livePreview.current).catch(() => {}); }; }, [workspaceId]);
  useEffect(() => {
    if (!previewId) return;
    let alive = true, timer: ReturnType<typeof setTimeout>;
    const poll = async () => {
      try {
        const first = await api.realtimePreviewResult(workspaceId,previewId,0);
        let rows = first.rows || [], token = first.nextToken;
        while (token !== null && token !== undefined && rows.length < 100) {
          const page = await api.realtimePreviewResult(workspaceId,previewId,token); rows = [...rows,...(page.rows || [])];
          if (page.nextToken === token) break; token = page.nextToken;
        }
        if (!alive) return;
        const state = JSON.stringify([first.status, first.flinkJobId, first.cleanupStatus, first.message]);
        if (lastPreviewState.current !== state) {
          lastPreviewState.current = state;
          appendLog(`预览 ${previewId}：${first.status || "UNKNOWN"}${first.flinkJobId ? `；Flink Job ID=${first.flinkJobId}` : ""}${first.cleanupStatus ? `；清理状态=${first.cleanupStatus}` : ""}${first.message ? `；${first.message}` : ""}`);
        }
        setPreview({ ...first, rows: rows.slice(0,100) }); setError("");
        if (["SUCCESS","CANCELLED","FAILED"].includes(first.status || "")) { setBusy(""); livePreview.current = ""; }
        else timer = setTimeout(poll,1000);
      } catch (reason) { if (alive) { const message = (reason as Error).message; setError(message); if (lastPreviewState.current !== `error:${message}`) { lastPreviewState.current = `error:${message}`; appendLog(`查询预览状态失败：${message}`); } timer = setTimeout(poll,2000); } }
    };
    void poll(); return () => { alive = false; clearTimeout(timer); };
  }, [workspaceId,previewId,appendLog]);
  const run = async (mode: "validate" | "plan" | "preview") => {
    setBusy(mode); setError("");
    const label = { validate: "SQL 校验", plan: "执行计划", preview: "结果预览" }[mode];
    appendLog(`提交${label}请求`);
    try {
      if (mode === "preview") {
        if (livePreview.current) await api.realtimeCancelPreview(workspaceId,livePreview.current);
        setPreviewId(""); setPreview(undefined); setTab("results"); lastPreviewState.current = "";
        const submission = await api.realtimePreview(workspaceId,structuredClone(task),query);
        if (!submission.previewId) throw new Error("服务未返回结果预览 ID");
        if (!mounted.current) { void api.realtimeCancelPreview(workspaceId,submission.previewId).catch(() => {}); return; }
        appendLog(`结果预览请求已接受：operationId=${submission.operationId}；previewId=${submission.previewId}`);
        livePreview.current = submission.previewId; setPreviewId(submission.previewId); return;
      }
      const result = await (mode === "plan" ? api.realtimePlan(workspaceId,task) : api.realtimeValidate(workspaceId,task));
      if (!mounted.current) return;
      appendLog(`${label}完成：${result.valid ? "通过" : result.errors.join("；") || "未通过"}${mode === "plan" ? `；计划${result.plan ? "已返回" : "为空"}` : ""}`);
      if (mode === "plan") { setPlan(result.plan || "此脚本没有执行计划"); setTab("plan"); }
      else { setDiagnostic(result.valid ? "Flink SQL 校验通过" : result.errors.join("\n")); setTab("diagnostics"); }
    } catch (reason) { if (mounted.current) { setError((reason as Error).message); appendLog(`${label}失败：${(reason as Error).message}`); } }
    if (mounted.current) setBusy("");
  };
  const stop = async () => { if (!livePreview.current) return; appendLog(`请求停止预览 ${livePreview.current}`); try { await api.realtimeCancelPreview(workspaceId,livePreview.current); appendLog("停止请求已接受，等待服务端清理状态"); } catch (reason) { setError((reason as Error).message); appendLog(`停止预览失败：${(reason as Error).message}`); } };
  const rows = preview ? previewRows(preview) : [];
  const statuses: Record<string,string> = {STARTING:"正在提交",RUNNING:"采集中",CANCELLING:"停止并清理中",SUCCESS:"采集完成",CANCELLED:"已停止",FAILED:"失败"};
  return <section className="rt-debug-panel" aria-label="Flink SQL 调试">
    <div className="rt-debug-toolbar"><Space wrap><strong>SQL 调试</strong><Button size="small" disabled={!!busy} loading={busy === "validate"} onClick={() => void run("validate")}>校验 SQL</Button><Button size="small" disabled={!!busy} loading={busy === "plan"} onClick={() => void run("plan")}>执行计划</Button><Button size="small" type="primary" disabled={!!busy || !query.trim()} loading={busy === "preview"} onClick={() => void run("preview")}>结果预览</Button><Button size="small" disabled={!livePreview.current} onClick={() => void stop()}>停止调试</Button><Tag>最多 100 行 / 30 秒</Tag></Space><Button type="text" size="small" aria-label="关闭 SQL 调试" icon={<X size={14} />} onClick={onClose} /></div>
    <Input.TextArea aria-label="结果预览 SELECT" autoSize={{minRows:2,maxRows:4}} value={query} onChange={event => setQuery(event.target.value)} placeholder="输入 SELECT 查询；自动复用任务中的表定义与运行配置。" />
    <p className="rt-muted">结果预览只执行 SELECT。校验及计划检查完整脚本；正式写入通过发布和启动作业执行。</p>
    {error && <Alert type="error" showIcon title="SQL 调试失败" description={error} />}
    <Tabs activeKey={tab} onChange={setTab} items={[
      {key:"diagnostics",label:"校验诊断",children:<pre className="rt-debug-code">{diagnostic || "点击校验 SQL，检查当前草稿。"}</pre>},
      {key:"plan",label:"执行计划",children:<pre className="rt-debug-code">{plan || "点击执行计划，查看 Flink 生成的计划。"}</pre>},
      {key:"logs",label:"调试日志",children:<pre className="rt-debug-code" aria-live="polite">{logs.length ? logs.map(log => `[${new Date(log.at).toLocaleTimeString()}] ${log.message}`).join("\n") : "本次会话暂无调试请求。"}</pre>},
      {key:"results",label:`结果预览 (${rows.length})`,children:<><Space><Tag color={preview?.status === "FAILED" ? "error" : "blue"}>{statuses[preview?.status || ""] || (busy === "preview" ? "正在提交" : "未运行")}</Tag>{preview?.message && <span>{preview.message}</span>}{preview?.truncated && <span>结果达到采集上限，已自动停止</span>}</Space><Table size="small" scroll={{x:"max-content",y:200}} rowKey="index" pagination={false} dataSource={rows} columns={[{title:"RowKind",dataIndex:"kind",width:92},...(preview?.columns || []).map((column,index) => ({title:typeof column === "string" ? column : column.name,key:String(index),render:(_:unknown,row:ReturnType<typeof previewRows>[number]) => row.fields[index] === null ? <span className="rt-muted">NULL</span> : String(row.fields[index] ?? "")}))]} /></>},
    ]} />
  </section>;
}
