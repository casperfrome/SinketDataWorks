import BusinessDateInput from "./BusinessDateInput";
import { useEffect, useState } from "react";
import { Alert, Button, Drawer, Empty, Input, Modal, Space, Table, Tabs, Tag } from "antd";
import type { Run, StudioObject, StudioRecord, WorkflowRelease } from "../types";
import { api } from "../api";
import { RunGraph } from "./WorkflowRunView";
import { yesterday } from "../state/schedules";

interface Props {
  open: boolean; workspaceId: string; target?: StudioObject; initialTab: "publish" | "history";
  dirtyNames: string[]; legacyRecords: StudioRecord[]; onClose: () => void;
  onPublish: (note: string) => Promise<WorkflowRelease>; onRun: (run: Run) => void;
}
export default function WorkflowReleases({ open, workspaceId, target, initialTab, dirtyNames, legacyRecords, onClose, onPublish, onRun }: Props) {
  const [tab, setTab] = useState(initialTab), [note, setNote] = useState(""), [error, setError] = useState("");
  const [releases, setReleases] = useState<WorkflowRelease[]>([]), [busy, setBusy] = useState(false), [revision, setRevision] = useState(0);
  const [snapshot, setSnapshot] = useState<WorkflowRelease | null>(null), [all, setAll] = useState(!target), [loading, setLoading] = useState(false);
  const [day, setDay] = useState(yesterday);
  useEffect(() => { if (open) { setTab(initialTab); setNote(""); setError(""); setSnapshot(null); setAll(!target); } }, [open, target?.id, initialTab]);
  useEffect(() => {
    if (!open) return;
    let alive = true; setLoading(true);
    void api.workflowReleases(workspaceId, all ? "" : target?.id).then(rows => { if (alive) { setReleases(rows); setError(""); } }).catch(e => { if (alive) setError(e.message); }).finally(() => { if (alive) setLoading(false); });
    return () => { alive = false; };
  }, [open, workspaceId, target?.id, all, revision]);
  const act = async (action: () => Promise<void>) => { setBusy(true); setError(""); try { await action(); } catch (e) { setError((e as Error).message); } finally { setBusy(false); } };
  return <>
    <Modal title={target ? `${target.name} · 工作流发布` : "工作空间发布记录"} open={open} onCancel={onClose} width={980} footer={null} destroyOnHidden>
      <Alert type="info" showIcon title="保存版本 V 与发布版本 R 相互独立" description="发布固定整张图和所有子节点代码。运行已发布版本使用其代码快照；已有调度需单独应用新的 R 版本。" />
      {error && <Alert className="spaced-form" type="error" showIcon title={error} />}
      <Tabs activeKey={tab} onChange={key => setTab(key as typeof tab)} items={[
        ...(target ? [{ key: "publish", label: "发布新版本", children: <div className="spaced-form">
          <p>发布对象：<strong>{target.name}</strong></p>
          <p className="panel-muted">{dirtyNames.length ? `发布时将保存相关草稿：${dirtyNames.join("、")}` : "当前没有相关未保存草稿；发布时会校验全部节点版本。"}</p>
          <Input.TextArea aria-label="工作流发布说明" placeholder="说明本次发布的变更" value={note} maxLength={4000} onChange={e => setNote(e.target.value)} rows={3} />
          <Button type="primary" className="spaced-form" loading={busy} onClick={() => void act(async () => { await onPublish(note); setNote(""); setTab("history"); setRevision(r => r + 1); })}>发布新版本</Button>
        </div> }] : []),
        { key: "history", label: "已发布版本", children: <>
          <Space className="spaced-form"><span>业务日期</span><BusinessDateInput label="发布版本运行日期" value={day} onChange={setDay}/></Space>
          <Space className="spaced-form"><Button onClick={() => setRevision(r => r + 1)}>刷新</Button>{target && <Button onClick={() => setAll(!all)}>{all ? "仅看当前工作流" : "查看整个工作空间"}</Button>}<span className="panel-muted">运行指定 R 版本；源开发对象删除后仍可查看和运行。</span></Space>
          <Table size="small" rowKey="id" loading={loading} dataSource={releases} pagination={{ pageSize: 6 }} scroll={{ x: 800 }} locale={{ emptyText: <Empty description="尚无真实发布版本，请先发布工作流" /> }} columns={[
            { title: "工作流", dataIndex: "name" }, { title: "发布版本", render: (_, r) => <Tag color="purple">R{r.releaseNo}</Tag> },
            { title: "开发版本", render: (_, r) => `V${r.workflowVersion}` }, { title: "说明", dataIndex: "note", ellipsis: true },
            { title: "发布时间", dataIndex: "createdAt", render: (s: string) => new Date(s).toLocaleString("zh-CN") },
            { title: "操作", render: (_, r) => <Space><Button size="small" disabled={busy} onClick={() => void act(async () => setSnapshot(await api.workflowRelease(r.id)))}>查看快照</Button><Button size="small" type="primary" disabled={busy || !day} onClick={() => void act(async () => { onRun(await api.runRelease(r.id, day)); onClose(); })}>运行 R{r.releaseNo}</Button></Space> },
          ]} />
        </> },
        { key: "legacy", label: "旧模拟发布", children: <><Alert type="info" title="旧记录没有完整子节点快照，仅供查看，不能用于真实运行。" /><Table size="small" rowKey="id" dataSource={legacyRecords.filter(r => r.kind === "RELEASE" && (all || !target || r.objectId === target.id))} pagination={{ pageSize: 6 }} columns={[{ title: "对象", dataIndex: "title" }, { title: "版本", render: (_, r) => `V${r.payload.version || 1}` }, { title: "发布时间", dataIndex: "createdAt", render: (s: string) => new Date(s).toLocaleString("zh-CN") }]} /></> },
      ]} />
    </Modal>
    <Drawer title={snapshot ? `${snapshot.name} · R${snapshot.releaseNo} 发布快照` : "发布快照"} open={!!snapshot} onClose={() => setSnapshot(null)} size={900} destroyOnHidden>
      {snapshot?.bundle && <><p>{snapshot.note || "无发布说明"}</p><Space wrap>{snapshot.bundle.datasourceBindings.map(source => <Tag key={source.id}>{source.name} · {source.host}:{source.port}/{source.database}</Tag>)}</Space><RunGraph graph={snapshot.bundle.workflow.config.graph} />
        <Tabs items={snapshot.bundle.nodes.map(node => ({ key: node.graphNodeId, label: `${node.object.name} · V${node.object.version}`, children: <><p>图节点：{node.graphNodeId}</p><pre className="detail-code">{node.object.content}</pre></> }))} /></>}
    </Drawer>
  </>;
}
