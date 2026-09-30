import { useEffect, useState, type ReactNode } from "react";
import { ReactFlow, Background, Controls, type Node, type Edge } from "@xyflow/react";
import { Alert, Button, Drawer, Empty, Space, Table, Tabs, Tag } from "antd";
import { api } from "../api";
import type { Run, WorkflowGraph } from "../types";
import { isPending, sourceLabel, statusNames } from "../state/workflows";
import "@xyflow/react/dist/style.css";

const colors: Record<string, string> = { RECOVERING: "#b28935", SUCCESS: "#328568", FAILED: "#cb5158", RUNNING: "#468fdb", QUEUED: "#b28935", WAITING: "#68778a", SKIPPED: "#808080", CANCELLED: "#808080" };
export function RunGraph({ graph, runs = [], onSelect }: { graph: WorkflowGraph; runs?: Run[]; onSelect?: (run: Run) => void }) {
  const byNode = new Map(runs.map(run => [run.graphNodeId, run]));
  const nodes: Node[] = graph.nodes.map(node => {
    const run = byNode.get(node.id);
    return { id: node.id, position: { x: node.x || 0, y: node.y || 0 },
      data: { label: <div><strong>{node.label}</strong><div>{run ? `${statusNames[run.status]} · V${run.objectVersion}` : node.nodeType}</div></div> },
      style: { border: `2px solid ${run ? colors[run.status] : "#68778a"}`, background: "var(--panel, #20262d)", color: "var(--text, #c4c9cd)", width: 185, borderRadius: 6, fontSize: 12, cursor: onSelect ? "pointer" : "default" } };
  });
  const edges: Edge[] = graph.edges.map(edge => ({ ...edge, type: "smoothstep" }));
  return <div className="workflow-run-graph"><ReactFlow nodes={nodes} edges={edges} fitView fitViewOptions={{ padding: 0.3 }} nodesDraggable={false} nodesConnectable={false} elementsSelectable={false} deleteKeyCode={null}
    onNodeClick={(_, node) => { const run = byNode.get(node.id); if (run) onSelect?.(run); }} minZoom={0.2} maxZoom={1.5}><Background /><Controls showInteractive={false} /></ReactFlow></div>;
}
export default function WorkflowRunView({ run, tab, refresh, renderChild }: { run: Run; tab: string; refresh: () => void; renderChild: (id: string, tab: string) => ReactNode }) {
  const [nodes, setNodes] = useState<Run[]>([]), [error, setError] = useState("");
  const [selected, setSelected] = useState<Run | null>(null), [childTab, setChildTab] = useState("logs");
  useEffect(() => {
    let alive = true; let timer: ReturnType<typeof setTimeout>;
    const load = async () => { try { const rows = await api.runNodes(run.id); if (alive) { setNodes(rows); setError(""); } } catch (e) { if (alive) setError((e as Error).message); }
      finally { if (alive && isPending(run)) timer = setTimeout(load, 1000); } };
    void load(); return () => { alive = false; clearTimeout(timer); };
  }, [run.id, run.status]);
  const open = (node: Run) => { setSelected(node); setChildTab("logs"); };
  const graph = run.snapshot?.config.graph as WorkflowGraph | undefined;
  return <div className="query-detail workflow-run-view">
    <Space wrap><Tag color="green">真实工作流</Tag><Tag color="purple">{sourceLabel(run)}</Tag><Tag>{statusNames[run.status]}</Tag>
      <span>完成 {nodes.filter(n => !isPending(n)).length} / {run.nodeCount || nodes.length}</span>
      {isPending(run) && <Button size="small" danger onClick={async () => { try { await api.stop(run.id); refresh(); } catch (e) { setError((e as Error).message); } }}>停止整个流程</Button>}
    </Space>
    {run.materialization && <Space wrap className="spaced-form"><Tag>业务日期 {run.businessDate}</Tag><Tag color={run.publicationStatus==="PUBLISHED"?"green":"gold"}>{({PUBLISHED:"正式结果已发布",NOT_PUBLISHED:"未发布，保留此前结果",RECOVERING:"正在核实提交凭据",STAGING:"节点暂存中"} as Record<string,string>)[run.publicationStatus || ""] || run.publicationStatus}</Tag><span className="panel-muted">批次 {run.buildId} · 截止 {run.sourceCutoffAt ? new Date(run.sourceCutoffAt).toLocaleString("zh-CN") : "—"}</span></Space>}
    {error && <Alert type="error" title={error} />}
    {tab === "logs" ? <><pre className="detail-code run-log">{run.logs?.join("\n")}</pre><p className="panel-muted">在“节点与结果”中点击节点，查看独立日志和查询结果。独立分支继续，失败下游跳过。</p></> : tab === "result" ? <>
      {graph && <RunGraph graph={graph} runs={nodes} onSelect={open} />}
      <Table size="small" rowKey="id" dataSource={nodes} pagination={false} scroll={{ x: 760 }} columns={[
        { title: "节点", dataIndex: "objectName", render: (name: string, node: Run) => <Button type="link" onClick={() => open(node)}>{name}</Button> },
        { title: "图节点", dataIndex: "graphNodeId" }, { title: "开发版本", render: (_, node) => `V${node.objectVersion}` },
        { title: "状态", render: (_, node) => <Tag color={colors[node.status]}>{statusNames[node.status]}</Tag> },
        { title: "耗时", render: (_, node) => node.elapsedMs === undefined ? "—" : `${node.elapsedMs} ms` },
        { title: "写入行数", dataIndex: "writtenRows", render: (n?:number)=>n ?? "—" },
        { title: "说明", dataIndex: "errorCode", render: (code?: string) => code === "UPSTREAM_FAILED" ? "上游失败，已跳过" : code || "—" },
        { title: "操作", render: (_, node) => <Button type="link" onClick={() => open(node)}>日志与结果</Button> },
      ]} />
    </> : <><p>运行 ID：{run.id}</p><p>工作流开发版本：V{run.objectVersion} · {sourceLabel(run)}</p><p className="panel-muted">以下为本次执行冻结的图，后续编辑不会改变它。</p>{graph ? <RunGraph graph={graph} runs={nodes} onSelect={open} /> : <Empty />}</>}
    <Drawer title={selected ? `${selected.objectName} · V${selected.objectVersion}` : "节点详情"} open={!!selected} onClose={() => setSelected(null)} size={780} destroyOnHidden>
      <Tabs activeKey={childTab} onChange={setChildTab} items={[{ key: "logs", label: "运行日志" }, { key: "result", label: "查询结果" }, { key: "details", label: "代码快照" }]} />
      {selected && renderChild(selected.id, childTab)}
    </Drawer>
  </div>;
}
