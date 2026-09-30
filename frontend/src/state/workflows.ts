import type { Run, StudioObject, WorkflowGraph } from "../types.ts";

export const isRealWorkflow = (object?: StudioObject) => object?.kind === "WORKFLOW" && object.config.run?.provider === "WORKFLOW";
export const sqlProvider = (nodeType: string) => nodeType === "Doris" ? "DORIS" : "MYSQL";
export const isSqlTask = (object?: StudioObject) => object?.kind === "NODE" && ["MySQL", "Doris"].includes(object.nodeType) && object.config.run?.provider === sqlProvider(object.nodeType);
export const isRealTask = (object?: StudioObject) => isSqlTask(object) || (object?.kind === "NODE" && ["离线同步", "数据集成"].includes(object.nodeType) && object.config.run?.provider === "SYNC");
export const isPending = (run: Run) => ["WAITING", "QUEUED", "RUNNING", "RECOVERING"].includes(run.status);
export const runLabel = (run: Run) => run.simulation ? "本地模拟" : run.provider === "SYNC" ? "数据集成" : run.materialization ? (run.provider==="WORKFLOW"?"库存落表工作流":"库存落表任务") : run.executionMode === "MATERIALIZE" ? "MySQL 落表" : run.provider === "WORKFLOW" ? "真实工作流" : run.provider === "DORIS" ? "真实 Doris" : "真实 MySQL";
export const sourceLabel = (run: Run) => `${run.triggerType === "SCHEDULED" ? "定时调度 · " : run.triggerType === "RERUN" ? "历史重跑 · " : ""}${run.executionSource === "RELEASE" ? `已发布版本 R${run.releaseNo}` : run.executionSource === "DEVELOPMENT" ? "开发调试" : "—"}`;
export const statusNames: Record<Run["status"], string> = { WAITING: "等待依赖", QUEUED: "排队中", RUNNING: "运行中", SUCCESS: "成功", FAILED: "失败", CANCELLED: "已停止", SKIPPED: "已跳过", RECOVERING: "核实提交中" };

export interface WorkflowSubmission {
  workflow: StudioObject;
  nodes: StudioObject[];
  dirtyIds: Set<string>;
}
/** Retain immutable React state references captured at the user's click, not live editor state. */
export function captureWorkflowSubmission(id: string, objects: StudioObject[], drafts: Record<string, StudioObject>): WorkflowSubmission {
  const byId = new Map(objects.map(object => [object.id, object]));
  const workflow = drafts[id] || byId.get(id);
  if (!workflow || !isRealWorkflow(workflow)) throw new Error("请选择真实工作流执行方式");
  const graph = workflow.config.graph as WorkflowGraph | undefined;
  if (!graph?.nodes?.length) throw new Error("请先导入至少一个 MySQL、Doris 或数据集成节点");
  const nodes: StudioObject[] = [];
  const seen = new Set<string>();
  for (const item of graph.nodes) {
    const node = item.objectId && (drafts[item.objectId] || byId.get(item.objectId));
    if (!node || node.deleted) throw new Error(`「${item.label}」未绑定有效节点，请从已有节点导入`);
    if (node.workspaceId !== workflow.workspaceId || !isRealTask(node)) throw new Error(`「${item.label}」必须绑定本空间的 MySQL、Doris 或数据集成节点`);
    if (!seen.has(node.id)) { seen.add(node.id); nodes.push(node); }
  }
  return { workflow, nodes, dirtyIds: new Set([workflow, ...nodes].filter(object => !!drafts[object.id]).map(object => object.id)) };
}
export async function saveWorkflowSubmission(captured: WorkflowSubmission, save: (object: StudioObject, dirty: boolean) => Promise<StudioObject | undefined>) {
  const expectedNodeVersions: Record<string, number> = {};
  let workflow = captured.workflow;
  for (const object of [captured.workflow, ...captured.nodes]) {
    const saved = await save(object, captured.dirtyIds.has(object.id));
    if (!saved) throw new Error(`「${object.name}」保存失败，本次未创建发布或运行；已保存的对象保留，其他草稿仍可继续编辑。`);
    if (object.id === workflow.id) workflow = saved;
    else expectedNodeVersions[object.id] = saved.version;
  }
  return { workflow, expectedNodeVersions };
}
