import type { WorkflowGraph } from "../types";

/** Validate a proposed dependency before it is added to the saved graph. */
export function connectionProblem(
  graph: WorkflowGraph,
  source: string,
  target: string,
): string | null {
  const ids = new Set(graph.nodes.map((node) => node.id));
  if (!ids.has(source) || !ids.has(target))
    return "连线引用的节点不存在，请重新选择";
  if (source === target) return "不能将节点连接到自身";
  if (
    graph.edges.some((edge) => edge.source === source && edge.target === target)
  )
    return "这两个节点之间已存在相同依赖";
  const downstream = new Map<string, string[]>();
  for (const edge of graph.edges) {
    const children = downstream.get(edge.source) ?? [];
    children.push(edge.target);
    downstream.set(edge.source, children);
  }
  const pending = [target],
    seen = new Set<string>();
  while (pending.length) {
    const id = pending.pop()!;
    if (id === source) return "该连线会形成循环依赖，请调整上下游关系";
    if (seen.has(id)) continue;
    seen.add(id);
    pending.push(...(downstream.get(id) ?? []));
  }
  return null;
}

/** Remove nodes and all incident edges in the same draft update. */
export function removeGraphElements(
  graph: WorkflowGraph,
  nodeIds: string[] = [],
  edgeIds: string[] = [],
): WorkflowGraph {
  const nodes = new Set(nodeIds),
    edges = new Set(edgeIds);
  return {
    nodes: graph.nodes.filter((node) => !nodes.has(node.id)),
    edges: graph.edges.filter(
      (edge) =>
        !edges.has(edge.id) &&
        !nodes.has(edge.source) &&
        !nodes.has(edge.target),
    ),
  };
}
