import { useEffect, useRef, useState } from "react";
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  Controls,
  MiniMap,
  Handle,
  Position,
  applyNodeChanges,
  applyEdgeChanges,
  useReactFlow,
} from "@xyflow/react";
import type {
  Node,
  Edge,
  NodeProps,
  Connection,
  NodeChange,
  EdgeChange,
} from "@xyflow/react";
import {
  Button,
  Empty,
  Input,
  Modal,
  Select,
  Space,
  Table,
  Tag,
  Tooltip,
  message,
} from "antd";
import {
  ApartmentOutlined,
  CodeOutlined,
  DeleteOutlined,
  ImportOutlined,
  PlusOutlined,
  SearchOutlined,
  UnorderedListOutlined,
  CloseOutlined,
  DatabaseOutlined,
  ClusterOutlined,
} from "@ant-design/icons";
import type { GraphNode, StudioObject, WorkflowGraph } from "../types";
import { isRealTask } from "../state/workflows";
import { nodeTypes, nodeGroups, getNodeType } from "../data/nodeTypes";
import { connectionProblem, removeGraphElements } from "../data/workflowRules";
import "@xyflow/react/dist/style.css";

interface Props {
  object: StudioObject;
  onChange: (patch: Partial<StudioObject>) => void;
  objects: StudioObject[];
  onOpen: (id: string) => void;
  theme: "dark" | "light";
}
type FlowData = {
  label: string;
  nodeType: string;
  objectId?: string;
  [key: string]: unknown;
};
type FlowNode = Node<FlowData>;
function WorkNode({ data, selected }: NodeProps<FlowNode>) {
  const type = getNodeType(data.nodeType);
  return (
    <div className={`workflow-node ${selected ? "selected" : ""}`}>
      <Handle type="target" position={Position.Top} />
      <span className="workflow-node-icon" style={{ color: type.color }}>
        {type.editor === "form" ? (
          <ClusterOutlined />
        ) : type.language === "sql" ? (
          <DatabaseOutlined />
        ) : (
          <CodeOutlined />
        )}
      </span>
      <div>
        <div className="workflow-node-label" title={data.label}>
          {data.label}
        </div>
        <span className="workflow-node-kind">{data.nodeType}</span>
      </div>
      {data.objectId && (
        <span className="workflow-linked" title="已关联开发对象">
          ↗
        </span>
      )}
      <Handle type="source" position={Position.Bottom} />
    </div>
  );
}
const customNodeTypes = { studio: WorkNode };
const emptyGraph: WorkflowGraph = { nodes: [], edges: [] };
const toFlowNodes = (graph: WorkflowGraph): FlowNode[] =>
  (graph.nodes ?? []).map((n) => ({
    id: n.id,
    type: "studio",
    position: { x: n.x ?? 0, y: n.y ?? 0 },
    data: { label: n.label, nodeType: n.nodeType, objectId: n.objectId },
  }));
const toFlowEdges = (graph: WorkflowGraph): Edge[] =>
  (graph.edges ?? []).map((e) => ({
    ...e,
    type: "smoothstep",
    style: { stroke: "#718293", strokeWidth: 1.5 },
  }));
export default function WorkflowEditor(props: Props) {
  return (
    <ReactFlowProvider key={props.object.id}>
      <WorkflowCanvas {...props} />
    </ReactFlowProvider>
  );
}
function WorkflowCanvas({ object, onChange, objects, onOpen, theme }: Props) {
  const graph: WorkflowGraph = object.config.graph ?? emptyGraph;
  const graphRef = useRef(graph);
  graphRef.current = graph;
  const [nodes, setNodes] = useState<FlowNode[]>(() => toFlowNodes(graph));
  const [edges, setEdges] = useState<Edge[]>(() => toFlowEdges(graph));
  const [library, setLibrary] = useState(false),
    [query, setQuery] = useState(""),
    [group, setGroup] = useState("全部"),
    [view, setView] = useState("canvas"),
    [importing, setImporting] = useState(false),
    [imports, setImports] = useState<string[]>([]),
    [selected, setSelected] = useState<string>();
  const [messageApi, holder] = message.useMessage();
  const flow = useReactFlow<FlowNode>();
  const signature = JSON.stringify(graph);
  useEffect(() => {
    setNodes((previous) =>
      toFlowNodes(graphRef.current).map((node) => ({
        ...previous.find((old) => old.id === node.id),
        ...node,
      })),
    );
    setEdges((previous) =>
      toFlowEdges(graphRef.current).map((edge) => ({
        ...previous.find((old) => old.id === edge.id),
        ...edge,
      })),
    );
  }, [object.id, signature]);
  const commit = (next: WorkflowGraph) => {
    if (JSON.stringify(next) === JSON.stringify(graphRef.current)) return;
    graphRef.current = next;
    onChange({ config: { ...object.config, graph: next } });
  };
  const addNode = (
    nodeType: string,
    position?: { x: number; y: number },
    linkedObject?: StudioObject,
  ) => {
    const current = graphRef.current,
      id = crypto.randomUUID();
    const count = current.nodes.filter((n) => n.nodeType === nodeType).length;
    const node: GraphNode = {
      id,
      nodeType,
      label: linkedObject?.name ?? `${nodeType}_${count + 1}`,
      objectId: linkedObject?.id,
      x: position?.x ?? 100 + (current.nodes.length % 3) * 250,
      y: position?.y ?? 80 + Math.floor(current.nodes.length / 3) * 140,
    };
    commit({ ...current, nodes: [...current.nodes, node] });
    setSelected(id);
  };
  const validConnection = (connection: Connection) => {
    const problem = connectionProblem(
      graphRef.current,
      connection.source,
      connection.target,
    );
    if (problem) {
      messageApi.warning(problem);
      return false;
    }
    return true;
  };
  const connect = (connection: Connection) => {
    if (validConnection(connection)) {
      const current = graphRef.current;
      commit({
        ...current,
        edges: [
          ...current.edges,
          {
            id: crypto.randomUUID(),
            source: connection.source,
            target: connection.target,
          },
        ],
      });
    }
  };
  const changeNodes = (changes: NodeChange<FlowNode>[]) => {
    setNodes((current) => applyNodeChanges(changes, current));
    const removed = changes.filter((c) => c.type === "remove").map((c) => c.id);
    if (removed.length) commit(removeGraphElements(graphRef.current, removed));
  };
  const changeEdges = (changes: EdgeChange[]) => {
    setEdges((current) => applyEdgeChanges(changes, current));
    const removed = changes.filter((c) => c.type === "remove").map((c) => c.id);
    if (removed.length)
      commit(removeGraphElements(graphRef.current, [], removed));
  };
  const deleteSelected = () => {
    const selectedNodes = nodes
        .filter((n) => n.selected || n.id === selected)
        .map((n) => n.id),
      selectedEdges = edges.filter((e) => e.selected).map((e) => e.id);
    if (!selectedNodes.length && !selectedEdges.length) {
      messageApi.info("请先选择节点或连线");
      return;
    }
    commit(removeGraphElements(graphRef.current, selectedNodes, selectedEdges));
    setSelected(undefined);
  };
  const libraryTypes = nodeTypes.filter(
    (n) =>
      n.editor !== "workflow" &&
      n.editor !== "notebook" &&
      (group === "全部" || n.group === group) &&
      `${n.label} ${n.group}`.toLowerCase().includes(query.toLowerCase()),
  );
  const available = objects.filter(o => object.config.run?.provider !== "WORKFLOW" || isRealTask(o)).filter(
    (o) =>
      !o.deleted &&
      ["NODE", "NOTEBOOK", "PERSONAL", "COMPONENT"].includes(o.kind) &&
      o.id !== object.id,
  );
  const importSelected = () => {
    const current = graphRef.current;
    const selectedObjects = imports
      .map((id) => available.find((o) => o.id === id))
      .filter((o): o is StudioObject => !!o);
    if (selectedObjects.length !== imports.length) {
      messageApi.warning("部分开发对象已被删除，请重新选择");
      setImports(selectedObjects.map((o) => o.id));
      return;
    }
    const added = selectedObjects.map((o, index) => ({
      id: crypto.randomUUID(),
      objectId: o.id,
      label: o.name,
      nodeType: o.nodeType,
      x: 100 + ((current.nodes.length + index) % 3) * 250,
      y: 80 + Math.floor((current.nodes.length + index) / 3) * 140,
    }));
    commit({ ...current, nodes: [...current.nodes, ...added] });
    setImporting(false);
    setImports([]);
    messageApi.success(`已导入 ${added.length} 个开发对象`);
  };
  const currentNode = graph.nodes.find((n) => n.id === selected);
  return (
    <div className="workflow-editor">
      {holder}
      <div className="workflow-toolbar">
        <Space size={4}>
          <Button
            size="small"
            type={library ? "primary" : "default"}
            icon={<PlusOutlined />}
            onClick={() => setLibrary(!library)}
          >
            添加节点
          </Button>
          <Button
            size="small"
            icon={<ImportOutlined />}
            onClick={() => setImporting(true)}
          >
            导入已有节点
          </Button>
          <Tooltip title="删除选中的节点或连线">
            <Button
              size="small"
              icon={<DeleteOutlined />}
              onClick={deleteSelected}
            />
          </Tooltip>
        </Space>
        <div className="workflow-view-switch">
          {[
            { key: "canvas", label: "工作流", icon: <ApartmentOutlined /> },
            { key: "list", label: "内部节点", icon: <UnorderedListOutlined /> },
            { key: "spec", label: "Spec", icon: <CodeOutlined /> },
          ].map((t) => (
            <Button
              key={t.key}
              size="small"
              type={view === t.key ? "primary" : "text"}
              icon={t.icon}
              onClick={() => setView(t.key)}
            >
              {t.label}
            </Button>
          ))}
        </div>
      </div>
      <div className="workflow-body">
        {library && (
          <aside className="node-library">
            <div className="node-library-heading">
              <strong>节点库</strong>
              <Button
                size="small"
                type="text"
                icon={<CloseOutlined />}
                aria-label="关闭节点库"
                onClick={() => setLibrary(false)}
              />
            </div>
            <Input
              size="small"
              allowClear
              prefix={<SearchOutlined />}
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="搜索节点类型"
            />
            <div className="node-library-layout">
              <div className="node-library-groups">
                {[
                  "全部",
                  ...nodeGroups.filter(
                    (g) => !["工作流", "个人开发", "Notebook"].includes(g),
                  ),
                ].map((g) => (
                  <button
                    className={group === g ? "active" : ""}
                    key={g}
                    onClick={() => setGroup(g)}
                  >
                    {g}
                  </button>
                ))}
              </div>
              <div className="node-library-items">
                {libraryTypes.map((t) => (
                  <Tooltip
                    key={t.type}
                    title="单击添加，或拖动至画布"
                    placement="right"
                  >
                    <button
                      className="node-library-item"
                      draggable
                      onDragStart={(e) => {
                        e.dataTransfer.setData(
                          "application/dataworks-node",
                          t.type,
                        );
                        e.dataTransfer.effectAllowed = "move";
                      }}
                      onClick={() => addNode(t.type)}
                    >
                      <span style={{ color: t.color }}>
                        {t.editor === "form" ? (
                          <ClusterOutlined />
                        ) : t.language === "sql" ? (
                          <DatabaseOutlined />
                        ) : (
                          <CodeOutlined />
                        )}
                      </span>
                      <span>{t.label}</span>
                    </button>
                  </Tooltip>
                ))}
                {!libraryTypes.length && (
                  <Empty
                    image={Empty.PRESENTED_IMAGE_SIMPLE}
                    description="没有匹配节点"
                  />
                )}
              </div>
            </div>
          </aside>
        )}
        <div className="workflow-content">
          {view === "canvas" ? (
            <div
              className="workflow-canvas"
              onDragOver={(e) => {
                e.preventDefault();
                e.dataTransfer.dropEffect = "move";
              }}
              onDrop={(e) => {
                e.preventDefault();
                const type = e.dataTransfer.getData(
                  "application/dataworks-node",
                );
                if (type)
                  addNode(
                    type,
                    flow.screenToFlowPosition({ x: e.clientX, y: e.clientY }),
                  );
              }}
            >
              <ReactFlow
                nodes={nodes}
                edges={edges}
                nodeTypes={customNodeTypes}
                onNodesChange={changeNodes}
                onEdgesChange={changeEdges}
                onConnect={connect}
                onNodeDragStop={(_e, node, dragged) => {
                  const changes = new Map(
                    (dragged?.length ? dragged : [node]).map((n) => [
                      n.id,
                      n.position,
                    ]),
                  );
                  const current = graphRef.current;
                  commit({
                    ...current,
                    nodes: current.nodes.map((n) =>
                      changes.has(n.id)
                        ? {
                            ...n,
                            x: changes.get(n.id)!.x,
                            y: changes.get(n.id)!.y,
                          }
                        : n,
                    ),
                  });
                }}
                onNodeClick={(_e, node) => setSelected(node.id)}
                onPaneClick={() => setSelected(undefined)}
                onEdgeClick={() => setSelected(undefined)}
                onNodeDoubleClick={(_e, node) => {
                  if (node.data.objectId) onOpen(node.data.objectId);
                  else
                    messageApi.info(
                      "可在右侧节点属性中修改名称；需要编辑代码时，请导入已有开发节点",
                    );
                }}
                colorMode={theme}
                fitView
                fitViewOptions={{ padding: 0.3, maxZoom: 1 }}
                minZoom={0.2}
                maxZoom={2}
                deleteKeyCode={["Backspace", "Delete"]}
                defaultEdgeOptions={{ type: "smoothstep" }}
                proOptions={{ hideAttribution: true }}
              >
                <Background
                  color={theme === "dark" ? "#303942" : "#d3dce5"}
                  gap={20}
                  size={1}
                />
                <Controls showInteractive={false} />
                <MiniMap
                  nodeColor="#5289b4"
                  maskColor={
                    theme === "dark"
                      ? "rgba(15,19,23,.8)"
                      : "rgba(240,243,246,.75)"
                  }
                  style={{ height: 80, width: 130 }}
                />
              </ReactFlow>
              {graph.nodes.length === 0 && (
                <div className="workflow-empty">
                  <ApartmentOutlined />
                  <h3>构建你的工作流</h3>
                  <p>从节点库拖入节点，连接上下游依赖</p>
                  <Button
                    type="primary"
                    size="small"
                    icon={<PlusOutlined />}
                    onClick={() => setLibrary(true)}
                  >
                    添加第一个节点
                  </Button>
                </div>
              )}
            </div>
          ) : view === "list" ? (
            <div className="workflow-node-list">
              <Table
                size="small"
                rowKey="id"
                dataSource={graph.nodes}
                pagination={false}
                columns={[
                  {
                    title: "节点名称",
                    dataIndex: "label",
                    render: (label, n) => (
                      <a
                        onClick={() => {
                          if (n.objectId) onOpen(n.objectId);
                          else {
                            setSelected(n.id);
                            setView("canvas");
                          }
                        }}
                      >
                        {label}
                      </a>
                    ),
                  },
                  { title: "节点类型", dataIndex: "nodeType" },
                  {
                    title: "上游依赖",
                    render: (_v, n) =>
                      graph.edges
                        .filter((e) => e.target === n.id)
                        .map(
                          (e) =>
                            graph.nodes.find((p) => p.id === e.source)?.label,
                        )
                        .join("、") || "—",
                  },
                  {
                    title: "关联开发对象",
                    render: (_v, n) =>
                      n.objectId ? (
                        <Tag color="blue">已关联</Tag>
                      ) : (
                        <span className="muted">工作流内部节点</span>
                      ),
                  },
                  {
                    title: "操作",
                    render: (_v, n) => (
                      <Button
                        type="text"
                        size="small"
                        danger
                        icon={<DeleteOutlined />}
                        onClick={() =>
                          commit({
                            nodes: graph.nodes.filter((x) => x.id !== n.id),
                            edges: graph.edges.filter(
                              (e) => e.source !== n.id && e.target !== n.id,
                            ),
                          })
                        }
                      />
                    ),
                  },
                ]}
                locale={{ emptyText: "暂无内部节点" }}
              />
            </div>
          ) : (
            <div className="workflow-spec">
              <div className="spec-heading">
                <Tag>JSON</Tag>
                <span>当前草稿 Spec · 修改工作流后同步更新</span>
                <Button
                  size="small"
                  onClick={() => {
                    navigator.clipboard
                      .writeText(
                        JSON.stringify(
                          {
                            name: object.name,
                            type: object.nodeType,
                            ...graph,
                          },
                          null,
                          2,
                        ),
                      )
                      .then(() => messageApi.success("Spec 已复制"))
                      .catch(() =>
                        messageApi.error("复制失败，请手动选择文本"),
                      );
                  }}
                >
                  复制 Spec
                </Button>
              </div>
              <pre>
                {JSON.stringify(
                  { name: object.name, type: object.nodeType, ...graph },
                  null,
                  2,
                )}
              </pre>
            </div>
          )}
          {currentNode && view === "canvas" && (
            <div className="workflow-node-inspector">
              <div className="node-library-heading">
                <strong>节点属性</strong>
                <Button
                  size="small"
                  type="text"
                  icon={<CloseOutlined />}
                  onClick={() => setSelected(undefined)}
                />
              </div>
              <label>节点名称</label>
              <Input
                size="small"
                value={currentNode.label}
                onChange={(e) =>
                  commit({
                    ...graph,
                    nodes: graph.nodes.map((n) =>
                      n.id === currentNode.id
                        ? { ...n, label: e.target.value }
                        : n,
                    ),
                  })
                }
              />
              <label>节点类型</label>
              <Tag>{currentNode.nodeType}</Tag>
              <label>上游依赖</label>
              <div className="muted">
                {graph.edges
                  .filter((e) => e.target === currentNode.id)
                  .map((e) => graph.nodes.find((n) => n.id === e.source)?.label)
                  .join("、") || "无上游依赖"}
              </div>
              {currentNode.objectId && (
                <Button
                  size="small"
                  block
                  style={{ marginTop: 18 }}
                  onClick={() => onOpen(currentNode.objectId!)}
                >
                  打开开发对象 ↗
                </Button>
              )}
              <p className="muted">
                拖动节点移动位置，从节点圆点拖动可创建依赖。
              </p>
            </div>
          )}
        </div>
      </div>
      <div className="workflow-status">
        <span>
          {graph.nodes.length} 个节点 · {graph.edges.length} 条依赖
        </span>
        <span>拖动平移 · 滚轮缩放 · Delete 删除 · 双击打开关联对象</span>
      </div>
      <Modal
        title="导入已有节点"
        open={importing}
        onCancel={() => setImporting(false)}
        onOk={importSelected}
        okText={`导入${imports.length ? `（${imports.length}）` : ""}`}
        okButtonProps={{ disabled: !imports.length }}
        cancelText="取消"
        width={620}
      >
        <p className="muted">
          关联项目中已有开发对象，双击工作流节点即可打开编辑。
        </p>
        <Select
          mode="multiple"
          showSearch
          optionFilterProp="label"
          style={{ width: "100%" }}
          placeholder="搜索并选择开发对象"
          value={imports}
          onChange={setImports}
          options={available.map((o) => ({
            value: o.id,
            label: `${o.name} · ${o.nodeType}`,
          }))}
        />
        {!available.length && (
          <Empty description="项目中尚无可导入的节点，请先新建开发对象" />
        )}
      </Modal>
    </div>
  );
}
