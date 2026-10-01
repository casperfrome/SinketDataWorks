import DatasourceBinding from "./components/DatasourceBinding";
import { schemaLabel } from "./state/sync";
import RecycleView from "./components/RecycleView";
import type { DataSource } from "./types";
import { defaultSchedule } from "./state/schedules";
import type { SchedulingDraft, TaskRelease } from "./types";
import SchedulingOperations, { type SchedulingSelection } from "./components/SchedulingOperations";
import { tabsToClose, afterClosingTabs, restoreEditorTabs, tabCloseLabels, type TabCloseAction } from "./state/tabs";
import {
  useState,
  useEffect,
  useCallback,
  useRef,
  useMemo,
} from "react";
import {
  App as AntApp,
  ConfigProvider,
  theme as antTheme,
  Button,
  Dropdown,
  Tooltip,
  Input,
  Select,
  Modal,
  Form,
  Tree,
  Tag,
  Table,
  Tabs,
  Spin,
  Empty,
  Alert,
  Switch,
  InputNumber,
  Menu,
  Drawer,
  Space,
} from "antd";
import zhCN from "antd/locale/zh_CN";
import type { DataNode } from "antd/es/tree";
import {
  Layers3,
  Menu as MenuIcon,
  ChevronDown,
  ChevronRight,
  CodeXml,
  Blocks,
  Table2,
  FolderClosed,
  FolderOpen,
  UserRoundCheck,
  MoreHorizontal,
  HelpCircle,
  Settings2,
  PanelLeftClose,
  PanelLeftOpen,
  Search,
  Plus,
  RefreshCw,
  ChevronsDownUp,
  PlayCircle,
  StopCircle,
  Save,
  Braces,
  Coins,
  Send,
  Share2,
  X,
  GitBranch,
  Database,
  FileCode2,
  BookOpen,
  Star,
  Trash2,
  Copy,
  FolderInput,
  FilePenLine,
  Moon,
  Sun,
  Check,
  Info,
  Bell,
  TriangleAlert,
  Maximize2,
  Minimize2,
  LayoutPanelLeft,
  Package,
  Cloud,
  Activity as ActivityIcon,
} from "lucide-react";
import { format as formatSQL } from "sql-formatter";
import { api, ApiError } from "./api";
import RunParameterDialog from "./components/RunParameterDialog";
import { effectiveRunParameters, shouldOpenRunParameters } from "./state/runParameters";
import type {
  Workspace,
  StudioObject,
  ObjectKind,
  Preferences,
  Run,
  StudioRecord,
  Activity,
  RunParameterPreparation,
} from "./types";
import StudioEditor from "./components/StudioEditor";
import Inspector from "./components/Inspector";
import DatasourceView from "./components/DatasourceView";
import RealtimeWorkbench from "./realtime/RealtimeWorkbench";
import WorkspaceManager from "./components/WorkspaceManager";
import RunDetails from "./components/RunDetails";
import WorkflowReleases from "./components/WorkflowReleases";
import BusinessDateInput from "./components/BusinessDateInput";
import { yesterday } from "./state/schedules";
import { captureWorkflowSubmission, saveWorkflowSubmission, isRealWorkflow, isRealTask, sqlProvider, runLabel, statusNames } from "./state/workflows";
import { nodeTypes, defaultContent } from "./data/nodeTypes";
import {
  settleSavedDraft,
  mergeMetadataDraft,
  pruneDeletedDrafts,
} from "./state/drafts";

interface NodeRunRequest {
  object: StudioObject;
  needsSave: boolean;
  fail: boolean;
  mode: "NORMAL" | "CUSTOM";
  phase: "PREPARING" | "EDITING" | "SAVING" | "SUBMITTING";
}

const activities: { id: Activity; label: string; icon: typeof CodeXml }[] = [
  { id: "development", label: "离线数据开发", icon: CodeXml },
  { id: "realtime", label: "实时数据开发", icon: ActivityIcon },
  { id: "scheduling", label: "调度运维", icon: GitBranch },
  { id: "datasources", label: "数据源", icon: Database },
  { id: "recycle", label: "回收站", icon: Trash2 },
];
const kindNames: Record<ObjectKind, string> = {
  FOLDER: "目录",
  NODE: "节点",
  WORKFLOW: "工作流",
  NOTEBOOK: "Notebook",
  TABLE: "表",
  RESOURCE: "资源",
  FUNCTION: "函数",
  COMPONENT: "组件",
  PERSONAL: "个人文件",
  ENVIRONMENT: "个人开发环境",
};
const errorText = (e: unknown) =>
  e instanceof Error ? e.message : "操作失败，请重试";
const stamp = (s?: string) =>
  s ? new Date(s).toLocaleString("zh-CN", { hour12: false }) : "—";
const runColor: Record<string, string> = {
  QUEUED: "default",
  RUNNING: "processing",
  RECOVERING: "warning",
  SUCCESS: "success",
  FAILED: "error",
  CANCELLED: "warning",
};
const runName: Record<string, string> = {
  QUEUED: "排队中",
  RUNNING: "运行中",
  RECOVERING: "恢复检查中",
  SUCCESS: "成功",
  FAILED: "失败",
  CANCELLED: "已停止",
};
function ObjectIcon({
  object,
  size = 15,
}: {
  object: Pick<StudioObject, "kind" | "nodeType">;
  size?: number;
}) {
  const kind = object.kind;
  const Icon =
    kind === "FOLDER"
      ? FolderClosed
      : kind === "WORKFLOW"
        ? GitBranch
        : kind === "TABLE"
          ? Table2
          : kind === "NOTEBOOK"
            ? BookOpen
            : kind === "RESOURCE"
              ? Package
              : kind === "ENVIRONMENT"
                ? Cloud
                : kind === "COMPONENT"
                  ? Blocks
                  : FileCode2;
  return (
    <Icon size={size} className={`object-icon kind-${kind.toLowerCase()}`} />
  );
}
function Tool({
  label,
  icon: Icon,
  onClick,
  disabled = false,
  active = false,
}: {
  label: string;
  icon: typeof Save;
  onClick: () => void;
  disabled?: boolean;
  active?: boolean;
}) {
  return (
    <Tooltip title={label}>
      <button
        aria-label={label}
        className={`icon-button ${active ? "active" : ""}`}
        disabled={disabled}
        onClick={onClick}
      >
        <Icon size={15} />
      </button>
    </Tooltip>
  );
}

export default function App() {
  const [mode, setMode] = useState<"dark" | "light">("dark");
  return (
    <ConfigProvider
      locale={zhCN}
      theme={{
        algorithm: [
          mode === "dark" ? antTheme.darkAlgorithm : antTheme.defaultAlgorithm,
          antTheme.compactAlgorithm,
        ],
        token: {
          colorPrimary: "#637dff",
          colorBgBase: mode === "dark" ? "#14181c" : "#f5f6f8",
          colorBgContainer: mode === "dark" ? "#191e23" : "#ffffff",
          colorBorder: mode === "dark" ? "#343b43" : "#d7dce3",
          borderRadius: 3,
          fontSize: 13,
          fontFamily: '"Segoe UI", "Microsoft YaHei", sans-serif',
          controlHeight: 29,
        },
        components: {
          Table: {
            headerBg: mode === "dark" ? "#20262c" : "#eef0f4",
            cellPaddingBlock: 10,
          },
          Modal: { contentBg: mode === "dark" ? "#1b2026" : "#ffffff" },
          Tree: { nodeSelectedBg: "#273760" },
          Tabs: { horizontalItemGutter: 22 },
        },
      }}
    >
      <AntApp>
        <Studio mode={mode} setMode={setMode} />
      </AntApp>
    </ConfigProvider>
  );
}

function Studio({
  mode,
  setMode,
}: {
  mode: "dark" | "light";
  setMode: (m: "dark" | "light") => void;
}) {
  const { message, modal } = AntApp.useApp();
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]),
    [workspaceId, setWorkspaceId] = useState("local-workspace");
  const [workspaceManagerOpen, setWorkspaceManagerOpen] = useState(false);
  const [objects, setObjects] = useState<StudioObject[]>([]),
    [runs, setRuns] = useState<Run[]>([]),
    [records, setRecords] = useState<StudioRecord[]>([]);
  const [loading, setLoading] = useState(true),
    [fatal, setFatal] = useState(""),
    [activity, setActivity] = useState<Activity>("development");
  const [tabs, setTabs] = useState<string[]>([]),
    [activeId, setActiveId] = useState(""),
    [drafts, setDrafts] = useState<Record<string, StudioObject>>({});
  const [taskReleaseHistory,setTaskReleaseHistory]=useState<TaskRelease[]>([]);
  const [scheduleRevisions,setScheduleRevisions]=useState<Record<string,number>>({});
  const [scheduleDrafts,setScheduleDrafts]=useState<Record<string,SchedulingDraft>>({});
  const [schedulingSelection,setSchedulingSelection]=useState<SchedulingSelection>();
  const changeScheduleDraft=useCallback((id:string,draft?:SchedulingDraft)=>setScheduleDrafts(current=>{if(JSON.stringify(current[id])===JSON.stringify(draft))return current;const next={...current};if(draft)next[id]=draft;else delete next[id];return next;}),[]);
  const [prefs, setPrefs] = useState<Preferences>({
      theme: "dark",
      editorFontSize: 13,
      wordWrap: false,
    }),
    [sidebarWidth, setSidebarWidth] = useState(238),
    [sidebarVisible, setSidebarVisible] = useState(true);
  const [notice, setNotice] = useState(true),
    [search, setSearch] = useState(""),
    [filter, setFilter] = useState("all"),
    [expanded, setExpanded] = useState<React.Key[]>([]);
  const [inspector, setInspector] = useState<
      "schedule" | "versions" | null
    >(null),
    [resultId, setResultId] = useState<string | null>(null),
    [resultTab, setResultTab] = useState("logs"),
    [resultHeight, setResultHeight] = useState(220);
  const [saving, setSaving] = useState(false),
    [settings, setSettings] = useState(false),
    [help, setHelp] = useState(false),
    [releaseOpen, setReleaseOpen] = useState(false),
    [shareOpen, setShareOpen] = useState(false);
  const [workflowRelease, setWorkflowRelease] = useState<{ workspaceId: string; target?: StudioObject; tab: "publish" | "history" } | null>(null);
  const [releaseNote, setReleaseNote] = useState(""),
    [releaseEnv, setReleaseEnv] = useState("开发"),
    [publishing, setPublishing] = useState(false),
    [palette, setPalette] = useState(false),
    [nodeSearch, setNodeSearch] = useState(""),
    [nodeGroup, setNodeGroup] = useState("全部");
  const [createSpec, setCreateSpec] = useState<{
      kind: ObjectKind;
      nodeType: string;
      parentId: string | null;
    } | null>(null),
    [createForm] = Form.useForm(),
    [creating, setCreating] = useState(false);
  const [editAction, setEditAction] = useState<{
      kind: "rename" | "move" | "copy";
      object: StudioObject;
    } | null>(null),
    [editName, setEditName] = useState(""),
    [editParent, setEditParent] = useState<string | null>(null);
  const [context, setContext] = useState<{
      x: number;
      y: number;
      object: StudioObject;
    } | null>(null),
    [selectedId, setSelectedId] = useState(""),
    [zen, setZen] = useState(false),
    [focusFolder, setFocusFolder] = useState<string | null>(null);
  const [notificationOpen, setNotificationOpen] = useState(false);
  const [creationSources, setCreationSources] = useState<DataSource[]>([]);
  const [creationSourceError, setCreationSourceError] = useState("");
  useEffect(() => {
    let alive = true;
    if (!createSpec || !["MySQL", "Doris"].includes(createSpec.nodeType)) return;
    setCreationSources([]); setCreationSourceError(""); createForm.setFieldValue("dataSourceId", undefined);
    void api.datasources(workspaceId).then(items => { if (alive) { const sql=items.filter((s):s is DataSource=>s.type!=="KAFKA"&&s.type===sqlProvider(createSpec.nodeType)); setCreationSources(sql); if (sql.length === 1) createForm.setFieldValue("dataSourceId", sql[0].id); } }).catch(e => { if (alive) setCreationSourceError(e.message); });
    return () => { alive = false; };
  }, [workspaceId, createSpec?.nodeType]);
  const initialized = useRef(false),
    prefLoaded = useRef(false),
    widRef = useRef(workspaceId);
  const saveRequests = useRef(
    new Map<string, Promise<StudioObject | undefined>>(),
  );
  const runRequests = useRef(new Set<string>());
  const nodeRunRequest = useRef<NodeRunRequest | null>(null);
  const activeIdRef = useRef(activeId);
  activeIdRef.current = activeId;
  const [nodeRunPendingId, setNodeRunPendingId] = useState<string | null>(null);
  const [nodeRunSubmitting, setNodeRunSubmitting] = useState(false);
  const [nodeRunDialog, setNodeRunDialog] = useState<{
    request: NodeRunRequest;
    preparation: RunParameterPreparation;
    error?: string;
  } | null>(null);
  const prefQueue = useRef<Promise<unknown>>(Promise.resolve());
  widRef.current = workspaceId;
  const current = objects.find((o) => o.id === activeId);
  const active = activeId ? drafts[activeId] || current : undefined;
  const realTask=isRealTask(active);
  useEffect(()=>{let alive=true;if(releaseOpen&&realTask){setTaskReleaseHistory([]);void api.taskReleases(activeId).then(r=>{if(alive)setTaskReleaseHistory(r);}).catch(e=>message.error(errorText(e)));}return ()=>{alive=false;};},[releaseOpen,realTask,activeId]);
  const dirty = !!drafts[activeId] || !!scheduleDrafts[activeId];
  const workspace = workspaces.find((w) => w.id === workspaceId);
  const [businessDate, setBusinessDate] = useState(yesterday);
  const [selectedRun, setSelectedRun] = useState<Run | null>(null);
  const chosenRun = runs.find((r) => r.id === resultId) || (selectedRun?.id === resultId ? selectedRun : undefined);
  const isRunning = runs.some(
    (r) => r.objectId === activeId && ["QUEUED", "RUNNING", "RECOVERING"].includes(r.status),
  );
  const updateServerObject = useCallback(
    (o: StudioObject, submitted?: StudioObject) => {
      if (o.workspaceId !== widRef.current) return;
      setObjects((prev) =>
        prev.some((x) => x.id === o.id)
          ? prev.map((x) => (x.id === o.id ? o : x))
          : [...prev, o],
      );
      setDrafts((prev) => {
        const next = { ...prev };
        const kept = submitted
          ? settleSavedDraft(prev[o.id], submitted, o)
          : undefined;
        if (kept) next[o.id] = kept;
        else delete next[o.id];
        return next;
      });
    },
    [],
  );
  const updateMetadata = useCallback((o: StudioObject, fields: string[]) => {
    if (o.workspaceId !== widRef.current) return;
    setObjects((os) => os.map((x) => (x.id === o.id ? o : x)));
    setDrafts((ds) => {
      const draft = mergeMetadataDraft(ds[o.id], o, fields);
      if (!draft) return ds;
      return { ...ds, [o.id]: draft };
    });
  }, []);
  const refresh = useCallback(async () => {
    const wid = widRef.current;
    const [os, rs, recs] = await Promise.all([
      api.objects(wid),
      api.runs(wid),
      api.records(wid),
    ]);
    if (wid === widRef.current) {
      setObjects(os);
      setRuns(rs);
      setRecords(recs);
      const alive = new Set(os.map((o) => o.id));
      setTabs((ids) => ids.filter((id) => alive.has(id)));
      setDrafts((ds) => pruneDeletedDrafts(ds, alive));
      setActiveId((id) => (alive.has(id) ? id : ""));
      setSelectedId((id) => (alive.has(id) ? id : ""));
      setFocusFolder((id) => (id && alive.has(id) ? id : null));
    }
  }, []);
  const bootstrap = useCallback(async () => {
    setLoading(true);
    setFatal("");
    try {
      const [ws, p] = await Promise.all([api.workspaces(), api.preferences()]);
      setWorkspaces(ws);
      const selected = ws.some((x) => x.id === p.workspaceId)
        ? p.workspaceId!
        : ws[0]?.id || "local-workspace";
      widRef.current = selected;
      setWorkspaceId(selected);
      setPrefs(p);
      setMode(p.theme === "light" ? "light" : "dark");
      setSidebarWidth(Number(p.sidebarWidth) || 238);
      const [os, rs, recs] = await Promise.all([
        api.objects(selected),
        api.runs(selected),
        api.records(selected),
      ]);
      setObjects(os);
      setRuns(rs);
      setRecords(recs);
      const fromUrl = new URLSearchParams(location.search).get("object");
      const restored=restoreEditorTabs(p,new Set(os.map(o=>o.id)),os.find(o=>o.id==="node-demo")?.id||os.find(o=>o.kind==="NODE")?.id||"",fromUrl);
      setTabs(restored.tabs);setActiveId(restored.activeId);
      setExpanded(os.filter((o) => o.kind === "FOLDER").map((o) => o.id));
      prefLoaded.current = true;
      initialized.current = true;
    } catch (e) {
      setFatal(errorText(e));
    } finally {
      setLoading(false);
    }
  }, [setMode]);
  useEffect(() => {
    void bootstrap();
  }, [bootstrap]);
  useEffect(() => {
    if (!prefLoaded.current) return;
    const timer = window.setTimeout(() => {
      const snapshot = {
        ...prefs,
        theme: mode,
        sidebarWidth,
        openTabs: tabs,
        activeId,
        workspaceId,
      };
      prefQueue.current = prefQueue.current
        .catch(() => {})
        .then(() => api.savePreferences(snapshot))
        .catch((e) => message.warning("布局偏好保存失败：" + errorText(e)));
    }, 750);
    return () => clearTimeout(timer);
  }, [prefs, mode, sidebarWidth, tabs, activeId, workspaceId, message]);
  useEffect(() => {
    if (!initialized.current) return;
    const timer = window.setInterval(() => {
      const wid = widRef.current;
      void api
        .runs(wid)
        .then((r) => {
          if (wid === widRef.current) setRuns(r);
        })
        .catch(() => {});
    }, 1600);
    return () => clearInterval(timer);
  }, [loading]);
  useEffect(() => {
    document.documentElement.dataset.theme = mode;
    document.title = `${workspace?.name || "默认工作空间"} | SinketDataWorks`;
  }, [mode, workspace]);
  useEffect(() => {
    const close = () => setContext(null);
    window.addEventListener("click", close);
    return () => window.removeEventListener("click", close);
  }, []);
  useEffect(() => {
    const before = (e: BeforeUnloadEvent) => {
      if (Object.keys(drafts).length || Object.keys(scheduleDrafts).length) {
        e.preventDefault();
        e.returnValue = "";
      }
    };
    window.addEventListener("beforeunload", before);
    return () => window.removeEventListener("beforeunload", before);
  }, [drafts, scheduleDrafts]);
  const openObject = (id: string) => {
    setActiveId(id);
    setSelectedId(id);
    setTabs((t) => (t.includes(id) ? t : [...t, id]));
    setActivity("development");
    setInspector(null);
  };
  const change = (patch: Partial<StudioObject>) => {
    if (active)
      setDrafts((d) => ({
        ...d,
        [active.id]: { ...(d[active.id] || active), ...patch },
      }));
  };
  const saveObject = async (submitted: StudioObject, needsSave: boolean): Promise<StudioObject | undefined> => {
    const pending = saveRequests.current.get(submitted.id);
    let payload = submitted;
    if (pending) {
      const previous = await pending;
      if (!previous) return undefined;
      payload = { ...submitted, version: previous.version };
    }
    if (!needsSave && !pending) return submitted;
    setSaving(true);
    const request = (async () => {
      try {
        const saved = await api.save(payload);
        updateServerObject(saved, submitted);
        return saved;
      } catch (e) { message.error(errorText(e)); return undefined; }
      finally { saveRequests.current.delete(submitted.id); setSaving(saveRequests.current.size > 0); }
    })();
    saveRequests.current.set(submitted.id, request);
    return request;
  };
  const applyScheduleDraft = (draft: SchedulingDraft) => "workflowId" in draft.input
    ? api.saveSchedule(draft.input.workflowId, draft.input, draft.id)
    : api.saveTaskSchedule(draft.input, draft.id);
  const saveCurrent = async (includeSchedule=true): Promise<StudioObject | undefined> => {
    if (!active) return;
    const saved = await saveObject(active, !!drafts[active.id]);
    if(saved&&includeSchedule&&scheduleDrafts[active.id]){try{await applyScheduleDraft(scheduleDrafts[active.id]);changeScheduleDraft(active.id,undefined);setScheduleRevisions(v=>({...v,[active.id]:(v[active.id]||0)+1}));}catch(e){message.error(errorText(e));return;}}
    if (saved && dirty) message.success("已保存到 MySQL");
    return saved;
  };
  const prepareWorkflow = (id: string) => saveWorkflowSubmission(captureWorkflowSubmission(id, objects, drafts), saveObject);
  const openPublication = () => {
    if (isRealWorkflow(active)) setWorkflowRelease({ workspaceId, target: active, tab: "publish" });
    else setReleaseOpen(true);
  };
  const openReleaseHistory = () => setWorkflowRelease({ workspaceId, tab: "history" });
  const closeTabs = (closing: string[]) => {
    if(!closing.length)return;
    const close=()=>{const next=afterClosingTabs(tabs,activeId,closing);setTabs(next.tabs);setActiveId(next.activeId);setSelectedId(next.activeId);setDrafts(current=>Object.fromEntries(Object.entries(current).filter(([id])=>!closing.includes(id))));setScheduleDrafts(current=>Object.fromEntries(Object.entries(current).filter(([id])=>!closing.includes(id))));};
    const unsaved=closing.filter(id=>drafts[id]||scheduleDrafts[id]);
    if(!unsaved.length){close();return;}
    let pending=false;const savedObjects=new Set<string>(),savedSchedules=new Set<string>();
    const dialog=modal.confirm({title:"关闭标签页 · 未保存的修改",content:<><p>以下文件有未保存的代码或调度配置：</p><ul>{unsaved.map(id=><li key={id}>{drafts[id]?.name||objects.find(o=>o.id===id)?.name||id}{scheduleDrafts[id]&&" · 调度配置"}</li>)}</ul></>,footer:()=> <Space><Button onClick={()=>{if(!pending)dialog.destroy();}}>取消</Button><Button danger onClick={()=>{if(!pending){close();dialog.destroy();}}}>丢弃并关闭</Button><Button type="primary" onClick={async()=>{if(pending)return;pending=true;try{for(const id of unsaved){const object=drafts[id]||objects.find(o=>o.id===id);if(object&&drafts[id]&&!savedObjects.has(id)){if(!await saveObject(object,true))return;savedObjects.add(id);}const schedule=scheduleDrafts[id];if(schedule&&!savedSchedules.has(id)){await applyScheduleDraft(schedule);savedSchedules.add(id);changeScheduleDraft(id,undefined);}}close();dialog.destroy();}catch(e){message.error(errorText(e));}finally{pending=false;}}}>保存并关闭</Button></Space>});
  };
  const closeTab=(id:string)=>closeTabs([id]);
  const tabMenu=(anchor:string)=>({items:(Object.keys(tabCloseLabels) as TabCloseAction[]).map(key=>({key,label:tabCloseLabels[key],disabled:!tabsToClose(tabs,anchor,key,new Set([...Object.keys(drafts),...Object.keys(scheduleDrafts)])).length})),onClick:({key}:{key:string})=>closeTabs(tabsToClose(tabs,anchor,key as TabCloseAction,new Set([...Object.keys(drafts),...Object.keys(scheduleDrafts)])))});
  const publishTask=async(id:string)=>{const object=drafts[id]||objects.find(o=>o.id===id);if(!object)return;const saved=await saveObject(object,!!drafts[id]);if(!saved)return;return api.publishTask(id,saved.version,"任务独立发布");};
  const switchWorkspace = async (id: string) => {
    if (id === widRef.current) return;
    const perform = async () => {
      try {
        const [os, rs, recs] = await Promise.all([
          api.objects(id),
          api.runs(id),
          api.records(id),
        ]);
        widRef.current = id;
        setWorkspaceId(id);
        setSchedulingSelection(undefined);
        setWorkflowRelease(null);
        setObjects(os);
        setRuns(rs);
        setRecords(recs);
        setDrafts({});setScheduleDrafts({});
        setTabs([]);
        setActiveId("");
        setSelectedId("");
        setFocusFolder(null);
        setResultId(null);
        setActivity(currentActivity => currentActivity === "realtime" ? "realtime" : "development");
        setExpanded(os.filter((o) => o.kind === "FOLDER").map((o) => o.id));
      } catch (e) {
        message.error(errorText(e));
      }
    };
    if (Object.keys(drafts).length || Object.keys(scheduleDrafts).length)
      modal.confirm({
        title: "切换工作空间",
        content: "当前工作空间有未保存的离线内容，切换会丢弃这些离线修改。实时草稿保留在各自工作空间。",
        okText: "丢弃并切换",
        onOk: perform,
      });
    else await perform();
  };
  const finishNodeRun = (request: NodeRunRequest) => {
    if (nodeRunRequest.current !== request) return;
    runRequests.current.delete(request.object.id);
    nodeRunRequest.current = null;
    setNodeRunPendingId(null);
    setNodeRunDialog(null);
    setNodeRunSubmitting(false);
  };
  const nodeRunIsCurrent = (request: NodeRunRequest) =>
    nodeRunRequest.current === request && request.object.id === activeIdRef.current && request.object.workspaceId === widRef.current;
  useEffect(() => {
    const pending = nodeRunRequest.current;
    if (pending && !nodeRunIsCurrent(pending)) {
      if (pending.phase === "SUBMITTING") setNodeRunDialog(null);
      else finishNodeRun(pending);
    }
  }, [activeId, workspaceId]);
  const submitNodeRun = async (request: NodeRunRequest, debugParameters?: Record<string, string>) => {
    if (!nodeRunIsCurrent(request) || ["SAVING", "SUBMITTING"].includes(request.phase)) return;
    request.phase = "SAVING";
    setNodeRunSubmitting(true);
    setNodeRunDialog(dialog => dialog?.request === request ? { ...dialog, error: undefined } : dialog);
    let keepDialog = false;
    try {
      const saved = await saveObject(request.object, request.needsSave);
      if (!nodeRunIsCurrent(request)) return;
      if (!saved) throw new Error("节点保存失败，请修正代码或调度配置后重试；本次未创建运行。");
      request.object = saved;
      request.needsSave = false;
      request.phase = "SUBMITTING";
      const run = await api.run(saved.id, request.fail, "MANUAL", saved.version, undefined, undefined, debugParameters);
      if (saved.workspaceId === widRef.current) {
        setRuns(items => [run, ...items]);
        if (saved.id === activeIdRef.current) {
          setResultId(run.id);
          setResultTab("logs");
        }
      }
      message.info(`${runLabel(run)}已提交`);
    } catch (error) {
      if (nodeRunIsCurrent(request)) {
        if (error instanceof ApiError && error.code === "MISSING_DEBUG_PARAMETERS") {
          try {
            const prepared = await api.prepareRunParameters(request.object);
            if (nodeRunIsCurrent(request)) {
              const overrides = debugParameters ?? prepared.debugParameters;
              const parameters = effectiveRunParameters(prepared, overrides);
              setNodeRunDialog({ request, preparation: { ...prepared, debugParameters: overrides, parameters, missingParameters: parameters.filter(row => !row.value).map(row => row.name) }, error: errorText(error) });
              keepDialog = true;
            }
          } catch (prepareError) { if (nodeRunIsCurrent(request)) message.error(errorText(prepareError)); }
        } else if (nodeRunDialog?.request === request) {
          setNodeRunDialog(dialog => dialog?.request === request ? { ...dialog, error: errorText(error) } : dialog);
          keepDialog = true;
        } else message.error(errorText(error));
      }
    } finally {
      if (nodeRunRequest.current === request) {
        if (keepDialog) { request.phase = "EDITING"; setNodeRunSubmitting(false); }
        else finishNodeRun(request);
      }
    }
  };
  const startNodeRun = async (object: StudioObject, fail: boolean, mode: "NORMAL" | "CUSTOM") => {
    const request: NodeRunRequest = { object, needsSave: !!drafts[object.id], fail, mode, phase: "PREPARING" };
    nodeRunRequest.current = request;
    runRequests.current.add(object.id);
    setNodeRunPendingId(object.id);
    try {
      const preparation = await api.prepareRunParameters(object);
      if (!nodeRunIsCurrent(request)) return;
      if (shouldOpenRunParameters(mode, preparation)) {
        request.phase = "EDITING";
        setNodeRunDialog({ request, preparation });
      } else await submitNodeRun(request);
    } catch (error) {
      if (nodeRunIsCurrent(request)) message.error(errorText(error));
      finishNodeRun(request);
    }
  };
  const runCurrent = async (fail = false, parameterMode: "NORMAL" | "CUSTOM" = "NORMAL") => {
    if (nodeRunRequest.current) return;
    if (
      !active ||
      runRequests.current.has(active.id) ||
      runs.some(
        (r) =>
          r.objectId === active.id && ["QUEUED", "RUNNING", "RECOVERING"].includes(r.status),
      )
    ) {
      message.info("当前节点已有正在提交或运行的任务");
      return;
    }
    if (active.kind === "NODE") {
      await startNodeRun(active, fail, parameterMode);
      return;
    }
    const requestedId = active.id;
    runRequests.current.add(requestedId);
    try {
      const prepared = isRealWorkflow(active) ? await prepareWorkflow(active.id) : undefined;
      const saved = prepared?.workflow || await saveCurrent(false);
      if (!saved) return;
      const run = await api.run(saved.id, fail, "MANUAL", saved.version, prepared?.expectedNodeVersions, isRealWorkflow(saved) ? businessDate : undefined);
      if (saved.workspaceId === widRef.current) {
        setRuns((r) => [run, ...r]);
        setResultId(run.id);
        setResultTab(run.provider === "WORKFLOW" ? "result" : "logs");
        if (run.provider === "WORKFLOW") setResultHeight(Math.min(480, Math.round(window.innerHeight * 0.52)));
      }
      message.info(`${runLabel(run)}已提交`);
    } catch (e) {
      message.error(errorText(e));
    } finally {
      runRequests.current.delete(requestedId);
    }
  };
  const stopCurrent = async () => {
    const run = runs.find(
      (r) =>
        r.objectId === activeId && ["QUEUED", "RUNNING", "RECOVERING"].includes(r.status),
    );
    if (run)
      try {
        const r = await api.stop(run.id);
        setRuns((rs) => rs.map((x) => (x.id === r.id ? r : x)));
      } catch (e) {
        message.error(errorText(e));
      }
  };
  const formatCurrent = () => {
    if (!active) return;
    try {
      if (/SQL|MySQL|Doris|Hive|PostgreSQL|Oracle/i.test(active.nodeType)) {
        change({
          content: formatSQL(active.content, {
            language: "sql",
            tabWidth: 4,
            keywordCase: "upper",
          }),
        });
        message.success("格式化完成，保存后生效");
      } else if (active.content.trim().startsWith("{"))
        change({
          content: JSON.stringify(JSON.parse(active.content), null, 4),
        });
      else message.info("当前类型保留原始代码格式，可使用编辑器格式化能力");
    } catch (e) {
      message.error("无法格式化：" + errorText(e));
    }
  };
  const publish = async () => {
    setPublishing(true);
    try {
      const saved = await saveCurrent(false);
      if (!saved) return;
      if(isRealTask(saved)){const release=await api.publishTask(saved.id,saved.version,releaseNote);message.success(`已发布任务 R${release.releaseNo}`);setReleaseOpen(false);setReleaseNote("");return;}
      await api.record({
        workspaceId,
        objectId: saved.id,
        kind: "RELEASE",
        title: saved.name,
        status: "SUCCESS",
        payload: {
          simulation: true,
          environment: releaseEnv,
          note: releaseNote,
          version: saved.version,
          content: saved.content,
          config: saved.config,
        },
      });
      await refresh();
      setReleaseOpen(false);
      setReleaseNote("");
      message.success(`本地发布记录已保存 · ${releaseEnv}环境`);
    } catch (e) {
      message.error(errorText(e));
    } finally {
      setPublishing(false);
    }
  };
  const openCreate = (
    kind: ObjectKind,
    nodeType = "",
    parentId: string | null = null,
  ) => {
    const type =
      nodeType ||
      (kind === "WORKFLOW"
        ? "周期工作流"
        : kind === "NOTEBOOK"
          ? "Notebook"
          : kind === "NODE"
            ? "MySQL"
            : kind === "PERSONAL"
              ? "SQL"
              : kind === "TABLE"
                ? "MaxCompute"
                : kindNames[kind]);
    setCreateSpec({ kind, nodeType: type, parentId });
    createForm.resetFields();
    createForm.setFieldsValue({ name: "", description: "", parentId });
    if (kind === "NODE" && !nodeType) {
      setPalette(true);
      setNodeGroup("全部");
      setNodeSearch("");
    } else setPalette(false);
  };
  const createObject = async () => {
    try {
      const values = await createForm.validateFields();
      if (!createSpec) return;
      setCreating(true);
      const cfg: Record<string, unknown> = {
        run: {
          computeResource: "local_demo",
          resourceGroup: "本地模拟资源组",
          parameters: {},
          priority: 3,
          mode: "DEVELOPMENT",
        },
        schedule: {
          ...defaultSchedule, parameters: [],
          type:
            createSpec.nodeType === "MANUAL_WORKFLOW" ||
            createSpec.nodeType.includes("手动")
              ? "MANUAL"
              : createSpec.nodeType.includes("触发")
                ? "TRIGGER"
                : "CYCLE",
          cycle: "DAY",
          cron: "0 0 2 * * *",
          retries: 0,
          dependencies: [],
        },
      };
      if (createSpec.kind === "NODE" && ["MySQL", "Doris"].includes(createSpec.nodeType)) cfg.run = { provider: sqlProvider(createSpec.nodeType), dataSourceId: values.dataSourceId, executionMode: "QUERY", timeoutSeconds: 30 };
      if (createSpec.kind === "NODE" && ["离线同步","数据集成"].includes(createSpec.nodeType)) {cfg.run={provider:"SYNC"};cfg.sync={writeMode:"append",columns:[],mapping:[],batchRows:10000,timeoutSeconds:3600,parallelism:1};}
      if (createSpec.kind === "WORKFLOW") { cfg.graph = { nodes: [], edges: [] }; cfg.run = { provider: "WORKFLOW" }; }
      if (createSpec.kind === "NOTEBOOK") cfg.cells = [];
      if (createSpec.kind === "TABLE") {
        cfg.columns = [
          { name: "id", type: "BIGINT", comment: "主键", primaryKey: true },
        ];
        cfg.sampleRows = [];
      }
      const created = await api.create({
        workspaceId,
        kind: createSpec.kind,
        nodeType: createSpec.nodeType,
        name: values.name.trim(),
        description: values.description || "",
        parentId: values.parentId || null,
        content: defaultContent(createSpec.nodeType) || "",
        config: cfg,
        tags: [],
      });
      await refresh();
      setCreateSpec(null);
      setPalette(false);
      openObject(created.id);
      setExpanded((e) => [
        ...new Set([
          ...e,
          ...(created.parentId ? [created.parentId] : []),
          created.id,
        ]),
      ]);
      message.success("创建成功");
    } catch (e) {
      if (!(e as { errorFields?: unknown })?.errorFields)
        message.error(errorText(e));
    } finally {
      setCreating(false);
    }
  };
  const deleteObject = (object: StudioObject) =>
    modal.confirm({
      title: `删除${kindNames[object.kind]}？`,
      content: `“${object.name}”${object.kind === "FOLDER" ? "及其下属对象" : ""}将移入回收站，可恢复。`,
      okText: "移入回收站",
      okButtonProps: { danger: true },
      onOk: async () => {
        await api.remove(object.id);
        await refresh();
        setTabs((t) => t.filter((id) => id !== object.id));
        setDrafts((d) => {
          const copy = { ...d };
          delete copy[object.id];
          return copy;
        });
        if (activeId === object.id) setActiveId("");
        message.success("已移入回收站");
      },
    });
  const favorite = async (o: StudioObject) => {
    try {
      const updated = await api.save({ ...o, favorite: !o.favorite });
      setObjects((os) => os.map((x) => (x.id === o.id ? updated : x)));
      setDrafts((ds) =>
        ds[o.id]
          ? {
              ...ds,
              [o.id]: {
                ...ds[o.id],
                favorite: updated.favorite,
                version: updated.version,
              },
            }
          : ds,
      );
    } catch (e) {
      message.error(errorText(e));
    }
  };
  const editObject = (kind: "rename" | "move" | "copy", o: StudioObject) => {
    setEditAction({ kind, object: o });
    setEditName(kind === "copy" ? o.name + "_copy" : o.name);
    setEditParent(o.parentId);
  };
  const completeEdit = async () => {
    if (!editAction) return;
    const { kind, object } = editAction;
    try {
      if (kind === "copy") {
        const copied = await api.copy(object.id, editName.trim(), editParent);
        await refresh();
        openObject(copied.id);
      } else {
        const updated = await api.save({
          ...object,
          name: kind === "rename" ? editName.trim() : object.name,
          parentId: kind === "move" ? editParent : object.parentId,
        });
        updateMetadata(updated, kind === "rename" ? ["name"] : ["parentId"]);
        await refresh();
      }
      setEditAction(null);
      message.success("操作成功");
    } catch (e) {
      message.error(errorText(e));
    }
  };
  const menuFor = (o: StudioObject) => [
    { key: "open", label: "打开", icon: <FileCode2 size={14} /> },
    {
      key: "favorite",
      label: o.favorite ? "取消收藏" : "收藏",
      icon: <Star size={14} />,
    },
    { type: "divider" as const },
    { key: "rename", label: "重命名", icon: <FilePenLine size={14} /> },
    { key: "copy", label: "复制", icon: <Copy size={14} /> },
    { key: "move", label: "移动", icon: <FolderInput size={14} /> },
    ...(o.kind === "FOLDER"
      ? [
          {
            key: "focus",
            label: "进入专注模式",
            icon: <Maximize2 size={14} />,
          },
          { key: "create", label: "新建节点", icon: <Plus size={14} /> },
        ]
      : []),
    { type: "divider" as const },
    { key: "delete", label: "删除", danger: true, icon: <Trash2 size={14} /> },
  ];
  const contextAction = (key: string, o: StudioObject) => {
    setContext(null);
    if (key === "open") openObject(o.id);
    if (key === "favorite") void favorite(o);
    if (["rename", "copy", "move"].includes(key))
      editObject(key as "rename", o);
    if (key === "delete") deleteObject(o);
    if (key === "create") openCreate("NODE", "", o.id);
    if (key === "focus") {
      setFocusFolder(o.id);
      setExpanded((e) => [...e, o.id]);
    }
  };
  const safeRefresh = () =>
    void refresh().catch((e) => message.error(errorText(e)));
  const savePreferences = (patch: Partial<Preferences>) =>
    setPrefs((p) => ({ ...p, ...patch }));
  const resize = (e: React.PointerEvent, kind: "side" | "result") => {
    e.preventDefault();
    const sx = e.clientX,
      sy = e.clientY,
      start = kind === "side" ? sidebarWidth : resultHeight;
    const move = (ev: PointerEvent) =>
      kind === "side"
        ? setSidebarWidth(Math.max(170, Math.min(440, start + ev.clientX - sx)))
        : setResultHeight(
            Math.max(
              120,
              Math.min(window.innerHeight - 260, start + sy - ev.clientY),
            ),
          );
    const up = () => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", up);
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", up);
  };
  useEffect(() => {
    const key = (e: KeyboardEvent) => {
      const target = e.target as HTMLElement;
      if (activity === "development" && nodeRunDialog && (
        e.key === "F8" ||
        ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "s") ||
        (e.altKey && e.shiftKey && e.key.toLowerCase() === "f")
      )) {
        e.preventDefault();
        return;
      }
      if (activity === "development" && (e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "s") {
        e.preventDefault();
        void saveCurrent();
      }
      if (activity === "development" && e.key === "F8") {
        e.preventDefault();
        if (!e.shiftKey || active?.kind === "NODE") void runCurrent(false, e.shiftKey ? "CUSTOM" : "NORMAL");
      }
      if (activity === "development" && e.key === "F9") {
        e.preventDefault();
        void stopCurrent();
      }
      if (activity === "development" && e.altKey && e.shiftKey && e.key.toLowerCase() === "f") {
        e.preventDefault();
        formatCurrent();
      }
      if (e.key === "Escape") {
        setContext(null);
        setZen(false);
      }
      if (e.ctrlKey && e.shiftKey && e.key.toLowerCase() === "e") {
        e.preventDefault();
        setActivity("development");
        setSidebarVisible(true);
      }
      if (
        e.ctrlKey &&
        activity === "development" &&
        e.key.toLowerCase() === "p" &&
        !["INPUT", "TEXTAREA"].includes(target.tagName)
      ) {
        e.preventDefault();
        document.getElementById("explorer-search")?.focus();
      }
    };
    window.addEventListener("keydown", key);
    return () => window.removeEventListener("keydown", key);
  });
  const projectObjects = objects.filter(o => o.kind !== "PERSONAL");
  const visibleSet = useMemo(() => {
    const set = new Set<string>();
    const candidates = projectObjects.filter(
      (o) =>
        (!search ||
          `${o.name} ${o.id} ${o.owner}`
            .toLowerCase()
            .includes(search.toLowerCase())) &&
        (filter !== "favorite" || o.favorite) &&
        (filter !== "mine" || o.owner === "local_admin"),
    );
    for (const o of candidates) {
      set.add(o.id);
      let parent = objects.find((x) => x.id === o.parentId);
      const seen = new Set<string>();
      while (parent && !seen.has(parent.id)) {
        set.add(parent.id);
        seen.add(parent.id);
        parent = objects.find((x) => x.id === parent!.parentId);
      }
    }
    return set;
  }, [objects, search, filter]);
  const treeNodes = (
    parentId: string | null,
    visited = new Set<string>(),
  ): DataNode[] =>
    projectObjects
      .filter(
        (o) =>
          o.parentId === parentId && visibleSet.has(o.id) && !visited.has(o.id),
      )
      .map((o) => ({
        key: o.id,
        title: (
          <span className="tree-title">
            <span>{o.name}</span>
            {drafts[o.id] ? (
              <i className="dirty-dot" />
            ) : o.favorite ? (
              <Star size={10} className="star" />
            ) : null}
          </span>
        ),
        icon: <ObjectIcon object={o} />,
        isLeaf: o.kind !== "FOLDER",
        children:
          o.kind === "FOLDER"
            ? treeNodes(o.id, new Set([...visited, o.id]))
            : undefined,
      }));
  const folderOptions = [
    { value: "", label: "项目根目录" },
    ...objects
      .filter((o) => o.kind === "FOLDER")
      .map((o) => ({ value: o.id, label: o.name })),
  ];
  const selected = objects.find((o) => o.id === selectedId);
  const defaultParent =
    selected?.kind === "FOLDER" ? selected.id : selected?.parentId || null;
  const newMenu = [
    { key: "NODE", label: "新建节点", icon: <FileCode2 size={14} /> },
    { key: "WORKFLOW", label: "新建工作流", icon: <GitBranch size={14} /> },
    { key: "NOTEBOOK", label: "新建 Notebook", icon: <BookOpen size={14} /> },
    { key: "FOLDER", label: "新建目录", icon: <FolderClosed size={14} /> },
    { type: "divider" as const },
    { key: "PERSONAL", label: "新建个人文件", icon: <FilePenLine size={14} /> },
  ];
  const nodeGroups = ["全部", ...new Set(nodeTypes.map((t) => t.group))];
  const filteredTypes = nodeTypes.filter(
    (t) =>
      (nodeGroup === "全部" || t.group === nodeGroup) &&
      (!nodeSearch ||
        `${t.label} ${t.group}`
          .toLowerCase()
          .includes(nodeSearch.toLowerCase())),
  );
  const runSelect = (r: Run) => {
    setSelectedRun(r);
    setResultId(r.id);
    setResultTab(r.provider === "WORKFLOW" ? "result" : "logs");
    if (r.provider === "WORKFLOW") setResultHeight(Math.min(480, Math.round(window.innerHeight * 0.52)));
  };
  if (loading)
    return (
      <div className="boot-screen">
        <Layers3 size={35} />
        <h3>SinketDataWorks</h3>
        <Spin />
        <span>正在连接本地工作空间</span>
      </div>
    );
  if (fatal)
    return (
      <div className="boot-screen">
        <Database size={35} />
        <h3>工作空间连接失败</h3>
        <Alert type="error" title={fatal} />
        <p>请运行项目根目录下的启动脚本，再重试连接。</p>
        <Button type="primary" onClick={() => void bootstrap()}>
          重新连接
        </Button>
      </div>
    );
  return (
    <div className={`studio-app ${zen ? "zen" : ""}`}>
      {!zen && notice && (
        <div className="announcement">
          <Info size={13} />
          <span>
            【本地工作空间】SinketDataWorks
            离线与实时任务、发布版本保存在服务器，浏览器保留实时草稿与标签页状态。
          </span>
          <button onClick={() => setHelp(true)}>使用说明</button>
          <span className="announcement-spacer" />
          <button onClick={() => setNotice(false)}>本次关闭</button>
          <Tool label="关闭公告" icon={X} onClick={() => setNotice(false)} />
        </div>
      )}
      {!zen && (
        <header className="global-header">
          <Dropdown trigger={["click"]} menu={{items: activities.map(a => ({ key: a.id, label: a.label })), onClick: ({key}) => {if(key==="scheduling")setSchedulingSelection(undefined);setActivity(key as Activity);}}}><button className="product-toggle" aria-label="切换板块"><MenuIcon size={17}/></button></Dropdown>
          <div className="brand">
            <Layers3 size={21} />
            <span>
              SinketDataWorks
            </span>
          </div>
          <div className="header-divider" />
          <span className="header-select region-select">{workspace?.region || "本地"}</span>
          <div className="header-divider" />
          <Select
            aria-label="工作空间"
            className="workspace-select"
            variant="borderless"
            value={workspaceId}
            onChange={(id) => void switchWorkspace(id)}
            popupRender={menu => <>{menu}<div className="workspace-popup-footer"><Button type="text" block icon={<Settings2 size={14} />} onClick={() => setWorkspaceManagerOpen(true)}>工作空间管理</Button></div></>}
            options={workspaces.map((w) => ({
              value: w.id,
              label: (
                <span className="workspace-option">
                  <LayoutPanelLeft size={15} />
                  <span>
                    {w.name}
                    <small>{w.code}</small>
                  </span>
                </span>
              ),
            }))}
          />
          <Tool label="工作空间管理" icon={Settings2} onClick={() => setWorkspaceManagerOpen(true)} />
          <div className="header-divider" />
          <div className="header-fill" />
          <Dropdown
            menu={{
              items: [
                { key: "workspace", label: "工作空间管理" },
                { key: "settings", label: "工作台设置" },
                { key: "release", label: "发布记录" },
                { key: "help", label: "使用帮助" },
              ],
              onClick: ({ key }) =>
                key === "workspace"
                  ? setWorkspaceManagerOpen(true)
                  : key === "settings"
                  ? setSettings(true)
                  : key === "help"
                    ? setHelp(true)
                    : openReleaseHistory(),
            }}
          >
            <button className="header-select">
              更多
              <ChevronDown size={11} />
            </button>
          </Dropdown>
          <Tool
            label="通知"
            icon={Bell}
            onClick={() => setNotificationOpen(true)}
          />
          <span className="local-avatar" title="本地单用户 local_admin">
            L
          </span>
        </header>
      )}
      <div className="workspace-body">
        {!zen && (
          <nav className="activity-bar" aria-label="活动视图切换器">
            {activities.map((a) => (
              <Tooltip key={a.id} title={a.label} placement="right">
                <button
                  aria-label={a.label}
                  className={`activity-button ${activity === a.id ? "selected" : ""}`}
                  onClick={() => {
                    if(a.id==="scheduling")setSchedulingSelection(undefined);
                    setActivity(a.id);
                    setSidebarVisible(true);
                  }}
                >
                  <a.icon size={19} />
                </button>
              </Tooltip>
            ))}
            <div className="activity-fill" />
            <Tooltip title="查看帮助" placement="right">
              <button
                className="activity-button"
                onClick={() => setHelp(true)}
                aria-label="查看帮助"
              >
                <HelpCircle size={17} />
              </button>
            </Tooltip>
            <Tooltip title="切换主题" placement="right">
              <button
                className="activity-button"
                onClick={() => setMode(mode === "dark" ? "light" : "dark")}
                aria-label="切换主题"
              >
                {mode === "dark" ? <Moon size={17} /> : <Sun size={17} />}
              </button>
            </Tooltip>
            <Tooltip title="管理设置" placement="right">
              <button
                className="activity-button"
                onClick={() => setSettings(true)}
                aria-label="管理设置"
              >
                <Settings2 size={17} />
              </button>
            </Tooltip>
            <Tooltip title="折叠侧栏" placement="right">
              <button
                className="activity-button"
                onClick={() => setSidebarVisible(!sidebarVisible)}
                aria-label="折叠侧栏"
              >
                {sidebarVisible ? (
                  <PanelLeftClose size={16} />
                ) : (
                  <PanelLeftOpen size={16} />
                )}
              </button>
            </Tooltip>
          </nav>
        )}
        {!zen && sidebarVisible && activity !== "realtime" && (
          <>
            <aside className="explorer" style={{ width: sidebarWidth }}>
              <div className="pane-heading">
                <span>{activities.find((a) => a.id === activity)?.label}</span>
                <Dropdown
                  menu={{
                    items: [
                      { key: "refresh", label: "刷新" },
                      { key: "collapse", label: "全部折叠" },
                      {
                        key: "focus",
                        label: focusFolder ? "退出专注模式" : "定位当前文件",
                      },
                    ],
                    onClick: ({ key }) =>
                      key === "refresh"
                        ? safeRefresh()
                        : key === "collapse"
                          ? setExpanded([])
                          : focusFolder
                            ? setFocusFolder(null)
                            : setSelectedId(activeId),
                  }}
                  trigger={["click"]}
                >
                  <button className="icon-button" aria-label="视图和更多操作">
                    <MoreHorizontal size={15} />
                  </button>
                </Dropdown>
              </div>
              {activity === "development" ? (
                <>
                  <div className="explorer-search">
                    <Input
                      id="explorer-search"
                      aria-label="搜索名称或节点ID"
                      value={search}
                      onChange={(e) => setSearch(e.target.value)}
                      allowClear
                      placeholder="搜索名称 / 节点ID / 修改人"
                      suffix={<Search size={13} />}
                    />
                  </div>
                  <div className="explorer-section-head">
                    <ChevronDown size={12} />
                    <b>
                      {focusFolder
                        ? objects.find((o) => o.id === focusFolder)?.name
                        : "项目目录"}
                    </b>
                    <span className="small-muted">
                      {focusFolder ? "专注" : "全部"}
                    </span>
                    <div className="section-tools">
                      <Tool
                        label="代码搜索"
                        icon={Search}
                        onClick={() => {
                          setHelp(false);
                          document.getElementById("explorer-search")?.focus();
                        }}
                      />
                      <Tool
                        label="刷新目录"
                        icon={RefreshCw}
                        onClick={safeRefresh}
                      />
                      <Dropdown
                        trigger={["click"]}
                        menu={{
                          items: newMenu,
                          onClick: ({ key }) =>
                            openCreate(key as ObjectKind, "", defaultParent),
                        }}
                      >
                        <button className="icon-button" aria-label="新建">
                          <Plus size={15} />
                        </button>
                      </Dropdown>
                      <Tool
                        label="全部折叠"
                        icon={ChevronsDownUp}
                        onClick={() => setExpanded([])}
                      />
                    </div>
                  </div>
                  <div className="explorer-filters">
                    {[
                      ["all", "全部"],
                      ["mine", "我负责的"],
                      ["favorite", "我收藏的"],
                    ].map(([value, label]) => (
                      <button
                        key={value}
                        className={filter === value ? "selected" : ""}
                        onClick={() => setFilter(value)}
                      >
                        {label}
                      </button>
                    ))}
                  </div>
                  <div className="project-tree">
                    {focusFolder && (
                      <button
                        className="focus-exit"
                        onClick={() => setFocusFolder(null)}
                      >
                        ← 返回全部项目
                      </button>
                    )}
                    <Tree.DirectoryTree
                      blockNode
                      showIcon
                      expandAction="doubleClick"
                      treeData={treeNodes(focusFolder)}
                      expandedKeys={
                        search
                          ? objects
                              .filter((o) => o.kind === "FOLDER")
                              .map((o) => o.id)
                          : expanded
                      }
                      onExpand={(keys) => setExpanded(keys)}
                      selectedKeys={[selectedId || activeId]}
                      onSelect={(keys, info) => {
                        const id = String(keys[0] || "");
                        setSelectedId(id);
                        if (info.node.isLeaf) openObject(id);
                      }}
                      onDoubleClick={(_, node) => openObject(String(node.key))}
                      onRightClick={({ event, node }) => {
                        event.preventDefault();
                        const o = objects.find(
                          (x) => x.id === String(node.key),
                        );
                        if (o) {
                          setSelectedId(o.id);
                          setContext({
                            x: event.clientX,
                            y: event.clientY,
                            object: o,
                          });
                        }
                      }}
                    />
                    {!treeNodes(focusFolder).length && (
                      <div className="tree-empty">
                        没有匹配的项目
                        <br />
                        <button onClick={() => openCreate("NODE")}>
                          新建节点
                        </button>
                      </div>
                    )}
                  </div>
                  <div className="personal-explorer">
                    <div className="explorer-section-head">
                      <ChevronDown size={12} />
                      <b>个人目录</b>
                      <div className="section-tools">
                        <Tool
                          label="刷新个人目录"
                          icon={RefreshCw}
                          onClick={safeRefresh}
                        />
                        <Tool
                          label="新建个人文件"
                          icon={Plus}
                          onClick={() => openCreate("PERSONAL")}
                        />
                      </div>
                    </div>
                    <div className="personal-label">
                      <ChevronDown size={12} />
                      <UserRoundCheck size={14} />
                      我的文件
                    </div>
                    {objects
                      .filter((o) => o.kind === "PERSONAL")
                      .map((o) => (
                        <button
                          key={o.id}
                          className={`personal-file ${activeId === o.id ? "selected" : ""}`}
                          onClick={() => openObject(o.id)}
                          onContextMenu={(e) => {
                            e.preventDefault();
                            setContext({
                              x: e.clientX,
                              y: e.clientY,
                              object: o,
                            });
                          }}
                        >
                          <ObjectIcon object={o} />
                          {o.name}
                        </button>
                      ))}
                    <div className="personal-label muted">
                      <ChevronRight size={12} />
                      <UserRoundCheck size={14} />
                      他人文件（只读）
                    </div>
                  </div>
                </>
              ) : (
                <div className="secondary-explorer"><div className="section-summary"><span className="small-eyebrow">{workspace?.name}</span><h3>{activities.find(a => a.id === activity)?.label}</h3></div><button className="side-view-button" onClick={() => setActivity("development")}><CodeXml size={15} />返回离线数据开发</button></div>
              )}
            </aside>
            <div
              className="vertical-resizer"
              role="separator"
              aria-label="调整侧栏宽度"
              onPointerDown={(e) => resize(e, "side")}
            />
          </>
        )}
        <main className="workbench-main">
          {activity === "realtime" ? <RealtimeWorkbench key={workspaceId} workspaceId={workspaceId} theme={mode} fontSize={prefs.editorFontSize || 13} wordWrap={!!prefs.wordWrap} sidebarVisible={sidebarVisible} sidebarWidth={sidebarWidth} onManageDatasources={() => setActivity("datasources")} /> : <>
          <div className="editor-tabs" role="tablist" aria-label="编辑文件">
            {activity !== "development" && (
              <button
                className="editor-tab selected management-tab"
                role="tab"
                aria-selected="true"
              >
                <LayoutPanelLeft size={14} />
                {activities.find((a) => a.id === activity)?.label}
                <span onClick={() => setActivity("development")}>
                  <X size={13} />
                </span>
              </button>
            )}
            {tabs
              .filter((id) => objects.some((o) => o.id === id))
              .map((id) => {
                const o = drafts[id] || objects.find((x) => x.id === id)!;
                return (
                  <Dropdown key={id} menu={tabMenu(id)} trigger={["contextMenu"]}><div
                    role="tab"
                    tabIndex={0}
                    aria-selected={
                      id === activeId && activity === "development"
                    }
                    aria-label={o.name}
                    key={id}
                    className={`editor-tab ${id === activeId && activity === "development" ? "selected" : ""}`}
                    onClick={() => openObject(id)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") openObject(id);
                    }}
                  >
                    <ObjectIcon object={o} />
                    <span>{o.name}</span>
                    <button
                      aria-label={`关闭 ${o.name}`}
                      onClick={(e) => {
                        e.stopPropagation();
                        closeTab(id);
                      }}
                    >
                      {drafts[id] || scheduleDrafts[id] ? (
                        <i className="dirty-dot" />
                      ) : (
                        <X size={13} />
                      )}
                    </button>
                  </div></Dropdown>
                );
              })}
            <div className="tabs-fill" />
            <Tool
              label={zen ? "退出专注模式" : "专注模式"}
              icon={zen ? Minimize2 : Maximize2}
              onClick={() => setZen(!zen)}
            />
            <Dropdown
              menu={{...tabMenu(activeId),items:[...tabMenu(activeId).items,{key:"settings",label:"编辑器设置",disabled:false}],onClick:({key})=>key==="settings"?setSettings(true):tabMenu(activeId).onClick({key})}}
              trigger={["click"]}
            >
              <button className="icon-button" aria-label="标签页更多操作">
                <MoreHorizontal size={16} />
              </button>
            </Dropdown>
          </div>
          {activity === "development" ? (
            active ? (
              <>
                <div className="editor-toolbar">
                  <div className="toolbar-actions">
                    {isRealWorkflow(active) && <BusinessDateInput size="small" label="开发调试业务日期" value={businessDate} style={{width:140}} onChange={setBusinessDate}/>}
                    <button
                      disabled={isRunning || nodeRunPendingId === active.id || active.kind === "FOLDER"}
                      onClick={() => void runCurrent()}
                      title={isRealWorkflow(active) ? "开发调试 (F8)" : "运行 (F8)"}
                    >
                      <PlayCircle size={15} />
                      <span>{isRealWorkflow(active) ? "开发调试" : "运行"}</span>
                    </button>
                    {active.kind === "NODE" && <button
                      disabled={isRunning || nodeRunPendingId === active.id}
                      onClick={() => void runCurrent(false, "CUSTOM")}
                      title="带参运行 (Shift+F8)"
                    >
                      <Settings2 size={15} />
                      <span>带参运行</span>
                    </button>}
                    <button
                      disabled={!isRunning}
                      onClick={() => void stopCurrent()}
                      title="停止 (F9)"
                    >
                      <StopCircle size={15} />
                      <span>停止</span>
                    </button>
                    <button
                      disabled={!dirty || saving}
                      onClick={() => void saveCurrent()}
                      title="保存 (Ctrl+S)"
                    >
                      <Save size={15} />
                      <span>{saving ? "保存中" : "保存"}</span>
                    </button>
                    <i />
                    <button
                      onClick={formatCurrent}
                      disabled={["FOLDER", "WORKFLOW", "TABLE"].includes(
                        active.kind,
                      )}
                      title="格式化 (Shift+Alt+F)"
                    >
                      <Braces size={15} />
                      <span>格式化</span>
                    </button>
                    <button
                      onClick={() => {
                        if (dirty) {
                          modal.confirm({
                            title: "重新加载文件？",
                            content: "这会丢弃当前未保存的修改。",
                            onOk: async () =>
                              updateServerObject(await api.object(active.id)),
                          });
                        } else
                          void api
                            .object(active.id)
                            .then(updateServerObject)
                            .catch((e) => message.error(errorText(e)));
                      }}
                    >
                      <RefreshCw size={14} />
                      <span>刷新</span>
                    </button>
                    <button
                      onClick={() =>
                        modal.info({
                          title: "费用预估",
                          content: (
                            <>
                              <Tag color="blue">本地模拟</Tag>
                              <p>
                                当前环境不调用云计算服务，不产生云端计算费用。
                              </p>
                              <p>
                                实际费用需要连接真实引擎后按其计费规则估算。
                              </p>
                            </>
                          ),
                        })
                      }
                    >
                      <Coins size={14} />
                      <span>费用预估</span>
                    </button>
                    <button
                      onClick={openPublication}
                      disabled={active.kind === "FOLDER"}
                    >
                      <Send size={14} />
                      <span>发布</span>
                    </button>
                    {active.kind === "WORKFLOW" && <button onClick={() => setWorkflowRelease({ workspaceId, target: isRealWorkflow(active) ? active : undefined, tab: "history" })}><PlayCircle size={14} /><span>运行已发布版本</span></button>}
                    <button onClick={() => setShareOpen(true)}>
                      <Share2 size={14} />
                      <span>分享</span>
                    </button>
                  </div>
                  <Dropdown
                    menu={{
                      items: [
                        {
                          key: "release",
                          label: "发布到本地",
                          disabled: active.kind === "FOLDER",
                        },
                        { key: "share", label: "分享开发对象" },
                        { type: "divider" },
                        { key: "fail", label: "模拟失败运行", disabled: ["MYSQL", "DORIS", "SYNC", "WORKFLOW"].includes(active.config?.run?.provider) },
                      ],
                      onClick: ({ key }) => {
                        if (key === "release") openPublication();
                        else if (key === "share") setShareOpen(true);
                        else if (key === "fail") void runCurrent(true);
                      },
                    }}
                    trigger={["click"]}
                  >
                    <button className="toolbar-more" aria-label="更多节点操作">
                      <MoreHorizontal size={16} />
                    </button>
                  </Dropdown>
                </div>
                {active.kind === "NODE" && ["MySQL", "Doris"].includes(active.nodeType) && <DatasourceBinding key={active.id} object={active} onChange={change} onManage={() => setActivity("datasources")} />}
                <div className="editor-content">
                  <div className="editor-surface">
                    {active.kind === "FOLDER" ? (
                      <div className="folder-dashboard">
                        <div className="folder-breadcrumb">
                          <FolderOpen size={22} />
                          <div>
                            <h2>{active.name}</h2>
                            <p>
                              {active.description ||
                                "项目目录 · 开发对象与工作流"}
                            </p>
                          </div>
                          <Button
                            icon={<Plus size={14} />}
                            onClick={() => openCreate("NODE", "", active.id)}
                          >
                            新建节点
                          </Button>
                        </div>
                        <div className="folder-stats">
                          <span>
                            <b>
                              {
                                objects.filter((o) => o.parentId === active.id)
                                  .length
                              }
                            </b>{" "}
                            目录对象
                          </span>
                          <span>
                            <b>
                              {
                                objects.filter(
                                  (o) =>
                                    o.parentId === active.id &&
                                    o.kind === "WORKFLOW",
                                ).length
                              }
                            </b>{" "}
                            工作流
                          </span>
                        </div>
                        <Table
                          size="small"
                          rowKey="id"
                          pagination={false}
                          dataSource={objects.filter(
                            (o) => o.parentId === active.id,
                          )}
                          columns={[
                            {
                              title: "名称",
                              dataIndex: "name",
                              render: (_, o) => (
                                <button
                                  className="text-link"
                                  onClick={() => openObject(o.id)}
                                >
                                  <ObjectIcon object={o} />
                                  {o.name}
                                </button>
                              ),
                            },
                            { title: "类型", dataIndex: "nodeType" },
                            {
                              title: "修改时间",
                              dataIndex: "updatedAt",
                              render: stamp,
                            },
                          ]}
                        />
                      </div>
                    ) : (
                      <StudioEditor
                        object={active}
                        onChange={change}
                        objects={objects}
                        onOpen={openObject}
                        theme={mode}
                        fontSize={Number(prefs.editorFontSize) || 13}
                        wordWrap={!!prefs.wordWrap}
                      />
                    )}
                  </div>
                  {inspector && (
                    <aside className={`inspector-pane ${inspector === "schedule" ? "schedule-wide" : ""}`}>
                      <div className="pane-heading">
                        <b>
                          {
                            {
                              schedule: "调度配置",
                              versions: "版本",
                            }[inspector]
                          }
                        </b>
                        <Tool
                          label="关闭配置面板"
                          icon={X}
                          onClick={() => setInspector(null)}
                        />
                      </div>
                      <Inspector
                        object={active}
                        section={inspector}
                        onRun={runSelect}
                        onChange={change}
                        onRestored={(o) => {
                          updateServerObject(o);
                          message.success("版本已恢复");
                        }}
                        objects={objects}
                        scheduleRevision={scheduleRevisions[active.id]||0}
                        scheduleDraft={scheduleDrafts[active.id]}
                        onScheduleDraft={draft=>changeScheduleDraft(active.id,draft)}
                        onPublishTask={()=>publishTask(active.id)}
                        onViewInstances={(kind,scheduleId)=>{setSchedulingSelection({kind,scheduleId});setActivity("scheduling");}}
                        onSaveObject={async()=>!!await saveObject(active,!!drafts[active.id])}
                      />
                    </aside>
                  )}
                  <div className="inspector-rail">
                    {(
                      ["schedule", "versions"] as const
                    ).map((section) => (
                      <button
                        className={section === inspector ? "selected" : ""}
                        key={section}
                        onClick={() => {
                          if (section === "versions" && dirty) {
                            modal.confirm({
                              title: "当前文件有未保存修改",
                              content:
                                "版本恢复将替换当前内容。建议先保存；仍可查看版本记录。",
                              okText: "查看版本",
                              onOk: () => setInspector(section),
                            });
                          } else
                            setInspector(
                              inspector === section ? null : section,
                            );
                        }}
                      >
                        {
                          {
                            schedule: "调度配置",
                            versions: "版本",
                          }[section]
                        }
                      </button>
                    ))}
                  </div>
                </div>
              </>
            ) : (
              <div className="welcome-workbench">
                <Layers3 size={70} strokeWidth={1} />
                <h2>SinketDataWorks</h2>
                <p>在左侧目录打开文件，或创建新的开发任务</p>
                <div className="welcome-actions">
                  <button onClick={() => openCreate("NODE")}>
                    <FileCode2 size={17} />
                    新建节点<span>Ctrl + N</span>
                  </button>
                  <button onClick={() => openCreate("WORKFLOW")}>
                    <GitBranch size={17} />
                    新建工作流
                  </button>
                  <button onClick={() => openCreate("NOTEBOOK")}>
                    <BookOpen size={17} />
                    新建 Notebook
                  </button>
                </div>
                <span className="welcome-foot">
                  本地工作空间 · 所有开发元数据持久化保存
                </span>
              </div>
            )
          ) : (
            <div className="management-content">
              {activity === "scheduling" ? <SchedulingOperations workspaceId={workspaceId} selection={schedulingSelection} onConfigure={id=>{openObject(id);setInspector("schedule");setScheduleRevisions(current=>({...current,[id]:(current[id]||0)+1}));}} /> : activity === "datasources" ? <DatasourceView key={workspaceId} workspaceId={workspaceId} /> : <RecycleView workspaceId={workspaceId} onRestored={refresh} />}
            </div>
          )}
          {chosenRun && (
            <section className="result-panel" style={{ height: resultHeight }}>
              <div
                className="horizontal-resizer"
                onPointerDown={(e) => resize(e, "result")}
                role="separator"
                aria-label="调整结果区域高度"
              />
              <div className="result-header">
                <Tabs
                  activeKey={resultTab}
                  onChange={setResultTab}
                  size="small"
                  items={[
                    { key: "logs", label: "运行日志" },
                    {
                      key: "result",
                      label: chosenRun.provider === "WORKFLOW" ? "节点与结果" : `运行结果${chosenRun.rowCount ? " (" + chosenRun.rowCount + ")" : ""}`,
                    },
                    { key: "details", label: "运行详情" },
                  ]}
                />
                <span className="run-context">{chosenRun.objectName}</span>
                <Tag color={chosenRun.simulation ? "blue" : "green"}>{runLabel(chosenRun)}</Tag>
                <Tag color={runColor[chosenRun.status]}>
                  {statusNames[chosenRun.status]}
                </Tag>
                <Tool
                  label="关闭结果面板"
                  icon={X}
                  onClick={() => setResultId(null)}
                />
              </div>
              <div className="result-body">
                <RunDetails key={chosenRun.id} id={chosenRun.id} tab={resultTab} />
              </div>
            </section>
          )}
          </>}
        </main>
      </div>
      {activity !== "realtime" && <footer className="status-bar">
        <span>
          <X size={11} />0 <TriangleAlert size={11} />0
        </span>
        <button onClick={openReleaseHistory}>
          <Send size={12} />
          {records.filter((r) => r.kind === "RELEASE").length}
        </button>
        <span className="status-fill" />
        {dirty && <span className="unsaved-label">● 未保存</span>}
        <span className="status-hide-small">
          {active?.nodeType || "SinketDataWorks"}
        </span>
        <button onClick={() => setSettings(true)}>空格: 4</button>
        <span>UTF-8</span>
        <span>LF</span>
        <span className="status-hide-small">
          <Check size={12} />
          {isRealWorkflow(active) ? "真实工作流 · 可发布调度" : active?.config?.run?.provider === "SYNC" ? "数据集成 · 可发布调度" : active?.config?.run?.provider === "DORIS" ? "Doris SQL 执行" : active?.config?.run?.provider === "MYSQL" ? (active.config.run.executionMode === "MATERIALIZE" ? "MySQL 库存落表" : "MySQL SQL 执行") : "本地模拟"}
        </span>
        <Tool
          label="打开通知"
          icon={Bell}
          onClick={() => setNotificationOpen(true)}
        />
      </footer>}

      {context && (
        <div
          className="tree-context"
          style={{
            left: Math.min(context.x, window.innerWidth - 205),
            top: Math.min(context.y, window.innerHeight - 310),
          }}
          onClick={(e) => e.stopPropagation()}
        >
          <Menu
            selectable={false}
            items={menuFor(context.object)}
            onClick={({ key }) => contextAction(key, context.object)}
          />
        </div>
      )}
      <Modal
        title={createSpec ? `新建${kindNames[createSpec.kind]}` : ""}
        open={!!createSpec}
        onCancel={() => {
          setCreateSpec(null);
          setPalette(false);
        }}
        width={palette ? 860 : 520}
        okText={palette ? "下一步" : "创建"}
        confirmLoading={creating}
        onOk={() => (palette ? setPalette(false) : void createObject())}
        forceRender
      >
        {palette && (
          <div className="node-picker">
            <Input
              placeholder="搜索节点类型"
              prefix={<Search size={14} />}
              value={nodeSearch}
              onChange={(e) => setNodeSearch(e.target.value)}
              allowClear
            />
            <div className="node-picker-body">
              <div className="node-categories">
                {nodeGroups.map((g) => (
                  <button
                    className={g === nodeGroup ? "selected" : ""}
                    key={g}
                    onClick={() => setNodeGroup(g)}
                  >
                    {g}
                  </button>
                ))}
              </div>
              <div className="node-grid">
                {filteredTypes.map((t) => (
                  <button
                    key={t.type}
                    className={
                      createSpec?.nodeType === t.type ? "selected" : ""
                    }
                    onClick={() =>
                      setCreateSpec((s) =>
                        s
                          ? {
                              ...s,
                              nodeType: t.type,
                              kind:
                                t.editor === "notebook"
                                  ? "NOTEBOOK"
                                  : t.editor === "workflow"
                                    ? "WORKFLOW"
                                    : "NODE",
                            }
                          : s,
                      )
                    }
                    onDoubleClick={() => {
                      setCreateSpec((s) =>
                        s
                          ? {
                              ...s,
                              nodeType: t.type,
                              kind:
                                t.editor === "notebook"
                                  ? "NOTEBOOK"
                                  : t.editor === "workflow"
                                    ? "WORKFLOW"
                                    : "NODE",
                            }
                          : s,
                      );
                      setPalette(false);
                    }}
                  >
                    <FileCode2 size={20} style={{ color: t.color }} />
                    <span>
                      {t.label}
                      <small>{t.group}</small>
                    </span>
                    {createSpec?.nodeType === t.type && <Check size={14} />}
                  </button>
                ))}
              </div>
            </div>
          </div>
        )}
        <Form
          form={createForm}
          layout="vertical"
          requiredMark={false}
          style={{ display: palette ? "none" : undefined }}
        >
          <div className="create-type">
            <ObjectIcon
              object={{
                kind: createSpec?.kind || "NODE",
                nodeType: createSpec?.nodeType || "",
              }}
              size={24}
            />
            <div>
              <b>{createSpec?.nodeType}</b>
              <small>创建到 {workspace?.name}</small>
            </div>
            {createSpec?.kind === "NODE" && (
              <Button size="small" onClick={() => setPalette(true)}>
                切换类型
              </Button>
            )}
          </div>
          {createSpec?.kind === "NODE" && ["MySQL", "Doris"].includes(createSpec.nodeType) && <Form.Item name="dataSourceId" label="数据源" rules={[{required:true,message:`请选择 ${createSpec.nodeType} Schema`}]} extra={creationSourceError || (!creationSources.length && <Button type="link" size="small" onClick={() => { setCreateSpec(null); setPalette(false); setActivity("datasources"); }}>添加数据源</Button>)}><Select aria-label="新建节点数据源" placeholder={`选择 ${createSpec.nodeType} Schema`} options={creationSources.map(source => ({value:source.id,label:schemaLabel(source)}))} /></Form.Item>}
          {createSpec?.kind === "WORKFLOW" && (
            <Form.Item label="工作流类型">
              <Select
                value={createSpec.nodeType}
                onChange={(value) =>
                  setCreateSpec((s) => (s ? { ...s, nodeType: value } : s))
                }
                options={[
                  { value: "周期工作流", label: "周期工作流" },
                  { value: "触发式工作流", label: "触发式工作流" },
                  { value: "手动工作流", label: "手动工作流" },
                ]}
              />
            </Form.Item>
          )}
          <Form.Item
            name="name"
            label="名称"
            rules={[
              { required: true, message: "请输入名称" },
              { max: 128, message: "名称最多128个字符" },
              { pattern: /^[^/\\]+$/, message: "名称不能包含斜杠" },
            ]}
          >
            <Input
              autoFocus
              placeholder="请输入名称"
              onPressEnter={() => void createObject()}
            />
          </Form.Item>
          <Form.Item name="parentId" label="所在目录">
            <Select
              allowClear
              options={folderOptions}
              placeholder="项目根目录"
            />
          </Form.Item>
          <Form.Item name="description" label="描述">
            <Input.TextArea
              rows={3}
              placeholder="请输入描述（可选）"
              maxLength={1000}
            />
          </Form.Item>
        </Form>
      </Modal>
      <Modal
        open={!!editAction}
        title={
          { rename: "重命名", move: "移动", copy: "复制" }[
            editAction?.kind || "rename"
          ]
        }
        onCancel={() => setEditAction(null)}
        onOk={() => void completeEdit()}
        okText="确定"
      >
        <Form layout="vertical">
          {editAction?.kind !== "move" && (
            <Form.Item label="名称" required>
              <Input
                value={editName}
                onChange={(e) => setEditName(e.target.value)}
              />
            </Form.Item>
          )}
          {editAction?.kind !== "rename" && (
            <Form.Item label="目标目录">
              <Select
                className="full-width"
                value={editParent || ""}
                onChange={(v) => setEditParent(v || null)}
                options={folderOptions.filter(
                  (o) => o.value !== editAction?.object.id,
                )}
              />
            </Form.Item>
          )}
        </Form>
      </Modal>
      {workspaceManagerOpen && <WorkspaceManager workspaces={workspaces} currentId={workspaceId}
        onClose={() => setWorkspaceManagerOpen(false)}
        onCreated={created => setWorkspaces(current => [...current, created])}
        onEnter={id => { setWorkspaceManagerOpen(false); void switchWorkspace(id); }} />}
      <Drawer
        title="工作台设置"
        open={settings}
        onClose={() => setSettings(false)}
        size={380}
      >
        <Form layout="vertical">
          <Form.Item label="颜色主题">
            <Select
              value={mode}
              onChange={setMode}
              options={[
                { value: "dark", label: "SinketDataWorks 深色" },
                { value: "light", label: "SinketDataWorks 浅色" },
              ]}
            />
          </Form.Item>
          <Form.Item label="编辑器字号">
            <InputNumber
              min={11}
              max={24}
              value={Number(prefs.editorFontSize) || 13}
              onChange={(v) => savePreferences({ editorFontSize: v || 13 })}
            />
          </Form.Item>
          <Form.Item label="自动换行">
            <Switch
              checked={!!prefs.wordWrap}
              onChange={(v) => savePreferences({ wordWrap: v })}
            />
          </Form.Item>
          <Form.Item label="显示侧栏">
            <Switch checked={sidebarVisible} onChange={setSidebarVisible} />
          </Form.Item>
          <Form.Item label="显示公告">
            <Switch checked={notice} onChange={setNotice} />
          </Form.Item>
          <Form.Item label="工作区布局">
            <Button
              onClick={() => {
                setSidebarWidth(238);
                setInspector(null);
                setResultHeight(220);
                setSidebarVisible(true);
                setZen(false);
              }}
            >
              恢复默认布局
            </Button>
          </Form.Item>
        </Form>
        <Alert type="info" title="偏好设置自动保存到本地 MySQL" />
      </Drawer>
      <Modal
        title="使用帮助"
        open={help}
        onCancel={() => setHelp(false)}
        footer={<Button onClick={() => setHelp(false)}>知道了</Button>}
        width={650}
      >
        <div className="help-content">
          <Tag color="blue">SinketDataWorks · 本地项目</Tag>
          <h3>从一个开发节点开始</h3>
          <p>
            在离线数据开发的项目目录选择节点类型并新建文件。MySQL、Doris 节点可执行 SQL；数据集成节点选择来源和目标后可在 MySQL 与 Doris 之间批量传输。调度参数在右侧配置，发布后可选择版本并应用到调度。
          </p>
          <p>实时数据开发提供 Source / Sink 配置、Flink SQL 校验、计划与结果预览，以及独立的发布与实时运维。任务、数据源与版本保存到服务器，作业提交到真实 Flink 集群；浏览器保留未保存草稿与标签页状态。</p>
          <table>
            <tbody>
              {[
                ["Ctrl + S", "保存当前文件"],
                ["F8 / F9", "离线运行 / 停止；实时运维 / 停止当前 Flink 作业"],
                ["Shift + F8", "带参运行当前节点"],
                ["Shift + Alt + F", "格式化 SQL"],
                ["Ctrl + Shift + E", "打开离线数据开发"],
                ["右键目录或文件", "重命名、移动、复制、收藏、删除"],
                ["版本面板", "查看历史、对比代码、恢复版本"],
              ].map(([a, b]) => (
                <tr key={a}>
                  <td>
                    <kbd>{a}</kbd>
                  </td>
                  <td>{b}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <p>
            离线元数据存储于
            MySQL。MySQL、Doris 节点支持真实 SQL 读写与表结构操作；数据集成支持 MySQL ↔ Doris 批量传输、发布与调度。
          </p>
        </div>
      </Modal>
      {nodeRunDialog && <RunParameterDialog
        open
        objectName={nodeRunDialog.request.object.name}
        mode={nodeRunDialog.request.mode}
        preparation={nodeRunDialog.preparation}
        busy={nodeRunSubmitting}
        error={nodeRunDialog.error}
        onCancel={() => { if (!nodeRunSubmitting) finishNodeRun(nodeRunDialog.request); }}
        onRun={values => submitNodeRun(nodeRunDialog.request, values)}
      />}
      <WorkflowReleases key={`${workflowRelease?.workspaceId || workspaceId}:${workflowRelease?.target?.id || "all"}:${!!workflowRelease}`}
        open={!!workflowRelease} workspaceId={workflowRelease?.workspaceId || workspaceId} target={workflowRelease?.target}
        initialTab={workflowRelease?.tab || "history"} onClose={() => setWorkflowRelease(null)} legacyRecords={records}
        dirtyNames={workflowRelease?.target ? (() => {
          const id = workflowRelease.target.id;
          const graph = (drafts[id] || objects.find(o => o.id === id))?.config.graph;
          const ids = new Set<string>([id, ...(graph?.nodes || []).map((n: { objectId?: string }) => n.objectId).filter(Boolean)]);
          return Object.values(drafts).filter(o => ids.has(o.id)).map(o => o.name);
        })() : []}
        onPublish={async note => {
          if (!workflowRelease?.target) throw new Error("请选择工作流");
          const prepared = await prepareWorkflow(workflowRelease.target.id);
          const release = await api.publishWorkflow(prepared.workflow.id, prepared.workflow.version, prepared.expectedNodeVersions, note);
          message.success(`已发布 R${release.releaseNo}，开发区后续保存不会改变此版本`);
          return release;
        }}
        onRun={run => { if (run.workspaceId === widRef.current) { setRuns(r => [run, ...r]); runSelect(run); } message.success(`已提交发布版本 R${run.releaseNo}`); }}
      />
      <Modal
        title="发布"
        open={releaseOpen}
        onCancel={() => setReleaseOpen(false)}
        width={760}
        confirmLoading={publishing}
        okText="发布到本地"
        okButtonProps={{ disabled: !active || active.kind === "FOLDER" }}
        onOk={() => void publish()}
      >
        <Alert
          type="info"
          title={realTask?"任务独立发布":"本地模拟发布"}
          description={realTask?"保存当前 SQL 和调度参数，生成不可变任务 R 版本。发布后可在调度配置中选择并应用。":"保存当前内容并记录版本与配置，不提交任何云端任务。"}
          showIcon
        />
        <Form layout="vertical" className="spaced-form">
          <Form.Item label="发布对象">
            <Input readOnly value={active?.name || "请先打开一个开发节点"} />
          </Form.Item>
          {!realTask&&<>
          <Form.Item label="目标环境">
            <Select
              value={releaseEnv}
              onChange={setReleaseEnv}
              options={[
                { value: "开发", label: "开发环境" },
                { value: "生产", label: "生产环境（本地模拟）" },
              ]}
            />
          </Form.Item>
          </>}
          <Form.Item label="发布说明">
            <Input.TextArea
              value={releaseNote}
              onChange={(e) => setReleaseNote(e.target.value)}
              rows={2}
              placeholder="本次变更说明"
            />
          </Form.Item>
        </Form>
        {realTask?<><h4>本任务发布版本</h4><Table size="small" rowKey="id" dataSource={taskReleaseHistory} pagination={{pageSize:4}} columns={[{title:"版本",render:(_,r)=>`R${r.releaseNo}`},{title:"说明",dataIndex:"note"},{title:"发布时间",dataIndex:"createdAt",render:stamp}]}/></>:<>
        <h4>最近发布记录</h4>
        <Table
          rowKey="id"
          size="small"
          pagination={{ pageSize: 4 }}
          dataSource={records.filter((r) => r.kind === "RELEASE")}
          columns={[
            { title: "对象", dataIndex: "title" },
            {
              title: "环境",
              render: (_, r) => r.payload.environment || "开发",
            },
            { title: "版本", render: (_, r) => `v${r.payload.version || 1}` },
            { title: "发布时间", dataIndex: "createdAt", render: stamp },
          ]}
        />
        </>}
      </Modal>
      <Modal
        title="分享文件"
        open={shareOpen}
        onCancel={() => setShareOpen(false)}
        footer={
          <Button
            type="primary"
            onClick={async () => {
              try {
                await navigator.clipboard.writeText(
                  `${location.origin}/?object=${activeId}`,
                );
                message.success("链接已复制");
              } catch {
                message.warning("请手动复制链接");
              }
            }}
          >
            复制链接
          </Button>
        }
      >
        <p>此链接可在运行本项目的电脑上打开对应文件。</p>
        <Input readOnly value={`${location.origin}/?object=${activeId}`} />
        <p className="muted">本地文件链接，不创建公网分享。</p>
      </Modal>
      <Drawer
        title="通知"
        open={notificationOpen}
        onClose={() => setNotificationOpen(false)}
        size={380}
      >
        <Alert
          type="success"
          title="本地工作空间已连接"
          description="离线与实时元数据使用 MySQL 持久化；浏览器仅保存实时草稿和界面状态。"
          showIcon
        />
        <div className="notification-list">
          {runs.slice(0, 8).map((r) => (
            <button
              key={r.id}
              onClick={() => {
                runSelect(r);
                setNotificationOpen(false);
              }}
            >
              <Tag color={runColor[r.status]}>{runName[r.status]}</Tag>
              <b>{r.objectName}</b>
              <small>{stamp(r.createdAt)}</small>
            </button>
          ))}
          {!runs.length && (
            <Empty
              image={Empty.PRESENTED_IMAGE_SIMPLE}
              description="暂无运行通知"
            />
          )}
        </div>
      </Drawer>
    </div>
  );
}
