import { useEffect, useMemo, useRef, useState } from "react";
import Editor, { DiffEditor } from "@monaco-editor/react";
import { Alert, App, Button, Drawer, Dropdown, Empty, Input, Modal, Select, Space, Tag, Tree } from "antd";
import type { DataNode } from "antd/es/tree";
import { Activity, ArrowRight, Braces, Check, CodeXml, Copy, Database, FileCode2, FolderClosed, FolderPlus, MoreHorizontal, PanelRightClose, PanelRightOpen, Plus, Save, Send, Trash2, X } from "lucide-react";
import { format as formatSQL } from "sql-formatter";
import { api } from "../api";
import { afterClosingTabs, tabsToClose, tabCloseLabels, type TabCloseAction } from "../state/tabs";
import type { DataSource } from "../types";
import type { RealtimeBinding, RealtimeState, RealtimeTask } from "./types";
import { applyManagedDDL, findManagedDDL, generateDDL } from "./ddl";
import { createRelease, createTask, settleJobs, transitionJob, uid, validateBinding, validateTask } from "./model";
import { useRealtimeStore } from "./store";
import BindingEditor from "./BindingEditor";
import RealtimeOperations, { isJobActive } from "./RealtimeOperations";
import "./realtime.css";

interface Props {
  workspaceId: string; theme: "dark" | "light"; fontSize: number; wordWrap: boolean;
  sidebarVisible: boolean; sidebarWidth: number; onManageDatasources: () => void;
}
type Section = "SOURCE" | "SINK" | "runtime" | "versions";
type Creation = { kind: "TASK" | "FOLDER" | "RENAME_TASK" | "RENAME_FOLDER"; id?: string; example?: boolean };

export default function RealtimeWorkbench({ workspaceId, theme, fontSize, wordWrap, sidebarVisible, sidebarWidth, onManageDatasources }: Props) {
  const { message, modal } = App.useApp();
  const { state, update, commit, error } = useRealtimeStore(workspaceId);
  const [databases, setDatabases] = useState<DataSource[]>([]), [sourceError, setSourceError] = useState("");
  const [search, setSearch] = useState(""), [section, setSection] = useState<Section>("SOURCE");
  const [configVisible, setConfigVisible] = useState(true), [drawerOpen, setDrawerOpen] = useState(false);
  const [narrow, setNarrow] = useState(() => window.matchMedia("(max-width: 1180px)").matches);
  const [creation, setCreation] = useState<Creation>(), [name, setName] = useState(""), [folderId, setFolderId] = useState<string | null>(null);
  const [publishOpen, setPublishOpen] = useState(false), [publishNote, setPublishNote] = useState("");
  const pendingCreation = useRef<RealtimeTask | undefined>(undefined);
  const [ddlChange, setDdlChange] = useState<{ taskId: string; bindingId: string; original: string; modified: string }>();
  const [now, setNow] = useState(Date.now());
  const currentRef = useRef(state); currentRef.current = state;
  const active = state.drafts[state.activeId] || state.tasks.find(task => task.id === state.activeId);
  const activeRef = useRef(active); activeRef.current = active;
  const sources = useMemo(() => [...databases, ...state.kafkaSources], [databases, state.kafkaSources]);
  const dirty = !!state.drafts[state.activeId];
  const releases = state.releases.filter(release => release.taskId === state.activeId).sort((a, b) => b.releaseNo - a.releaseNo);
  const needsClock = state.view === "operations" || state.jobs.some(job => ["STARTING", "STOPPING", "RESTARTING"].includes(job.status));

  useEffect(() => {
    let alive = true; setDatabases([]); setSourceError("");
    api.datasources(workspaceId).then(items => { if (alive) setDatabases(items); }).catch(reason => { if (alive) setSourceError(reason instanceof Error ? reason.message : "数据库源加载失败"); });
    return () => { alive = false; };
  }, [workspaceId]);
  useEffect(() => {
    const media = window.matchMedia("(max-width: 1180px)");
    const listener = () => setNarrow(media.matches);
    media.addEventListener("change", listener); return () => media.removeEventListener("change", listener);
  }, []);
  useEffect(() => {
    if (!needsClock) return;
    const timer = window.setInterval(() => {
      const time = Date.now(); if (currentRef.current.view === "operations") setNow(time);
      const settled = settleJobs(currentRef.current.jobs, time);
      if (JSON.stringify(settled) !== JSON.stringify(currentRef.current.jobs)) update(current => ({ ...current, jobs: settleJobs(current.jobs, time) }));
    }, 1000);
    return () => window.clearInterval(timer);
  }, [workspaceId, update, needsClock]);
  useEffect(() => {
    const before = (event: BeforeUnloadEvent) => { if (Object.keys(currentRef.current.drafts).length || error) event.preventDefault(); };
    window.addEventListener("beforeunload", before); return () => window.removeEventListener("beforeunload", before);
  }, [error]);

  const changeTask = (task: RealtimeTask) => update(current => {
    const saved = current.tasks.find(item => item.id === task.id); if (!saved) return current;
    const drafts = { ...current.drafts };
    if (JSON.stringify(saved) === JSON.stringify(task)) delete drafts[task.id]; else drafts[task.id] = task;
    return { ...current, drafts };
  });
  const saveTask = (id = state.activeId) => {
    const current = currentRef.current;
    const task = current.drafts[id] || current.tasks.find(item => item.id === id); if (!task) return false;
    const success = commit(snapshot => {
      const taskToSave = snapshot.drafts[id] || snapshot.tasks.find(item => item.id === id); if (!taskToSave) return snapshot;
      const drafts = { ...snapshot.drafts }; delete drafts[id];
      return { ...snapshot, drafts, tasks: snapshot.tasks.map(item => item.id === id ? { ...taskToSave, updatedAt: new Date().toISOString() } : item) };
    });
    if (success) message.success("实时任务已保存到当前浏览器");
    return success;
  };
  const check = () => {
    if (!active) return false;
    const errors = validateTask(active, sources);
    if (errors.length) modal.error({ title: "请完善实时任务配置", content: <ul className="rt-validation-list">{errors.map((item, index) => <li key={index}>{item}</li>)}</ul> });
    else message.success("配置检查通过；Flink SQL 语义将在运行后端接入后校验");
    return !errors.length;
  };
  const formatCurrent = () => {
    const task = activeRef.current; if (!task) return;
    try { changeTask({ ...task, sql: formatSQL(task.sql, { language: "sql", tabWidth: 4 }) }); }
    catch { message.warning("当前 Flink SQL 包含通用格式化器无法识别的语法，已保留原文"); }
  };
  const openTask = (id: string) => update(current => ({ ...current, view: "development", activeId: id || current.activeId, openTabs: id && !current.openTabs.includes(id) ? [...current.openTabs, id] : current.openTabs }));
  const openOperations = (id?: string) => update(current => ({ ...current, view: "operations", selectedJobId: id || current.jobs.filter(job => job.taskId === current.activeId).at(-1)?.id || current.selectedJobId }));
  useEffect(() => {
    const key = (event: KeyboardEvent) => {
      if (state.view !== "development" || publishOpen || creation || ddlChange || document.querySelector(".ant-modal-root .ant-modal-wrap:not([style*='display: none'])")) return;
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "s") { event.preventDefault(); saveTask(); }
      if (event.altKey && event.shiftKey && event.key.toLowerCase() === "f") { event.preventDefault(); formatCurrent(); }
      if (event.key === "F8") { event.preventDefault(); openOperations(); }
      if (event.key === "F9") {
        event.preventDefault();
        const job = state.jobs.filter(item => item.taskId === state.activeId).at(-1);
        if (job && ["STARTING", "RUNNING", "RESTARTING"].includes(job.status)) commit(current => ({ ...current, jobs: current.jobs.map(item => item.id === job.id ? transitionJob(item, "STOP") : item) }));
      }
    };
    window.addEventListener("keydown", key); return () => window.removeEventListener("keydown", key);
  });

  const showCreation = (kind: Creation["kind"], id?: string, example = false) => {
    pendingCreation.current = undefined;
    setCreation({ kind, id, example });
    setName(kind === "RENAME_TASK" ? (state.drafts[id!] || state.tasks.find(task => task.id === id))?.name || "" : kind === "RENAME_FOLDER" ? state.folders.find(folder => folder.id === id)?.name || "" : example ? "订单实时清洗" : "");
    if (kind === "TASK") setFolderId(active?.folderId || null);
  };
  const confirmCreation = () => {
    const value = name.trim(); if (!value) { message.warning("请输入名称"); return; }
    const conflict = creation?.kind.includes("FOLDER") ? state.folders.some(folder => folder.id !== creation.id && folder.name === value) : state.tasks.some(task => task.id !== creation?.id && task.id !== pendingCreation.current?.id && task.name === value && task.folderId === (creation?.kind === "RENAME_TASK" ? state.tasks.find(item => item.id === creation.id)?.folderId : folderId));
    if (conflict) { message.warning("同一目录内名称不能重复"); return; }
    if (creation?.kind === "TASK") {
      const task = { ...(pendingCreation.current || createTask(workspaceId, value, folderId, creation.example)), name: value, folderId };
      pendingCreation.current = task;
      if (update(current => ({ ...current, tasks: [...current.tasks.filter(item => item.id !== task.id), task], openTabs: current.openTabs.includes(task.id) ? current.openTabs : [...current.openTabs, task.id], activeId: task.id, view: "development" }))) setCreation(undefined);
    } else if (creation?.kind === "FOLDER") {
      if (commit(current => ({ ...current, folders: [...current.folders, { id: uid("rt-folder"), name: value }] }))) setCreation(undefined);
    } else if (creation?.kind === "RENAME_FOLDER") {
      if (commit(current => ({ ...current, folders: current.folders.map(folder => folder.id === creation.id ? { ...folder, name: value } : folder) }))) setCreation(undefined);
    } else if (creation?.kind === "RENAME_TASK") {
      const task = state.drafts[creation.id!] || state.tasks.find(item => item.id === creation.id);
      if (task && changeTask({ ...task, name: value })) setCreation(undefined);
    }
  };
  const deleteTasks = (ids: string[], deleteFolder?: string) => modal.confirm({
    title: deleteFolder ? "删除目录及其任务" : "删除实时任务", content: "将删除本地任务、草稿、发布版本与模拟记录，并停止关联模拟作业。", okText: "停止并删除", okButtonProps: { danger: true },
    onOk: () => {
      const success = commit(current => {
        const remaining = afterClosingTabs(current.openTabs, current.activeId, ids);
        return { ...current, tasks: current.tasks.filter(task => !ids.includes(task.id)), folders: current.folders.filter(folder => folder.id !== deleteFolder),
          drafts: Object.fromEntries(Object.entries(current.drafts).filter(([id]) => !ids.includes(id))), releases: current.releases.filter(release => !ids.includes(release.taskId)),
          jobs: current.jobs.filter(job => !ids.includes(job.taskId)), openTabs: remaining.tabs, activeId: remaining.activeId, selectedJobId: ids.includes(current.jobs.find(job => job.id === current.selectedJobId)?.taskId || "") ? "" : current.selectedJobId };
      });
      if (success) message.success("已删除本地实时任务"); else return Promise.reject(new Error("本地存储失败，请保留当前页面后重试"));
    },
  });
  const duplicate = (id: string) => {
    const original = state.drafts[id] || state.tasks.find(task => task.id === id); if (!original) return;
    const copy = structuredClone(original); copy.id = uid("rt-task");
    let suffix = 1; copy.name = original.name + "_副本";
    while (state.tasks.some(task => task.name === copy.name && task.folderId === copy.folderId)) copy.name = original.name + "_副本" + ++suffix;
    copy.updatedAt = new Date().toISOString();
    try {
      copy.bindings = copy.bindings.map(binding => {
        const oldId = binding.id;
        const next = { ...binding, id: uid("binding"), labelPrefix: binding.labelPrefix ? binding.labelPrefix + "_" + copy.id.slice(-8) : "" };
        const managed = findManagedDDL(copy.sql, oldId);
        if (managed !== undefined) {
          copy.sql = copy.sql.replaceAll("@realtime-binding:" + encodeURIComponent(oldId) + ":", "@realtime-binding:" + encodeURIComponent(next.id) + ":");
          const copiedBlock = binding.connector === "DORIS" ? managed.replace(/('sink\.label-prefix'\s*=\s*)'((?:[^']|'')*)'/g, (_, option: string, value: string) => {
            next.labelPrefix = value.replaceAll("''", "'") + "_" + copy.id.slice(-8);
            return option + "'" + next.labelPrefix.replaceAll("'", "''") + "'";
          }) : managed;
          copy.sql = applyManagedDDL(copy.sql, next.id, copiedBlock);
        }
        return next;
      });
    } catch (reason) { message.error(reason instanceof Error ? reason.message : "无法复制连接 SQL"); return; }
    update(current => ({ ...current, tasks: [...current.tasks, copy], activeId: copy.id, openTabs: [...current.openTabs, copy.id], view: "development" }));
  };
  const closeTabs = (anchor: string, action: TabCloseAction = "current") => {
    const closing = tabsToClose(state.openTabs, anchor, action, new Set(Object.keys(state.drafts)));
    if (!closing.length) return;
    const close = (discard = false) => update(current => ({ ...current, ...(() => { const result = afterClosingTabs(current.openTabs, current.activeId, closing); return { openTabs: result.tabs, activeId: result.activeId }; })(), drafts: discard ? Object.fromEntries(Object.entries(current.drafts).filter(([id]) => !closing.includes(id))) : current.drafts }));
    const unsaved = closing.filter(id => !!state.drafts[id]);
    if (!unsaved.length) { close(); return; }
    const dialog = modal.confirm({ title: "关闭标签页 · 未保存的实时任务", content: <ul>{unsaved.map(id => <li key={id}>{state.drafts[id].name}</li>)}</ul>, footer: () => <Space><Button onClick={() => dialog.destroy()}>取消</Button><Button danger onClick={() => { close(true); dialog.destroy(); }}>丢弃并关闭</Button><Button type="primary" onClick={() => { if (unsaved.every(id => saveTask(id))) { close(); dialog.destroy(); } }}>保存并关闭</Button></Space> });
  };
  const insertDDL = (binding: RealtimeBinding) => {
    if (!active) return;
    const errors = validateBinding(binding, sources);
    if (errors.length) { modal.error({ title: "请先完善连接配置", content: <ul>{errors.map((item, index) => <li key={index}>{item}</li>)}</ul> }); return; }
    try {
      const generated = generateDDL(binding, sources.find(source => source.id === binding.datasourceId), active.id);
      const old = findManagedDDL(active.sql, binding.id);
      if (old !== undefined) setDdlChange({ taskId: active.id, bindingId: binding.id, original: old, modified: generated });
      else changeTask({ ...active, sql: applyManagedDDL(active.sql, binding.id, generated) });
    } catch (reason) { message.error(reason instanceof Error ? reason.message : "连接 SQL 更新失败，原文已保留"); }
  };
  const publish = () => {
    const task = activeRef.current; if (!task) return;
    const errors = validateTask(task, sources);
    if (errors.length) { modal.error({ title: "配置检查未通过", content: <ul>{errors.map((item, index) => <li key={index}>{item}</li>)}</ul> }); return; }
    const success = commit(current => {
      const latest = { ...(current.drafts[task.id] || current.tasks.find(item => item.id === task.id)!), updatedAt: new Date().toISOString() };
      const release = createRelease(latest, current.releases, publishNote);
      const drafts = { ...current.drafts }; delete drafts[task.id];
      return { ...current, tasks: current.tasks.map(item => item.id === latest.id ? latest : item), drafts, releases: [...current.releases, release] };
    });
    if (success) { setPublishOpen(false); setPublishNote(""); setSection("versions"); message.success("已保存并发布本地版本，可前往实时运维启动"); }
  };
  const selectSection = (next: Section) => { setSection(next); setConfigVisible(true); if (narrow) setDrawerOpen(true); };
  const taskMenu = (task: RealtimeTask) => ({ items: [{ key: "rename", label: "重命名" }, { key: "copy", label: "复制任务", icon: <Copy size={13} /> }, { key: "delete", label: "删除", danger: true, icon: <Trash2 size={13} /> }], onClick: ({ key }: { key: string }) => key === "rename" ? showCreation("RENAME_TASK", task.id) : key === "copy" ? duplicate(task.id) : deleteTasks([task.id]) });
  const node = (task: RealtimeTask): DataNode => ({ key: task.id, icon: <FileCode2 size={14} />, title: <div className="rt-tree-title"><span>{state.drafts[task.id]?.name || task.name}{state.drafts[task.id] && <i className="dirty-dot" />}</span><Dropdown menu={taskMenu(task)} trigger={["click", "contextMenu"]}><button aria-label={task.name + " 更多操作"} onClick={event => event.stopPropagation()}><MoreHorizontal size={13} /></button></Dropdown></div> });
  const matching = state.tasks.filter(task => (state.drafts[task.id]?.name || task.name).toLowerCase().includes(search.toLowerCase()));
  const tree: DataNode[] = [...state.folders.map(folder => ({ key: folder.id, icon: <FolderClosed size={14} />, title: <div className="rt-tree-title"><span>{folder.name}</span><Dropdown menu={{ items: [{ key: "add", label: "新建任务" }, { key: "rename", label: "重命名" }, { key: "delete", label: "删除目录", danger: true }], onClick: ({ key }) => { if (key === "add") { showCreation("TASK"); setFolderId(folder.id); } else if (key === "rename") showCreation("RENAME_FOLDER", folder.id); else deleteTasks(state.tasks.filter(task => task.folderId === folder.id).map(task => task.id), folder.id); } }}><button aria-label={folder.name + " 目录操作"} onClick={event => event.stopPropagation()}><MoreHorizontal size={13} /></button></Dropdown></div>, children: matching.filter(task => task.folderId === folder.id).map(node) })), ...matching.filter(task => !task.folderId).map(node)];
  const bindingPanel = active && <BindingEditor key={active.id} task={active} sources={sources} onChange={changeTask} onInsertDDL={insertDDL} onManageDatasources={onManageDatasources} section={section} releases={releases} />;

  return <div className="rt-workbench">
    <header className="rt-header"><div className="rt-header-title"><Activity size={18} /><strong>实时数据开发</strong><Tag color="blue">前端预览</Tag></div>
      <div className="rt-mode-switch" role="tablist" aria-label="实时模块视图"><button role="tab" aria-selected={state.view === "development"} className={state.view === "development" ? "selected" : ""} onClick={() => update(current => ({ ...current, view: "development" }))}><CodeXml size={14} />开发</button><button role="tab" aria-selected={state.view === "operations"} className={state.view === "operations" ? "selected" : ""} onClick={() => openOperations()}><Activity size={14} />实时运维</button></div>
      <span className="rt-storage-label">当前浏览器 · 独立工作空间</span>
    </header>
    {error && <Alert className="rt-storage-error" type="error" showIcon title={error} description="内容仍保留在当前页面内存中。请保留页面，检查浏览器存储后重试。" action={<Space><Button size="small" onClick={() => {
      const blob = new Blob([JSON.stringify(currentRef.current, null, 2)], { type: "application/json;charset=utf-8" });
      const url = URL.createObjectURL(blob), link = document.createElement("a");
      link.href = url; link.download = "realtime-" + workspaceId + ".json"; link.click(); window.setTimeout(() => URL.revokeObjectURL(url), 1000);
    }}>导出当前空间</Button><Button size="small" onClick={() => update(current => ({ ...current }))}>重试保存</Button></Space>} />}
    <div className="rt-body">
      {sidebarVisible && state.view === "development" && <aside className="rt-explorer" style={{ width: Math.max(200, Math.min(sidebarWidth, 320)) }}>
        <div className="rt-explorer-heading"><span>实时任务</span><Space size={4}><Button size="small" type="text" aria-label="新建实时目录" icon={<FolderPlus size={15} />} onClick={() => showCreation("FOLDER")} /><Button size="small" type="text" aria-label="新建实时任务" icon={<Plus size={15} />} onClick={() => showCreation("TASK")} /></Space></div>
        <Input.Search aria-label="搜索实时任务" placeholder="搜索任务名称" value={search} onChange={event => setSearch(event.target.value)} allowClear />
        <div className="rt-tree"><Tree showIcon blockNode defaultExpandAll treeData={tree} selectedKeys={active ? [active.id] : []} onSelect={keys => { const id = String(keys[0] || ""); if (state.tasks.some(task => task.id === id)) openTask(id); else setFolderId(id || null); }} /></div>
        <button className="rt-explorer-source" onClick={onManageDatasources}><Database size={14} /><span>管理数据源</span><ArrowRight size={13} /></button>
        <div className="rt-explorer-note"><Activity size={14} /><p>常驻流任务<br /><span>独立发布与运维</span></p></div>
      </aside>}
      {state.view === "operations" ? <RealtimeOperations state={state} update={update} commit={commit} sources={sources} now={now} onOpen={openTask} /> : <div className="rt-development">
        <div className="rt-editor-tabs" role="tablist" aria-label="实时任务标签">
          {state.openTabs.map(id => { const task = state.drafts[id] || state.tasks.find(item => item.id === id); if (!task) return null; return <Dropdown key={id} trigger={["contextMenu"]} menu={{ items: (Object.keys(tabCloseLabels) as TabCloseAction[]).map(key => ({ key, label: tabCloseLabels[key] })), onClick: ({ key }) => closeTabs(id, key as TabCloseAction) }}><div role="tab" tabIndex={0} aria-label={task.name} aria-selected={id === state.activeId} className={"rt-editor-tab " + (id === state.activeId ? "selected" : "")} onClick={() => openTask(id)} onKeyDown={event => { if (event.key === "Enter") openTask(id); }}><FileCode2 size={13} /><span>{task.name}</span><button aria-label={"关闭实时任务 " + task.name} onClick={event => { event.stopPropagation(); closeTabs(id); }}>{state.drafts[id] ? <i className="dirty-dot" /> : <X size={12} />}</button></div></Dropdown>; })}
          <button className="rt-add-tab" aria-label="添加实时任务" onClick={() => showCreation("TASK")}><Plus size={15} /></button>
        </div>
        {active ? <>
          <div className="rt-toolbar">
            <Button type="text" size="small" icon={<Save size={14} />} onClick={() => saveTask()}>保存</Button>
            <Button type="text" size="small" icon={<Braces size={14} />} onClick={formatCurrent}>格式化</Button>
            <Button type="text" size="small" icon={<Check size={14} />} onClick={check}>配置检查</Button>
            <Button type="text" size="small" icon={<Send size={14} />} onClick={() => { if (check()) setPublishOpen(true); }}>发布</Button>
            <Button type="text" size="small" icon={<Activity size={14} />} onClick={() => openOperations()}>前往运维</Button>
            <span className="rt-flex" /><span className="rt-muted">{error ? "保存失败 · 仅内存" : dirty ? "● 未保存" : "已保存"}</span>
            <Button type="text" size="small" aria-label="切换实时配置面板" icon={configVisible ? <PanelRightClose size={15} /> : <PanelRightOpen size={15} />} onClick={() => narrow ? setDrawerOpen(true) : setConfigVisible(!configVisible)} />
          </div>
          <div className="rt-stream-summary">
            <button onClick={() => selectSection("SOURCE")}><Database size={16} /><span>Source<small>{active.bindings.filter(binding => binding.role === "SOURCE").map(binding => binding.tableName || "未配置").join(", ") || "添加输入"}</small></span><b>{active.bindings.filter(binding => binding.role === "SOURCE").length}</b></button>
            <ArrowRight size={16} className="rt-stream-arrow" /><div className="rt-stream-sql"><CodeXml size={17} /><span>Flink SQL<small>流处理逻辑</small></span></div><ArrowRight size={16} className="rt-stream-arrow" />
            <button onClick={() => selectSection("SINK")}><Database size={16} /><span>Sink<small>{active.bindings.filter(binding => binding.role === "SINK").map(binding => binding.tableName || "未配置").join(", ") || "添加输出"}</small></span><b>{active.bindings.filter(binding => binding.role === "SINK").length}</b></button>
          </div>
          <div className="rt-editor-body"><div className="rt-code-area">
            <div className="rt-code-caption"><span>Flink SQL</span><span>连接 SQL 按配置生成 · 处理逻辑直接编写</span></div>
            <Editor key={workspaceId + "/" + active.id} path={"realtime/" + workspaceId + "/" + active.id + ".sql"} language="sql" theme={theme === "dark" ? "dataworks-dark" : "vs"} value={active.sql} onChange={value => { if (currentRef.current.activeId !== active.id) return; const task = currentRef.current.drafts[active.id] || currentRef.current.tasks.find(item => item.id === active.id); if (task) changeTask({ ...task, sql: value || "" }); }} options={{ automaticLayout: true, fontSize, wordWrap: wordWrap ? "on" : "off", minimap: { enabled: false }, scrollBeyondLastLine: false, tabSize: 4, padding: { top: 14 }, renderLineHighlight: "line", fontFamily: "Consolas, 'Microsoft YaHei', monospace" }} />
            <div className="rt-code-footer"><span>SQL · UTF-8</span><span>Ctrl+S 保存 · F8 运维 · F9 停止</span></div>
          </div>
            {!narrow && configVisible && <aside className="rt-inspector"><div className="rt-inspector-tabs">{([{ key: "SOURCE", label: "Source" }, { key: "SINK", label: "Sink" }, { key: "runtime", label: "运行配置" }, { key: "versions", label: "版本" }] as const).map(item => <button key={item.key} className={section === item.key ? "selected" : ""} onClick={() => setSection(item.key)}>{item.label}</button>)}</div>{sourceError && <Alert type="warning" title={"数据库源未加载：" + sourceError} />}{bindingPanel}</aside>}
          </div>
        </> : <div className="rt-welcome">
          <div className="rt-welcome-mark"><Activity size={38} /></div><span className="rt-eyebrow">STREAMING SQL WORKSPACE</span><h1>让数据持续流动</h1><p>配置输入和输出，用 Flink SQL 编写实时处理逻辑。<br />在独立的实时运维中管理常驻作业与状态恢复。</p>
          <Space wrap><Button type="primary" icon={<Plus size={15} />} onClick={() => showCreation("TASK")}>创建任务</Button><Button icon={<FileCode2 size={15} />} onClick={() => showCreation("TASK", undefined, true)}>加载示例模板</Button></Space>
          <div className="rt-welcome-flow"><span>Kafka / MySQL CDC</span><ArrowRight size={15} /><strong>Flink SQL</strong><ArrowRight size={15} /><span>Kafka / MySQL / Doris</span></div>
          <p className="rt-welcome-foot">任务保存到当前浏览器 · 作业运行与指标为前端模拟</p>
        </div>}
      </div>}
    </div>
    <footer className="rt-status"><span><Activity size={12} />{state.view === "operations" ? "实时运维 · 模拟运行" : "Flink SQL · 独立开发"}</span><span className="rt-flex" />{state.jobs.some(isJobActive) && <span className="rt-running-dot">{state.jobs.filter(isJobActive).length} 个模拟作业活动中</span>}<span>{error ? "存储失败 · 内容保留在内存" : "当前浏览器保存"}</span></footer>
    <Drawer title="实时任务配置" open={drawerOpen && narrow && !!active} onClose={() => setDrawerOpen(false)} size={Math.min(440, window.innerWidth)}><div className="rt-inspector-tabs">{([{ key: "SOURCE", label: "Source" }, { key: "SINK", label: "Sink" }, { key: "runtime", label: "运行配置" }, { key: "versions", label: "版本" }] as const).map(item => <button key={item.key} className={section === item.key ? "selected" : ""} onClick={() => setSection(item.key)}>{item.label}</button>)}</div>{sourceError && <Alert type="warning" title={sourceError} />}{bindingPanel}</Drawer>
    <Modal title={creation?.kind === "FOLDER" ? "新建实时目录" : creation?.kind.startsWith("RENAME") ? "重命名" : creation?.example ? "加载订单实时处理模板" : "新建 Flink SQL 任务"} open={!!creation} onCancel={() => setCreation(undefined)} onOk={confirmCreation} okText="确定" destroyOnHidden>
      <div className="rt-form-row"><label>名称</label><Input aria-label="实时对象名称" autoFocus maxLength={100} value={name} onChange={event => setName(event.target.value)} onPressEnter={confirmCreation} /></div>
      {creation?.kind === "TASK" && <div className="rt-form-row"><label>目录</label><Select aria-label="实时任务目录" value={folderId || ""} onChange={id => setFolderId(id || null)} options={[{ value: "", label: "根目录" }, ...state.folders.map(folder => ({ value: folder.id, label: folder.name }))]} /></div>}
      {creation?.example && <Alert type="info" title="模板包含字段结构与订单处理 SQL，创建后选择自己的 Source / Sink 数据源。" />}
    </Modal>
    <Modal title="发布实时任务" open={publishOpen} onCancel={() => setPublishOpen(false)} onOk={publish} okText="保存并发布">
      <Alert type="info" showIcon title="发布到当前浏览器，生成独立版本快照" description="当前修改将一并保存。新版本不会自动替换正在运行的模拟作业。" />
      <div className="rt-form-row"><label>发布说明</label><Input.TextArea aria-label="实时发布说明" value={publishNote} maxLength={500} onChange={event => setPublishNote(event.target.value)} rows={3} /></div>
    </Modal>
    <Modal title="更新连接 SQL · 查看差异" open={!!ddlChange} onCancel={() => setDdlChange(undefined)} width={900} okText="替换此连接 SQL" onOk={() => {
      if (!ddlChange) return;
      const task = currentRef.current.drafts[ddlChange.taskId] || currentRef.current.tasks.find(item => item.id === ddlChange.taskId); if (!task) return;
      try {
        if (findManagedDDL(task.sql, ddlChange.bindingId) !== ddlChange.original) { message.warning("该连接 SQL 已变化，请重新生成预览"); setDdlChange(undefined); return; }
        changeTask({ ...task, sql: applyManagedDDL(task.sql, ddlChange.bindingId, ddlChange.modified) }); setDdlChange(undefined);
      } catch (reason) { message.error(reason instanceof Error ? reason.message : "SQL 标记无效，原文已保留"); }
    }}>
      <p className="rt-muted">仅替换该绑定的生成块。其他手写 SQL 保留。</p><DiffEditor height={360} original={ddlChange?.original || ""} modified={ddlChange?.modified || ""} language="sql" theme={theme === "dark" ? "dataworks-dark" : "vs"} options={{ readOnly: true, minimap: { enabled: false }, automaticLayout: true }} />
    </Modal>
  </div>;
}
