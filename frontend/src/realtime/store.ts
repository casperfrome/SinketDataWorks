import { useCallback, useEffect, useSyncExternalStore } from "react";
import { api } from "../api.ts";
import type { RealtimeState, RealtimeTask } from "./types.ts";

export const emptyState = (): RealtimeState => ({ schemaVersion: 1, tasks: [], folders: [], drafts: {}, releases: [], jobs: [], kafkaSources: [], openTabs: [], activeId: "", view: "development", selectedJobId: "" });
export const legacyStorageKey = (workspaceId: string) => `sinket-realtime:v1:${encodeURIComponent(workspaceId)}`;
export const storageKey = (workspaceId: string) => `sinket-realtime-drafts:v2:${encodeURIComponent(workspaceId)}`;
interface Entry { state: RealtimeState; error: string; loading: boolean; loaded: boolean; corrupt?: boolean }
const entries = new Map<string, Entry>();
const listeners = new Map<string, Set<() => void>>();
const requests = new Map<string, Promise<void>>();
const emit = (workspaceId: string) => listeners.get(workspaceId)?.forEach(listener => listener());
const record = (value: unknown): value is Record<string, unknown> => !!value && typeof value === "object" && !Array.isArray(value);

export function localState(state: RealtimeState) {
  return { schemaVersion: 2, drafts: state.drafts, openTabs: state.openTabs, activeId: state.activeId, view: state.view, selectedJobId: state.selectedJobId };
}
export function settleSavedDraft(current: RealtimeTask | undefined, submitted: RealtimeTask, saved: RealtimeTask): RealtimeTask | undefined {
  if (!current || JSON.stringify(current) === JSON.stringify(submitted)) return undefined;
  return { ...current, revision: saved.revision, updatedAt: saved.updatedAt };
}
export function mergeRemoteState(current: RealtimeState, remote: RealtimeState, requested: RealtimeState): RealtimeState {
  const tasks = remote.tasks.map(task => { const local = current.tasks.find(item => item.id === task.id); return local && (local.revision || 0) > (task.revision || 0) ? local : task; });
  for (const task of current.tasks) if (!requested.tasks.some(item => item.id === task.id) && !tasks.some(item => item.id === task.id)) tasks.push(task);
  const alive = new Set(tasks.map(task => task.id));
  const remoteDrafts = Object.fromEntries(Object.entries(remote.drafts || {}).filter(([id,draft]) => !(requested.drafts[id] && !current.drafts[id]) && (draft.revision || 0) >= (tasks.find(task => task.id === id)?.revision || 0)));
  const drafts = Object.fromEntries(Object.entries({ ...remoteDrafts, ...current.drafts }).filter(([id]) => alive.has(id)).map(([id,draft]) => [id,draft.revision ? draft : {...draft,revision:tasks.find(task=>task.id===id)?.revision}]));
  return { ...emptyState(), ...remote, tasks, drafts, openTabs: current.openTabs.filter(id => alive.has(id)), activeId: alive.has(current.activeId) ? current.activeId : "", selectedJobId: current.selectedJobId, view: current.view };
}
export function legacyImportId(workspaceId: string, raw: string): string {
  let hash = 2166136261;
  for (let index = 0; index < raw.length; index++) hash = Math.imul(hash ^ raw.charCodeAt(index), 16777619);
  return `browser-v1:${workspaceId}:${(hash >>> 0).toString(16)}`;
}
export function parseLegacyState(raw: string, workspaceId: string): RealtimeState {
  const state: unknown = JSON.parse(raw);
  if (!record(state) || state.schemaVersion !== 1 || ![state.tasks, state.folders, state.releases, state.jobs, state.kafkaSources].every(Array.isArray) || !record(state.drafts)) throw new Error("旧实时配置结构或版本无效，原始数据已保留，请导出后检查。");
  const validTask = (task: unknown): task is RealtimeTask => record(task) && task.workspaceId === workspaceId && typeof task.id === "string" && typeof task.name === "string" && typeof task.sql === "string" && Array.isArray(task.bindings) && record(task.runtime);
  if (!(state.tasks as unknown[]).every(validTask) || !Object.values(state.drafts).every(validTask) || !(state.releases as unknown[]).every(release => record(release) && validTask(release.snapshot)) || !(state.kafkaSources as unknown[]).every(source => record(source) && source.workspaceId === workspaceId && source.type === "KAFKA")) throw new Error("旧实时配置包含无效记录或跨工作空间数据，原始数据已保留。");
  return state as unknown as RealtimeState;
}
function getEntry(workspaceId: string): Entry {
  const cached = entries.get(workspaceId); if (cached) return cached;
  const entry: Entry = { state: emptyState(), error: "", loading: false, loaded: false };
  try {
    const raw = globalThis.localStorage?.getItem(storageKey(workspaceId));
    if (raw) {
      const parsed: unknown = JSON.parse(raw);
      if (!record(parsed) || parsed.schemaVersion !== 2 || !record(parsed.drafts) || !Array.isArray(parsed.openTabs) || !parsed.openTabs.every(id => typeof id === "string") || !["development", "operations"].includes(String(parsed.view))) throw new Error("本地草稿损坏，原始内容已保留，请导出后检查。");
      const drafts = parsed.drafts as Record<string, RealtimeTask>;
      if (Object.values(drafts).some(task => !task || task.workspaceId !== workspaceId || typeof task.sql !== "string" || !Array.isArray(task.bindings))) throw new Error("本地草稿结构无效，原始内容已保留。");
      entry.state = { ...entry.state, drafts, openTabs: parsed.openTabs as string[], activeId: String(parsed.activeId || ""), view: parsed.view as RealtimeState["view"], selectedJobId: String(parsed.selectedJobId || "") };
    }
  } catch (error) { entry.error = error instanceof Error ? error.message : "本地草稿读取失败"; entry.corrupt = true; }
  entries.set(workspaceId, entry); return entry;
}
function persist(workspaceId: string, entry: Entry) {
  if (entry.corrupt) return;
  try { globalThis.localStorage?.setItem(storageKey(workspaceId), JSON.stringify(localState(entry.state))); }
  catch { entry.error = "本地草稿保存失败，编辑保留在页面内存，请先保存到服务器或导出。"; }
}
export const getRealtimeState = (workspaceId: string) => getEntry(workspaceId).state;
export function updateRealtimeState(workspaceId: string, updater: (state: RealtimeState) => RealtimeState): boolean {
  const previous = getEntry(workspaceId), entry = { ...previous, state: updater(previous.state) };
  persist(workspaceId, entry); entries.set(workspaceId, entry); emit(workspaceId); return !entry.error;
}
export function acknowledgeTask(workspaceId: string, submitted: RealtimeTask, saved: RealtimeTask) {
  updateRealtimeState(workspaceId, state => {
    const draft = settleSavedDraft(state.drafts[submitted.id], submitted, saved), drafts = { ...state.drafts };
    if (draft) drafts[saved.id] = draft; else delete drafts[submitted.id];
    return { ...state, drafts, tasks: [...state.tasks.filter(task => task.id !== saved.id), saved] };
  });
}
export async function refreshRealtimeState(workspaceId: string): Promise<void> {
  const pending = requests.get(workspaceId); if (pending) return pending;
  const request = (async () => {
    const previous = getEntry(workspaceId); entries.set(workspaceId, { ...previous, loading: true }); emit(workspaceId);
    try {
      const raw = globalThis.localStorage?.getItem(legacyStorageKey(workspaceId));
      if (raw) {
        const importId = legacyImportId(workspaceId, raw), receiptKey = `${storageKey(workspaceId)}:import`;
        if (globalThis.localStorage?.getItem(receiptKey) !== importId) {
          const legacy = parseLegacyState(raw, workspaceId);
          const imported = await api.realtimeImport(workspaceId, importId, legacy);
          const current = getEntry(workspaceId);
          const remapDraft = (draft: RealtimeTask): RealtimeTask => {
            let sql = draft.sql;
            for (const [before,after] of Object.entries(imported.idMap || {})) { sql = sql.replaceAll(`-- 数据源引用：${before}`,`-- 数据源引用：${after}`); for (const key of ["studio.datasource-id","studio.datasource.id"]) sql = sql.replaceAll(`'${key}' = '${before}'`,`'${key}' = '${after}'`); }
            return { ...draft, sql, bindings:draft.bindings.map(binding => ({...binding,datasourceId:imported.idMap?.[binding.datasourceId] || binding.datasourceId})) };
          };
          const localDrafts = Object.fromEntries(Object.entries(current.state.drafts).map(([id,draft]) => [id,remapDraft(draft)]));
          entries.set(workspaceId, { ...current, state: { ...current.state, drafts: { ...(imported.drafts || legacy.drafts), ...localDrafts }, openTabs: current.state.openTabs.length ? current.state.openTabs : legacy.openTabs || [], activeId: current.state.activeId || legacy.activeId || "" } });
          globalThis.localStorage?.setItem(receiptKey, importId);
        }
      }
      const requested = getEntry(workspaceId).state;
      const remote = await api.realtimeState(workspaceId), current = getEntry(workspaceId);
      const state = mergeRemoteState(current.state,remote,requested);
      const entry = { state, error: current.corrupt ? current.error : "", loading: false, loaded: true, corrupt: current.corrupt }; persist(workspaceId, entry); entries.set(workspaceId, entry);
    } catch (error) {
      const current = getEntry(workspaceId); entries.set(workspaceId, { ...current, loading: false, error: error instanceof Error ? error.message : "实时服务加载失败" }); throw error;
    } finally { emit(workspaceId); requests.delete(workspaceId); }
  })();
  requests.set(workspaceId, request); return request;
}
export function useRealtimeStore(workspaceId: string) {
  const subscribe = useCallback((listener: () => void) => { const set = listeners.get(workspaceId) || new Set<() => void>(); set.add(listener); listeners.set(workspaceId, set); return () => { set.delete(listener); }; }, [workspaceId]);
  const getSnapshot = useCallback(() => getEntry(workspaceId), [workspaceId]);
  const entry = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
  const update = useCallback((updater: (state: RealtimeState) => RealtimeState) => updateRealtimeState(workspaceId, updater), [workspaceId]);
  const refresh = useCallback(() => refreshRealtimeState(workspaceId), [workspaceId]);
  useEffect(() => { void refresh().catch(() => {}); }, [refresh]);
  return { state: entry.state, update, refresh, error: entry.error, loading: entry.loading, loaded: entry.loaded };
}
