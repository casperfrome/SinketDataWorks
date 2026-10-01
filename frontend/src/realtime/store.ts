import { useCallback, useSyncExternalStore } from "react";
import type { KafkaDatasource, RealtimeState } from "./types.ts";

export const emptyState = (): RealtimeState => ({ schemaVersion: 1, tasks: [], folders: [], drafts: {}, releases: [], jobs: [], kafkaSources: [], openTabs: [], activeId: "", view: "development", selectedJobId: "" });
export const storageKey = (workspaceId: string) => `sinket-realtime:v1:${encodeURIComponent(workspaceId)}`;
interface Entry { state: RealtimeState; error: string; corrupt: boolean }
const entries = new Map<string, Entry>();
const listeners = new Map<string, Set<() => void>>();
const passwords = new Map<string, string>();

const record = (value: unknown): value is Record<string, unknown> => !!value && typeof value === "object" && !Array.isArray(value);
const strings = (value: Record<string, unknown>, keys: string[]) => keys.every(key => typeof value[key] === "string");
const numbers = (value: Record<string, unknown>, keys: string[]) => keys.every(key => typeof value[key] === "number" && Number.isFinite(value[key]));
const validBinding = (value: unknown) => record(value)
  && strings(value, ["id", "datasourceId", "tableName", "physicalTable", "topic", "consumerGroup", "serverId", "timezone", "eventTimeField", "labelPrefix", "feHttpUrls"])
  && ["SOURCE", "SINK"].includes(String(value.role)) && ["KAFKA", "MYSQL_CDC", "MYSQL_JDBC", "DORIS"].includes(String(value.connector))
  && ["json", "csv"].includes(String(value.format)) && ["group-offsets", "earliest-offset", "latest-offset"].includes(String(value.startupMode))
  && ["append", "upsert"].includes(String(value.writeMode)) && ["initial", "latest-offset"].includes(String(value.cdcStartupMode))
  && ["DUPLICATE", "UNIQUE", "AGGREGATE"].includes(String(value.dorisModel)) && typeof value.syncDeletes === "boolean" && numbers(value, ["watermarkSeconds"])
  && Array.isArray(value.fields) && value.fields.every(field => record(field) && strings(field, ["id", "name", "type"]) && typeof field.nullable === "boolean" && typeof field.primaryKey === "boolean");
const validTask = (value: unknown, workspaceId: string) => record(value)
  && strings(value, ["id", "workspaceId", "name", "description", "sql", "updatedAt"]) && value.workspaceId === workspaceId
  && (value.folderId === null || typeof value.folderId === "string") && Array.isArray(value.bindings) && value.bindings.every(validBinding)
  && record(value.runtime) && numbers(value.runtime, ["parallelism", "checkpointSeconds", "restartAttempts", "restartDelaySeconds"]);
const validKafka = (value: unknown, workspaceId: string) => record(value)
  && strings(value, ["id", "workspaceId", "name", "bootstrapServers", "username"]) && value.workspaceId === workspaceId && value.type === "KAFKA"
  && ["PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL"].includes(String(value.securityProtocol)) && ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"].includes(String(value.saslMechanism));
const validState = (value: unknown, workspaceId: string): value is RealtimeState => {
  if (!value || typeof value !== "object") return false;
  const state = value as Partial<RealtimeState>;
  if (!(state.schemaVersion === 1 && [state.tasks, state.folders, state.releases, state.jobs, state.kafkaSources, state.openTabs].every(Array.isArray) && record(state.drafts) && typeof state.activeId === "string" && typeof state.selectedJobId === "string" && ["development", "operations"].includes(state.view || ""))) return false;
  return state.tasks!.every(task => validTask(task, workspaceId)) && Object.values(state.drafts!).every(task => validTask(task, workspaceId))
    && state.kafkaSources!.every(source => validKafka(source, workspaceId))
    && state.folders!.every(folder => record(folder) && strings(folder, ["id", "name"])) && state.openTabs!.every(id => typeof id === "string")
    && state.releases!.every(release => record(release) && strings(release, ["id", "taskId", "createdAt", "note"]) && numbers(release, ["releaseNo"]) && validTask(release.snapshot, workspaceId))
    && state.jobs!.every(job => record(job) && strings(job, ["id", "taskId", "releaseId", "createdAt", "startedAt"]) && numbers(job, ["checkpointBase"])
      && ["STARTING", "RUNNING", "STOPPING", "STOPPED", "RESTARTING", "FAILED"].includes(String(job.status))
      && Array.isArray(job.logs) && job.logs.every(log => record(log) && strings(log, ["id", "at", "message"]))
      && Array.isArray(job.savepoints) && job.savepoints.every(point => record(point) && strings(point, ["id", "createdAt", "path", "releaseId"])));
};
const publicKafka = (source: KafkaDatasource): KafkaDatasource => ({ id: source.id, workspaceId: source.workspaceId, type: "KAFKA", name: source.name, bootstrapServers: source.bootstrapServers, securityProtocol: source.securityProtocol, saslMechanism: source.saslMechanism, username: source.username });
function getEntry(workspaceId: string): Entry {
  const cached = entries.get(workspaceId);
  if (cached) return cached;
  let entry: Entry = { state: emptyState(), error: "", corrupt: false };
  try {
    if (typeof globalThis.localStorage === "undefined") throw new Error("浏览器本地存储不可用");
    const raw = globalThis.localStorage.getItem(storageKey(workspaceId));
    if (raw) {
      try {
        const parsed: unknown = JSON.parse(raw);
        if (!validState(parsed, workspaceId)) throw new Error("配置结构或版本无效");
        entry.state = { ...parsed, kafkaSources: parsed.kafkaSources.map(publicKafka) };
      } catch {
        entry = { state: emptyState(), corrupt: true, error: "本地实时配置损坏或版本不支持，原内容已保留。当前更改仅保存在内存，请先导出草稿并修复本地存储。" };
      }
    }
  } catch {
    entry.error = "本地存储不可用，当前更改仅保存在内存，刷新前请导出草稿。";
  }
  entries.set(workspaceId, entry);
  return entry;
}

export const getRealtimeState = (workspaceId: string) => getEntry(workspaceId).state;
export function updateRealtimeState(workspaceId: string, updater: (state: RealtimeState) => RealtimeState, options: { requirePersist?: boolean } = {}): boolean {
  const previous = getEntry(workspaceId);
  // Commits use an isolated input so an in-place updater cannot erase a draft on a failed write.
  const next = updater(options.requirePersist ? structuredClone(previous.state) : previous.state);
  const state = { ...next, kafkaSources: next.kafkaSources.map(publicKafka) };
  let error = previous.error;
  let saved = false;
  if (!previous.corrupt) {
    try {
      if (typeof globalThis.localStorage === "undefined") throw new Error("本地存储不可用");
      globalThis.localStorage.setItem(storageKey(workspaceId), JSON.stringify(state));
      error = "";
      saved = true;
    } catch {
      error = "本地保存失败（可能容量不足或被浏览器禁用）。当前更改仍保存在内存，刷新前请导出草稿。";
    }
  }
  entries.set(workspaceId, { state: options.requirePersist && !saved ? previous.state : state, error, corrupt: previous.corrupt });
  listeners.get(workspaceId)?.forEach(listener => listener());
  return saved;
}

export function useRealtimeStore(workspaceId: string) {
  const subscribe = useCallback((listener: () => void) => {
    const set = listeners.get(workspaceId) || new Set<() => void>();
    set.add(listener); listeners.set(workspaceId, set);
    return () => { set.delete(listener); };
  }, [workspaceId]);
  const getSnapshot = useCallback(() => getEntry(workspaceId), [workspaceId]);
  const entry = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
  const update = useCallback((updater: (state: RealtimeState) => RealtimeState) => updateRealtimeState(workspaceId, updater), [workspaceId]);
  const commit = useCallback((updater: (state: RealtimeState) => RealtimeState) => updateRealtimeState(workspaceId, updater, { requirePersist: true }), [workspaceId]);
  return { state: entry.state, update, commit, error: entry.error };
}
export const getKafkaPassword = (id: string) => passwords.get(id) || "";
export function setKafkaPassword(id: string, password: string) { if (password) passwords.set(id, password); else passwords.delete(id); }
export function kafkaReferences(workspaceId: string, id: string): string[] {
  const state = getRealtimeState(workspaceId);
  const names = new Set<string>();
  [...state.tasks, ...Object.values(state.drafts)].filter(task => task.bindings.some(binding => binding.datasourceId === id)).forEach(task => names.add(task.name));
  state.releases.filter(release => release.snapshot.bindings.some(binding => binding.datasourceId === id)).forEach(release => names.add(`${release.snapshot.name} · 发布版本 v${release.releaseNo}`));
  return [...names];
}
