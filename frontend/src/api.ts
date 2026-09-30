import type { ScheduleParameter, ParameterPreview, TaskRelease, TaskSchedule, TaskScheduleInput, TaskPreview, TaskTrigger } from "./types";
import type {
  SyncMetadata,
  Workspace,
  StudioObject,
  ObjectInput,
  ObjectVersion,
  Run,
  StudioRecord,
  Preferences,
  DataSource, DataSourceInput, QueryResult, RunPage, WorkflowRelease,
  ScheduleConfig, WorkflowSchedule, ScheduleTrigger, SchedulePreview,
} from "./types";
export class ApiError extends Error {
  status: number;
  code: string;
  constructor(message: string, status: number, code = "ERROR") {
    super(message);
    this.status = status;
    this.code = code;
  }
}
async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  let response: Response;
  try {
    response = await fetch("/api/v1" + path, {
      ...options,
      headers: {
        ...(options.body instanceof FormData
          ? {}
          : { "Content-Type": "application/json" }),
        ...options.headers,
      },
    });
  } catch {
    throw new ApiError(
      "无法连接本地服务，请检查后端是否已启动。",
      0,
      "NETWORK",
    );
  }
  if (!response.ok) {
    const e = await response
      .json()
      .catch(() => ({ message: response.statusText }));
    throw new ApiError(e.message || "请求失败", response.status, e.code);
  }
  if (response.status === 204) return undefined as T;
  const text = await response.text();
  return (text ? JSON.parse(text) : undefined) as T;
}
const json = (method: string, body?: unknown): RequestInit => ({
  method,
  body: body === undefined ? undefined : JSON.stringify(body),
});
export const api = {
  syncMetadata:(id:string,table:string)=>request<SyncMetadata>(`/datasources/${id}/tables/${encodeURIComponent(table)}/sync-metadata`),
  validateSync:(object:StudioObject)=>request<{valid:boolean;message:string}>("/sync/validate",json("POST",object)),
  syncBatches:(id:string)=>request<{batches:Record<string,unknown>[]}>(`/runs/${id}/sync/batches`),
  resolveSync:(id:string,note:string)=>request<Run>(`/runs/${id}/sync/resolve`,json("POST",{note})),
  workspaces: () => request<Workspace[]>("/workspaces"),
  objects: (workspaceId: string, deleted = false) =>
    request<StudioObject[]>(
      `/objects?workspaceId=${encodeURIComponent(workspaceId)}&deleted=${deleted}`,
    ),
  object: (id: string) => request<StudioObject>(`/objects/${id}`),
  create: (o: ObjectInput) =>
    request<StudioObject>("/objects", json("POST", o)),
  save: (o: StudioObject) =>
    request<StudioObject>(`/objects/${o.id}`, json("PUT", o)),
  remove: (id: string) => request<void>(`/objects/${id}`, json("DELETE")),
  restore: (id: string, name?: string) =>
    request<StudioObject>(`/objects/${id}/restore`, json("POST", { name })),
  copy: (id: string, name?: string, parentId?: string | null) =>
    request<StudioObject>(
      `/objects/${id}/copy`,
      json("POST", { name, parentId }),
    ),
  versions: (id: string) => request<ObjectVersion[]>(`/objects/${id}/versions`),
  restoreVersion: (id: string, vid: string, version: number) =>
    request<StudioObject>(
      `/objects/${id}/versions/${vid}/restore`,
      json("POST", { version }),
    ),
  runs: async (wid: string) =>
    (await request<RunPage>(`/runs?workspaceId=${encodeURIComponent(wid)}&summary=true`)).items,
  runPage: (wid: string, page = 1, status = "", search = "") =>
    request<RunPage>(`/runs?workspaceId=${encodeURIComponent(wid)}&summary=true&page=${page}&pageSize=12&status=${encodeURIComponent(status)}&search=${encodeURIComponent(search)}`),
  run: (objectId: string, simulateFailure = false, mode = "MANUAL", expectedVersion?: number, expectedNodeVersions?: Record<string, number>, businessDate?: string) =>
    request<Run>("/runs", json("POST", { objectId, simulateFailure, mode, expectedVersion, expectedNodeVersions, businessDate })),
  runNodes: (id: string) => request<Run[]>(`/runs/${id}/nodes`),
  workflowReleases: (workspaceId: string, workflowId = "") => request<WorkflowRelease[]>(`/workflow-releases?workspaceId=${encodeURIComponent(workspaceId)}&workflowId=${encodeURIComponent(workflowId)}`),
  workflowRelease: (id: string) => request<WorkflowRelease>(`/workflow-releases/${id}`),
  publishWorkflow: (id: string, expectedVersion: number, expectedNodeVersions: Record<string, number>, note: string) => request<WorkflowRelease>(`/workflows/${id}/releases`, json("POST", { expectedVersion, expectedNodeVersions, note })),
  runRelease: (id: string, businessDate?: string) => request<Run>(`/workflow-releases/${id}/runs`, json("POST", {businessDate})),
  taskReleases: (id: string) => request<TaskRelease[]>(`/tasks/${id}/releases`),
  publishTask: (id: string, expectedVersion: number, note = "") => request<TaskRelease>(`/tasks/${id}/releases`, json("POST", {expectedVersion, note})),
  runTaskRelease: (id: string, businessDate?: string) => request<Run>(`/task-releases/${id}/runs`, json("POST", {businessDate})),
  taskSchedules: (workspaceId: string, taskId = "") => request<TaskSchedule[]>(`/task-schedules?${new URLSearchParams({workspaceId,taskId})}`),
  saveTaskSchedule: (input: TaskScheduleInput, id?: string) => request<TaskSchedule>(id ? `/task-schedules/${id}` : `/tasks/${input.taskId}/schedule`, json(id ? "PUT" : "POST", input)),
  previewTaskSchedule: (input: TaskScheduleInput) => request<TaskPreview[]>("/task-schedules/preview", json("POST",input)),
  taskTriggers: (id: string, page = 1, status = "") => request<{items:TaskTrigger[];total:number}>(`/task-schedules/${id}/triggers?${new URLSearchParams({page:String(page),pageSize:"20",status})}`),
  rerunTaskTrigger: (id: string) => request<TaskTrigger>(`/task-triggers/${id}/rerun`, json("POST")),
  schedules: (workspaceId: string, workflowId = "") => request<WorkflowSchedule[]>(`/workflow-schedules?${new URLSearchParams({workspaceId, workflowId})}`),
  saveSchedule: (workflowId: string, config: ScheduleConfig & {releaseId:string;expectedVersion?:number}, id?: string) => request<WorkflowSchedule>(id ? `/workflow-schedules/${id}` : `/workflows/${workflowId}/schedule`, json(id ? "PUT" : "POST", config)),
  previewSchedule: (config: ScheduleConfig) => request<SchedulePreview[]>("/workflow-schedules/preview", json("POST", config)),
  scheduleTriggers: (id: string, page = 1, status = "") => request<{items:ScheduleTrigger[];total:number}>(`/workflow-schedules/${id}/triggers?${new URLSearchParams({page:String(page),pageSize:"20",status})}`),
  rerunTrigger: (id: string) => request<Run>(`/schedule-triggers/${id}/rerun`, json("POST")),
  rerun: (id: string) => request<Run>(`/runs/${id}/rerun`, json("POST")),
  runDetail: (id: string) => request<Run>(`/runs/${id}`),
  runResults: (id: string, page = 1, statementIndex?: number) => request<QueryResult>(`/runs/${id}/results?page=${page}&pageSize=100${statementIndex === undefined ? "" : `&statementIndex=${statementIndex}`}`),
  datasources: (wid: string) => request<DataSource[]>(`/datasources?workspaceId=${encodeURIComponent(wid)}`),
  saveDatasource: (input: DataSourceInput, id?: string) => request<DataSource>(`/datasources${id ? "/" + id : ""}`, json(id ? "PUT" : "POST", input)),
  testDatasource: (input?: Partial<DataSourceInput>, id?: string) => request<{success: boolean; message: string; version: string; elapsedMs: number}>(`/datasources${id ? "/" + id : ""}/test`, json("POST", input || {})),
  sourceTables: (id: string) => request<{name: string; type: string; comment: string}[]>(`/datasources/${id}/tables`),
  sourceColumns: (id: string, table: string) => request<{name: string; type: string; nullable: string; columnKey: string; comment: string}[]>(`/datasources/${id}/tables/${encodeURIComponent(table)}/columns`),
  stop: (id: string) => request<Run>(`/runs/${id}/stop`, json("POST")),
  records: (wid: string, kind?: string) =>
    request<StudioRecord[]>(
      `/records?workspaceId=${encodeURIComponent(wid)}${kind ? "&kind=" + kind : ""}`,
    ),
  record: (r: Omit<StudioRecord, "id" | "createdAt">) =>
    request<StudioRecord>("/records", json("POST", r)),
  extractParameters: (code: string) => request<string[]>("/schedule-parameters/extract", json("POST", { code })),
  previewParameters: (input: ScheduleConfig & { parameters: ScheduleParameter[]; code: string; businessDate: string; count: number }) => request<ParameterPreview[]>("/schedule-parameters/preview", json("POST", input)),
  preferences: () => request<Preferences>("/preferences"),
  savePreferences: (p: Preferences) =>
    request<Preferences>("/preferences", json("PUT", p)),
  upload: (id: string, file: File) => {
    const body = new FormData();
    body.append("file", file);
    return request<StudioObject>(`/objects/${id}/file`, {
      method: "POST",
      body,
    });
  },
  download: (id: string) => `/api/v1/objects/${id}/file`,
};
