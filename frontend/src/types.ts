export type ObjectKind =
  | "FOLDER"
  | "NODE"
  | "WORKFLOW"
  | "NOTEBOOK"
  | "TABLE"
  | "RESOURCE"
  | "FUNCTION"
  | "COMPONENT"
  | "PERSONAL"
  | "ENVIRONMENT";
export interface Workspace {
  id: string;
  name: string;
  code: string;
  region: string;
  type: "DEFAULT" | "USER";
}
export interface WorkspaceInput { name: string; code: string }
export interface StudioObject {
  id: string;
  workspaceId: string;
  parentId: string | null;
  kind: ObjectKind;
  nodeType: string;
  name: string;
  description: string;
  content: string;
  config: Record<string, any>;
  tags: string[];
  favorite: boolean;
  deleted: boolean;
  version: number;
  owner: string;
  updatedAt: string;
}
export type ObjectInput = Partial<StudioObject> & {
  workspaceId: string;
  name: string;
  kind: ObjectKind;
};
export interface ObjectVersion {
  id: string;
  objectId: string;
  version: number;
  name: string;
  content: string;
  config: Record<string, any>;
  createdAt: string;
}
export interface GraphNode {
  id: string;
  objectId?: string;
  label: string;
  nodeType: string;
  x: number;
  y: number;
}
export interface GraphEdge {
  id: string;
  source: string;
  target: string;
}
export interface WorkflowGraph {
  nodes: GraphNode[];
  edges: GraphEdge[];
}
export interface Run {
  id: string;
  workspaceId: string;
  objectId: string;
  objectName: string;
  status: "WAITING" | "QUEUED" | "RUNNING" | "SUCCESS" | "FAILED" | "CANCELLED" | "SKIPPED" | "RECOVERING";
  mode: string;
  simulation: boolean;
  logs: string[];
  columns: string[];
  rows: unknown[][];
  createdAt: string;
  startedAt?: string;
  finishedAt?: string;
  provider?: "SYNC" | "MYSQL" | "DORIS" | "SIMULATION" | "WORKFLOW";
  parentRunId?: string;
  graphNodeId?: string;
  executionSource?: "DEVELOPMENT" | "RELEASE";
  releaseKind?: "TASK" | "WORKFLOW";
  upstreamRuns?: {taskId: string; name?: string; alias: string; runId: string; buildId?: string}[];
  lineage?: Record<string,string>;
  releaseId?: string;
  releaseNo?: number;
  nodeCount?: number;
  objectVersion?: number;
  elapsedMs?: number;
  rowCount?: number;
  affectedRows?: number;
  containsWrites?: boolean;
  statements?: SqlStatementResult[];
  truncated?: boolean;
  errorCode?: string;
  scheduleParameters?: Record<string, string>;
  debugParameters?: Record<string, string>;
  dataSource?: DataSource;
  sourceDataSource?:DataSource;targetDataSource?:DataSource;sourceTable?:string;
  writeMode?:string;syncStage?:string;syncMessage?:string;remoteRunId?:string;remoteState?:string;
  readRows?:number;readBytes?:number;committedBatches?:number;filteredRows?:number;serverAffectedRows?:number;
  partialWrite?:boolean;commitUnknown?:boolean;cancelRequested?:boolean;clearStatus?:string;
  resolvedTargetPartitions?:string[];resolvedPartitionAssignments?:{target:string;value?:string;source?:string}[];clearScope?:string;
  snapshot?: StudioObject;
  materialization?: boolean;
  executionMode?: "QUERY" | "MATERIALIZE";
  publicationStatus?: "STAGING" | "STAGED" | "PUBLISHED" | "NOT_PUBLISHED" | "RECOVERING";
  businessDate?: string; sourceCutoffAt?: string; buildId?: string;
  writtenRows?: number; targetTable?: string;
  triggerType?: "MANUAL" | "SCHEDULED" | "RERUN";
  scheduleId?: string; triggerId?: string; scheduledAt?: string; attempt?: number;
}
export interface WorkflowRelease {
  id: string; workspaceId: string; workflowId: string; releaseNo: number;
  workflowVersion: number; name: string; note: string; createdAt: string; containsWrites?: boolean;
  bundle?: { schemaVersion: number; workflow: StudioObject;
    nodes: { graphNodeId: string; object: StudioObject }[]; datasourceBindings: DataSource[] };
}
export interface DataSource {
  type:"MYSQL"|"DORIS";options?:{feHttpUrls?:string[];beHttpUrls?:string[];flightUri?:string;flightEndpointMap?:Record<string,string>;httpEndpointMap?:Record<string,string>;flinkHost?:string;flinkPort?:number;flinkFeHttpUrls?:string[];flinkBeHttpUrls?:string[]};
  id: string; workspaceId: string; name: string; host: string; port: number;
  database: string; username: string; passwordSet: boolean;
}
export interface ScheduleConfig {
  cron: string; cycle: string; timezone: string; startDate: string; endDate: string;
  businessDateOffset: number; retries: number; retryIntervalSeconds: number; enabled: boolean;
}
export interface WorkflowSchedule extends ScheduleConfig {
  id: string; workspaceId: string; workflowId: string; name: string; releaseId: string; releaseNo: number;
  version: number; nextFireAt: string | null; updatedAt: string;
}
export interface ScheduleTrigger {
  id: string; scheduleId: string; releaseId: string; releaseNo: number; scheduledAt: string; businessDate: string;
  sourceCutoffAt: string; attempt: number; status: string; reason?: string; missedUntil?: string; runId?: string;
  attempts: { attempt: number; runId: string; createdAt: string }[];
}
export interface SchedulePreview { scheduledAt: string; localTime: string; businessDate: string }
export interface TaskDependency { taskId: string; name?: string; alias: string; matchMode?: "LATEST" | "ALL_DAY" }
export interface TaskRelease { id: string; taskId: string; workspaceId: string; releaseNo: number; objectVersion: number; name: string; note: string; createdAt: string; snapshot?: StudioObject; containsWrites?: boolean }
export interface TaskScheduleInput extends ScheduleConfig { taskId: string; releaseId: string; dependencies: TaskDependency[]; expectedVersion?: number }
export interface TaskSchedule extends TaskScheduleInput { id: string; workspaceId: string; name: string; version: number; releaseNo?: number; nextFireAt?: string }
export interface TaskScheduleDraft { input: TaskScheduleInput; id?: string }
export interface WorkflowScheduleDraft { input: ScheduleConfig & { workflowId: string; releaseId: string; expectedVersion?: number }; id?: string }
export type SchedulingDraft = TaskScheduleDraft | WorkflowScheduleDraft;
export interface DependencySlot extends TaskDependency { scheduleId?: string; scheduledAt?: string; triggerId?: string; status?: string; reason?: string; warning?: string; cron?: string; expectedCount?: number; expectedSlots?: string[]; timezone?: string }
export interface TaskPreview extends SchedulePreview { dependencies: DependencySlot[] }
export interface TaskTrigger extends ScheduleTrigger { taskId: string; dependencySlots: DependencySlot[]; upstreamRuns?: {taskId: string; alias: string; runId: string; buildId?: string}[] }
export type DataSourceInput = Omit<DataSource, "id" | "passwordSet"> & { password?: string };
export interface SqlStatementResult {
  statementIndex: number;
  kind: "QUERY" | "UPDATE" | "DDL";
  status: "SKIPPED" | "RUNNING" | "SUCCESS" | "FAILED" | "CANCELLED" | "UNKNOWN";
  commitStatus: "NOT_STARTED" | "NOT_APPLICABLE" | "IN_PROGRESS" | "COMMITTED" | "NOT_COMMITTED" | "UNKNOWN";
  elapsedMs: number;
  affectedRows?: number;
  errorCode?: string;
  message?: string;
}
export interface QueryResult extends Partial<SqlStatementResult> {
  columns: string[]; rows: unknown[][]; total: number; page: number; pageSize: number; truncated: boolean;
  statements?: SqlStatementResult[];
}
export interface RunPage { items: Run[]; total: number; page: number; pageSize: number; stats: Record<string, number> }
export interface StudioRecord {
  id: string;
  workspaceId: string;
  objectId?: string;
  kind: "RELEASE";
  title: string;
  status: string;
  payload: Record<string, any>;
  createdAt: string;
}
export interface Preferences {
  theme?: "dark" | "light";
  sidebarWidth?: number;
  openTabs?: string[];
  activeId?: string;
  workspaceId?: string;
  editorFontSize?: number;
  wordWrap?: boolean;
  [key: string]: unknown;
}
export type Activity = "development" | "realtime" | "scheduling" | "datasources" | "recycle";
export type SchedulingKind = "TASK" | "WORKFLOW";
export interface SchedulingTask {
  kind: SchedulingKind; objectId: string; scheduleId?: string; name: string; nodeType?: string;
  enabled: boolean; status: "ENABLED" | "PAUSED" | "UNCONFIGURED" | "ENDED";
  releaseId?: string; releaseNo?: number; latestReleaseId?: string; latestReleaseNo?: number;
  version?: number; cron?: string; timezone?: string; nextFireAt?: string | null;
  latestStatus?: string; latestReason?: string; retrySupported?: boolean;
}
export interface SchedulingInstance extends ScheduleTrigger {
  kind: SchedulingKind; name: string; objectId?: string; timezone?: string;
  source?: "SCHEDULED" | "BACKFILL" | "RERUN"; batchKey?: string;
  dependencySlots?: DependencySlot[]; nextRetryAt?: string; finishedAt?: string;
  attempts: { attempt: number; runId: string; createdAt: string; status?: string; reason?: string; releaseNo?: number; sourceCutoffAt?: string }[];
}
export interface SchedulingPage<T> { items: T[]; total: number; page: number; pageSize: number }
export interface BackfillInput {
  workspaceId: string; kind: SchedulingKind; scheduleId: string; startDate: string; endDate: string;
  startTime?: string; endTime?: string; includeDownstream: boolean;
}
export interface BackfillPreviewItem {
  kind: SchedulingKind; scheduleId: string; taskId?: string; name: string;
  scheduledAt: string; businessDate: string; timezone?: string; status?: string; reason?: string; existingTriggerId?: string;
  releaseId?: string; releaseNo?: number; parameters?: Record<string, string>;
  dependencySlots?: DependencySlot[]; missingUpstreams?: unknown[];
}
export interface BackfillPreview { token: string; items: BackfillPreviewItem[]; total: number; warnings: string[] }
export interface BackfillResult { batchKey: string; instances: SchedulingInstance[]; total: number; stats?: Record<string, number> }
export interface ScheduleParameter { name: string; value: string; source: "CODE" | "MANUAL" }
export interface ParameterPreview { businessDate: string; scheduledAt: string; timezone: string; values: Record<string,string> }
export interface RunParameterValue {
  name: string;
  value?: string;
  defaultValue?: string;
  source: "DEBUG" | "SCHEDULE" | "MISSING";
}
export interface RunParameterPreparation {
  parameters: RunParameterValue[];
  debugParameters: Record<string, string>;
  missingParameters: string[];
}

export interface SyncConfig {
 sourceDataSourceId?:string;targetDataSourceId?:string;sourceTable?:string;targetTable?:string;
 columns?:string[];where?:string;mapping?:{source:string;target:string}[];
 sourcePartitionFilter?:{partitions?:string[];where?:string};
 targetPartitionAssignments?:SyncPartitionAssignment[];targetPartitions?:string[];
 writeMode?:"append"|"upsert"|"overwrite";keyColumns?:string[];batchRows?:number;parallelism?:number;timeoutSeconds?:number;
}
export interface SyncPartitionAssignment {target:string;mode:"value"|"column";value?:string;source?:string}
export interface SyncMetadata {
 model:string;uniqueKeys:string[][];columns:{name:string;type:string;nullable:string;columnKey:string;comment:string}[];
 partition:{type:"NONE"|"RANGE"|"LIST";automatic:boolean;expression?:string;columns:{name:string;type:string}[];partitions:{name:string;range:string;lower?:string;upper?:string;values?:string[]}[]};
}
