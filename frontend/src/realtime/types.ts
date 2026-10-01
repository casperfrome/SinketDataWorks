import type { DataSource } from "../types";

export interface KafkaDatasource {
  id: string; workspaceId: string; type: "KAFKA"; name: string; bootstrapServers: string;
  securityProtocol: "PLAINTEXT" | "SASL_PLAINTEXT" | "SASL_SSL";
  saslMechanism: "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512"; username: string;
  passwordSet?: boolean; flinkBootstrapServers?: string;
}
export type KafkaDatasourceInput = Omit<KafkaDatasource, "id" | "passwordSet"> & { password?: string };
export type RealtimeDatasource = DataSource | KafkaDatasource;
export interface RealtimeField { id: string; name: string; type: string; nullable: boolean; primaryKey: boolean }
export interface RealtimeBinding {
  id: string; role: "SOURCE" | "SINK"; connector: "KAFKA" | "MYSQL_CDC" | "MYSQL_JDBC" | "DORIS";
  datasourceId: string; tableName: string; physicalTable: string; topic: string; fields: RealtimeField[];
  format: "json" | "csv"; consumerGroup: string; startupMode: "group-offsets" | "earliest-offset" | "latest-offset";
  writeMode: "append" | "upsert"; cdcStartupMode: "initial" | "latest-offset"; serverId: string; timezone: string;
  eventTimeField: string; watermarkSeconds: number; dorisModel: "DUPLICATE" | "UNIQUE" | "AGGREGATE";
  syncDeletes: boolean; labelPrefix: string; feHttpUrls: string;
}
export interface RealtimeRuntime { parallelism: number; checkpointSeconds: number; restartAttempts: number; restartDelaySeconds: number }
export interface RealtimeTask {
  id: string; workspaceId: string; folderId: string | null; name: string; description: string; sql: string;
  bindings: RealtimeBinding[]; runtime: RealtimeRuntime; updatedAt: string; revision?: number;
}
export interface RealtimeFolder { id: string; name: string }
export interface RealtimeRelease { id: string; taskId: string; releaseNo: number; createdAt: string; note: string; snapshot: RealtimeTask; importWarning?: string }
export type RealtimeJobStatus = "STARTING" | "RUNNING" | "STOPPING" | "STOPPED" | "RESTARTING" | "FAILED" | "CREATED" | "RECONCILING" | "CANCELED" | "CANCELLING" | "FINISHED" | "SUSPENDED" | "UNKNOWN" | "UPGRADING";
export interface RealtimeJobEvent { id: string; at: string; message: string }
export interface RealtimeSavepoint { id: string; createdAt: string; path: string; releaseId: string; status?: string; jobId?: string }
export interface RealtimeJob {
  id: string; taskId: string; releaseId: string; status: RealtimeJobStatus; createdAt: string; startedAt: string;
  transitionAt?: string; stoppedAt?: string; restoredFrom?: string; logs: RealtimeJobEvent[]; savepoints: RealtimeSavepoint[];
  checkpointBase: number;
  flinkJobId?: string; error?: string; errorMessage?: string; operationId?: string; workspaceId?: string;
  metrics?: { inputRate?: number | null; outputRate?: number | null; latencyMs?: number | null; uptimeSeconds?: number; [key: string]: unknown };
  checkpoints?: unknown;
  checkpointCount?: number; flinkState?: string;
  exceptions?: Record<string,unknown>;
  nodeLogs?: {node:string;file:string;content:string;containsJobId:boolean;truncated:boolean}[];
  sharedNodeLogs?: boolean;
  connectionMessage?: string;
  gatewayCleanupStatus?: "PENDING" | "COMPLETED";
  gatewayCleanupMessage?: string;
  durationMs?: number;
}
export interface RealtimeOperation { id: string; operationId?: string; status: string; kind?: string; type?: string; phase?: string; jobId?: string; error?: string; errorCode?: string; errorMessage?: string; message?: string; result?: unknown; createdAt?: string; updatedAt?: string; canRollback?: boolean; rollbackAvailable?: boolean }
export interface RealtimeSubmission { operationId: string; jobId?: string; previewId?: string }
export interface RealtimePreview { id?: string; previewId?: string; status?: string; flinkJobId?: string; cleanupStatus?: string; columns?: (string | { name: string; type?: string; dataType?: string; logicalType?: unknown })[]; rows?: (unknown[] | { kind?: string; fields: unknown[] })[]; data?: { fields?: unknown[]; rowKind?: string }[]; rowKinds?: string[]; token?: number | string; nextToken?: number | string | null; resultType?: string; truncated?: boolean; message?: string; error?: string; rowCount?: number }
export interface RealtimeState {
  schemaVersion: 1; tasks: RealtimeTask[]; folders: RealtimeFolder[]; drafts: Record<string, RealtimeTask>;
  releases: RealtimeRelease[]; jobs: RealtimeJob[]; kafkaSources: KafkaDatasource[];
  openTabs: string[]; activeId: string; view: "development" | "operations"; selectedJobId: string;
  operations?: RealtimeOperation[];
  idMap?: Record<string,string>;
}
