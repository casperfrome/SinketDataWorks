import type { DataSource } from "../types";

export interface KafkaDatasource {
  id: string; workspaceId: string; type: "KAFKA"; name: string; bootstrapServers: string;
  securityProtocol: "PLAINTEXT" | "SASL_PLAINTEXT" | "SASL_SSL";
  saslMechanism: "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512"; username: string;
}
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
  bindings: RealtimeBinding[]; runtime: RealtimeRuntime; updatedAt: string;
}
export interface RealtimeFolder { id: string; name: string }
export interface RealtimeRelease { id: string; taskId: string; releaseNo: number; createdAt: string; note: string; snapshot: RealtimeTask }
export type RealtimeJobStatus = "STARTING" | "RUNNING" | "STOPPING" | "STOPPED" | "RESTARTING" | "FAILED";
export interface RealtimeJobEvent { id: string; at: string; message: string }
export interface RealtimeSavepoint { id: string; createdAt: string; path: string; releaseId: string }
export interface RealtimeJob {
  id: string; taskId: string; releaseId: string; status: RealtimeJobStatus; createdAt: string; startedAt: string;
  transitionAt?: string; stoppedAt?: string; restoredFrom?: string; logs: RealtimeJobEvent[]; savepoints: RealtimeSavepoint[];
  checkpointBase: number;
}
export interface RealtimeState {
  schemaVersion: 1; tasks: RealtimeTask[]; folders: RealtimeFolder[]; drafts: Record<string, RealtimeTask>;
  releases: RealtimeRelease[]; jobs: RealtimeJob[]; kafkaSources: KafkaDatasource[];
  openTabs: string[]; activeId: string; view: "development" | "operations"; selectedJobId: string;
}
