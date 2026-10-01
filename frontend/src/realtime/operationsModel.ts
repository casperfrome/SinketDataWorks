import type { RealtimeJob } from "./types.ts";

export function latestRealtimeJob(jobs: RealtimeJob[], taskId: string): RealtimeJob | undefined {
  return jobs.filter(job => job.taskId === taskId).reduce<RealtimeJob | undefined>((latest,job) => !latest || Date.parse(job.createdAt) > Date.parse(latest.createdAt) ? job : latest,undefined);
}

export function jobElapsedSeconds(job: RealtimeJob | undefined, now = Date.now()): number | undefined {
  if (!job) return undefined;
  if (typeof job.durationMs === "number" && job.durationMs >= 0) return Math.floor(job.durationMs / 1000);
  if (typeof job.metrics?.uptimeSeconds === "number" && Number.isFinite(job.metrics.uptimeSeconds)) return job.metrics.uptimeSeconds;
  const terminal = ["STOPPED","FAILED","CANCELED","FINISHED","SUSPENDED"].includes(job.status);
  const sampledAt = job.metrics?.sampledAt;
  const end = job.stoppedAt ? Date.parse(job.stoppedAt) : !terminal && job.connectionMessage && typeof sampledAt === "string" ? Date.parse(sampledAt) : !terminal && !job.connectionMessage ? now : undefined;
  const start = Date.parse(job.startedAt);
  return end !== undefined && Number.isFinite(end) && Number.isFinite(start) ? Math.max(0,Math.floor((end - start) / 1000)) : undefined;
}
