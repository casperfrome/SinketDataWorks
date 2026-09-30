import type { DataSource, StudioObject, SyncConfig, SyncMetadata, SyncPartitionAssignment } from "../types.ts";

export const schemaLabel = (source: DataSource) => `${source.database} · ${source.type === "DORIS" ? "Doris" : "MySQL"} / ${source.name}`;
const internalParameter = /^:(?:bizdate|source_cutoff|build_id|upstream_[A-Za-z][A-Za-z0-9_]{0,31}_build_id)$/;
function partitionParameterText(value = ""): string {
  if (internalParameter.exec(value)?.[0] === value) return value;
  return `'${value.replaceAll("\\", "\\\\").replaceAll("'", "''")}'`;
}
export function syncParameterText(config: SyncConfig = {}): string {
  return [config.where, config.sourcePartitionFilter?.where, ...(config.targetPartitionAssignments || []).filter(a => a.mode === "value").map(a => partitionParameterText(a.value))].filter(Boolean).join("\n");
}
export const parameterText = (object: StudioObject) => object.config.run?.provider === "SYNC" ? syncParameterText(object.config.sync) : object.content;

export function defaultPartitionAssignments(metadata: SyncMetadata): SyncPartitionAssignment[] {
  return (metadata.partition?.columns || []).filter(column => /^DATE(?:V2)?(?:\b|$)/i.test(column.type)).map(column => ({ target: column.name, mode: "value", value: "${bizdate}" }));
}
export function applySyncChange(object: StudioObject, patch: Partial<SyncConfig>): Partial<StudioObject> {
  const sync: SyncConfig = { ...object.config.sync, ...patch };
  const config: StudioObject["config"] = { ...object.config, run: { ...object.config.run, provider: "SYNC" }, sync };
  if ((patch.targetPartitionAssignments || []).some(a => a.mode === "value" && a.value === "${bizdate}")) {
    const schedule = config.schedule || {};
    if (!(schedule.parameters || []).some((p: { name: string }) => p.name === "bizdate")) {
      config.schedule = { ...schedule, parameters: [...(schedule.parameters || []), { name: "bizdate", value: "$bizdate", source: "CODE" }] };
      if (schedule.parameterExpressionDraft !== undefined) config.schedule.parameterExpressionDraft = `${schedule.parameterExpressionDraft.trim()} bizdate=$bizdate`.trim();
    }
  }
  return { config };
}
export function sourceTableReset(config: SyncConfig): Partial<SyncConfig> {
  return { columns: [], mapping: [], where: undefined, sourcePartitionFilter: undefined, targetPartitionAssignments: (config.targetPartitionAssignments || []).filter(a => a.mode === "value") };
}
export const targetTableReset = (): Partial<SyncConfig> => ({ mapping: [], keyColumns: [], targetPartitionAssignments: [], targetPartitions: [] });
