import type { DependencySlot, ScheduleParameter } from "../types.ts";

export const activeInstanceStatuses = new Set(["PENDING", "WAITING_DEPENDENCY", "WAITING_RESOURCE", "RUNNING", "RETRY_WAIT"]);
export const taskStateNames: Record<string, string> = { ENABLED: "已启用", PAUSED: "已暂停", UNCONFIGURED: "未配置", ENDED: "已结束" };
export const instanceSourceNames: Record<string, string> = { SCHEDULED: "周期调度", BACKFILL: "补数", RERUN: "重跑" };

export function parametersDiffer(development: ScheduleParameter[] = [], released: ScheduleParameter[] = []): boolean {
  const canonical = (rows: ScheduleParameter[]) => JSON.stringify(rows.map(({ name, value }) => [name, value]).sort(([a], [b]) => a.localeCompare(b)));
  return canonical(development) !== canonical(released);
}

export function dependencySummary(slots: DependencySlot[]): { key: string; name: string; mode: string; count: number; slots: DependencySlot[] }[] {
  const groups = new Map<string, DependencySlot[]>();
  for (const slot of slots) { const key = `${slot.taskId}:${slot.alias}`; groups.set(key, [...(groups.get(key) || []), slot]); }
  return Array.from(groups, ([key, rows]) => ({ key, name: rows[0].name || rows[0].taskId, mode: rows[0].matchMode || "LATEST", count: rows[0].expectedCount ?? rows.filter(row => !!row.scheduledAt).length, slots: rows }));
}

export function validBackfillRange(startDate: string, endDate: string, startTime: string, endTime: string): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(startDate) || !/^\d{4}-\d{2}-\d{2}$/.test(endDate)) return "请选择开始和结束业务日期";
  if (endDate < startDate) return "结束业务日期不能早于开始日期";
  if ((startTime && !/^\d{2}:\d{2}$/.test(startTime)) || (endTime && !/^\d{2}:\d{2}$/.test(endTime))) return "请选择有效的期次时间";
  if (startTime && endTime && endTime < startTime) return "结束期次时间不能早于开始时间";
  return "";
}
