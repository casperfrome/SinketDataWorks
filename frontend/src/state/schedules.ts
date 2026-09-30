import type { ScheduleConfig } from "../types.ts";

export const defaultSchedule: ScheduleConfig = { cron: "0 0 2 * * *", cycle: "DAY", timezone: "Asia/Shanghai", startDate: "", endDate: "", businessDateOffset: -1, retries: 0, retryIntervalSeconds: 60, enabled: false };
export interface CycleFields { cycle: string; interval: number; time: string; weekdays: string[]; monthdays: number[] }
export function cycleCron({ cycle, interval, time, weekdays, monthdays }: CycleFields): string {
  const [hour, minute] = time.split(":").map(Number);
  if (!Number.isInteger(hour) || !Number.isInteger(minute) || hour < 0 || hour > 23 || minute < 0 || minute > 59) throw new Error("请选择有效时间");
  if (cycle === "MINUTE") { if (!Number.isInteger(interval) || interval < 1 || interval > 59) throw new Error("分钟间隔须为 1–59"); return `0 */${interval} * * * *`; }
  if (cycle === "HOUR") { if (!Number.isInteger(interval) || interval < 1 || interval > 23) throw new Error("小时间隔须为 1–23"); return `0 ${minute} */${interval} * * *`; }
  if (cycle === "WEEK") { if (!weekdays.length || weekdays.some(d => !["MON","TUE","WED","THU","FRI","SAT","SUN"].includes(d))) throw new Error("请选择每周执行日"); return `0 ${minute} ${hour} * * ${weekdays.join(",")}`; }
  if (cycle === "MONTH") { if (!monthdays.length || monthdays.some(d => !Number.isInteger(d) || d < 1 || d > 31)) throw new Error("请选择每月执行日"); return `0 ${minute} ${hour} ${monthdays.join(",")} * *`; }
  return `0 ${minute} ${hour} * * *`;
}
/** Recover ordinary form cycles from the persisted Cron; advanced expressions stay editable verbatim. */
export function cronFields(cron: string): CycleFields {
  const result: CycleFields = {cycle:"CUSTOM",interval:1,time:"02:00",weekdays:["MON"],monthdays:[1]};
  const p=cron.trim().split(/\s+/);if(p.length!==6 || !["0","00"].includes(p[0]) || p[4]!=="*")return result;
  const [,m,h,d,,w]=p;
  if(d==="*" && w==="*" && h==="*" && (m==="*" || /^\*\/\d+$/.test(m)))return {...result,cycle:"MINUTE",interval:m==="*"?1:Number(m.slice(2))};
  if(!/^\d+$/.test(m))return result;
  if(d==="*" && w==="*" && /^\*\/\d+$/.test(h))return {...result,cycle:"HOUR",interval:Number(h.slice(2)),time:`00:${m.padStart(2,"0")}`};
  if(!/^\d+$/.test(h))return result;const time=`${h.padStart(2,"0")}:${m.padStart(2,"0")}`;
  if(d==="*" && ["*","?"].includes(w))return {...result,cycle:"DAY",time};
  if(["*","?"].includes(d) && w.split(",").every(v=>["MON","TUE","WED","THU","FRI","SAT","SUN"].includes(v)))return {...result,cycle:"WEEK",time,weekdays:w.split(",")};
  if(["*","?"].includes(w) && /^\d+(,\d+)*$/.test(d))return {...result,cycle:"MONTH",time,monthdays:d.split(",").map(Number)};
  return result;
}
export const triggerNames: Record<string,string> = { PENDING:"待提交", WAITING_DEPENDENCY:"等待上游", WAITING_RESOURCE:"等待资源", BLOCKED:"依赖阻断", RUNNING:"运行中", SUCCESS:"成功", FAILED:"失败", CANCELLED:"已停止", SKIPPED:"已跳过", RETRY_WAIT:"等待重试" };
export const reasonNames: Record<string,string> = { OVERLAP:"上一轮任务未结束", WAITING_UPSTREAM:"等待对应期次的上游完成", UPSTREAM_NO_PLAN:"上游尚未配置计划", NO_MATCHING_UPSTREAM:"同业务日期内无匹配上游时点", UPSTREAM_INSTANCE_MISSING:"对应上游实例不存在（暂停或未启用）", UPSTREAM_FAILED:"对应上游失败", UPSTREAM_SKIPPED:"对应上游漏跑或跳过", UPSTREAM_BLOCKED:"对应上游依赖阻断", UPSTREAM_CANCELLED:"对应上游已停止", MATERIALIZATION_BUSY:"等待业务库执行资源", MISSED_INTERVAL:"停机或休眠期间漏跑", SERVICE_RESTARTED:"服务重启中断", DATASOURCE_UNAVAILABLE:"业务库暂不可用", DB_TRANSIENT:"数据库暂时繁忙", WORKFLOW_LIMIT:"活动工作流已达上限", QUERY_TIMEOUT:"运行超时", DATASOURCE_BINDING_CHANGED:"发布绑定的数据源已变化", RECOVERY_REQUIRED:"等待核实落表提交", COMMIT_UNKNOWN:"提交结果未知，请核实业务数据" };
export function yesterday(): string { const parts = new Intl.DateTimeFormat("en-CA", {timeZone:"Asia/Shanghai",year:"numeric",month:"2-digit",day:"2-digit"}).formatToParts(new Date()); const p = Object.fromEntries(parts.map(x=>[x.type,x.value])); const d = new Date(`${p.year}-${p.month}-${p.day}T00:00:00Z`); d.setUTCDate(d.getUTCDate()-1); return d.toISOString().slice(0,10); }
