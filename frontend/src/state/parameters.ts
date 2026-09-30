import type { ScheduleParameter } from "../types.ts";

export function parameterError(rows: ScheduleParameter[]): string {
  const names = new Set<string>();
  for (const row of rows) {
    if (!/^[A-Za-z_][A-Za-z0-9_]{0,63}$/.test(row.name)) return "参数名须以字母或下划线开头，且不超过 64 个字符";
    if (names.has(row.name)) return `参数名重复：${row.name}`;
    names.add(row.name);
    if (!row.value || /[\s=]/.test(row.value)) return `参数「${row.name}」的值不能为空或包含空白、等号`;
  }
  return "";
}
export function parameterExpression(rows: ScheduleParameter[]): string {
  return rows.map(row => `${row.name}=${row.value}`).join(" ");
}
export function parseParameterExpression(text: string, previous: ScheduleParameter[]): ScheduleParameter[] {
  const rows = text.trim() ? text.trim().split(/\s+/).map(token => {
    const parts = token.split("=");
    if (parts.length !== 2) throw new Error("请使用 name=value，多个参数之间用空格分隔");
    const [name, value] = parts;
    return { name, value, source: previous.find(row => row.name === name)?.source || "MANUAL" } as ScheduleParameter;
  }) : [];
  const error = parameterError(rows);
  if (error) throw new Error(error);
  return rows;
}
export function mergeCodeParameters(rows: ScheduleParameter[], names: string[]): ScheduleParameter[] {
  const result = rows.map(row => ({ ...row, source: names.includes(row.name) ? "CODE" as const : row.source }));
  const existing = new Set(rows.map(row => row.name));
  for (const name of new Set(names)) if (!existing.has(name)) result.push({ name, value: "", source: "CODE" });
  return result;
}
