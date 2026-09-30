import type { RunParameterPreparation, RunParameterValue } from "../types.ts";

export type RunParameterMode = "NORMAL" | "CUSTOM";

export function initialDebugParameters(preparation: RunParameterPreparation): Record<string, string> {
  return { ...preparation.debugParameters };
}

/** Restoring a default must not reuse the previous DEBUG value from preparation. */
export function effectiveRunParameters(preparation: RunParameterPreparation, debugParameters: Record<string, string>): RunParameterValue[] {
  return preparation.parameters.map(parameter => {
    const defaultValue = parameter.defaultValue ?? (parameter.source === "SCHEDULE" ? parameter.value : undefined);
    if (Object.hasOwn(debugParameters, parameter.name)) {
      return { ...parameter, defaultValue, value: debugParameters[parameter.name], source: "DEBUG" };
    }
    return { ...parameter, defaultValue, value: defaultValue, source: defaultValue === undefined ? "MISSING" : "SCHEDULE" };
  });
}

export function setDebugParameter(parameters: Record<string, string>, name: string, value: string): Record<string, string> {
  return { ...parameters, [name]: value };
}

export function restoreRunParameterDefault(parameters: Record<string, string>, name: string): Record<string, string> {
  const next = { ...parameters };
  delete next[name];
  return next;
}

export function runParameterValueError(value: string | undefined): string {
  if (value === undefined || value.length === 0) return "请填写参数值";
  if (value.length > 4096) return "参数值不能超过 4096 个字符";
  return "";
}

export function runParameterError(preparation: RunParameterPreparation, debugParameters: Record<string, string>): string {
  for (const parameter of effectiveRunParameters(preparation, debugParameters)) {
    const error = runParameterValueError(parameter.value);
    if (error) return `参数「${parameter.name}」：${error}`;
  }
  return "";
}

export function shouldOpenRunParameters(mode: RunParameterMode, preparation: RunParameterPreparation): boolean {
  return mode === "CUSTOM" || preparation.missingParameters.length > 0;
}
