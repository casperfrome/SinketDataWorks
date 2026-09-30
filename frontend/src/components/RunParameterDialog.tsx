import { useEffect, useId, useRef, useState } from "react";
import { Alert, Button, Empty, Input, Modal, Space, Tag } from "antd";
import type { InputRef } from "antd";
import type { RunParameterPreparation } from "../types";
import {
  effectiveRunParameters,
  initialDebugParameters,
  restoreRunParameterDefault,
  runParameterError,
  runParameterValueError,
  setDebugParameter,
} from "../state/runParameters";
import type { RunParameterMode } from "../state/runParameters";
import "../run-parameters.css";

export interface RunParameterDialogProps {
  open: boolean;
  objectName: string;
  mode: RunParameterMode;
  preparation: RunParameterPreparation;
  busy: boolean;
  error?: string;
  onCancel: () => void;
  onRun: (debugParameters: Record<string, string>) => void | Promise<void>;
}

const sourceNames = { DEBUG: "调试参数", SCHEDULE: "调度默认", MISSING: "待填写" };

export default function RunParameterDialog({ open, objectName, mode, preparation, busy, error, onCancel, onRun }: RunParameterDialogProps) {
  const [debugParameters, setDebugParameters] = useState(() => initialDebugParameters(preparation));
  const [submitError, setSubmitError] = useState("");
  const inputs = useRef(new Map<string, InputRef>());
  const submitting = useRef(false);
  const id = useId();
  useEffect(() => {
    if (open) {
      setDebugParameters(initialDebugParameters(preparation));
      setSubmitError("");
    }
  }, [open, preparation]);
  const rows = effectiveRunParameters(preparation, debugParameters);
  const validationError = runParameterError(preparation, debugParameters);
  const firstMissing = rows.find(row => row.value === undefined || row.value.length === 0)?.name;
  const changeValue = (name: string, value: string) => {
    setDebugParameters(current => setDebugParameter(current, name, value));
    setSubmitError("");
  };
  const run = async () => {
    if (busy || submitting.current || validationError) return;
    submitting.current = true;
    setSubmitError("");
    try { await onRun({ ...debugParameters }); }
    catch (reason) { setSubmitError(reason instanceof Error ? reason.message : "运行提交失败，请重试"); }
    finally { submitting.current = false; }
  };
  const cancel = () => { if (!busy && !submitting.current) onCancel(); };
  return <Modal
    title={`${mode === "CUSTOM" ? "带参运行" : "运行"} · ${objectName}`}
    open={open}
    width={740}
    className="run-parameter-dialog"
    onCancel={cancel}
    keyboard={!busy}
    closable={!busy}
    maskClosable={!busy}
    afterOpenChange={visible => { if (visible && firstMissing) inputs.current.get(firstMissing)?.focus(); }}
    footer={<Space><Button disabled={busy} onClick={cancel}>取消</Button><Button type="primary" loading={busy} disabled={!!validationError} onClick={() => void run()}>运行</Button></Space>}
  >
    <p className="run-parameter-description">{mode === "NORMAL" ? "代码中的参数还未赋值，请补齐后运行。" : "调整本次运行使用的参数。"}运行后记住本节点的手填参数。调试参数不会修改调度配置。</p>
    {!!rows.length ? <>
      <div className="run-parameter-heading"><span>运行参数</span><Button type="link" size="small" disabled={busy || !Object.keys(debugParameters).length} onClick={() => { setDebugParameters({}); setSubmitError(""); }}>恢复调度默认值</Button></div>
      <div className="run-parameter-table-wrap"><table className="run-parameter-table"><thead><tr><th>参数名</th><th>参数值</th><th>来源</th><th>操作</th></tr></thead><tbody>
        {rows.map(row => {
          const valueError = runParameterValueError(row.value);
          const inputId = `${id}-${row.name}`;
          return <tr key={row.name}><td><label htmlFor={inputId}><code>{row.name}</code></label></td><td>
            <Input
              ref={input => { if (input) inputs.current.set(row.name, input); else inputs.current.delete(row.name); }}
              id={inputId}
              aria-label={`运行参数 ${row.name}`}
              aria-invalid={!!valueError}
              aria-describedby={valueError ? `${inputId}-error` : undefined}
              size="small"
              disabled={busy}
              status={valueError ? "error" : undefined}
              value={row.value ?? ""}
              maxLength={4096}
              placeholder="填写常量值"
              onChange={event => changeValue(row.name, event.target.value)}
            />
            {valueError && <span id={`${inputId}-error`} className="run-parameter-value-error">{valueError}</span>}
            {row.defaultValue !== undefined && row.source === "DEBUG" && <span className="run-parameter-default">默认：<code>{row.defaultValue}</code></span>}
          </td><td><Tag color={row.source === "DEBUG" ? "blue" : row.source === "MISSING" ? "orange" : undefined}>{sourceNames[row.source]}</Tag></td><td><Button type="link" size="small" disabled={busy || !Object.hasOwn(debugParameters, row.name)} aria-label={`恢复参数 ${row.name} 的默认值`} onClick={() => { setDebugParameters(current => restoreRunParameterDefault(current, row.name)); setSubmitError(""); }}>恢复默认</Button></td></tr>;
        })}
      </tbody></table></div>
    </> : <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="当前节点没有需要填写的参数，可直接运行" />}
    {(error || submitError) && <Alert type="error" showIcon title={error || submitError} className="run-parameter-submit-error" />}
  </Modal>;
}
