import TaskScheduleEditor from "./TaskScheduleEditor";
import ScheduleEditor from "./ScheduleEditor";
import ScheduleParameterEditor from "./ScheduleParameterEditor";
import type { TaskRelease, SchedulingDraft, SchedulingKind, ObjectVersion, Run, StudioObject } from "../types";
import { useEffect, useState } from "react";
import { Alert, App, Button, Collapse, Empty, Form, Input, InputNumber, Modal, Select, Space, Spin, Tag } from "antd";
import { DiffEditor } from "@monaco-editor/react";
import { History, RotateCcw, GitCompareArrows } from "lucide-react";
import { api } from "../api";
import { getNodeType } from "../data/nodeTypes";
import { isRealTask } from "../state/workflows";
import "../panels.css";

interface InspectorProps {
  object: StudioObject;
  section: "schedule" | "versions";
  onChange: (patch: Partial<StudioObject>) => void;
  onRestored: (obj: StudioObject) => void;
  objects: StudioObject[];
  onRun: (run: Run) => void;
  onSaveObject: () => Promise<boolean>;
  scheduleRevision: number; scheduleDraft?: SchedulingDraft; onScheduleDraft: (draft?: SchedulingDraft) => void; onPublishTask: () => Promise<TaskRelease | undefined>;
  onViewInstances: (kind: SchedulingKind, scheduleId: string) => void;
}
const dateLabel = (value: string) => value ? new Date(value).toLocaleString("zh-CN", { hour12: false }) : "—";
export default function Inspector({ object, section, onChange, onRestored, objects, onRun, onSaveObject, scheduleRevision, scheduleDraft, onScheduleDraft, onPublishTask, onViewInstances }: InspectorProps) {
  const { message, modal } = App.useApp();
  const [versions, setVersions] = useState<ObjectVersion[]>([]);
  const [loading, setLoading] = useState(false);
  const [compare, setCompare] = useState<ObjectVersion | null>(null);
  const config = object.config || {}, run = config.run || {}, schedule = config.schedule || {};
  const scheduleType = schedule.type === "NORMAL" ? "CYCLE" : schedule.type || "CYCLE";
  const patchConfig = (key: string, patch: Record<string, unknown>) => onChange({ config: { ...config, [key]: { ...config[key], ...patch } } });
  useEffect(() => {
    let alive = true;
    if (section !== "versions") return;
    setLoading(true);
    void api.versions(object.id).then(data => { if (alive) setVersions(data); }).catch(e => message.error(e.message)).finally(() => { if (alive) setLoading(false); });
    return () => { alive = false; };
  }, [object.id, object.version, section]);
  const restore = (version: ObjectVersion) => modal.confirm({
    title: `恢复到版本 V${version.version}？`, content: "恢复后新增版本，保留历史版本。当前尚未保存的编辑将被替换。", okText: "恢复版本", cancelText: "取消",
    onOk: async () => { try { onRestored(await api.restoreVersion(object.id, version.id, object.version)); } catch (e) { message.error((e as Error).message); throw e; } },
  });
  return (<div className="studio-inspector">
      {section === "schedule" && run.provider === "WORKFLOW" && <ScheduleEditor key={`${object.id}/${scheduleRevision}`} object={object} onObjectChange={onChange} onSaveObject={onSaveObject} onRun={onRun} draft={scheduleDraft && "workflowId" in scheduleDraft.input ? scheduleDraft as import("../types").WorkflowScheduleDraft : undefined} onDraftChange={onScheduleDraft} onViewInstances={id=>onViewInstances("WORKFLOW",id)} />}
      {section === "schedule" && isRealTask(object) && <TaskScheduleEditor key={`${object.id}/${scheduleRevision}`} object={object} objects={objects} onObjectChange={onChange} onSaveObject={onSaveObject} draft={scheduleDraft && "taskId" in scheduleDraft.input ? scheduleDraft as import("../types").TaskScheduleDraft : undefined} onDraftChange={onScheduleDraft} onRun={onRun} onPublish={onPublishTask} onViewInstances={id=>onViewInstances("TASK",id)} />}
      {section === "schedule" && run.provider !== "WORKFLOW" && !isRealTask(object) && (
        <Form layout="vertical" size="small">
          <ScheduleParameterEditor key={object.id} object={object} onChange={onChange} />
          <h3>调度策略</h3>
          <Form.Item label="调度类型"><Select value={scheduleType} options={[{value:"CYCLE",label:"周期调度"},{value:"MANUAL",label:"手动调度"},{value:"TRIGGER",label:"事件触发"}]} onChange={type=>patchConfig("schedule",{type})}/></Form.Item>
          <Form.Item label="失败重试次数"><InputNumber min={0} max={10} value={schedule.retries??0} onChange={retries=>patchConfig("schedule",{retries:retries??0})}/></Form.Item>
          <Form.Item label="重试间隔（秒）"><InputNumber min={1} max={3600} value={schedule.retryInterval??60} onChange={retryInterval=>patchConfig("schedule",{retryInterval:retryInterval??60})}/></Form.Item>
          <h3>调度时间</h3>
          <Form.Item label="调度时区"><Select value={schedule.timezone||"Asia/Shanghai"} options={["Asia/Shanghai","UTC","Asia/Tokyo","Europe/London","America/New_York"].map(value=>({value,label:value}))} onChange={timezone=>patchConfig("schedule",{timezone})}/></Form.Item>
          <Form.Item label="业务日期偏移"><InputNumber min={-365} max={0} value={schedule.businessDateOffset??-1} onChange={businessDateOffset=>patchConfig("schedule",{businessDateOffset:businessDateOffset??-1})}/></Form.Item>
          <Form.Item label="调度周期"><Select value={schedule.cycle||"DAY"} options={[{value:"MINUTE",label:"分钟"},{value:"HOUR",label:"小时"},{value:"DAY",label:"日"},{value:"WEEK",label:"周"},{value:"MONTH",label:"月"}]} onChange={cycle=>patchConfig("schedule",{cycle})}/></Form.Item>
          <Form.Item label="Cron 表达式"><Input value={schedule.cron||"0 0 2 * * *"} onChange={e=>patchConfig("schedule",{cron:e.target.value})}/></Form.Item>
          <Form.Item label="生效日期"><Input type="date" value={schedule.startDate||""} onChange={e=>patchConfig("schedule",{startDate:e.target.value})}/></Form.Item>
          <Form.Item label="结束日期"><Input type="date" value={schedule.endDate||""} onChange={e=>patchConfig("schedule",{endDate:e.target.value})}/></Form.Item>
          <h3>调度依赖</h3>
          <Form.Item label="上游节点"><Select mode="multiple" value={schedule.dependencies||[]} options={objects.filter(o=>o.kind==="NODE"&&o.id!==object.id).map(o=>({value:o.id,label:o.name}))} onChange={dependencies=>patchConfig("schedule",{dependencies})}/></Form.Item>
          <p className="panel-muted">此类型为本地模拟节点，配置可保存和预览，不自动执行。</p>
          <Button type="primary" onClick={()=>void onSaveObject()}>保存配置</Button>
        </Form>
      )}
      {section === "versions" && (
        <>
          <Alert
            type="info"
            title="每次成功保存都会生成版本快照。"
            className="panel-notice"
          />
          <Spin spinning={loading}>
            <ul
              className="ant-list-items"
              style={{ margin: 0, padding: 0, listStyle: "none" }}
            >
              {versions.length === 0 && (
                <li>
                  <Empty
                    image={Empty.PRESENTED_IMAGE_SIMPLE}
                    description="暂无历史版本"
                  />
                </li>
              )}
              {versions.map((version) => (
                <li key={version.id} className="ant-list-item version-list-row">
                  <div className="version-entry">
                    <div>
                      <History size={14} />
                      <b>V{version.version}</b>
                      {version.version === object.version && (
                        <Tag color="blue">当前</Tag>
                      )}
                    </div>
                    <span>{dateLabel(version.createdAt)}</span>
                    <small>{version.name}</small>
                    <Space size={5}>
                      <Button
                        size="small"
                        icon={<GitCompareArrows size={13} />}
                        onClick={() => setCompare(version)}
                      >
                        对比
                      </Button>
                      <Button
                        size="small"
                        icon={<RotateCcw size={13} />}
                        onClick={() => restore(version)}
                      >
                        恢复
                      </Button>
                    </Space>
                  </div>
                </li>
              ))}
            </ul>
          </Spin>
          <Modal
            title={`版本 V${compare?.version} 与当前编辑内容对比`}
            open={!!compare}
            width="85vw"
            footer={<Button onClick={() => setCompare(null)}>关闭</Button>}
            onCancel={() => setCompare(null)}
            destroyOnHidden
          >
            <div className="diff-headings">
              <span>历史版本 V{compare?.version}</span>
              <span>当前内容</span>
            </div>
            <DiffEditor
              height="55vh"
              theme="vs-dark"
              original={compare?.content || ""}
              modified={object.content || ""}
              language={getNodeType(object.nodeType).language}
              options={{
                readOnly: true,
                minimap: { enabled: false },
                fontSize: 13,
                automaticLayout: true,
              }}
            />
            <Collapse
              size="small"
              items={[
                {
                  key: "config",
                  label: "配置差异",
                  children: (
                    <div className="config-compare">
                      <pre>
                        {JSON.stringify(compare?.config || {}, null, 2)}
                      </pre>
                      <pre>{JSON.stringify(object.config || {}, null, 2)}</pre>
                    </div>
                  ),
                },
              ]}
            />
          </Modal>
        </>
      )}
    </div>
  );
}
