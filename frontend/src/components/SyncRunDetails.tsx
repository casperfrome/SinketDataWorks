import { useState } from "react";
import { Alert, App, Button, Descriptions, Input, Modal, Space, Table, Tag } from "antd";
import { api } from "../api";
import type { Run } from "../types";
import { isPending } from "../state/workflows";
import RunParameters from "./RunParameters";

const stages:Record<string,string>={WAITING_TARGET:"等待目标表",SUBMITTING:"提交任务",queued:"等待引擎",validate:"预检",pre_sql:"清空目标",transfer:"批量传输",post_sql:"收尾",complete:"完成",CANCELLING:"停止中"};
export default function SyncRunDetails({run,refresh}:{run:Run;refresh:()=>void}) {
  const {message}=App.useApp();const [batches,setBatches]=useState<Record<string,unknown>[]>([]);const [show,setShow]=useState(false);const [note,setNote]=useState("");const [busy,setBusy]=useState(false);
  const targetPartitions=run.resolvedTargetPartitions?.length?run.resolvedTargetPartitions.join("、"):run.resolvedPartitionAssignments?.some(a=>a.source)?"按来源字段自动路由":run.resolvedPartitionAssignments?.some(a=>a.value!==undefined)?"尚无物理分区":"—";
  return <div className="query-detail"><Space wrap><Tag color="green">数据集成</Tag><Tag>{run.cancelRequested&&isPending(run)?"停止并核实中":stages[run.syncStage||""]||run.status}</Tag>{isPending(run)&&<Button size="small" danger onClick={async()=>{try{await api.stop(run.parentRunId||run.id);refresh();}catch(e){message.error((e as Error).message);}}}>{run.parentRunId?"停止所属工作流":"停止任务"}</Button>}</Space>
    <Descriptions size="small" column={2} items={[
      {key:"source",label:"来源",children:`${run.sourceDataSource?.database || run.sourceDataSource?.name} / ${run.sourceTable}`},{key:"target",label:"目标",children:`${run.targetDataSource?.database || run.targetDataSource?.name} / ${run.targetTable}`},
      {key:"read",label:"读取行数",children:(run.readRows||0).toLocaleString()},{key:"write",label:"确认写入",children:(run.writtenRows||0).toLocaleString()},
      {key:"batch",label:"已提交批次",children:run.committedBatches||0},{key:"bytes",label:"读取字节",children:(run.readBytes||0).toLocaleString()},
      {key:"business",label:"业务日期",children:run.businessDate||"—"},{key:"elapsed",label:"耗时",children:run.elapsedMs!==undefined?`${(run.elapsedMs/1000).toFixed(1)} 秒`:"执行中"},
      {key:"version",label:"版本",children:`V${run.objectVersion}${run.releaseNo?` / R${run.releaseNo}`:""}`},{key:"remote",label:"引擎运行 ID",children:run.remoteRunId||"尚未提交"},
      {key:"partitions",label:"目标分区",children:targetPartitions},
      {key:"partitionValues",label:"分区字段",children:run.resolvedPartitionAssignments?.map(a=>`${a.target} = ${a.value ?? a.source ?? "—"}`).join("；") || "—"},
      {key:"clearScope",label:"覆盖范围",children:run.clearScope || "—"}
    ]}/>
    <RunParameters parameters={run.scheduleParameters} />
    {run.writeMode==="overwrite"&&(run.clearStatus==="CLEARED"||run.clearStatus==="MAY_HAVE_CLEARED")&&<Alert type="warning" title={run.clearStatus==="CLEARED"?"目标覆盖范围已清空，已提交的数据保留":"目标覆盖范围可能已清空，请结合运行阶段核实"} description={run.clearScope}/>}
    {run.clearStatus==="SKIPPED_NO_PARTITION"&&<Alert type="info" title="执行前目标分区不存在，已跳过清空" description="后续写入按分区字段值创建分区，其他已有分区保留。"/>}
    {!!run.syncMessage&&<Alert type={run.status==="SUCCESS"?"info":"warning"} title={run.syncMessage} description={run.errorCode}/>}
    <pre className="detail-code run-log">{run.logs?.join("\n")}</pre>
    <Space><Button onClick={async()=>{try{setBatches((await api.syncBatches(run.id)).batches);}catch(e){message.error((e as Error).message);}}}>查看批次凭据</Button>{run.status==="RECOVERING"&&<Button danger onClick={()=>setShow(true)}>人工核实结果</Button>}</Space>
    {!!batches.length&&<Table size="small" rowKey={r=>String(r.batch_id)} dataSource={batches} pagination={{pageSize:8}} scroll={{x:"max-content"}} columns={[{title:"批次",dataIndex:"batch_id"},{title:"状态",dataIndex:"state"},{title:"行数",dataIndex:"rows"},{title:"Label",dataIndex:"label"},{title:"凭据",dataIndex:"detail",render:v=><span className="query-cell">{String(v||"")}</span>}]}/>}
    <Modal title="确认已核实目标数据与远端停止状态" open={show} onCancel={()=>setShow(false)} confirmLoading={busy} okText="结束为失败并解除占用" okButtonProps={{danger:true,disabled:note.trim().length<5}} onOk={async()=>{setBusy(true);try{await api.resolveSync(run.id,note);setShow(false);refresh();}catch(e){message.error((e as Error).message);}finally{setBusy(false);}}}>
      <p>仅在确认远端已停止、数据库在途操作已结束后处理。此操作保留已写入数据，不重新执行任务。</p><Input.TextArea aria-label="人工核实说明" value={note} maxLength={2000} onChange={e=>setNote(e.target.value)} placeholder="记录检查的批次、目标数据与停止结果"/>
    </Modal>
  </div>;
}
