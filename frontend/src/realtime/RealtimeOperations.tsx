import { useEffect, useRef, useState } from "react";
import { Alert, App, Button, Descriptions, Empty, Input, Modal, Select, Space, Table, Tabs, Tag } from "antd";
import { Activity, ArrowUpRight, Clock3, Database, Gauge, Save } from "lucide-react";
import { api, ApiError } from "../api";
import { uid } from "./model";
import { jobElapsedSeconds, latestRealtimeJob } from "./operationsModel";
import type { RealtimeJob, RealtimeOperation, RealtimeRelease, RealtimeState, RealtimeSubmission } from "./types";

export const jobStatusNames: Record<string, string> = { STARTING:"启动中", RUNNING:"运行中", STOPPING:"停止中", STOPPED:"已停止", RESTARTING:"重启中", FAILED:"失败", CREATED:"已创建", RECONCILING:"核实中", CANCELED:"已取消", CANCELLING:"取消中", FINISHED:"已完成", SUSPENDED:"已挂起", UNKNOWN:"状态未知", UPGRADING:"升级中" };
const statusColors: Record<string,string> = {STARTING:"processing",RUNNING:"success",STOPPING:"warning",STOPPED:"default",RESTARTING:"processing",FAILED:"error",RECONCILING:"warning",UNKNOWN:"warning"};
export const isJobActive = (job?: RealtimeJob) => !!job && !["STOPPED","FAILED","CANCELED","FINISHED","SUSPENDED"].includes(job.status);
const time = (value?: string | number) => value ? new Date(value).toLocaleString("zh-CN",{hour12:false}) : "—";
const operationPending = (op?: RealtimeOperation) => !!op && ["RUNNING","RECOVERING"].includes(op.status);
const productionOperationPending = (op: RealtimeOperation) => operationPending(op) && ["START","CANCEL","STOP","SAVEPOINT","RESTART","UPGRADE","ROLLBACK"].includes(op.type || "");
const types: Record<string,string> = {START:"启动",CANCEL:"停止",STOP:"保存点后停止",SAVEPOINT:"生成保存点",RESTART:"重启",UPGRADE:"升级",ROLLBACK:"回滚",PUBLISH:"发布",VALIDATE:"SQL 校验",PLAN:"执行计划",PREVIEW:"结果预览"};
export function checkpointRows(value: unknown): Record<string,unknown>[] {
  if (Array.isArray(value)) return value;
  if (value && typeof value === "object" && "history" in value && Array.isArray(value.history)) return value.history;
  return [];
}
const operatorMetricNames: Record<string,string> = { numRecordsInPerSecond:"输入吞吐 / 秒",numRecordsOutPerSecond:"输出吞吐 / 秒",backPressuredTimeMsPerSecond:"反压时长 ms / 秒",idleTimeMsPerSecond:"空闲时长 ms / 秒",busyTimeMsPerSecond:"处理时长 ms / 秒" };
export function operatorMetricRows(value: unknown): {id:string;name:string;metric:string;key:string;value:string}[] {
  if (!Array.isArray(value)) return [];
  return value.flatMap((vertex: {id?:string;name?:string;metrics?:Record<string,unknown>;metricValues?:Record<string,unknown>}) => {
    const entries = Object.entries(vertex.metrics || vertex.metricValues || {}).filter(([key]) => Object.keys(operatorMetricNames).some(name => key === name || key.endsWith("." + name)));
    return entries.length ? entries.map(([key,metricValue]) => ({id:`${vertex.id}:${key}`,name:vertex.name || vertex.id || "—",metric:operatorMetricNames[key.split(".").at(-1)!],key,value:metricValue === undefined || metricValue === null ? "不可用" : String(metricValue)})) : [{id:vertex.id || "unknown",name:vertex.name || vertex.id || "—",metric:"算子指标",key:"—",value:"不可用"}];
  });
}
interface Props { workspaceId:string;state:RealtimeState;update:(fn:(state:RealtimeState)=>RealtimeState)=>boolean;refresh:()=>Promise<void>;onOpen:(id:string)=>void }
export default function RealtimeOperations({workspaceId,state,update,refresh,onOpen}:Props) {
  const {message,modal}=App.useApp();
  const [search,setSearch]=useState(""),[status,setStatus]=useState("all"),[versions,setVersions]=useState<Record<string,string>>({});
  const [detail,setDetail]=useState<RealtimeJob>(),[error,setError]=useState(""),[busy,setBusy]=useState("");
  const [operationIds,setOperationIds]=useState<string[]>([]),[operations,setOperations]=useState<Record<string,RealtimeOperation>>({});
  const [restore,setRestore]=useState<{taskId:string;releaseId:string}>(),[restoreId,setRestoreId]=useState("");
  const mounted=useRef(true);
  const requestIds=useRef(new Map<string,string>());
  useEffect(()=>{mounted.current=true;return()=>{mounted.current=false;};},[]);
  const job=detail?.id===state.selectedJobId?detail:state.jobs.find(item=>item.id===state.selectedJobId);
  const release=state.releases.find(item=>item.id===job?.releaseId);
  const knownOperations=Object.values({...Object.fromEntries((state.operations||[]).map(op=>[op.id,op])),...operations});
  const ownOperations=knownOperations.filter(op=>op.jobId===job?.id || (job && "taskId" in op && op.taskId===job.taskId)).sort((a,b)=>(a.createdAt||"").localeCompare(b.createdAt||""));
  const rollback=ownOperations.slice().reverse().find(op=>op.rollbackAvailable);
  const activeOperation=ownOperations.slice().reverse().find(productionOperationPending);
  const pollIds=[...new Set([...operationIds,...(state.operations||[]).filter(operationPending).map(op=>op.id),...(job?.operationId?[job.operationId]:[])])].sort().join("|");
  useEffect(()=>{
    let alive=true,timer:ReturnType<typeof setTimeout>;
    const load=async()=>{
      try {
        const ids=pollIds?pollIds.split("|"):[];
        const results=await Promise.allSettled([state.selectedJobId?api.realtimeJob(workspaceId,state.selectedJobId):Promise.resolve(undefined),...ids.map(id=>api.realtimeOperation(workspaceId,id))]);
        if(!alive)return;
        const detailResult=results[0];if(detailResult.status==="fulfilled"&&detailResult.value)setDetail(detailResult.value as RealtimeJob);else if(detailResult.status==="rejected")setError(detailResult.reason.message);
        const loaded=results.slice(1).filter((result):result is PromiseFulfilledResult<RealtimeOperation>=>result.status==="fulfilled").map(result=>result.value);
        setOperations(current=>({...current,...Object.fromEntries(loaded.map(op=>[op.id,op]))}));
        const replacement=loaded.find(op=>op.jobId&&op.jobId!==state.selectedJobId&&["RESTART","UPGRADE","ROLLBACK"].includes(op.type||"")&&["RUNNING","SUCCESS"].includes(op.status)&&"taskId" in op&&op.taskId===job?.taskId);
        if(replacement?.jobId)update(current=>current.selectedJobId===state.selectedJobId?{...current,selectedJobId:replacement.jobId!}:current);
        timer=setTimeout(load,2500);
      }catch(reason){if(alive){setError((reason as Error).message);timer=setTimeout(load,4000);}}
    };
    void load();return()=>{alive=false;clearTimeout(timer);};
  },[workspaceId,state.selectedJobId,pollIds]);
  const accept=async(submission:RealtimeSubmission)=>{
    if(!mounted.current)return;
    setOperationIds(current=>[...new Set([...current,submission.operationId])]);
    if(submission.jobId)update(current=>({...current,selectedJobId:submission.jobId!}));
    await refresh();message.success("操作已提交，正在查询 Flink 状态");
  };
  const act=async(key:string,action:()=>Promise<RealtimeSubmission>)=>{
    setBusy(key);setError("");
    try{await accept(await action());}catch(reason){if(mounted.current)setError((reason as Error).message);}finally{if(mounted.current)setBusy("");}
  };
  const submitRequest=async(key:string,submit:(requestId:string)=>Promise<RealtimeSubmission>)=>{
    const requestId=requestIds.current.get(key)||uid("request");requestIds.current.set(key,requestId);
    try{const submission=await submit(requestId);requestIds.current.delete(key);return submission;}
    catch(reason){if(reason instanceof ApiError&&reason.status>=400&&reason.status<500)requestIds.current.delete(key);throw reason;}
  };
  const start=(taskId:string,releaseId:string,savepointId?:string)=>{
    if(!releaseId){message.warning("请先发布任务");return;}
    if(!savepointId)modal.confirm({title:"全新启动实时作业",content:"此次启动不使用保存点，将按来源启动配置读取数据。",okText:"全新启动",onOk:()=>act(taskId,()=>submitRequest("start:"+releaseId,requestId=>api.realtimeStart(workspaceId,releaseId,requestId)))});
    else void act(taskId,()=>submitRequest("start:"+releaseId+":"+savepointId,requestId=>api.realtimeStart(workspaceId,releaseId,requestId,savepointId))).then(()=>setRestore(undefined));
  };
  const control=(item:RealtimeJob,action:"cancel"|"stop"|"savepoint"|"restart"|"upgrade",target?:string)=>{
    const submit=(allowFreshStart?:boolean)=>act(item.taskId,()=>submitRequest([item.id,action,target,allowFreshStart].join(":"),requestId=>api.realtimeControl(workspaceId,item.id,action,requestId,target,allowFreshStart)));
    if(action==="restart") {
      const hasSavepoint=state.jobs.filter(candidate=>candidate.taskId===item.taskId).flatMap(candidate=>candidate.savepoints||[]).some(point=>point.releaseId===item.releaseId&&point.status==="COMPLETED");
      const fresh=item.status!=="RUNNING"&&!hasSavepoint;
      modal.confirm({title:item.status==="RUNNING"?"保存状态并重启":fresh?"全新重启当前版本":"从保存点重启当前版本",content:item.status==="RUNNING"?"先生成保存点并停止当前作业，再严格恢复当前版本；保存点失败时保留错误，不会自动全新启动。":fresh?"此版本没有可用保存点。确认后全新启动，将按来源启动配置重新读取数据，不恢复历史状态。":"使用此任务当前版本的最新保存点恢复。恢复失败时保留错误，不会自动全新启动。",okText:fresh?"确认全新重启":"保存状态重启",onOk:()=>submit(fresh?true:undefined)});
    }
    else if(action==="upgrade")modal.confirm({title:"从保存点升级作业",content:"生成保存点并停止旧作业，再恢复启动所选版本。升级失败且保存点可用时，可以回滚。来源、目标及字段结构须兼容。",okText:"保存点升级",onOk:()=>submit()});
    else void submit();
  };
  const rows=state.tasks.map(task=>({task,job:latestRealtimeJob(state.jobs,task.id),releases:state.releases.filter(item=>item.taskId===task.id).sort((a,b)=>b.releaseNo-a.releaseNo)})).filter(row=>row.task.name.toLowerCase().includes(search.toLowerCase())&&(status==="all"||(row.job?.status||"UNDEPLOYED")===status));
  const releaseOptions=(items:RealtimeRelease[])=>items.map(item=>({value:item.id,label:`R${item.releaseNo} · ${time(item.createdAt)}`}));
  const points=state.jobs.filter(item=>item.taskId===restore?.taskId).flatMap(item=>item.savepoints||[]).filter(point=>point.releaseId===restore?.releaseId&&(!point.status||point.status==="COMPLETED"));
  const metrics=job?.metrics||{};
  const uptime=jobElapsedSeconds(job);
  const eventLogs=(job?.logs||[]).filter(log=>log.id!=="remote" || !job?.nodeLogs?.length);
  const checkpoints=checkpointRows(job?.checkpoints);
  const operatorMetrics=operatorMetricRows(metrics.vertices);
  return <div className="rt-operations">
    <div className="rt-operations-heading"><div><span className="rt-eyebrow">STREAMING OPERATIONS</span><h2>实时运维</h2><p>按发布版本管理 Flink 作业，查看检查点、日志与状态恢复记录。</p></div><Space><Tag color="green">真实运行</Tag><Button size="small" onClick={()=>void refresh().catch(reason=>setError(reason.message))}>刷新</Button></Space></div>
    {error&&<Alert type="error" showIcon title="实时操作失败" description={error} closable onClose={()=>setError("")} />}
    <div className="rt-operations-filter"><Input.Search aria-label="搜索实时作业" placeholder="搜索任务名称" value={search} onChange={event=>setSearch(event.target.value)} allowClear/><Select aria-label="实时作业状态" value={status} onChange={setStatus} options={[{value:"all",label:"全部状态"},{value:"UNDEPLOYED",label:"未启动"},...Object.entries(jobStatusNames).map(([value,label])=>({value,label}))]}/></div>
    <Table size="small" rowKey={row=>row.task.id} dataSource={rows} scroll={{x:1120}} pagination={{pageSize:8}} locale={{emptyText:<Empty description="暂无实时任务"><Button onClick={()=>onOpen("")}>前往开发创建任务</Button></Empty>}} columns={[
      {title:"任务",width:200,render:(_,row)=><button className="rt-name-button" onClick={()=>row.job?update(current=>({...current,selectedJobId:row.job!.id})):onOpen(row.task.id)}><Activity size={15}/><span>{row.task.name}<small>{row.task.bindings.filter(binding=>binding.role==="SOURCE").length} Source · {row.task.bindings.filter(binding=>binding.role==="SINK").length} Sink</small></span></button>},
      {title:"状态",width:112,render:(_,row)=><Space size={0} orientation="vertical"><Tag color={statusColors[row.job?.status||""]}>{row.job?(jobStatusNames[row.job.status]||row.job.status):"未启动"}</Tag>{row.job?.connectionMessage && <small className="rt-muted">上次确认状态</small>}</Space>},
      {title:"目标版本",width:220,render:(_,row)=><Select aria-label={row.task.name+" 目标版本"} size="small" value={versions[row.task.id]||row.releases[0]?.id} placeholder="先发布任务" style={{width:"100%"}} options={releaseOptions(row.releases)} onChange={id=>setVersions(current=>({...current,[row.task.id]:id}))}/>},
      {title:"运行版本",width:86,render:(_,row)=>{const item=state.releases.find(item=>item.id===row.job?.releaseId);return item?"R"+item.releaseNo:"—";}},
      {title:"开始时间",width:170,render:(_,row)=>time(row.job?.startedAt)},
      {title:"操作",width:320,render:(_,row)=>{const pendingOps=knownOperations.filter(op=>productionOperationPending(op)&&(op.jobId===row.job?.id||("taskId" in op&&op.taskId===row.task.id))),pending=!!pendingOps.length,pendingCancel=pendingOps.some(op=>op.type==="CANCEL"),blocked=busy===row.task.id||pending;return <Space size={3} wrap>
        <Button type="link" size="small" disabled={blocked||isJobActive(row.job)||!row.releases.length} onClick={()=>start(row.task.id,versions[row.task.id]||row.releases[0]?.id)}>全新启动</Button>
        <Button type="link" size="small" disabled={busy===row.task.id||pendingCancel||!row.job||(!isJobActive(row.job)&&!pending)} onClick={()=>row.job&&control(row.job,"cancel")}>{pending ? "取消并等待终态" : "停止"}</Button>
        <Button type="link" size="small" disabled={blocked||!row.job||row.job.status!=="RUNNING"} onClick={()=>row.job&&control(row.job,"stop")}>保存点后停止</Button>
        <Button type="link" size="small" disabled={blocked||!row.job||(isJobActive(row.job)&&row.job?.status!=="RUNNING")} onClick={()=>row.job&&control(row.job,"restart")}>重启</Button>
        <Button type="link" size="small" disabled={blocked||!row.job||row.job.status!=="RUNNING"||row.job.releaseId===(versions[row.task.id]||row.releases[0]?.id)} onClick={()=>row.job&&control(row.job,"upgrade",versions[row.task.id]||row.releases[0]?.id)}>升级</Button>
        <Button type="link" size="small" disabled={blocked||isJobActive(row.job)||!state.jobs.some(item=>item.taskId===row.task.id&&(item.savepoints||[]).length)} onClick={()=>{const saved=state.jobs.filter(item=>item.taskId===row.task.id).flatMap(item=>item.savepoints||[]),preferred=versions[row.task.id]||row.releases[0]?.id;setRestore({taskId:row.task.id,releaseId:saved.some(point=>point.releaseId===preferred)?preferred:saved.at(-1)!.releaseId});setRestoreId("");}}>恢复启动</Button>
        <Button type="link" size="small" onClick={()=>onOpen(row.task.id)}>开发 <ArrowUpRight size={12}/></Button>
      </Space>;}},
    ]}/>
    {job&&release?<section className="rt-job-detail"><div className="rt-detail-heading"><div><h3>{release.snapshot.name}</h3><Tag color={statusColors[job.status]}>{jobStatusNames[job.status]||job.status}</Tag><Tag>R{release.releaseNo}</Tag></div><Space wrap><Button size="small" icon={<Save size={13}/>} disabled={!!busy||job.status!=="RUNNING"||!!activeOperation} onClick={()=>control(job,"savepoint")}>生成 Savepoint</Button>{rollback&&<Button size="small" danger disabled={!!busy||!!activeOperation} onClick={()=>void act(job.taskId,()=>submitRequest("rollback:"+rollback.id,requestId=>api.realtimeRollback(workspaceId,rollback.id,requestId)))}>从升级保存点回滚</Button>}</Space></div>
      {job.connectionMessage && <Alert type="warning" showIcon title="Flink 连接异常，当前为上次确认状态" description={job.connectionMessage} />}
      {job.gatewayCleanupStatus==="PENDING" && <Alert type="info" showIcon title="SQL Gateway 会话正在后台清理" description={job.gatewayCleanupMessage || "作业状态仍以 Flink 返回结果为准。"} />}
      {activeOperation&&<Alert type="info" showIcon title={`${types[activeOperation.type||""]||"作业操作"}正在处理`} description={`${activeOperation.phase||activeOperation.status} · ${activeOperation.message||"正在等待 Flink 返回"}`} />}
      {rollback&&<Alert type="warning" showIcon title="升级失败，保存点可用于回滚" description={rollback.message||"旧作业已停止。请选择从升级保存点回滚，恢复先前版本。"} />}
      <div className="rt-metrics">{[{label:"输入吞吐",value:metrics.inputRate,unit:"条 / 秒",icon:Database},{label:"输出吞吐",value:metrics.outputRate,unit:"条 / 秒",icon:Activity},{label:"Watermark 延迟",value:metrics.watermarkLagMs,unit:"ms",icon:Gauge},{label:"运行时长",value:uptime,unit:"秒",icon:Clock3}].map(item=><div key={item.label} className="rt-metric"><span><item.icon size={14}/>{item.label}</span><strong>{typeof item.value==="number"&&Number.isFinite(item.value)?item.value.toLocaleString():"不可用"}<small>{typeof item.value==="number"&&Number.isFinite(item.value)?item.unit:""}</small></strong></div>)}</div>
      <p className="rt-muted">指标采样时间：{typeof metrics.sampledAt==="string" || typeof metrics.sampledAt==="number" ? time(metrics.sampledAt) : "Flink 未返回"}{job.connectionMessage ? " · 当前显示上次成功采样的缓存" : ""}</p>
      <Tabs items={[
        {key:"overview",label:"作业详情",children:<><Descriptions size="small" column={{xs:1,sm:2,lg:3}} items={[{key:"id",label:"作业记录 ID",children:job.id},{key:"flink",label:"Flink Job ID",children:job.flinkJobId||"等待提交"},{key:"version",label:"发布版本",children:"R"+release.releaseNo},{key:"parallelism",label:"并行度",children:release.snapshot.runtime.parallelism},{key:"checkpoint",label:"Checkpoint 周期",children:release.snapshot.runtime.checkpointSeconds+" 秒"},{key:"restore",label:"恢复起点",children:job.restoredFrom||"全新启动"}]} /><div className="rt-bindings-overview">{(["SOURCE","SINK"] as const).map(role=><div key={role}><h4>{role==="SOURCE"?"Source 输入":"Sink 输出"}</h4>{release.snapshot.bindings.filter(binding=>binding.role===role).map(binding=><div key={binding.id}><Tag>{binding.connector}</Tag><code>{binding.tableName}</code><span>{binding.topic||binding.physicalTable}</span></div>)}</div>)}</div></>},
        {key:"logs",label:"运行日志",children:<><h4>Flink 返回日志</h4><div className="rt-log-list">{eventLogs.length?eventLogs.map(log=><div key={log.id}><time>{time(log.at)}</time><span>{log.message}</span></div>):<Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无单独返回日志"/>}</div><h4>Flink 异常</h4>{job.exceptions ? <pre className="rt-debug-code">{JSON.stringify(job.exceptions,null,2)}</pre> : <p className="rt-muted">Flink 未返回结构化异常</p>}<h4>节点日志</h4>{job.sharedNodeLogs && <Alert type="info" showIcon title="节点日志由多个作业共享" description="以下内容来自 JobManager / TaskManager，可能包含其他作业的消息。"/>}{job.nodeLogs?.length ? job.nodeLogs.map((log,index)=><div key={`${log.node}:${log.file}:${index}`}><Space><strong>{log.node} · {log.file}</strong><Tag>{log.containsJobId ? "包含当前 Job ID" : "未匹配当前 Job ID"}</Tag>{log.truncated && <Tag>内容已截断</Tag>}</Space><pre className="rt-debug-code">{log.content}</pre></div>) : <p className="rt-muted">暂未读取到节点日志</p>}</>},
        {key:"metrics",label:"算子指标",children:<Table size="small" rowKey="id" dataSource={operatorMetrics} pagination={{pageSize:10}} scroll={{x:700}} locale={{emptyText:"Flink 未返回算子指标"}} columns={[{title:"算子",dataIndex:"name"},{title:"指标",dataIndex:"metric"},{title:"原始指标 ID",dataIndex:"key"},{title:"实际值",dataIndex:"value"}]} />},
        {key:"checkpoints",label:"Checkpoint",children:<Table size="small" rowKey={item=>String(item.id)} dataSource={checkpoints} pagination={{pageSize:5}} columns={[{title:"ID",dataIndex:"id"},{title:"状态",dataIndex:"status"},{title:"触发时间",render:(_,item)=>time(Number(item.trigger_timestamp||0))},{title:"耗时",render:(_,item)=>typeof item.end_to_end_duration==="number"?item.end_to_end_duration+" ms":"—"}]} />},
        {key:"savepoints",label:"Savepoint",children:<Table size="small" rowKey="id" dataSource={job.savepoints||[]} pagination={{pageSize:5}} locale={{emptyText:"尚无保存点；全新启动不恢复历史状态"}} columns={[{title:"创建时间",dataIndex:"createdAt",render:time},{title:"保存路径",dataIndex:"path",render:path=><code>{path}</code>},{title:"状态",dataIndex:"status"}]} />},
        {key:"history",label:"操作历史",children:<Table size="small" rowKey="id" dataSource={ownOperations.slice().reverse()} pagination={{pageSize:8}} columns={[{title:"时间",dataIndex:"createdAt",render:time},{title:"操作",dataIndex:"type",render:value=>types[value]||value},{title:"状态",dataIndex:"status"},{title:"阶段",dataIndex:"phase"},{title:"详情",dataIndex:"message",render:(value:string)=>value?.length>140?<details><summary>{value.slice(0,140)}…</summary><pre className="rt-debug-code">{value}</pre></details>:value}]} />},
      ]}/>
    </section>:<div className="rt-detail-empty"><Gauge size={28}/><p>选择作业查看 Flink 指标、日志与保存点</p></div>}
    <Modal title="从同版本 Savepoint 恢复启动" open={!!restore} onCancel={()=>setRestore(undefined)} okText="恢复启动" okButtonProps={{disabled:!restoreId,loading:!!busy}} onOk={()=>restore&&start(restore.taskId,restore.releaseId,restoreId)}><Alert type="info" showIcon title="恢复启动会校验保存点与发布版本，升级使用作业升级操作。"/><div className="rt-form-row"><label>恢复版本</label><Select value={restore?.releaseId} options={releaseOptions(state.releases.filter(item=>item.taskId===restore?.taskId))} onChange={releaseId=>{setRestoreId("");setRestore(current=>current&&({...current,releaseId}));}}/></div><div className="rt-form-row"><label>Savepoint</label><Select aria-label="恢复 Savepoint" value={restoreId||undefined} placeholder="选择此版本的真实保存点" options={points.map(point=>({value:point.id,label:time(point.createdAt)+" · "+point.path}))} onChange={setRestoreId}/></div></Modal>
  </div>;
}
