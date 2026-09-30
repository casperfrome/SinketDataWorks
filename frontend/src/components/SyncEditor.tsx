import { useEffect, useState } from "react";
import { Alert, App, Button, Form, Input, InputNumber, Select, Space, Table, Tag } from "antd";
import { api } from "../api";
import type { DataSource, StudioObject, SyncConfig, SyncMetadata } from "../types";

export default function SyncEditor({ object, onChange }: {object:StudioObject;onChange:(patch:Partial<StudioObject>)=>void}) {
  const {message}=App.useApp();const c:SyncConfig=object.config.sync||{};
  const [sources,setSources]=useState<DataSource[]>([]);const [sourceTables,setSourceTables]=useState<{name:string}[]>([]);const [targetTables,setTargetTables]=useState<{name:string}[]>([]);
  const [sourceColumns,setSourceColumns]=useState<SyncMetadata["columns"]>([]);const [target,setTarget]=useState<SyncMetadata>();const [error,setError]=useState("");const [busy,setBusy]=useState(false);
  const change=(patch:Partial<SyncConfig>)=>onChange({config:{...object.config,run:{...object.config.run,provider:"SYNC"},sync:{...c,...patch}}});
  useEffect(()=>{let alive=true;void api.datasources(object.workspaceId).then(s=>{if(alive)setSources(s);}).catch(e=>{if(alive)setError(e.message);});return()=>{alive=false;};},[object.workspaceId]);
  useEffect(()=>{let alive=true;setSourceTables([]);setError("");if(c.sourceDataSourceId)void api.sourceTables(c.sourceDataSourceId).then(s=>{if(alive)setSourceTables(s);}).catch(e=>{if(alive)setError(e.message);});return()=>{alive=false;};},[c.sourceDataSourceId]);
  useEffect(()=>{let alive=true;setTargetTables([]);setError("");if(c.targetDataSourceId)void api.sourceTables(c.targetDataSourceId).then(s=>{if(alive)setTargetTables(s);}).catch(e=>{if(alive)setError(e.message);});return()=>{alive=false;};},[c.targetDataSourceId]);
  useEffect(()=>{let alive=true;setSourceColumns([]);if(c.sourceDataSourceId&&c.sourceTable)void api.sourceColumns(c.sourceDataSourceId,c.sourceTable).then(s=>{if(alive)setSourceColumns(s);}).catch(e=>{if(alive)setError(e.message);});return()=>{alive=false;};},[c.sourceDataSourceId,c.sourceTable]);
  useEffect(()=>{let alive=true;setTarget(undefined);if(c.targetDataSourceId&&c.targetTable)void api.syncMetadata(c.targetDataSourceId,c.targetTable).then(s=>{if(alive)setTarget(s);}).catch(e=>{if(alive)setError(e.message);});return()=>{alive=false;};},[c.targetDataSourceId,c.targetTable]);
  const sourceType=sources.find(s=>s.id===c.sourceDataSourceId)?.type;
  const options=(list:{name:string}[])=>list.map(v=>({value:v.name,label:v.name}));
  const selectedColumns=c.columns?.length?sourceColumns.filter(v=>c.columns!.includes(v.name)):sourceColumns;
  const mapping=c.mapping||[];
  return <div className="node-configuration sync-editor">
    <div className="configuration-title"><div><h3>离线批量同步</h3><p>MySQL ↔ Doris · 选择来源、目标和字段映射</p></div><Tag color="green">真实同步</Tag></div>
    {error&&<Alert type="error" title={error}/>}
    <Form layout="vertical">
      <div className="sync-endpoints">
        <section><h4>来源</h4>
          <Form.Item label="来源数据源" required><Select aria-label="来源数据源" placeholder="选择 MySQL 或 Doris 连接" value={c.sourceDataSourceId} options={sources.map(s=>({value:s.id,label:`${s.name} · ${s.type||"MYSQL"}`}))} onChange={sourceDataSourceId=>change({sourceDataSourceId,sourceTable:undefined,columns:[],mapping:[],targetDataSourceId:undefined,targetTable:undefined,keyColumns:[]})}/></Form.Item>
          <Form.Item label="来源表" required><Select aria-label="来源表" showSearch optionFilterProp="label" value={c.sourceTable} options={options(sourceTables)} onChange={sourceTable=>change({sourceTable,columns:[],mapping:[]})}/></Form.Item>
          <Form.Item label="读取字段" extra="留空读取全部字段"><Select aria-label="读取字段" mode="multiple" value={c.columns||[]} options={options(sourceColumns)} onChange={columns=>change({columns,mapping:[]})}/></Form.Item>
        </section>
        <section><h4>目标</h4>
          <Form.Item label="目标数据源" required><Select aria-label="目标数据源" placeholder="选择另一种数据库连接" value={c.targetDataSourceId} options={sources.filter(s=>sourceType&&s.type!==sourceType).map(s=>({value:s.id,label:`${s.name} · ${s.type}`}))} onChange={targetDataSourceId=>change({targetDataSourceId,targetTable:undefined,mapping:[],keyColumns:[]})}/></Form.Item>
          <Form.Item label="目标表" required extra={target&&`表模型：${target.model}`}><Select aria-label="目标表" showSearch optionFilterProp="label" value={c.targetTable} options={options(targetTables)} onChange={targetTable=>change({targetTable,mapping:[],keyColumns:[]})}/></Form.Item>
          <Form.Item label="写入方式"><Select aria-label="写入方式" value={c.writeMode||"append"} options={[{value:"append",label:"追加"},{value:"upsert",label:"主键更新"},{value:"overwrite",label:"清空后覆盖"}]} onChange={writeMode=>change({writeMode})}/></Form.Item>
          {c.writeMode==="upsert"&&target?.model==="INNODB"&&<Form.Item label="主键 / 唯一索引" required><Select aria-label="更新主键" value={JSON.stringify(c.keyColumns||[])} options={target.uniqueKeys.map(k=>({value:JSON.stringify(k),label:k.join(", ")}))} onChange={v=>change({keyColumns:JSON.parse(v)})}/></Form.Item>}
        </section>
      </div>
      {c.writeMode==="overwrite"&&<Alert type="warning" showIcon title="预检通过后清空目标表，再分批导入" description="空来源会清空目标；停止或失败不撤回清空和已提交的数据。"/>}
      <Form.Item label="筛选条件（WHERE）" extra="填写条件表达式，留空读取全表。例如 business_date = '${bizdate}'；参数在右侧调度配置中定义。">
        <Input.TextArea aria-label="同步筛选条件" className="monospace" autoSize={{minRows:3,maxRows:8}} value={c.where||""} onChange={e=>change({where:e.target.value})}/>
      </Form.Item>
      <Space wrap><h4>字段映射</h4><Button size="small" disabled={!target} onClick={()=>change({mapping:selectedColumns.filter(s=>target?.columns.some(t=>s.name===t.name)).map(s=>({source:s.name,target:s.name}))})}>按同名匹配</Button><Button size="small" onClick={()=>change({mapping:[...mapping,{source:"",target:""}]})}>添加映射</Button><span className="panel-muted">留空按读取字段同名映射</span></Space>
      <Table size="small" rowKey="index" pagination={false} dataSource={mapping.map((m,index)=>({...m,index}))} columns={[
        {title:"来源字段",render:(_,row)=><Select aria-label={`来源字段 ${row.index+1}`} style={{width:"100%"}} value={row.source||undefined} options={options(selectedColumns)} onChange={source=>change({mapping:mapping.map((m,i)=>i===row.index?{...m,source}:m)})}/>},
        {title:"目标字段",render:(_,row)=><Select aria-label={`目标字段 ${row.index+1}`} style={{width:"100%"}} value={row.target||undefined} options={options(target?.columns||[])} onChange={target=>change({mapping:mapping.map((m,i)=>i===row.index?{...m,target}:m)})}/>},
        {title:"操作",width:70,render:(_,row)=><Button type="link" onClick={()=>change({mapping:mapping.filter((_,i)=>i!==row.index)})}>删除</Button>}
      ]}/>
      <Space wrap className="sync-options"><Form.Item label="每批行数"><InputNumber aria-label="每批行数" min={1} max={100000} value={c.batchRows||10000} onChange={v=>change({batchRows:v||10000})}/></Form.Item><Form.Item label="写入并行度"><InputNumber min={1} max={16} value={c.parallelism||1} onChange={v=>change({parallelism:v||1})}/></Form.Item><Form.Item label="超时（秒）"><InputNumber min={1} max={86400} value={c.timeoutSeconds||3600} onChange={v=>change({timeoutSeconds:v||3600})}/></Form.Item></Space>
      <Button loading={busy} disabled={!c.sourceTable||!c.targetTable} onClick={async()=>{setBusy(true);setError("");try{const result=await api.validateSync(object);message.success(result.message);}catch(e){setError((e as Error).message);}finally{setBusy(false);}}}>校验连接与字段映射</Button>
    </Form>
  </div>;
}
