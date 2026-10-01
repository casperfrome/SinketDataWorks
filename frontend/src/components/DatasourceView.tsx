import { useEffect, useState } from "react";
import { Alert, App, Button, Drawer, Form, Input, InputNumber, Modal, Select, Space, Table, Tag } from "antd";
import { Database, Plus, RefreshCw } from "lucide-react";
import { api } from "../api";
import type { DataSource, DataSourceInput } from "../types";
import { validateKafkaSource } from "../realtime/model";
import type { KafkaDatasource, KafkaDatasourceInput, RealtimeDatasource } from "../realtime/types";
import "./datasource-view.css";

export function SourceCatalog({ workspaceId }: { workspaceId: string }) {
  const [sources, setSources] = useState<DataSource[]>([]);
  const [sourceId, setSourceId] = useState<string>();
  const [tables, setTables] = useState<Awaited<ReturnType<typeof api.sourceTables>>>([]);
  const [columns, setColumns] = useState<Awaited<ReturnType<typeof api.sourceColumns>>>([]);
  const [table, setTable] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [revision, setRevision] = useState(0);
  useEffect(() => {
    let alive = true; setSourceId(undefined); setSources([]); setError("");
    void api.datasources(workspaceId).then(items => { if(alive) { const sql=items.filter((item):item is DataSource=>item.type!=="KAFKA");setSources(sql); setSourceId(sql[0]?.id); } }).catch(e => {if(alive) setError(e.message);});
    return () => { alive = false; };
  }, [workspaceId]);
  useEffect(() => {
    let alive = true; setTables([]); setTable(""); setError("");
    if (!sourceId) { setLoading(false); return; }
    setLoading(true);
    void api.sourceTables(sourceId).then(items => { if(alive) setTables(items); }).catch(e => {if(alive) setError(e.message);}).finally(() => {if(alive) setLoading(false);});
    return () => { alive = false; };
  }, [sourceId, revision]);
  useEffect(() => {
    let alive = true; setColumns([]);
    if (!sourceId || !table) return;
    void api.sourceColumns(sourceId, table).then(items => {if(alive) setColumns(items);}).catch(e => {if(alive) setError(e.message);});
    return () => { alive = false; };
  }, [sourceId, table]);
  return <section className="source-catalog">
    <Space wrap><Database size={16}/><strong>真实数据库</strong>
      <Select aria-label="浏览数据源" placeholder="请先在数据源页面添加连接" value={sourceId} onChange={setSourceId} style={{ minWidth: 230 }} options={sources.map(s => ({value:s.id,label:`${s.name} · ${s.database}`}))}/>
      <Button icon={<RefreshCw size={14}/>} onClick={() => setRevision(r=>r+1)} disabled={!sourceId}>刷新表</Button>
    </Space>
    {error && <Alert type="error" title={error}/>}
    <Table size="small" loading={loading} rowKey="name" dataSource={tables} pagination={{pageSize:8}}
      columns={[{title:"表名",dataIndex:"name",render:(name:string)=><Button type="link" onClick={()=>setTable(name)}>{name}</Button>},{title:"类型",dataIndex:"type"},{title:"说明",dataIndex:"comment"}]}/>
    <Drawer title={`${table} · 字段`} open={!!table} onClose={()=>setTable("")} size="large" destroyOnHidden>
      <Table size="small" rowKey="name" dataSource={columns} pagination={false} columns={[
        {title:"字段",dataIndex:"name"},{title:"类型",dataIndex:"type"},{title:"可空",dataIndex:"nullable"},{title:"索引",dataIndex:"columnKey"},{title:"说明",dataIndex:"comment"},
      ]}/>
    </Drawer>
  </section>;
}

type DatasourceFormValues = Omit<DataSourceInput, "type" | "workspaceId"> & {
  type: RealtimeDatasource["type"];
  bootstrapServers?: string;
  securityProtocol?: KafkaDatasource["securityProtocol"];
  saslMechanism?: KafkaDatasource["saslMechanism"];
  flinkBootstrapServers?: string; flinkHost?: string; flinkPort?: number;
};

export default function DatasourceView({ workspaceId }: { workspaceId: string }) {
  const {message,modal}=App.useApp();
  const [sources,setSources]=useState<RealtimeDatasource[]>([]);
  const [editing,setEditing]=useState<RealtimeDatasource|null>(null);
  const [open,setOpen]=useState(false);
  const [loading,setLoading]=useState(false);
  const [busy,setBusy]=useState(false);
  const [error,setError]=useState("");
  const [revision,setRevision]=useState(0);
  const [search,setSearch]=useState("");
  const [typeFilter,setTypeFilter]=useState<RealtimeDatasource["type"]|"ALL">("ALL");
  const [topicSource,setTopicSource]=useState<KafkaDatasource>(),[topics,setTopics]=useState<{name:string;partitions?:number}[]>([]),[topicLoading,setTopicLoading]=useState(false);
  const [form]=Form.useForm<DatasourceFormValues>();
  const sourceType=Form.useWatch("type",form);
  const securityProtocol=Form.useWatch("securityProtocol",form);
  const defaults={feHttpUrls:["http://127.0.0.1:8030"],beHttpUrls:["http://127.0.0.1:8040"],flightUri:"grpc://127.0.0.1:8070",flightEndpointMap:{"grpc+tcp://172.30.41.2:8070":"grpc://127.0.0.1:8070","grpc+tcp://172.30.41.3:8050":"grpc://127.0.0.1:8050"},httpEndpointMap:{}};
  const [dorisOptions,setDorisOptions]=useState(JSON.stringify(defaults,null,2));
  useEffect(()=>{
    let alive=true;setLoading(true);setSources([]);setError("");setOpen(false);
    void api.datasources(workspaceId).then(items=>{if(alive)setSources(items);}).catch(e=>{if(alive)setError(e.message);}).finally(()=>{if(alive)setLoading(false);});
    return ()=>{alive=false;};
  },[workspaceId,revision]);
  useEffect(()=>{let alive=true;setTopics([]);if(!topicSource)return;setTopicLoading(true);void api.datasourceTopics(topicSource.id).then(items=>{if(alive)setTopics(items);}).catch(reason=>{if(alive)message.error(reason.message);}).finally(()=>{if(alive)setTopicLoading(false);});return()=>{alive=false;};},[topicSource?.id]);
  const show=(source?:RealtimeDatasource)=>{
    setDorisOptions(JSON.stringify(source && source.type!=="KAFKA" ? source.options||defaults : defaults,null,2));
    setEditing(source||null);form.resetFields();form.setFieldsValue(source?{...source,...(source.type!=="KAFKA"?{flinkHost:source.options?.flinkHost,flinkPort:source.options?.flinkPort}:{})}:{type:"MYSQL",name:"",host:"127.0.0.1",port:3307,database:"studio_demo",username:"studio_reader",bootstrapServers:"",securityProtocol:"PLAINTEXT",saslMechanism:"PLAIN"});setOpen(true);
  };
  const values=async()=>{
    const v=await form.validateFields();
    if(v.type==="KAFKA")throw new Error("Kafka 配置不能发送到数据库接口");
    const input:DataSourceInput={workspaceId,type:v.type,name:v.name.trim(),host:v.host.trim(),port:v.port,database:v.database.trim(),username:v.username.trim()};
    if(v.password)input.password=v.password;
    input.options=editing&&editing.type!=="KAFKA"?{...editing.options}:{};
    if(input.type==="DORIS"){try{input.options=JSON.parse(dorisOptions);}catch{throw new Error("Doris 连接参数 JSON 格式无效");}}
    if(v.flinkHost?.trim())input.options={...input.options,flinkHost:v.flinkHost.trim()};else if(input.options)delete input.options.flinkHost;
    if(v.flinkPort)input.options={...input.options,flinkPort:v.flinkPort};else if(input.options)delete input.options.flinkPort;
    return input;
  };
  const kafkaValues=async()=>{
    const v=await form.validateFields();
    const source:KafkaDatasource={id:editing?.type==="KAFKA"?editing.id:"new",workspaceId,type:"KAFKA",name:v.name.trim(),bootstrapServers:(v.bootstrapServers||"").trim(),flinkBootstrapServers:v.flinkBootstrapServers?.trim(),securityProtocol:v.securityProtocol||"PLAINTEXT",saslMechanism:v.saslMechanism||"PLAIN",username:v.securityProtocol==="PLAINTEXT"?"":(v.username||"").trim()};
    const issues=validateKafkaSource(source);
    if(source.securityProtocol!=="PLAINTEXT"&&!v.password&&!(editing?.type==="KAFKA"&&editing.passwordSet))issues.push("SASL 认证需要密码，请填写后保存到服务器");
    if(issues.length)throw new Error(issues.join("；"));
    const {id:_id,...input}=source;return {...input,...(v.password?{password:v.password}:{})} as KafkaDatasourceInput;
  };
  const checkKafka=async()=>{
    setBusy(true);try {const result=await api.testDatasource(await kafkaValues(),editing?.id);message.success(`${result.message} · ${result.elapsedMs} ms`);}
    catch(e){if(e instanceof Error)message.error(e.message);}finally{setBusy(false);}
  };
  const test=async()=>{
    setBusy(true);try {const result=await api.testDatasource(await values(),editing?.id);message.success(`${result.message} · ${result.version} · ${result.elapsedMs} ms`);}catch(e){if(e instanceof Error)message.error(e.message);}finally{setBusy(false);}
  };
  const save=async()=>{
    setBusy(true);
    try{
      if(sourceType==="KAFKA"){
        await api.saveDatasource(await kafkaValues(),editing?.id);setOpen(false);setRevision(r=>r+1);message.success("Kafka 数据源及凭据已保存到服务器");
      }else{
        await api.saveDatasource(await values(),editing?.id);setOpen(false);setRevision(r=>r+1);message.success("数据源已保存");
      }
    }catch(e){if(e instanceof Error)message.error(e.message);}finally{setBusy(false);}
  };
  const removeKafka=(source:KafkaDatasource)=>{
    modal.confirm({title:`删除 Kafka 数据源「${source.name}」？`,content:"服务器将检查任务及发布引用；被引用的数据源无法删除。",okText:"删除",okButtonProps:{danger:true},cancelText:"取消",onOk:async()=>{
      try{await api.removeDatasource(source.id);setRevision(r=>r+1);message.success("Kafka 数据源已删除");}catch(reason){message.error((reason as Error).message);throw reason;}
    }});
  };
  const catalog:RealtimeDatasource[]=sources;
  const filtered=catalog.filter(s=>(typeFilter==="ALL"||s.type===typeFilter)&&`${s.name} ${s.type} ${s.type==="KAFKA"?s.bootstrapServers:`${s.host} ${s.database}`}`.toLowerCase().includes(search.toLowerCase()));
  return <section className="management-view">
    <div className="management-page-header"><div className="management-title"><Database size={18}/><h2>数据源</h2><Tag color="blue">MySQL / Doris / Kafka</Tag></div><Space>
      <Button icon={<RefreshCw size={14}/>} onClick={()=>setRevision(r=>r+1)}>刷新</Button><Button type="primary" icon={<Plus size={14}/>} onClick={()=>show()}>新增数据源</Button>
    </Space></div>
    <div className="source-content">
      <p className="panel-muted">统一配置离线和实时开发的数据源。连接与认证凭据由服务器保存，Flink 执行地址可独立配置。</p>
      {error&&<Alert type="error" title={`数据源加载失败：${error}`} showIcon/>}
      <div className="datasource-catalog-toolbar"><Input.Search aria-label="搜索数据源" allowClear placeholder="搜索名称、地址或数据库" value={search} onChange={e=>setSearch(e.target.value)}/><Select aria-label="筛选数据源类型" value={typeFilter} onChange={setTypeFilter} options={[{value:"ALL",label:"全部类型"},{value:"MYSQL",label:"MySQL"},{value:"DORIS",label:"Doris"},{value:"KAFKA",label:"Kafka"}]}/><span className="panel-muted">{catalog.length} 个数据源</span></div>
      <Table<RealtimeDatasource> loading={loading&&catalog.length===0} size="small" rowKey="id" dataSource={filtered} scroll={{x:1020}} pagination={{pageSize:10}} locale={{emptyText:"暂无数据源，点击“新增数据源”配置数据库或 Kafka"}} columns={[
        {title:"名称",dataIndex:"name",width:170},{title:"类型",dataIndex:"type",width:100,render:(type:string)=><Tag color={type==="KAFKA"?"blue":type==="DORIS"?"purple":"green"}>{type}</Tag>},
        {title:"地址",width:220,ellipsis:true,render:(_,s)=>s.type==="KAFKA"?s.bootstrapServers:`${s.host}:${s.port}`},
        {title:"数据库 / 认证",width:160,render:(_,s)=>s.type==="KAFKA"?s.securityProtocol:s.database},{title:"账号",dataIndex:"username",width:130,render:(value:string)=>value||"—"},
        {title:"保存与连接",width:160,render:(_,s)=><Space orientation="vertical" size={0}><Tag>服务器保存</Tag><span className="panel-muted">{s.type==="KAFKA"&&s.securityProtocol==="PLAINTEXT"?"无需认证":s.passwordSet?"凭据已保存":"尚未配置凭据"}</span></Space>},
        {title:"操作",width:270,render:(_,s)=><Space size={0}><Button type="link" onClick={()=>show(s)}>编辑</Button><Button type="link" onClick={async()=>{try{const r=await api.testDatasource(undefined,s.id);message.success(`连接成功 · ${r.elapsedMs} ms`);}catch(e){message.error((e as Error).message);}}}>测试连接</Button>{s.type==="KAFKA"&&<><Button type="link" onClick={()=>setTopicSource(s)}>Topics</Button><Button type="link" danger onClick={()=>removeKafka(s)}>删除</Button></>}</Space>},
      ]}/>
      <SourceCatalog key={`${workspaceId}-${revision}`} workspaceId={workspaceId}/>
    </div>
    <Modal title={editing?`编辑数据源 · ${editing.name}`:"新增数据源"} open={open} onCancel={()=>setOpen(false)} forceRender footer={<Space><Button onClick={()=>setOpen(false)}>取消</Button><Button loading={busy} onClick={()=>void (sourceType==="KAFKA"?checkKafka():test())}>测试连接</Button><Button type="primary" loading={busy} onClick={()=>void save()}>保存</Button></Space>}>
      <Form name="datasource" form={form} layout="vertical">
        <Form.Item name="type" label="数据源类型" rules={[{required:true}]}><Select disabled={!!editing} options={[{value:"MYSQL",label:"MySQL · 离线 / 实时"},{value:"DORIS",label:"Doris · 离线 / 实时 Sink"},{value:"KAFKA",label:"Kafka · 实时 Source / Sink"}]} onChange={type=>{if(!editing)form.setFieldsValue({port:type==="DORIS"?9030:3307,username:type==="KAFKA"?"":type==="DORIS"?"dunnelean":"studio_reader",database:type==="DORIS"?"dunnelean_test":"studio_demo",password:""});}}/></Form.Item>
        <Form.Item name="name" label="名称" rules={[{required:true,whitespace:true}]}><Input maxLength={100}/></Form.Item>
        {sourceType==="KAFKA"?<>
          <Alert className="datasource-kafka-notice" type="info" showIcon title="Kafka 连接由服务器管理" description="测试连接会读取 Broker 元数据；密码只上传到服务器凭据存储，不写入 SQL 和浏览器草稿。"/>
          <Form.Item name="bootstrapServers" label="Bootstrap Servers" rules={[{required:true,whitespace:true}]} extra="多个 Broker 使用英文逗号分隔，例如 broker-1:9092,broker-2:9092。"><Input.TextArea aria-label="Bootstrap Servers" className="monospace" placeholder="broker-1:9092,broker-2:9092" autoSize={{minRows:2,maxRows:4}}/></Form.Item>
          <Form.Item name="flinkBootstrapServers" label="Flink Bootstrap Servers" extra="可选，Flink 集群使用的 Broker 地址。留空由后端使用默认执行地址。"><Input.TextArea aria-label="Flink Bootstrap Servers" className="monospace" autoSize={{minRows:1,maxRows:3}}/></Form.Item>
          <Form.Item name="securityProtocol" label="安全协议" rules={[{required:true}]}><Select options={[{value:"PLAINTEXT",label:"PLAINTEXT"},{value:"SASL_PLAINTEXT",label:"SASL_PLAINTEXT"},{value:"SASL_SSL",label:"SASL_SSL"}]}/></Form.Item>
          {securityProtocol!=="PLAINTEXT"&&<>
            <Form.Item name="saslMechanism" label="SASL 机制" rules={[{required:true}]}><Select options={[{value:"PLAIN",label:"PLAIN"},{value:"SCRAM-SHA-256",label:"SCRAM-SHA-256"},{value:"SCRAM-SHA-512",label:"SCRAM-SHA-512"}]}/></Form.Item>
            <Form.Item name="username" label="SASL 用户名" rules={[{required:true,whitespace:true}]}><Input autoComplete="off"/></Form.Item>
            <Form.Item name="password" label="SASL 密码" extra={editing?.passwordSet?"留空保留服务器已保存的密码":"请输入密码，保存后由服务器管理"} rules={[{required:!editing?.passwordSet}]}><Input.Password autoComplete="new-password"/></Form.Item>
          </>}
        </>:<>
          <div className="source-form-address"><Form.Item name="host" label="主机" rules={[{required:true}]}><Input/></Form.Item><Form.Item name="port" label="端口" rules={[{required:true}]}><InputNumber min={1} max={65535}/></Form.Item></div>
          <div className="source-form-address"><Form.Item name="flinkHost" label="Flink 执行主机" extra="可选，Flink 集群访问的地址"><Input placeholder="留空使用默认执行地址"/></Form.Item><Form.Item name="flinkPort" label="Flink 执行端口"><InputNumber min={1} max={65535} placeholder="可选"/></Form.Item></div>
          <Form.Item name="database" label="业务数据库" rules={[{required:true}]}><Input/></Form.Item>
          <Form.Item name="username" label="业务账号" rules={[{required:true}]}><Input autoComplete="off"/></Form.Item>
          {sourceType==="DORIS"&&<Form.Item label="Doris HTTP / Flight 连接参数" extra="可配置 flinkFeHttpUrls 和 flinkBeHttpUrls 指定 Flink 的 HTTP 地址，留空使用默认执行地址。"><Input.TextArea aria-label="Doris 连接参数" className="monospace" autoSize={{minRows:6,maxRows:12}} value={dorisOptions} onChange={e=>setDorisOptions(e.target.value)}/></Form.Item>}
          <Form.Item name="password" label="密码" extra={editing?"留空保留已保存的密码":"使用独立的业务库账号"} rules={[{required:!editing}]}><Input.Password autoComplete="new-password"/></Form.Item>
        </>}
      </Form>
    </Modal>
    <Drawer title={`${topicSource?.name||"Kafka"} · Topics`} open={!!topicSource} onClose={()=>setTopicSource(undefined)} size="large"><Table size="small" loading={topicLoading} rowKey="name" dataSource={topics} columns={[{title:"Topic",dataIndex:"name"},{title:"分区数",dataIndex:"partitions"}]} /></Drawer>
  </section>;
}
