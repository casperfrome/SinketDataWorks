import { useEffect, useState } from "react";
import { Alert, App, Button, Drawer, Form, Input, InputNumber, Modal, Select, Space, Table, Tag } from "antd";
import { Database, Plus, RefreshCw } from "lucide-react";
import { api } from "../api";
import type { DataSource, DataSourceInput } from "../types";

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
    void api.datasources(workspaceId).then(items => { if(alive) { setSources(items); setSourceId(items[0]?.id); } }).catch(e => {if(alive) setError(e.message);});
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

export default function DatasourceView({ workspaceId }: { workspaceId: string }) {
  const {message}=App.useApp();
  const [sources,setSources]=useState<DataSource[]>([]);
  const [editing,setEditing]=useState<DataSource|null>(null);
  const [open,setOpen]=useState(false);
  const [loading,setLoading]=useState(false);
  const [busy,setBusy]=useState(false);
  const [error,setError]=useState("");
  const [revision,setRevision]=useState(0);
  const [form]=Form.useForm<DataSourceInput>();
  const sourceType=Form.useWatch("type",form);
  const defaults={feHttpUrls:["http://127.0.0.1:8030"],beHttpUrls:["http://127.0.0.1:8040"],flightUri:"grpc://127.0.0.1:8070",flightEndpointMap:{"grpc+tcp://172.30.41.2:8070":"grpc://127.0.0.1:8070","grpc+tcp://172.30.41.3:8050":"grpc://127.0.0.1:8050"},httpEndpointMap:{}};
  const [dorisOptions,setDorisOptions]=useState(JSON.stringify(defaults,null,2));
  useEffect(()=>{
    let alive=true;setLoading(true);setSources([]);setError("");setOpen(false);
    void api.datasources(workspaceId).then(items=>{if(alive)setSources(items);}).catch(e=>{if(alive)setError(e.message);}).finally(()=>{if(alive)setLoading(false);});
    return ()=>{alive=false;};
  },[workspaceId,revision]);
  const show=(source?:DataSource)=>{
    setDorisOptions(JSON.stringify(source?.options||defaults,null,2));setEditing(source||null);form.resetFields();form.setFieldsValue(source||{type:"MYSQL",name:"",host:"127.0.0.1",port:3307,database:"studio_demo",username:"studio_reader"});setOpen(true);
  };
  const values=async()=>{
    const input={...await form.validateFields(),workspaceId};if(!input.password)delete input.password;if(input.type==="DORIS"){try{input.options=JSON.parse(dorisOptions);}catch{throw new Error("Doris 连接参数 JSON 格式无效");}}return input;
  };
  const test=async()=>{
    setBusy(true);try {const result=await api.testDatasource(await values(),editing?.id);message.success(`${result.message} · ${result.version} · ${result.elapsedMs} ms`);}catch(e){if(e instanceof Error)message.error(e.message);}finally{setBusy(false);}
  };
  const save=async()=>{
    setBusy(true);try{await api.saveDatasource(await values(),editing?.id);setOpen(false);setRevision(r=>r+1);message.success("数据源已保存");}catch(e){if(e instanceof Error)message.error(e.message);}finally{setBusy(false);}
  };
  return <section className="management-view">
    <div className="management-page-header"><div className="management-title"><Database size={18}/><h2>数据源</h2><Tag color="green">MySQL / Doris</Tag></div><Space>
      <Button icon={<RefreshCw size={14}/>} onClick={()=>setRevision(r=>r+1)}>刷新</Button><Button type="primary" icon={<Plus size={14}/>} onClick={()=>show()}>新增数据源</Button>
    </Space></div>
    <div className="source-content">
      <p className="panel-muted">连接业务数据库，浏览真实表结构，为 SQL 和离线同步节点选择执行数据源。</p>
      {error&&<Alert type="error" title={error}/>}
      <Table loading={loading} size="small" rowKey="id" dataSource={sources} scroll={{x:800}} pagination={{pageSize:10}} columns={[
        {title:"名称",dataIndex:"name"},{title:"类型",dataIndex:"type"},{title:"地址",render:(_,s)=>`${s.host}:${s.port}`},{title:"数据库",dataIndex:"database"},{title:"账号",dataIndex:"username"},
        {title:"凭据",render:()=> <Tag>已加密保存</Tag>},
        {title:"操作",render:(_,s)=><Space><Button type="link" onClick={()=>show(s)}>编辑</Button><Button type="link" onClick={async()=>{try{const r=await api.testDatasource(undefined,s.id);message.success(`连接成功 · ${r.elapsedMs} ms`);}catch(e){message.error((e as Error).message);}}}>测试连接</Button></Space>},
      ]}/>
      <SourceCatalog key={`${workspaceId}-${revision}`} workspaceId={workspaceId}/>
    </div>
    <Modal title={editing?"编辑数据源":"新增数据源"} open={open} onCancel={()=>setOpen(false)} forceRender footer={<Space><Button onClick={()=>setOpen(false)}>取消</Button><Button loading={busy} onClick={()=>void test()}>测试连接</Button><Button type="primary" loading={busy} onClick={()=>void save()}>保存</Button></Space>}>
      <Form name="datasource" form={form} layout="vertical">
        <Form.Item name="type" label="数据库类型" rules={[{required:true}]}><Select disabled={!!editing} options={[{value:"MYSQL",label:"MySQL"},{value:"DORIS",label:"Doris"}]} onChange={type=>{if(!editing)form.setFieldsValue({port:type==="DORIS"?9030:3307,username:type==="DORIS"?"dunnelean":"studio_reader",database:type==="DORIS"?"dunnelean_test":"studio_demo"});}}/></Form.Item>
        <Form.Item name="name" label="名称" rules={[{required:true,whitespace:true}]}><Input maxLength={100}/></Form.Item>
        <div className="source-form-address"><Form.Item name="host" label="主机" rules={[{required:true}]}><Input/></Form.Item><Form.Item name="port" label="端口" rules={[{required:true}]}><InputNumber min={1} max={65535}/></Form.Item></div>
        <Form.Item name="database" label="业务数据库" rules={[{required:true}]}><Input/></Form.Item>
        <Form.Item name="username" label="业务账号" rules={[{required:true}]}><Input autoComplete="off"/></Form.Item>
        {sourceType==="DORIS"&&<Form.Item label="Doris HTTP / Flight 连接参数" extra="本机容器默认值已填写。SQL 端口用于元数据，HTTP 用于导入，Flight 用于读取。"><Input.TextArea aria-label="Doris 连接参数" className="monospace" autoSize={{minRows:6,maxRows:12}} value={dorisOptions} onChange={e=>setDorisOptions(e.target.value)}/></Form.Item>}
        <Form.Item name="password" label="密码" extra={editing?"留空保留已保存的密码":"使用独立的业务库账号"} rules={[{required:!editing}]}><Input.Password autoComplete="new-password"/></Form.Item>
      </Form>
    </Modal>
  </section>;
}
