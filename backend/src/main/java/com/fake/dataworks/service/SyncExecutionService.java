package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.StudioRepository;
import io.github.casperfrome.dunnelean.RunSpec;
import io.github.casperfrome.dunnelean.RunSpecJson;
import jakarta.annotation.PreDestroy;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/** Single-host durable control plane. Remote data and credentials never enter public snapshots. */
@Service
public class SyncExecutionService {
    public record Prepared(StudioObject snapshot,DatasourceService.ConnectionSpec source,DatasourceService.ConnectionSpec target,Map<String,Object> config,String targetKey,String targetModel) {
        @Override public String toString(){return "PreparedSync["+snapshot.id()+"]";}
        public List<Map<String,Object>> bindings(){return List.of(source.publicView(),target.publicView());}
    }
    private static final Set<String> PENDING=Set.of("WAITING","QUEUED","RUNNING","RECOVERING");
    private static final Set<String> TERMINAL=Set.of("SUCCEEDED","FAILED","CANCELLED","INTERRUPTED");
    private final StudioRepository repo;private final DatasourceService sources;private final DunneleanClient client;private final SqlGuard guard;
    private final JdbcTemplate jdbc;private final TransactionTemplate tx;private final JsonCodec json;private final boolean recoverOnStart;
    private final Map<String,Prepared> prepared=new ConcurrentHashMap<>();
    private final Map<String,RunSpec> specs=new ConcurrentHashMap<>();
    private final ScheduledExecutorService clock=Executors.newSingleThreadScheduledExecutor(r->new Thread(r,"sync-clock"));
    private final ExecutorService workers=Executors.newVirtualThreadPerTaskExecutor();
    private final Set<String> working=ConcurrentHashMap.newKeySet();
    private final Map<String,Object> mutexes=new ConcurrentHashMap<>();
    private volatile boolean started,closing;
    public SyncExecutionService(StudioRepository repo,DatasourceService sources,DunneleanClient client,SqlGuard guard,JdbcTemplate jdbc,TransactionTemplate tx,JsonCodec json,@Value("${studio.simulation.recover-on-start:true}") boolean recoverOnStart){this.repo=repo;this.sources=sources;this.client=client;this.guard=guard;this.jdbc=jdbc;this.tx=tx;this.json=json;this.recoverOnStart=recoverOnStart;}
    public static boolean isSync(StudioObject o){return "SYNC".equals(RunService.config(o).get("provider"));}
    @SuppressWarnings("unchecked") public static Map<String,Object> config(StudioObject o){return o.config().get("sync") instanceof Map<?,?> m?(Map<String,Object>)m:Map.of();}
    public static String parameterCode(StudioObject o){return isSync(o)?Objects.toString(config(o).get("where"),""):o.content();}
    private static String text(Map<String,Object> m,String key){return Objects.toString(m.get(key),"").trim();}
    private static int number(Map<String,Object> m,String key,int fallback,int max){Object v=m.getOrDefault(key,fallback);if(!(v instanceof Number n)||n.doubleValue()!=n.intValue()||n.intValue()<1||n.intValue()>max)throw StudioException.bad("INVALID_SYNC","同步参数无效："+key);return n.intValue();}
    @SuppressWarnings("unchecked") private static List<String> strings(Map<String,Object> m,String key){Object v=m.getOrDefault(key,List.of());if(!(v instanceof List<?> l)||l.size()>1024||l.stream().anyMatch(x->!(x instanceof String)))throw StudioException.bad("INVALID_SYNC","字段列表无效："+key);return (List<String>)v;}
    public Prepared prepare(StudioObject o){return prepare(o,List.of());}
    public Prepared prepare(StudioObject o,List<Map<String,String>> inherited){
        if(!"NODE".equals(o.kind())||!Set.of("离线同步","数据集成").contains(o.nodeType())||!isSync(o))throw StudioException.bad("SYNC_NODE_REQUIRED","请选择离线同步节点");
        var c=new LinkedHashMap<>(config(o));
        if(!Set.of("sourceDataSourceId","targetDataSourceId","sourceTable","targetTable","columns","where","mapping","writeMode","keyColumns","batchRows","timeoutSeconds","parallelism").containsAll(c.keySet()))throw StudioException.bad("INVALID_SYNC","存在不支持的同步配置");
        var source=sources.forWorkspace(text(c,"sourceDataSourceId"),o.workspaceId());var target=sources.forWorkspace(text(c,"targetDataSourceId"),o.workspaceId());
        if(source.type().equals(target.type()))throw StudioException.bad("SYNC_DIRECTION","仅支持 MySQL 与 Doris 之间的双向同步");
        DatasourceService.quote(text(c,"sourceTable"));DatasourceService.quote(text(c,"targetTable"));
        var sourceColumns=sources.columns(source.id(),text(c,"sourceTable"));if(sourceColumns.isEmpty())throw StudioException.bad("TABLE_NOT_FOUND","源表不存在");
        for(String column:strings(c,"columns"))if(sourceColumns.stream().noneMatch(row->column.equals(row.get("name"))))throw StudioException.bad("INVALID_SYNC","源字段不存在："+column);
        if(!Set.of("append","upsert","overwrite").contains(c.getOrDefault("writeMode","append")))throw StudioException.bad("INVALID_SYNC","写入模式无效");
        number(c,"batchRows",10000,100000);number(c,"timeoutSeconds",3600,86400);number(c,"parallelism",1,16);strings(c,"keyColumns");
        var metadata=sources.syncMetadata(target.id(),text(c,"targetTable"));String model=metadata.get("model").toString();
        if("UNSUPPORTED".equals(model))throw StudioException.bad("SYNC_TARGET_MODEL","目标仅支持 MySQL InnoDB、Doris Duplicate Key 或 Unique Key Merge-on-Write");
        var p=new Prepared(o,source,target,Collections.unmodifiableMap(c),targetKey(target,text(c,"targetTable")),model);
        var preview=newRun(p,"VALIDATE");ScheduleParameters.attach(preview,o,Map.of(),inherited);
        var previewParameters=new LinkedHashMap<String,Object>(Map.of("bizdate",preview.get("businessDate"),"source_cutoff",Instant.now().toString(),"build_id",preview.get("id")));
        for(String name:SqlParameters.compile(text(c,"where")).names())if(name.matches("upstream_[A-Za-z][A-Za-z0-9_]{0,31}_build_id"))previewParameters.put(name,preview.get("id"));
        preview.put("parameters",previewParameters);
        client.validate(spec(p,preview));return p;
    }
    private String targetKey(DatasourceService.ConnectionSpec s,String table){
        try {String host=InetAddress.getByName(s.host()).getHostAddress();String key=s.type()+"|"+host+"|"+s.port()+"|"+s.database().toLowerCase(Locale.ROOT)+"|"+table.toLowerCase(Locale.ROOT);return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(key.getBytes(StandardCharsets.UTF_8)));}
        catch(Exception e){throw StudioException.bad("INVALID_DATASOURCE","无法解析目标数据库地址");}
    }
    public Map<String,Object> newRun(Prepared p,String mode){
        var o=p.snapshot();var r=new LinkedHashMap<String,Object>();r.put("id",UUID.randomUUID().toString());r.put("workspaceId",o.workspaceId());r.put("objectId",o.id());r.put("objectName",o.name());r.put("objectVersion",o.version());
        r.put("provider","SYNC");r.put("simulation",false);r.put("status","QUEUED");r.put("mode",mode);r.put("containsWrites",true);r.put("createdAt",ObjectService.now());r.put("sourceDataSource",p.source().publicView());r.put("targetDataSource",p.target().publicView());r.put("targetTable",text(p.config(),"targetTable"));r.put("sourceTable",text(p.config(),"sourceTable"));r.put("writeMode",p.config().getOrDefault("writeMode","append"));r.put("syncStage","WAITING_TARGET");r.put("logs",List.of("[离线同步] 配置已固定，等待目标表与执行资源。"));r.put("columns",List.of());r.put("rows",List.of());return r;
    }
    private Map<String,Object> connection(DatasourceService.ConnectionSpec s){var m=new LinkedHashMap<String,Object>();m.put("host",s.host());m.put("port",s.port());m.put("database",s.database());m.put("credentials",sources.credentials(s));m.put("time_zone","+08:00");if("DORIS".equals(s.type()))m.put("session_variables",Map.of("enable_decimal256","true"));return m;}
    public RunSpec spec(Prepared p,Map<String,Object> run){
        var c=p.config();boolean fromDoris="DORIS".equals(p.source().type());var source=new LinkedHashMap<String,Object>();source.put("table",text(c,"sourceTable"));source.put("columns",strings(c,"columns"));
        String where=text(c,"where");if(!where.isBlank()) {
            var compiled=SyncParameters.compile(where,run,fromDoris);
            // Reject clause injection, subqueries, and executable fragments; a WHERE must remain one predicate.
            try {var expression=net.sf.jsqlparser.parser.CCJSqlParserUtil.parseCondExpression(compiled.sql(),false);if(expression==null)throw new IllegalArgumentException();}
            catch(Exception e){throw StudioException.bad("INVALID_SYNC_FILTER","筛选条件必须是完整的 WHERE 条件表达式");}
            String sql="SELECT * FROM "+DatasourceService.quote(p.source().database())+"."+DatasourceService.quote(text(c,"sourceTable"))+" WHERE ("+compiled.sql()+")";
            guard.validate(sql,p.source().database());source.put("where",compiled.sql());if(!compiled.params().isEmpty())source.put("params",compiled.params());
        }
        var reader=new LinkedHashMap<String,Object>();reader.put("type",fromDoris?"doris":"mysql");reader.put("source",source);reader.put("batch",Map.of("rows",number(c,"batchRows",10000,100000),"bytes",16777216));
        if(fromDoris){reader.put("flight_uri",p.source().options().get("flightUri"));reader.put("database",p.source().database());reader.put("credentials",sources.credentials(p.source()));reader.put("endpoint_map",p.source().options().get("flightEndpointMap"));reader.put("session_variables",Map.of("enable_decimal256","true","time_zone","+08:00"));}
        else reader.put("connection",connection(p.source()));
        var writer=new LinkedHashMap<String,Object>();writer.put("type",fromDoris?"mysql":"doris");writer.put("table",text(c,"targetTable"));String mode=Objects.toString(c.get("writeMode"),"append");
        if(fromDoris){writer.put("connection",connection(p.target()));writer.put("mode",mode.equals("upsert")?"upsert":"insert");if(mode.equals("upsert"))writer.put("key_columns",strings(c,"keyColumns"));}
        else {writer.put("sql",connection(p.target()));writer.put("fe_http_urls",p.target().options().get("feHttpUrls"));writer.put("be_http_urls",p.target().options().get("beHttpUrls"));writer.put("endpoint_map",p.target().options().get("httpEndpointMap"));writer.put("time_zone","+08:00");writer.put("mode",mode.equals("overwrite")?(p.targetModel().equals("UNIQUE_MOW")?"upsert":"append"):mode);}
        var options=new LinkedHashMap<String,Object>();options.put("batch",Map.of("rows",number(c,"batchRows",10000,100000),"bytes",16777216));options.put("parallelism",number(c,"parallelism",1,16));
        if(mode.equals("overwrite"))options.put("pre_sql",List.of("TRUNCATE TABLE "+DatasourceService.quote(p.target().database())+"."+DatasourceService.quote(text(c,"targetTable"))));writer.put("options",options);
        Object mapping=c.getOrDefault("mapping",List.of());if(!(mapping instanceof List<?>))throw StudioException.bad("INVALID_SYNC","字段映射格式无效");
        var generated=new LinkedHashMap<>(Map.of("request_id",run.get("id"),"reader",reader,"writer",writer,"mapping",mapping,"execution",Map.of("timeout_ms",number(c,"timeoutSeconds",3600,86400)*1000L)));
        try {return RunSpecJson.fromJson(json.write(generated));}
        catch(Exception e){throw StudioException.bad("INVALID_SYNC","同步配置格式无效，请检查连接、字段映射与写入设置");}
    }
    /** Called inside the transaction that creates the local run. */
    public void register(String id,Prepared p){jdbc.update("INSERT INTO dw_sync_execution(run_id,service_url,target_key,updated_at) VALUES(?,?,?,?)",id,client.baseUrl,p.targetKey(),ObjectService.now());}
    public void enqueue(String id,Prepared p){prepared.put(id,p);startClock();}
    private synchronized void startClock(){if(!started){started=true;clock.scheduleWithFixedDelay(this::tick,100,1000,TimeUnit.MILLISECONDS);}}
    private Object mutex(String id){return mutexes.computeIfAbsent(id,k->new Object());}
    public Map<String,Object> stop(String id){synchronized(mutex(id)){
        var run=repo.run(id).orElseThrow();if(!PENDING.contains(run.get("status")))return run;
        jdbc.update("UPDATE dw_sync_execution SET cancel_requested=TRUE,updated_at=? WHERE run_id=?",ObjectService.now(),id);run.put("cancelRequested",true);run.put("syncStage","CANCELLING");repo.updateRun(run);startClock();return run;
    }}
    private Map<String,Object> row(String id){return jdbc.queryForMap("SELECT * FROM dw_sync_execution WHERE run_id=?",id);}
    private static boolean yes(Object x){return Boolean.TRUE.equals(x)||x instanceof Number n&&n.intValue()!=0;}
    private void tick(){
        if(closing)return;
        try {for(var run:repo.unfinishedRuns())if("SYNC".equals(run.get("provider"))&&working.add(run.get("id").toString())){String id=run.get("id").toString();workers.submit(()->{try{synchronized(mutex(id)){advance(id);}}finally{working.remove(id);}});}finishRecoveringParents();}
        catch(Exception ignored){/* Durable records are retried on the next tick. */}
    }
    private void advance(String id){
        var run=repo.run(id).orElseThrow();if(!PENDING.contains(run.get("status")))return;
        Map<String,Object> state;
        try {state=row(id);}catch(Exception e){return;}
        boolean submitted=yes(state.get("submitted"));boolean cancel=yes(state.get("cancel_requested"));String base=state.get("service_url").toString();String store=Objects.toString(state.get("state_store_id"),null);
        try {
            if(!submitted){
                if(cancel){finish(run,"CANCELLED",Objects.toString(state.get("recovery_reason"),"SYNC_CANCELLED"));return;}
                var p=prepared.get(id);if(p==null){finish(run,"FAILED","SERVICE_RESTARTED");return;}
                boolean acquired=Boolean.TRUE.equals(tx.execute(t->{jdbc.update("INSERT IGNORE INTO dw_sync_target_lock(target_key,run_id) VALUES(?,?)",p.targetKey(),id);return id.equals(jdbc.queryForObject("SELECT run_id FROM dw_sync_target_lock WHERE target_key=?",String.class,p.targetKey()));}));
                if(!acquired)return;
                var health=client.health(base);store=Objects.toString(health.get("state_store_id"),null);if(store==null)throw StudioException.bad("SYNC_UPGRADE_REQUIRED","请更新 Dunnelean 至支持可靠取消的版本");
                var spec=specs.computeIfAbsent(id,k->spec(p,run));
                jdbc.update("UPDATE dw_sync_execution SET submitted=TRUE,state_store_id=?,updated_at=? WHERE run_id=?",store,ObjectService.now(),id);
                run.put("syncStage","SUBMITTING");repo.updateRun(run);
                var remote=client.submit(base,spec,store);acceptRemote(run,remote);return;
            }
            Map<String,Object> response;
            if(cancel)response=client.cancelRequest(base,id,store);
            else {
                try {response=client.getRequest(base,id,store);}
                catch(StudioException e){
                    if(!"SYNC_NOT_FOUND".equals(e.code()))throw e;
                    if(!specs.containsKey(id))throw StudioException.conflict("SYNC_REQUEST_MISSING","远端记录丢失，需核实后处理");
                    acceptRemote(run,client.submit(base,specs.get(id),store));return;
                }
            }
            if(response.get("run") instanceof Map<?,?> remote)acceptRemote(run,cast(remote));
            else if(cancel&&yes(response.get("cancel_requested")))finish(run,"CANCELLED",Objects.toString(state.get("recovery_reason"),"SYNC_CANCELLED"));
        }catch(StudioException e){
            if(Set.of("SYNC_BUSY","SYNC_UNAVAILABLE","SYNC_STATE_STORE_CHANGED","SYNC_REQUEST_MISSING").contains(e.code())||submitted){run.put("status","RECOVERING");run.put("syncMessage",e.getMessage());run.put("errorCode",e.code());repo.updateRun(run);}
            else {run.put("syncMessage",e.getMessage());finish(run,"FAILED",e.code());}
        }catch(Exception e){run.put("status","RECOVERING");run.put("syncMessage","控制状态暂不可确认，将继续核实");repo.updateRun(run);}
    }
    @SuppressWarnings("unchecked") private static Map<String,Object> cast(Map<?,?> m){return (Map<String,Object>)m;}
    private void acceptRemote(Map<String,Object> run,Map<String,Object> remote){
        String id=run.get("id").toString();jdbc.update("UPDATE dw_sync_execution SET remote_run_id=?,updated_at=? WHERE run_id=?",remote.get("run_id"),ObjectService.now(),id);
        String state=remote.get("state").toString();String phase=remote.get("stage").toString();run.put("remoteRunId",remote.get("run_id"));run.put("remoteState",state);run.put("syncStage",phase);run.put("syncMessage","");
        for(var pair:Map.of("rows_read","readRows","rows_committed","writtenRows","bytes_read","readBytes","batches_committed","committedBatches","rows_filtered","filteredRows","server_affected_rows","serverAffectedRows","partial_write","partialWrite","commit_unknown","commitUnknown").entrySet())run.put(pair.getValue(),remote.get(pair.getKey()));
        run.put("clearStatus",!"overwrite".equals(run.get("writeMode"))?"NOT_APPLICABLE":Set.of("transfer","post_sql","complete").contains(phase)?"CLEARED":phase.equals("pre_sql")?"MAY_HAVE_CLEARED":"NOT_STARTED");
        if(!state.equals("QUEUED"))run.putIfAbsent("startedAt",remote.getOrDefault("created_at",run.get("createdAt")));
        if(TERMINAL.contains(state)){
            specs.remove(id);prepared.remove(id);
            if(remote.get("error") instanceof Map<?,?> error){run.put("errorCode","SYNC_"+error.get("code"));run.put("syncMessage",error.get("message"));}
            if(yes(remote.get("commit_unknown"))){run.put("status","RECOVERING");run.put("syncMessage","提交结果未知，目标保持占用；请检查批次凭据和数据库后人工核实。" );repo.updateRun(run);return;}
            String local=state.equals("SUCCEEDED")?"SUCCESS":state.equals("CANCELLED")?"CANCELLED":"FAILED";
            finish(run,local,Objects.toString(run.get("errorCode"),null));
        }else {run.put("status",state.equals("QUEUED")?"QUEUED":"RUNNING");run.remove("errorCode");repo.updateRun(run);}
    }
    private void finish(Map<String,Object> run,String status,String code){
        String id=run.get("id").toString();String before=run.get("status").toString();run.put("status",status);run.put("finishedAt",ObjectService.now());run.put("elapsedMs",Duration.between(Instant.parse(run.getOrDefault("startedAt",run.get("createdAt")).toString()),Instant.now()).toMillis());
        if(code!=null&&!status.equals("SUCCESS"))run.put("errorCode",code);else run.remove("errorCode");
        var logs=new ArrayList<Object>((List<?>)run.getOrDefault("logs",List.of()));logs.add("[离线同步] "+status+"，确认写入 "+run.getOrDefault("writtenRows",0)+" 行。"+("overwrite".equals(run.get("writeMode"))?" 清空状态："+run.getOrDefault("clearStatus","NOT_STARTED"):""));run.put("logs",logs);
        tx.executeWithoutResult(t->{if(repo.transitionRun(run,before))jdbc.update("DELETE FROM dw_sync_target_lock WHERE run_id=?",id);});specs.remove(id);prepared.remove(id);
    }
    public Map<String,Object> batches(String id){var s=row(id);if(s.get("remote_run_id")==null)return Map.of("batches",List.of());return client.listBatches(s.get("service_url").toString(),s.get("remote_run_id").toString(),Objects.toString(s.get("state_store_id"),null));}
    public Map<String,Object> resolve(String id,String note){synchronized(mutex(id)){
        var run=repo.run(id).orElseThrow();if(!"RECOVERING".equals(run.get("status"))||note==null||note.trim().length()<5||note.length()>2000)throw StudioException.bad("SYNC_RESOLUTION_REQUIRED","请在核实状态填写至少 5 字的数据库检查及停止结果");
        var s=row(id);try {var remote=client.getRequest(s.get("service_url").toString(),id,Objects.toString(s.get("state_store_id"),null));if(remote.get("run") instanceof Map<?,?> r&&!TERMINAL.contains(r.get("state")))throw StudioException.conflict("SYNC_STILL_ACTIVE","远端仍在运行，请先停止");}
        catch(StudioException e){if(!Set.of("SYNC_NOT_FOUND","SYNC_STATE_STORE_CHANGED","SYNC_UNAVAILABLE").contains(e.code()))throw e;}
        run.put("resolution",Map.of("note",note.trim(),"resolvedAt",ObjectService.now()));finish(run,"FAILED","SYNC_MANUALLY_RESOLVED");return run;
    }}
    public void recover(){
        if(recoverOnStart){
            for(var run:repo.unfinishedRuns())if("SYNC".equals(run.get("provider"))){String id=run.get("id").toString();jdbc.update("UPDATE dw_sync_execution SET cancel_requested=TRUE,recovery_reason='SERVICE_RESTARTED' WHERE run_id=?",id);run.put("status","RECOVERING");run.put("cancelRequested",true);run.put("recoveryReason","SERVICE_RESTARTED");repo.updateRun(run);}
            for(var parent:repo.unfinishedRuns())if(Boolean.TRUE.equals(parent.get("containsSync"))){parent.put("status","RECOVERING");parent.put("recoveryReason","SERVICE_RESTARTED");parent.put("cancelRequested",true);repo.updateRun(parent);for(var child:repo.childRuns(parent.get("id").toString()))if("WAITING".equals(child.get("status"))||"SYNC".equals(child.get("provider"))&&!exists(child.get("id").toString())){child.put("status","SKIPPED");child.put("errorCode","SERVICE_RESTARTED");child.put("finishedAt",ObjectService.now());repo.updateRun(child);}}
        }
        startClock();
    }
    private boolean exists(String id){return jdbc.queryForObject("SELECT COUNT(*) FROM dw_sync_execution WHERE run_id=?",Integer.class,id)>0;}
    private void finishRecoveringParents(){for(var parent:repo.unfinishedRuns())if(Boolean.TRUE.equals(parent.get("containsSync"))&&"SERVICE_RESTARTED".equals(parent.get("recoveryReason"))&&repo.childRuns(parent.get("id").toString()).stream().noneMatch(r->PENDING.contains(r.get("status")))){parent.put("status","FAILED");parent.put("errorCode","SERVICE_RESTARTED");parent.put("finishedAt",ObjectService.now());repo.transitionRun(parent,"RECOVERING");}}
    @PreDestroy public void close(){closing=true;clock.shutdownNow();workers.shutdown();specs.clear();prepared.clear();}
}
