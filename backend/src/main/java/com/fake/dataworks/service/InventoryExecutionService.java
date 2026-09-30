package com.fake.dataworks.service;

import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.StudioRepository;
import jakarta.annotation.PreDestroy;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/** Stages remain private to a build. Only the final business transaction publishes all layers. */
@Service
public class InventoryExecutionService {
    public record PreparedStage(StudioObject snapshot,DatasourceService.ConnectionSpec source,String target,SqlParameters query,int timeout) {
        @Override public String toString(){return "PreparedStage[target="+target+"]";}
    }
    public static final class Batch {
        final String id;final DatasourceService.ConnectionSpec source;final Map<String,Object> parameters;final Connection lease;
        volatile boolean cancelled,recovering,committed;volatile String publishError;
        List<String> targets=InventorySql.TARGETS;
        boolean single;
        Map<String,Object> lineage=Map.of();
        Batch(String id,DatasourceService.ConnectionSpec source,Map<String,Object> parameters,Connection lease){this.id=id;this.source=source;this.parameters=parameters;this.lease=lease;}
    }
    private static final class Job {volatile Connection connection;volatile Statement statement;volatile boolean cancelled,timedOut;volatile Future<?> future;volatile ScheduledFuture<?> deadline;}
    private final DatasourceService sources;private final SqlGuard guard;private final StudioRepository repo;private final TransactionTemplate tx;
    private final ExecutorService workers=Executors.newFixedThreadPool(2,r->new Thread(r,"inventory-stage"));
    private final ScheduledExecutorService deadlines=Executors.newScheduledThreadPool(1,r->new Thread(r,"inventory-deadline"));
    private final Map<String,Job> jobs=new ConcurrentHashMap<>();private final Map<String,Batch> batches=new ConcurrentHashMap<>();
    private volatile boolean closing;
    public InventoryExecutionService(DatasourceService sources,SqlGuard guard,StudioRepository repo,TransactionTemplate tx){this.sources=sources;this.guard=guard;this.repo=repo;this.tx=tx;}
    public static boolean materializes(StudioObject object){return RunService.isMysql(object)&&"MATERIALIZE".equals(RunService.config(object).get("executionMode"));}
    public PreparedStage prepare(StudioObject object,DatasourceService.ConnectionSpec source){
        if(!materializes(object)||!object.kind().equals("NODE")||!object.nodeType().equals("MySQL"))throw StudioException.bad("MATERIALIZATION_NODE_REQUIRED","请选择 MySQL 落表节点");
        String target=Objects.toString(RunService.config(object).get("targetTable"),"");
        if(!InventorySql.TARGETS.contains(target))throw StudioException.bad("INVALID_TARGET","请选择预建库存目标表");
        Object raw=RunService.config(object).get("timeoutSeconds");int timeout=raw==null?60:raw instanceof Number n&&n.doubleValue()==n.intValue()?n.intValue():0;
        if(timeout<1||timeout>300)throw StudioException.bad("INVALID_TIMEOUT","节点超时须为 1–300 秒");
        var query=SqlParameters.compile(object.content());guard.validate(query.sql(),source.database());
        return new PreparedStage(object,source,target,query,timeout);
    }
    public Map<String,Object> parameters(Map<String,Object> requested,String buildId){
        return runtimeParameters(requested,buildId);
    }
    public static Map<String,Object> runtimeParameters(Map<String,Object> requested,String buildId){
        try {
            String date=Objects.toString(requested.get("businessDate"),LocalDate.now(ZoneId.of("Asia/Shanghai")).minusDays(1).toString());
            LocalDate parsed=LocalDate.parse(date);if(parsed.isAfter(LocalDate.now(ZoneId.of("Asia/Shanghai"))))throw new IllegalArgumentException();
            String cutoff=Objects.toString(requested.get("sourceCutoffAt"),Instant.now().toString());Instant instant=Instant.parse(cutoff);
            if(instant.isAfter(Instant.now().plusSeconds(1)))throw new IllegalArgumentException();
            return Map.of("bizdate",parsed.toString(),"source_cutoff",instant.toString(),"build_id",buildId);
        }catch(Exception e){throw StudioException.bad("INVALID_BUSINESS_DATE","业务日期须为有效的非未来日期，数据截止时间须为有效 UTC 时间");}
    }
    private static String lockName(String database){return "inventory:"+UUID.nameUUIDFromBytes(database.toLowerCase(Locale.ROOT).getBytes(java.nio.charset.StandardCharsets.UTF_8));}
    public Batch begin(String id,DatasourceService.ConnectionSpec source,Map<String,Object> parameters){
        if(closing)throw new StudioException("SERVICE_STOPPING","服务正在停止",503);
        if(repo.unfinishedRuns().stream().anyMatch(r->"RECOVERING".equals(r.get("status"))))throw StudioException.conflict("RECOVERY_REQUIRED","存在待核实的落表提交，请等待业务库恢复");
        if(batches.values().stream().anyMatch(b->b.source.host().equalsIgnoreCase(source.host())&&b.source.port()==source.port()&&b.source.database().equalsIgnoreCase(source.database())))throw StudioException.conflict("MATERIALIZATION_BUSY","该业务库已有落表流程在运行");
        Connection lease=null;
        try {
            lease=sources.open(source,300,true);
            try(PreparedStatement s=lease.prepareStatement("SELECT GET_LOCK(?,0)")){s.setString(1,lockName(source.database()));s.setQueryTimeout(5);try(ResultSet r=s.executeQuery()){r.next();if(r.getInt(1)!=1)throw StudioException.conflict("MATERIALIZATION_BUSY","该业务库已有落表流程在运行");}}
            Batch batch=new Batch(id,source,parameters,lease);batches.put(id,batch);return batch;
        }catch(SQLException e){if(lease!=null)try{lease.close();}catch(SQLException ignored){}throw sources.connectionError(e);}
        catch(RuntimeException e){if(lease!=null)try{lease.close();}catch(SQLException ignored){}throw e;}
    }
    public void release(Batch batch){if(batch==null)return;batches.remove(batch.id,batch);try{batch.lease.close();}catch(SQLException ignored){}}
    public Batch beginTask(String id,PreparedStage stage,Map<String,Object> parameters,Map<String,Object> lineage){
        Batch batch=begin(id,stage.source(),parameters);batch.targets=List.of(stage.target());batch.single=true;batch.lineage=Map.copyOf(lineage);return batch;
    }
    public void cancelTask(String id){Batch batch=batches.get(id);if(batch==null)return;synchronized(batch){if(batch.committed)return;batch.cancelled=true;stop(id);}}
    public Map<String,Object> newRun(PreparedStage stage){
        Map<String,Object> run=new LinkedHashMap<>();run.put("id",UUID.randomUUID().toString());run.put("workspaceId",stage.snapshot().workspaceId());run.put("objectId",stage.snapshot().id());run.put("objectName",stage.snapshot().name());run.put("objectVersion",stage.snapshot().version());run.put("provider","MYSQL");run.put("simulation",false);run.put("executionMode","MATERIALIZE");run.put("targetTable",stage.target());run.put("dataSource",stage.source().publicView());run.put("status","WAITING");run.put("createdAt",ObjectService.now());run.put("columns",List.of());run.put("rows",List.of());return run;
    }
    public void enqueue(String id,PreparedStage stage,Batch batch){
        Job job=new Job();jobs.put(id,job);
        job.future=workers.submit(()->execute(id,stage,batch,job));
    }
    private void execute(String id,PreparedStage stage,Batch batch,Job job){
        long start=System.nanoTime();
        try {
            var run=repo.run(id).orElseThrow();if(job.cancelled||batch.cancelled){finish(id,"CANCELLED","WORKFLOW_CANCELLED","任务已取消",0,null,0);return;}
            run.put("status","RUNNING");run.put("startedAt",ObjectService.now());run.put("logs",List.of("[库存] 按固定日期及数据截止时间生成暂存结果，正式表尚未更新。"));
            if(!repo.transitionRun(run,"QUEUED"))return;
            job.deadline=deadlines.schedule(()->{synchronized(batch){if(!batch.committed){job.timedOut=true;abort(job);}}},stage.timeout(),TimeUnit.SECONDS);
            try(Connection connection=sources.open(stage.source(),stage.timeout(),true)) {
                job.connection=connection;check(job,batch);
                if(stage.target().equals(InventorySql.DWD))validateSources(connection,batch,job);
                var query=stage.query();String columns=InventorySql.COLUMNS.get(stage.target());
                String projected=String.join(",",Arrays.stream(columns.split(",")).map(s->"q.`"+s+"`").toList());
                String sql="INSERT INTO etl_stage_"+stage.target()+" (build_id,"+columns+") SELECT ?,"+projected+" FROM ("+query.sql()+") q";
                int count;
                try(PreparedStatement statement=connection.prepareStatement(sql)){job.statement=statement;statement.setQueryTimeout(stage.timeout());statement.setString(1,batch.id);query.bind(statement,batch.parameters,SqlParameters.custom(run),1);check(job,batch);count=statement.executeUpdate();}
                try(PreparedStatement s=connection.prepareStatement("SELECT COUNT(*) FROM etl_stage_"+stage.target()+" WHERE build_id=? AND business_date<>?")){s.setString(1,batch.id);s.setObject(2,LocalDate.parse(batch.parameters.get("bizdate").toString()));try(ResultSet r=s.executeQuery()){r.next();if(r.getLong(1)>0)throw StudioException.bad("OUTPUT_DATE_MISMATCH","节点输出含有其他业务日期");}}
                Map<String,Object> preview=preview(connection,stage.target(),batch.id);
                synchronized(job){check(job,batch);connection.commit();}
                if(batch.single){var staged=repo.run(id).orElseThrow();staged.put("writtenRows",count);repo.updateRun(staged);repo.saveResult(id,preview);synchronized(batch){check(job,batch);publish(batch);finish(id,"SUCCESS",null,"已独立提交 "+count+" 行正式结果。",count,preview,(System.nanoTime()-start)/1_000_000);}}
                else finish(id,"SUCCESS",null,"已生成 "+count+" 行暂存结果；等待整个流程发布。",count,preview,(System.nanoTime()-start)/1_000_000);
            }
        }catch(Exception e){
            if(!closing){
                if(batch.single&&(batch.recovering||batch.committed)){var run=repo.run(id).orElseThrow();run.put("status","RECOVERING");run.put("publicationStatus","RECOVERING");run.put("logs",List.of("[恢复] 正在核实本任务的提交凭据，暂不启动冲突落表。"));repo.updateRun(run);}
                else {String code=job.timedOut?"QUERY_TIMEOUT":job.cancelled||batch.cancelled?"WORKFLOW_CANCELLED":errorCode(e);finish(id,job.cancelled||batch.cancelled?"CANCELLED":"FAILED",code,errorMessage(e,code),0,null,(System.nanoTime()-start)/1_000_000);}
            }
        }finally{if(job.deadline!=null)job.deadline.cancel(false);job.connection=null;job.statement=null;jobs.remove(id,job);if(batch.single)release(batch);}
    }
    private void check(Job job,Batch batch){if(job.cancelled||job.timedOut||batch.cancelled)throw new CancellationException();}
    private void validateSources(Connection c,Batch batch,Job job)throws SQLException{
        var sql=SqlParameters.compile(InventorySql.validation());
        try(PreparedStatement s=c.prepareStatement(sql.sql())){job.statement=s;s.setQueryTimeout(30);sql.bind(s,batch.parameters,0);try(ResultSet r=s.executeQuery()){r.next();if(r.getLong(1)>0)throw StudioException.bad("INVALID_INVENTORY_SOURCE","库存源数据含缺失维度、非法数量或无效调拨，请先修正源数据");}}
    }
    private Map<String,Object> preview(Connection c,String target,String build)throws SQLException{
        try(PreparedStatement s=c.prepareStatement("SELECT "+InventorySql.COLUMNS.get(target)+" FROM etl_stage_"+target+" WHERE build_id=? LIMIT 1001")){s.setString(1,build);s.setQueryTimeout(10);try(ResultSet r=s.executeQuery()){List<String> columns=new ArrayList<>();var m=r.getMetaData();for(int i=1;i<=m.getColumnCount();i++)columns.add(m.getColumnLabel(i));List<List<Object>> rows=new ArrayList<>();while(r.next()&&rows.size()<1001){List<Object> row=new ArrayList<>();for(int i=1;i<=columns.size();i++)row.add(r.getString(i));rows.add(row);}boolean truncated=rows.size()>1000;if(truncated)rows.removeLast();return Map.of("columns",columns,"rows",rows,"truncated",truncated);}}
    }
    private void finish(String id,String status,String code,String message,int count,Map<String,Object> result,long elapsed){
        tx.executeWithoutResult(t->{var run=repo.run(id).orElseThrow();String expected=run.get("status").toString();if(!Set.of("QUEUED","RUNNING").contains(expected))return;run.put("status",status);run.put("finishedAt",ObjectService.now());run.put("elapsedMs",elapsed);run.put("writtenRows",count);run.put("publicationStatus",status.equals("SUCCESS")?(Boolean.TRUE.equals(run.get("taskExecution"))?"PUBLISHED":"STAGED"):"NOT_PUBLISHED");if(code!=null)run.put("errorCode",code);run.put("logs",List.of("[库存] 本次结果仅属于运行批次。",message));if(repo.transitionRun(run,expected)&&result!=null&&repo.result(id).isEmpty())repo.saveResult(id,result);});
    }
    public void stop(String id){Job job=jobs.get(id);if(job!=null){synchronized(job){job.cancelled=true;abort(job);}}else {var r=repo.run(id).orElseThrow();if("QUEUED".equals(r.get("status")))finish(id,"CANCELLED","WORKFLOW_CANCELLED","任务已取消",0,null,0);}}
    public boolean busy(Batch batch){return repo.childRuns(batch.id).stream().anyMatch(r->jobs.containsKey(r.get("id").toString()));}
    public void cancel(Batch batch){batch.cancelled=true;repo.childRuns(batch.id).forEach(r->stop(r.get("id").toString()));}
    private void abort(Job job){try{if(job.statement!=null)job.statement.cancel();}catch(SQLException ignored){}try{if(job.connection!=null)job.connection.abort(r->Thread.ofVirtual().start(r));}catch(SQLException ignored){}}
    public boolean publish(Batch batch)throws SQLException{
        // Caller serializes this operation against cancellation using the workflow context monitor.
        if(batch.cancelled)throw new CancellationException();
        if(hasReceipt(batch.source,batch.id)){batch.committed=true;return true;}
        if(batch.recovering){if(recoveredReceipt(batch.source,batch.id)){batch.committed=true;return true;}throw StudioException.bad("PUBLISH_FAILED","已核实上次提交未成功，正式表保持不变，请重新运行");}
        // Publish on the same session that owns the advisory lock. If that session
        // is lost, it cannot silently publish after another workflow acquires it.
        try {
            Connection c=batch.lease;
            for(String target:batch.targets){
                try(PreparedStatement d=c.prepareStatement("DELETE FROM "+target+" WHERE business_date=?")){d.setQueryTimeout(60);d.setObject(1,LocalDate.parse(batch.parameters.get("bizdate").toString()));d.executeUpdate();}
                try(PreparedStatement s=c.prepareStatement("INSERT INTO "+target+" ("+InventorySql.COLUMNS.get(target)+") SELECT "+InventorySql.COLUMNS.get(target)+" FROM etl_stage_"+target+" WHERE build_id=?")){s.setQueryTimeout(60);s.setString(1,batch.id);s.executeUpdate();}
                // Older business installations can still replay legacy workflow releases.
                if(batch.single||publicationTableExists(c)){
                    Map<String,Object> lineage=new LinkedHashMap<>(batch.lineage);lineage.put(target,batch.id);
                    if(!batch.single)for(String t:InventorySql.TARGETS)lineage.put(t,batch.id);
                    try(PreparedStatement s=c.prepareStatement("INSERT INTO etl_partition_publication(target_table,business_date,build_id,lineage_json,committed_at) VALUES(?,?,?,?,UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE build_id=VALUES(build_id),lineage_json=VALUES(lineage_json),committed_at=VALUES(committed_at)")){
                        s.setString(1,target);s.setObject(2,LocalDate.parse(batch.parameters.get("bizdate").toString()));s.setString(3,batch.id);s.setString(4,new com.google.gson.Gson().toJson(lineage));s.executeUpdate();
                    }
                }
            }
            try(PreparedStatement s=c.prepareStatement("INSERT INTO etl_publish_receipt VALUES(?,?,UTC_TIMESTAMP(6))")){s.setString(1,batch.id);s.setObject(2,LocalDate.parse(batch.parameters.get("bizdate").toString()));s.executeUpdate();}
            c.commit();batch.committed=true;return true;
        }catch(SQLException e){batch.recovering=true;batch.publishError=errorCode(e);try{batch.lease.close();}catch(SQLException ignored){}throw e;}
    }
    public boolean hasReceipt(DatasourceService.ConnectionSpec source,String id)throws SQLException{
        try(Connection c=sources.open(source,5);PreparedStatement s=c.prepareStatement("SELECT COUNT(*) FROM etl_publish_receipt WHERE build_id=?")){s.setQueryTimeout(5);s.setString(1,id);try(ResultSet r=s.executeQuery()){r.next();return r.getLong(1)==1;}}
    }
    private boolean publicationTableExists(Connection c)throws SQLException{try(PreparedStatement s=c.prepareStatement("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='etl_partition_publication'")){try(ResultSet r=s.executeQuery()){r.next();return r.getInt(1)>0;}}}
    private boolean recoveredReceipt(DatasourceService.ConnectionSpec source,String id)throws SQLException{
        try(Connection c=sources.open(source,5,true);PreparedStatement lock=c.prepareStatement("SELECT GET_LOCK(?,0)")){
            lock.setString(1,lockName(source.database()));lock.setQueryTimeout(5);
            try(ResultSet r=lock.executeQuery()){r.next();if(r.getInt(1)!=1)throw new SQLTransientConnectionException("Previous publishing session is still active");}
            return hasReceipt(source,id);
        }
    }
    @SuppressWarnings("unchecked")
    public synchronized void recover(){
        for(var run:repo.unfinishedRuns()){
            if(!Boolean.TRUE.equals(run.get("materialization"))||(run.get("parentRunId")!=null&&!Boolean.TRUE.equals(run.get("taskExecution")))||batches.containsKey(run.get("id").toString()))continue;
            String expected=run.get("status").toString();
            try {
                var binding=(Map<String,Object>)run.get("dataSource");var source=sources.forWorkspace(binding.get("id").toString(),run.get("workspaceId").toString());
                for(String key:List.of("host","database","username"))if(!Objects.equals(binding.get(key),source.publicView().get(key)))throw StudioException.bad("DATASOURCE_BINDING_CHANGED","恢复需要原数据源目标");
                if(((Number)binding.get("port")).intValue()!=source.port())throw StudioException.bad("DATASOURCE_BINDING_CHANGED","恢复需要原数据源端口");
                boolean committed;
                // Wait for the old publishing session to release its lock before
                // deciding whether an interrupted COMMIT became durable.
                committed=recoveredReceipt(source,run.get("id").toString());
                run.put("status",committed?"SUCCESS":"FAILED");run.put("publicationStatus",committed?"PUBLISHED":"NOT_PUBLISHED");if(!committed)run.put("errorCode","SERVICE_RESTARTED");run.put("finishedAt",ObjectService.now());run.put("logs",List.of(committed?"[恢复] 已核实数据提交凭据，结果已发布。":"[恢复] 无数据提交凭据，流程已中断，正式表保持不变。"));repo.transitionRun(run,expected);
                for(var child:repo.childRuns(run.get("id").toString()))if(Set.of("WAITING","QUEUED","RUNNING").contains(child.get("status"))){String old=child.get("status").toString();child.put("status","FAILED");child.put("errorCode","SERVICE_RESTARTED");child.put("finishedAt",ObjectService.now());repo.transitionRun(child,old);}
            }catch(Exception e){run.put("status","RECOVERING");run.put("publicationStatus","RECOVERING");run.put("logs",List.of("[恢复] 无法核实业务库提交状态；恢复连接后自动核实，期间禁止新落表。"));repo.transitionRun(run,expected);}
        }
    }
    public static String errorCode(Exception error){
        if(error instanceof StudioException s)return s.code();
        if(error instanceof SQLTimeoutException)return "QUERY_TIMEOUT";
        if(error instanceof SQLException e){if(Set.of(1213,1205,1040,1041,1203,1206).contains(e.getErrorCode()))return "DB_TRANSIENT";if(e.getSQLState()!=null&&e.getSQLState().startsWith("08"))return "DATASOURCE_UNAVAILABLE";}
        return "MATERIALIZATION_FAILED";
    }
    private String errorMessage(Exception e,String code){if(e instanceof StudioException s)return s.getMessage();return switch(code){case "QUERY_TIMEOUT"->"节点超时，暂存事务已终止。";case "WORKFLOW_CANCELLED"->"用户已停止，正式数据未更新。";case "DATASOURCE_UNAVAILABLE"->"业务数据库连接中断。";case "DB_TRANSIENT"->"数据库暂时繁忙，事务未完成。";default->"落表失败，请检查 SELECT 输出列、数据类型及数据库权限"+(e instanceof SQLException s?"（错误码 "+s.getErrorCode()+"）":"");};}
    @PreDestroy public void shutdown(){closing=true;jobs.values().forEach(j->{j.cancelled=true;abort(j);});workers.shutdownNow();deadlines.shutdownNow();batches.values().forEach(this::release);}
}
