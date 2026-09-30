package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.*;
import java.time.*;
import java.util.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.*;

/** A real node owns its immutable code release and independently committed output. */
@Service
public class TaskService {
    private final TaskRepository tasks;private final StudioRepository repo;private final ObjectService objects;
    private final SyncExecutionService sync;private final DatasourceService sources;private final MysqlExecutionProvider mysql;private final InventoryExecutionService inventory;private final JsonCodec json;private final TransactionTemplate tx;
    public TaskService(TaskRepository tasks,StudioRepository repo,ObjectService objects,DatasourceService sources,MysqlExecutionProvider mysql,InventoryExecutionService inventory,JsonCodec json,TransactionTemplate tx,SyncExecutionService sync){this.sync=sync;this.tasks=tasks;this.repo=repo;this.objects=objects;this.sources=sources;this.mysql=mysql;this.inventory=inventory;this.json=json;this.tx=tx;}
    public StudioObject task(String id){var o=objects.active(id);boolean sql=("MySQL".equals(o.nodeType())&&RunService.isMysql(o))||("Doris".equals(o.nodeType())&&RunService.isDoris(o));if(!"NODE".equals(o.kind())||!sql&&!SyncExecutionService.isSync(o))throw StudioException.bad("REAL_TASK_REQUIRED","请选择真实 MySQL、Doris 或离线同步任务");return o;}
    public List<Map<String,Object>> releases(String id){task(id);return tasks.releases(id).stream().map(this::summary).toList();}
    private Map<String,Object> summary(Map<String,Object> r){var result=new LinkedHashMap<>(r);result.remove("snapshot");return result;}
    public Map<String,Object> release(String id){return tasks.release(id);}
    private DatasourceService.ConnectionSpec source(StudioObject o){return sources.forWorkspace(Objects.toString(RunService.config(o).get("dataSourceId"),""),o.workspaceId());}
    public Map<String,Object> publish(String id,Integer expected,String note){return tx.execute(t->{
        var initial=task(id);repo.lockWorkspace(initial.workspaceId());var o=task(id);
        if(expected==null||expected!=o.version())throw StudioException.conflict("VERSION_CONFLICT","请先保存任务再发布");
        if(note.length()>4000)throw StudioException.bad("INVALID_RELEASE_NOTE","发布说明过长");
        ScheduleParameters.attach(new LinkedHashMap<>(),o,Map.of(),List.of());
        if(SyncExecutionService.isSync(o)) {
            var p=sync.prepare(o);var r=new LinkedHashMap<String,Object>();r.put("id",UUID.randomUUID().toString());r.put("taskId",id);r.put("workspaceId",o.workspaceId());r.put("releaseNo",tasks.releases(id).size()+1);r.put("objectVersion",o.version());r.put("name",o.name());r.put("note",note);r.put("createdAt",ObjectService.now());r.put("snapshot",o);r.put("datasourceBindings",p.bindings());r.put("containsWrites",true);tasks.insertRelease(r);return summary(r);
        }
        var source=source(o);boolean writes=false;if(InventoryExecutionService.materializes(o))inventory.prepare(o,source);else writes=mysql.prepare(o,source).script().writes();
        var r=new LinkedHashMap<String,Object>();r.put("id",UUID.randomUUID().toString());r.put("taskId",id);r.put("workspaceId",o.workspaceId());r.put("releaseNo",tasks.releases(id).size()+1);r.put("objectVersion",o.version());r.put("name",o.name());r.put("note",note);r.put("createdAt",ObjectService.now());r.put("snapshot",o);r.put("dataSource",source.publicView());r.put("containsWrites",writes);tasks.insertRelease(r);return summary(r);
    });}
    @SuppressWarnings("unchecked") public static List<Map<String,Object>> dependencies(Map<String,Object> config){return config.get("dependencies") instanceof List<?> list?(List<Map<String,Object>>)list:List.of();}
    public List<Map<String,Object>> manualInputs(String task,String date){
        var plan=tasks.forTask(task).orElse(Map.of());List<Map<String,Object>> inputs=new ArrayList<>();
        for(var dep:dependencies(plan)){
            var run=repo.topRuns(plan.get("workspaceId").toString()).stream().filter(r->Objects.equals(r.get("objectId"),dep.get("taskId"))&&Objects.equals(r.get("businessDate"),date)&&"SUCCESS".equals(r.get("status"))&&"TASK".equals(r.get("releaseKind"))&&(!Boolean.TRUE.equals(r.get("materialization"))||"PUBLISHED".equals(r.get("publicationStatus")))).findFirst().orElseThrow(()->StudioException.bad("UPSTREAM_REQUIRED","业务日期 "+date+" 缺少上游「"+dep.get("name")+"」的成功发布运行，请先运行上游"));
            inputs.add(input(dep,run));
        }return inputs;
    }
    public Map<String,Object> input(Map<String,Object> dep,Map<String,Object> run){var result=new LinkedHashMap<String,Object>();for(String k:List.of("taskId","alias","name","scheduledAt","triggerId"))if(dep.containsKey(k))result.put(k,dep.get(k));result.put("runId",run.get("id"));result.put("releaseId",Objects.toString(run.get("releaseId"),""));if(run.get("buildId")!=null)result.put("buildId",run.get("buildId"));return result;}
    public Map<String,Object> startDevelopment(StudioObject o,Map<String,Object> requested){var options=fixedOptions(o,requested);String date=Objects.toString(options.get("businessDate"),LocalDate.now(ZoneId.of("Asia/Shanghai")).minusDays(1).toString());options.put("upstreamRuns",manualInputs(o.id(),date));return start(o,null,options);}
    public Map<String,Object> startRelease(String id,Map<String,Object> requested){
        var r=tasks.release(id);var o=json.read(json.write(r.get("snapshot")),StudioObject.class);var options=fixedOptions(o,requested);
        if(!options.containsKey("upstreamRuns")){String date=Objects.toString(options.get("businessDate"),LocalDate.now(ZoneId.of("Asia/Shanghai")).minusDays(1).toString());options.put("upstreamRuns",manualInputs(o.id(),date));}
        return start(o,r,options);
    }
    private Map<String,Object> fixedOptions(StudioObject object,Map<String,Object> requested) {
        var options=new LinkedHashMap<>(requested);
        options.putAll(ScheduleParameters.context(ScheduleParameters.schedule(object),requested).toMap());return options;
    }
    @SuppressWarnings("unchecked") private Map<String,Object> start(StudioObject o,Map<String,Object> release,Map<String,Object> options){return tx.execute(t->{
        repo.lockWorkspace(o.workspaceId());
        if((release!=null||InventoryExecutionService.materializes(o)||options.containsKey("parentRunId"))&&repo.topRuns(o.workspaceId()).stream().anyMatch(r->o.id().equals(r.get("objectId"))&&Set.of("QUEUED","RUNNING","RECOVERING").contains(r.get("status"))))throw StudioException.conflict("TASK_OVERLAP","当前任务仍在运行");
        if(SyncExecutionService.isSync(o))return startSync(o,release,options);
        var source=source(o);if(release!=null){var bound=(Map<String,Object>)release.get("dataSource");for(String k:List.of("workspaceId","type","host","port","database","username","options"))if(!(bound.get(k) instanceof Number a&&source.publicView().get(k) instanceof Number b?a.doubleValue()==b.doubleValue():Objects.equals(bound.get(k),source.publicView().get(k))))throw StudioException.conflict("DATASOURCE_BINDING_CHANGED","任务发布时的数据源目标已变化，请发布新版本");}
        boolean material=InventoryExecutionService.materializes(o);var stage=material?inventory.prepare(o,source):null;var query=material?null:mysql.prepare(o,source);
        if(material&&!InventorySql.DWD.equals(stage.target())&&stage.query().names().contains("build_id")&&o.content().toLowerCase(Locale.ROOT).contains("etl_stage_"))throw StudioException.bad("LEGACY_SHARED_BATCH_SQL","此 SQL 仍按整条工作流共享批次读取上游。请将输入筛选改为 :upstream_别名_build_id 后重新发布任务");
        var run=material?inventory.newRun(stage):mysql.newRun(query,"MANUAL");if(options.get("existingRunId")!=null)run.put("id",options.get("existingRunId"));String id=run.get("id").toString();
        ScheduleParameters.attach(run,o,options,List.of());
        var parameters=new LinkedHashMap<>(inventory.parameters(options,id));var lineage=new LinkedHashMap<String,Object>();
        var inputs=(List<Map<String,Object>>)options.getOrDefault("upstreamRuns",List.of());
        for(var input:inputs){var upstream=repo.run(input.get("runId").toString()).orElseThrow(()->StudioException.bad("UPSTREAM_REQUIRED","上游运行已不存在"));
            if(!o.workspaceId().equals(upstream.get("workspaceId"))||!Objects.equals(parameters.get("bizdate"),upstream.get("businessDate"))||!"SUCCESS".equals(upstream.get("status"))||Boolean.TRUE.equals(upstream.get("simulation")))throw StudioException.bad("INVALID_UPSTREAM","上游运行须为同一业务日期的真实成功任务");
            if(upstream.get("lineage") instanceof Map<?,?> map)map.forEach((k,v)->{if(lineage.containsKey(k.toString())&&!Objects.equals(lineage.get(k.toString()),v))throw StudioException.bad("INCONSISTENT_LINEAGE","多个上游使用了不同的库存批次，请重新计算上游");lineage.put(k.toString(),v);});
            if(upstream.get("buildId")!=null){if(!"PUBLISHED".equals(upstream.get("publicationStatus")))throw StudioException.bad("UPSTREAM_NOT_PUBLISHED","上游尚未提交正式结果");parameters.put("upstream_"+input.get("alias")+"_build_id",upstream.get("buildId"));lineage.put(upstream.get("targetTable").toString(),upstream.get("buildId"));}
        }
        if(material)for(String p:stage.query().names())if(!p.startsWith("$")&&!p.startsWith("\u0001")&&!parameters.containsKey(p))throw StudioException.bad("MISSING_DEPENDENCY_PARAMETER","请配置 SQL 参数对应的上游依赖："+p);
        run.put("taskExecution",true);run.put("status","QUEUED");run.put("businessDate",parameters.get("bizdate"));run.put("sourceCutoffAt",parameters.get("source_cutoff"));run.put("parameters",parameters);run.put("upstreamRuns",inputs);run.put("lineage",lineage);run.put("executionSource",release==null?"DEVELOPMENT":"RELEASE");run.put("triggerType",options.getOrDefault("triggerType","MANUAL"));run.put("mode",run.get("triggerType"));run.put("attempt",options.getOrDefault("attempt",1));
        for(String k:List.of("scheduleId","triggerId","scheduledAt","retryOfRunId","parentRunId","graphNodeId"))if(options.containsKey(k))run.put(k,options.get(k));
        if(release!=null){run.put("releaseKind","TASK");run.put("releaseId",release.get("id"));run.put("releaseNo",release.get("releaseNo"));}
        final InventoryExecutionService.Batch batch;
        if(material){run.put("materialization",true);run.put("buildId",id);run.put("publicationStatus","STAGING");batch=inventory.beginTask(id,stage,parameters,lineage);}else batch=null;
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization(){
            @Override public void afterCommit(){if(material)inventory.enqueue(id,stage,batch);else mysql.enqueueExisting(id,query);}
            @Override public void afterCompletion(int status){if(status!=STATUS_COMMITTED)inventory.release(batch);}
        });if(options.containsKey("existingRunId"))repo.updateRun(run);else repo.insertRun(run,o);return run;
    });}
    @SuppressWarnings("unchecked") private Map<String,Object> startSync(StudioObject o,Map<String,Object> release,Map<String,Object> options) {
        var p=options.get("_syncPrepared") instanceof SyncExecutionService.Prepared fixed?fixed:sync.prepare(o,List.of(),options);
        if(release!=null)verifyBindings((List<Map<String,Object>>)release.get("datasourceBindings"),p.bindings());
        var run=sync.newRun(p,"MANUAL");if(options.get("existingRunId")!=null)run.put("id",options.get("existingRunId"));String id=run.get("id").toString();
        ScheduleParameters.attach(run,o,options,List.of());var parameters=new LinkedHashMap<>(inventory.parameters(options,id));run.put("parameters",parameters);
        var inputs=(List<Map<String,Object>>)options.getOrDefault("upstreamRuns",List.of());
        for(var input:inputs){var upstream=repo.run(input.get("runId").toString()).orElseThrow();if(!o.workspaceId().equals(upstream.get("workspaceId"))||!Objects.equals(run.get("businessDate"),upstream.get("businessDate"))||!"SUCCESS".equals(upstream.get("status"))||Boolean.TRUE.equals(upstream.get("simulation")))throw StudioException.bad("INVALID_UPSTREAM","上游须为同一业务日期的真实成功任务");if(upstream.get("buildId")!=null)parameters.put("upstream_"+input.get("alias")+"_build_id",upstream.get("buildId"));}
        sync.spec(p,run); // Resolve every runtime parameter before admitting any remote write.
        run.put("taskExecution",true);run.put("upstreamRuns",inputs);run.put("executionSource",release==null?"DEVELOPMENT":"RELEASE");run.put("triggerType",options.getOrDefault("triggerType","MANUAL"));run.put("mode",run.get("triggerType"));run.put("attempt",options.getOrDefault("attempt",1));
        for(String k:List.of("scheduleId","triggerId","scheduledAt","retryOfRunId","parentRunId","graphNodeId"))if(options.containsKey(k))run.put(k,options.get(k));
        if(release!=null){run.put("releaseKind","TASK");run.put("releaseId",release.get("id"));run.put("releaseNo",release.get("releaseNo"));}
        if(options.containsKey("existingRunId"))repo.updateRun(run);else repo.insertRun(run,o);sync.register(id,p);
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization(){@Override public void afterCommit(){sync.enqueue(id,p);}});return run;
    }
    public static void verifyBindings(List<Map<String,Object>> before,List<Map<String,Object>> now) {
        if(before==null)throw StudioException.bad("INVALID_RELEASE","发布包缺少连接绑定");
        for(var current:now){var bound=before.stream().filter(b->Objects.equals(b.get("id"),current.get("id"))).findFirst().orElseThrow(()->StudioException.bad("INVALID_RELEASE","发布包缺少连接绑定"));for(String k:List.of("workspaceId","type","host","port","database","username","options")){Object a=bound.get(k),b=current.get(k);if(!(a instanceof Number x&&b instanceof Number y?x.doubleValue()==y.doubleValue():Objects.equals(a,b)))throw StudioException.conflict("DATASOURCE_BINDING_CHANGED","发布时的数据源连接已变化，请发布新版本");}}
    }
    public Map<String,Object> startWorkflowNode(StudioObject o,Map<String,Object> child,List<Map<String,Object>> upstream){return startWorkflowNode(o,child,upstream,null);}
    public Map<String,Object> startWorkflowNode(StudioObject o,Map<String,Object> child,List<Map<String,Object>> upstream,SyncExecutionService.Prepared syncPlan){
        var options=new LinkedHashMap<String,Object>();for(String key:List.of("businessDate","sourceCutoffAt","parentRunId","graphNodeId","triggerType","scheduledAt","timezone","scheduleParameters","attempt"))if(child.containsKey(key))options.put(key,child.get(key));options.put("existingRunId",child.get("id"));if(syncPlan!=null)options.put("_syncPrepared",syncPlan);
        var configured=dependencies(tasks.forTask(o.id()).orElse(Map.of()));List<Map<String,Object>> inputs=new ArrayList<>();
        for(var run:upstream){String alias=configured.stream().filter(d->Objects.equals(d.get("taskId"),run.get("objectId"))).map(d->d.get("alias").toString()).findFirst().orElseGet(()->Objects.toString(run.get("targetTable"),"upstream").split("_")[0]);inputs.add(input(Map.of("taskId",run.get("objectId"),"name",run.get("objectName"),"alias",alias),run));}
        options.put("upstreamRuns",inputs);return start(o,null,options);
    }
    public Map<String,Object> rerun(String id){var run=repo.run(id).orElseThrow(()->StudioException.missing("运行不存在"));if(!"TASK".equals(run.get("releaseKind")))throw StudioException.bad("TASK_RELEASE_REQUIRED","请选择任务发布版本运行");var options=new LinkedHashMap<String,Object>();for(String k:List.of("businessDate","sourceCutoffAt","upstreamRuns","scheduledAt","timezone","scheduleParameters"))if(run.containsKey(k))options.put(k,run.get(k));options.put("triggerType","RERUN");options.put("retryOfRunId",id);return startRelease(run.get("releaseId").toString(),options);}
    public Map<String,Object> stop(String id){var run=repo.run(id).orElseThrow();if("SYNC".equals(run.get("provider")))return sync.stop(id);if(!Boolean.TRUE.equals(run.get("materialization")))return mysql.stop(id);inventory.cancelTask(id);return repo.run(id).orElseThrow();}
}
