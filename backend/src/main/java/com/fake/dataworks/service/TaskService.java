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
    public List<Map<String,Object>> dependencyBindings(String taskId){return dependencies(tasks.forTask(taskId).orElse(Map.of())).stream().map(d->Map.copyOf(d)).toList();}
    public List<Map<String,Object>> manualInputs(String task,String date){
        var plan=tasks.forTask(task).orElse(Map.of());List<Map<String,Object>> inputs=new ArrayList<>();
        for(var dep:dependencies(plan)){
            if(allDay(dep)){
                var upstreamPlan=tasks.forTask(dep.get("taskId").toString()).orElseThrow(()->StudioException.bad("UPSTREAM_REQUIRED","全天依赖尚未配置调度，请在调度实例页面补齐上游"));
                var expected=TaskScheduleService.requiredDaySlots(upstreamPlan,date);if(expected.isEmpty())throw StudioException.bad("UPSTREAM_REQUIRED","该业务日期没有上游期次，请检查上游调度有效期");
                var slots=tasks.slotRange(upstreamPlan.get("id").toString(),"",expected.getFirst().toString(),expected.getLast().toString());var byTime=new HashMap<String,Map<String,Object>>();slots.forEach(s->byTime.put(s.get("scheduledAt").toString(),s));
                var ids=new ArrayList<String>();for(var at:expected){var instance=byTime.get(at.toString());if(instance==null||!"SUCCESS".equals(instance.get("status"))||instance.get("runId")==null)throw StudioException.bad("UPSTREAM_REQUIRED","全天依赖尚未全部成功，请在调度实例页面完成或补齐上游期次");ids.add(instance.get("runId").toString());}var availableRuns=repo.runsByIds(ids);
                for(var at:expected){var instance=byTime.get(at.toString());var run=availableRuns.get(instance.get("runId").toString());if(run==null)throw StudioException.bad("UPSTREAM_REQUIRED","上游运行已不存在");var selected=new LinkedHashMap<>(dep);selected.put("matchMode","ALL_DAY");selected.put("scheduledAt",at.toString());selected.put("triggerId",instance.get("id"));inputs.add(input(selected,run));}continue;
            }
            var run=repo.topRuns(plan.get("workspaceId").toString()).stream().filter(r->Objects.equals(r.get("objectId"),dep.get("taskId"))&&Objects.equals(r.get("businessDate"),date)&&"SUCCESS".equals(r.get("status"))&&"TASK".equals(r.get("releaseKind"))&&(!Boolean.TRUE.equals(r.get("materialization"))||"PUBLISHED".equals(r.get("publicationStatus")))).findFirst().orElseThrow(()->StudioException.bad("UPSTREAM_REQUIRED","业务日期 "+date+" 缺少上游「"+dep.get("name")+"」的成功发布运行，请先运行上游"));
            inputs.add(input(dep,run));
        }return inputs;
    }
    public Map<String,Object> input(Map<String,Object> dep,Map<String,Object> run){var result=new LinkedHashMap<String,Object>();for(String k:List.of("taskId","alias","name","scheduledAt","triggerId","matchMode","mode","scheduleId","batchKey","timezone"))if(dep.containsKey(k))result.put(k,dep.get(k));result.put("runId",run.get("id"));result.put("releaseId",Objects.toString(run.get("releaseId"),""));if(run.get("buildId")!=null)result.put("buildId",run.get("buildId"));return result;}
    public Map<String,Object> startDevelopment(StudioObject o,Map<String,Object> requested){var options=fixedOptions(o,requested);String date=Objects.toString(options.get("businessDate"),LocalDate.now(ZoneId.of("Asia/Shanghai")).minusDays(1).toString());options.put("upstreamRuns",manualInputs(o.id(),date));return start(o,null,options);}
    public Map<String,Object> startRelease(String id,Map<String,Object> requested){
        var r=tasks.release(id);var o=json.read(json.write(r.get("snapshot")),StudioObject.class);var options=fixedOptions(o,requested);
        if(!options.containsKey("upstreamRuns")){String date=Objects.toString(options.get("businessDate"),LocalDate.now(ZoneId.of("Asia/Shanghai")).minusDays(1).toString());options.put("upstreamRuns",manualInputs(o.id(),date));}
        return start(o,r,options);
    }
    private Map<String,Object> fixedOptions(StudioObject object,Map<String,Object> requested) {
        var options=new LinkedHashMap<>(requested);
        var timeConfig=requested.containsKey("scheduledAt")?ScheduleParameters.schedule(object):tasks.forTask(object.id()).orElse(ScheduleParameters.schedule(object));
        options.putAll(ScheduleParameters.context(timeConfig,requested).toMap());return options;
    }
    private Map<String,Object> start(StudioObject o,Map<String,Object> release,Map<String,Object> options){return start(o,release,options,null,null,null);}
    @SuppressWarnings("unchecked") private Map<String,Object> start(StudioObject o,Map<String,Object> release,Map<String,Object> options,MysqlExecutionProvider.PreparedQuery fixedQuery,InventoryExecutionService.PreparedStage fixedStage,SyncExecutionService.Prepared fixedSync){return tx.execute(t->{
        repo.lockWorkspace(o.workspaceId());
        if(repo.hasActiveTaskRun(o.workspaceId(),o.id(),Objects.toString(options.get("existingRunId"),"")))throw StudioException.conflict("TASK_OVERLAP","当前任务仍在运行或核实提交");
        if(SyncExecutionService.isSync(o))return startSync(o,release,options,fixedSync);
        var source=fixedQuery!=null?fixedQuery.source():fixedStage!=null?fixedStage.source():source(o);if(release!=null){var bound=(Map<String,Object>)release.get("dataSource");for(String k:List.of("workspaceId","type","host","port","database","username","options"))if(!(bound.get(k) instanceof Number a&&source.publicView().get(k) instanceof Number b?a.doubleValue()==b.doubleValue():Objects.equals(bound.get(k),source.publicView().get(k))))throw StudioException.conflict("DATASOURCE_BINDING_CHANGED","任务发布时的数据源目标已变化，请发布新版本");}
        boolean material=InventoryExecutionService.materializes(o);var stage=material?(fixedStage!=null?fixedStage:inventory.prepare(o,source)):null;var query=material?null:fixedQuery!=null?fixedQuery:mysql.prepare(o,source);
        if(material&&!InventorySql.DWD.equals(stage.target())&&stage.query().names().contains("build_id")&&o.content().toLowerCase(Locale.ROOT).contains("etl_stage_"))throw StudioException.bad("LEGACY_SHARED_BATCH_SQL","此 SQL 仍按整条工作流共享批次读取上游。请将输入筛选改为 :upstream_别名_build_id 后重新发布任务");
        var run=material?inventory.newRun(stage):mysql.newRun(query,"MANUAL");if(options.get("existingRunId")!=null)run.put("id",options.get("existingRunId"));String id=run.get("id").toString();
        ScheduleParameters.attach(run,o,options,List.of());
        var parameters=new LinkedHashMap<>(inventory.parameters(options,id));var lineage=new LinkedHashMap<String,Object>();
        var inputs=(List<Map<String,Object>>)options.getOrDefault("upstreamRuns",List.of());
        bindInputs(o,inputs,parameters,lineage);
        var names=material?stage.query().names():query.script().commands().stream().flatMap(command->command.parameters().names().stream()).toList();
        validateDependencyParameters(inputs,names,parameters);
        run.put("taskExecution",true);run.put("status","QUEUED");run.put("businessDate",parameters.get("bizdate"));run.put("sourceCutoffAt",parameters.get("source_cutoff"));run.put("parameters",parameters);run.put("upstreamRuns",inputs);run.put("lineage",lineage);run.put("executionSource",options.getOrDefault("executionSource",release==null?"DEVELOPMENT":"RELEASE"));run.put("triggerType",options.getOrDefault("triggerType","MANUAL"));run.put("mode",run.get("triggerType"));run.put("attempt",options.getOrDefault("attempt",1));
        for(String k:List.of("scheduleId","triggerId","scheduledAt","retryOfRunId","parentRunId","graphNodeId","releaseId","releaseNo","dependencySlots","scheduleSnapshot"))if(options.containsKey(k))run.put(k,options.get(k));
        if(release!=null){run.put("releaseKind","TASK");run.put("releaseId",release.get("id"));run.put("releaseNo",release.get("releaseNo"));}
        final InventoryExecutionService.Batch batch;
        if(material){run.put("materialization",true);run.put("buildId",id);run.put("publicationStatus","STAGING");batch=inventory.beginTask(id,stage,parameters,lineage);}else batch=null;
        if(!material&&!mysql.reserve(id))throw new StudioException("QUEUE_FULL","执行资源已满，等待空闲资源",429);
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization(){
            @Override public void afterCommit(){if(material)inventory.enqueue(id,stage,batch);else if(options.containsKey("parentRunId")){if(!mysql.tryEnqueueExisting(id,query)){var waiting=repo.run(id).orElseThrow();waiting.put("status","WAITING");waiting.put("waitingReason","QUEUE_FULL");repo.transitionRun(waiting,"QUEUED");}}else mysql.enqueueDeferred(id,query);}
            @Override public void afterCompletion(int status){if(status!=STATUS_COMMITTED){inventory.release(batch);if(!material)mysql.releaseReservation(id);}}
        });if(options.containsKey("existingRunId"))repo.updateRun(run);else repo.insertRun(run,o);return run;
    });}
    @SuppressWarnings("unchecked") private Map<String,Object> startSync(StudioObject o,Map<String,Object> release,Map<String,Object> options,SyncExecutionService.Prepared fixedSync) {
        var p=fixedSync!=null?fixedSync:options.get("_syncPrepared") instanceof SyncExecutionService.Prepared fixed?fixed:sync.prepare(o,List.of(),options);
        if(release!=null)verifyBindings((List<Map<String,Object>>)release.get("datasourceBindings"),p.bindings());
        var run=sync.newRun(p,"MANUAL");if(options.get("existingRunId")!=null)run.put("id",options.get("existingRunId"));String id=run.get("id").toString();
        ScheduleParameters.attach(run,o,options,List.of());var parameters=new LinkedHashMap<>(inventory.parameters(options,id));run.put("parameters",parameters);
        var inputs=(List<Map<String,Object>>)options.getOrDefault("upstreamRuns",List.of());
        var lineage=new LinkedHashMap<String,Object>();bindInputs(o,inputs,parameters,lineage);run.put("lineage",lineage);validateDependencyParameters(inputs,SqlParameters.compile(SyncExecutionService.parameterCode(o)).names(),parameters);
        sync.spec(p,run); // Resolve every runtime parameter before admitting any remote write.
        run.put("taskExecution",true);run.put("upstreamRuns",inputs);run.put("executionSource",options.getOrDefault("executionSource",release==null?"DEVELOPMENT":"RELEASE"));run.put("triggerType",options.getOrDefault("triggerType","MANUAL"));run.put("mode",run.get("triggerType"));run.put("attempt",options.getOrDefault("attempt",1));
        for(String k:List.of("scheduleId","triggerId","scheduledAt","retryOfRunId","parentRunId","graphNodeId","releaseId","releaseNo","dependencySlots","scheduleSnapshot"))if(options.containsKey(k))run.put(k,options.get(k));
        if(release!=null){run.put("releaseKind","TASK");run.put("releaseId",release.get("id"));run.put("releaseNo",release.get("releaseNo"));}
        if(options.containsKey("existingRunId"))repo.updateRun(run);else repo.insertRun(run,o);sync.register(id,p);
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization(){@Override public void afterCommit(){sync.enqueue(id,p);}});return run;
    }
    private static boolean allDay(Map<String,Object> input){return "ALL_DAY".equals(input.getOrDefault("matchMode",input.getOrDefault("mode","LATEST")));}
    private void bindInputs(StudioObject o,List<Map<String,Object>> inputs,Map<String,Object> parameters,Map<String,Object> lineage){
        var selected=new HashSet<String>();var modes=new HashMap<String,Boolean>();
        var available=inputs.isEmpty()?Map.<String,Map<String,Object>>of():repo.runsByIds(inputs.stream().map(i->i.get("runId").toString()).distinct().toList());
        for(var input:inputs){String alias=Objects.toString(input.get("alias"),"");boolean barrier=allDay(input);if(modes.putIfAbsent(alias,barrier)!=null&&modes.get(alias)!=barrier)throw StudioException.bad("INVALID_DEPENDENCY","同一依赖别名不能混用期次匹配方式");
            if(!barrier&&!selected.add(alias))throw StudioException.bad("AMBIGUOUS_DEPENDENCY_PARAMETER","上游别名「"+alias+"」需要唯一运行批次");
            var upstream=available.get(input.get("runId").toString());if(upstream==null)throw StudioException.bad("UPSTREAM_REQUIRED","上游运行已不存在");
            if(!o.workspaceId().equals(upstream.get("workspaceId"))||!Objects.equals(parameters.get("bizdate"),upstream.get("businessDate"))||!Objects.equals(input.get("taskId"),upstream.get("objectId"))||!"SUCCESS".equals(upstream.get("status"))||Boolean.TRUE.equals(upstream.get("simulation")))throw StudioException.bad("INVALID_UPSTREAM","上游运行须为同一任务、业务日期和空间的真实成功运行");
            if(upstream.get("buildId")!=null&&!"PUBLISHED".equals(upstream.get("publicationStatus")))throw StudioException.bad("UPSTREAM_NOT_PUBLISHED","上游尚未提交正式结果");
            if(barrier)continue;
            if(upstream.get("lineage") instanceof Map<?,?> map)map.forEach((k,v)->{if(lineage.containsKey(k.toString())&&!Objects.equals(lineage.get(k.toString()),v))throw StudioException.bad("INCONSISTENT_LINEAGE","多个上游使用了不同的库存批次，请重新计算上游");lineage.put(k.toString(),v);});
            if(upstream.get("buildId")!=null){parameters.put("upstream_"+alias+"_build_id",upstream.get("buildId"));if(upstream.get("targetTable")!=null)lineage.put(upstream.get("targetTable").toString(),upstream.get("buildId"));}
        }
    }
    private static void validateDependencyParameters(List<Map<String,Object>> inputs,List<String> names,Map<String,Object> parameters){
        for(var input:inputs)if(allDay(input)&&names.contains("upstream_"+input.get("alias")+"_build_id"))throw StudioException.bad("AMBIGUOUS_DEPENDENCY_PARAMETER","ALL_DAY 依赖「"+input.get("alias")+"」等待全天完成，不能读取单个运行批次");
        for(String name:names)if(!name.startsWith("$")&&!name.startsWith("\u0001")&&!parameters.containsKey(name))throw StudioException.bad("MISSING_DEPENDENCY_PARAMETER","请配置 SQL 参数对应的上游依赖："+name);
    }
    public static void verifyBindings(List<Map<String,Object>> before,List<Map<String,Object>> now) {
        if(before==null)throw StudioException.bad("INVALID_RELEASE","发布包缺少连接绑定");
        for(var current:now){var bound=before.stream().filter(b->Objects.equals(b.get("id"),current.get("id"))).findFirst().orElseThrow(()->StudioException.bad("INVALID_RELEASE","发布包缺少连接绑定"));for(String k:List.of("workspaceId","type","host","port","database","username","options")){Object a=bound.get(k),b=current.get(k);if(!(a instanceof Number x&&b instanceof Number y?x.doubleValue()==y.doubleValue():Objects.equals(a,b)))throw StudioException.conflict("DATASOURCE_BINDING_CHANGED","发布时的数据源连接已变化，请发布新版本");}}
    }
    public Map<String,Object> startWorkflowNode(StudioObject o,Map<String,Object> child,List<Map<String,Object>> upstream){return startWorkflowNode(o,child,upstream,null);}
    public Map<String,Object> startWorkflowNode(StudioObject o,Map<String,Object> child,List<Map<String,Object>> upstream,SyncExecutionService.Prepared syncPlan){
        return startWorkflowNode(o,child,upstream,null,null,syncPlan,dependencies(tasks.forTask(o.id()).orElse(Map.of())));
    }
    public Map<String,Object> startWorkflowNode(StudioObject o,Map<String,Object> child,List<Map<String,Object>> upstream,MysqlExecutionProvider.PreparedQuery query,InventoryExecutionService.PreparedStage stage,SyncExecutionService.Prepared syncPlan,List<Map<String,Object>> configured){
        var options=new LinkedHashMap<String,Object>();for(String key:List.of("businessDate","sourceCutoffAt","parentRunId","graphNodeId","triggerType","scheduledAt","timezone","scheduleParameters","attempt","releaseId","releaseNo","executionSource"))if(child.containsKey(key))options.put(key,child.get(key));options.put("existingRunId",child.get("id"));if(syncPlan!=null)options.put("_syncPrepared",syncPlan);
        List<Map<String,Object>> inputs=new ArrayList<>();
        for(var run:upstream){String alias=configured.stream().filter(d->Objects.equals(d.get("taskId"),run.get("objectId"))).map(d->d.get("alias").toString()).findFirst().orElseGet(()->run.get("targetTable")!=null?run.get("targetTable").toString().split("_")[0]:graphAlias(run));inputs.add(input(Map.of("taskId",run.get("objectId"),"name",run.get("objectName"),"alias",alias),run));}
        var counts=new HashMap<String,Integer>();inputs.forEach(i->counts.merge(i.get("alias").toString(),1,Integer::sum));var names=SqlParameters.compile(SyncExecutionService.parameterCode(o)).names();
        for(int i=0;i<inputs.size();i++){var input=inputs.get(i);String alias=input.get("alias").toString();if(counts.get(alias)>1&&!names.contains("upstream_"+alias+"_build_id"))input.put("alias",graphAlias(upstream.get(i)));}
        options.put("upstreamRuns",inputs);return start(o,null,options,query,stage,syncPlan);
    }
    private static String graphAlias(Map<String,Object> run){String identity=Objects.toString(run.get("graphNodeId"),Objects.toString(run.get("objectId"),"upstream"));return "node_"+UUID.nameUUIDFromBytes(identity.getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString().replace("-","").substring(0,16);}
    public Map<String,Object> rerun(String id){var run=repo.run(id).orElseThrow(()->StudioException.missing("运行不存在"));if(!"TASK".equals(run.get("releaseKind")))throw StudioException.bad("TASK_RELEASE_REQUIRED","请选择任务发布版本运行");var options=new LinkedHashMap<String,Object>();for(String k:List.of("businessDate","sourceCutoffAt","upstreamRuns","scheduledAt","timezone","scheduleParameters","dependencySlots","scheduleSnapshot"))if(run.containsKey(k))options.put(k,run.get(k));options.put("triggerType","RERUN");options.put("retryOfRunId",id);return startRelease(run.get("releaseId").toString(),options);}
    public Map<String,Object> stop(String id){var run=repo.run(id).orElseThrow();if("SYNC".equals(run.get("provider")))return sync.stop(id);if(!Boolean.TRUE.equals(run.get("materialization")))return mysql.stop(id);inventory.cancelTask(id);return repo.run(id).orElseThrow();}
    public boolean pauseQueuedRun(String id){return withdrawQueued(id,"SCHEDULE_PAUSED");}
    public boolean withdrawQueued(String id,String reason){var run=repo.run(id).orElseThrow();if(!"QUEUED".equals(run.get("status"))||("SCHEDULE_PAUSED".equals(reason)?!"SCHEDULED".equals(run.get("triggerType")):run.get("triggerId")==null))return false;if("SYNC".equals(run.get("provider")))return sync.withdrawQueued(id,reason);if(Boolean.TRUE.equals(run.get("materialization")))return inventory.withdrawQueued(id,reason);return mysql.withdrawQueued(id,reason);}
}
