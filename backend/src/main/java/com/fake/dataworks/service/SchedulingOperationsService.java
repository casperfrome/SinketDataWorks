package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.time.*;
import java.util.*;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/** Operations and backfill use the same persisted instances and dispatchers as periodic scheduling. */
@Service
public class SchedulingOperationsService {
    private final SchedulingRepository store;
    private final TaskRepository taskStore;
    private final StudioRepository repo;
    private final TaskService tasks;
    private final TaskScheduleService taskSchedules;
    private final ScheduleService workflows;
    private final ObjectService objects;
    private final JsonCodec json;
    private final TransactionTemplate tx;
    private final byte[] previewSecret=new byte[32];
    public SchedulingOperationsService(SchedulingRepository store,TaskRepository taskStore,StudioRepository repo,TaskService tasks,TaskScheduleService taskSchedules,ScheduleService workflows,ObjectService objects,JsonCodec json,TransactionTemplate tx) {
        this.store=store;this.taskStore=taskStore;this.repo=repo;this.tasks=tasks;this.taskSchedules=taskSchedules;this.workflows=workflows;this.objects=objects;this.json=json;this.tx=tx;new SecureRandom().nextBytes(previewSecret);
    }
    private String kind(String value) { if(!Set.of("TASK","WORKFLOW").contains(value))throw StudioException.bad("INVALID_SCHEDULING_KIND","类型须为 TASK 或 WORKFLOW");return value; }
    private static String string(Map<?,?> map,String key) { return Objects.toString(map.get(key),""); }
    private static int integer(String value,int fallback) { try { return value.isBlank()?fallback:Integer.parseInt(value); }catch(Exception e){throw StudioException.bad("INVALID_PAGE","分页参数无效");} }
    public Map<String,Object> tasks(Map<String,String> query) {
        String workspace=query.getOrDefault("workspaceId","");objects.workspace(workspace);
        var plans=new HashMap<String,Map<String,Object>>();taskStore.schedules(workspace).forEach(p->plans.put(p.get("taskId").toString(),p));workflows.list(workspace,"").forEach(p->plans.put(p.get("workflowId").toString(),p));
        var latestInstances=store.latestByObject(workspace);
        List<Map<String,Object>> result=new ArrayList<>();
        for(var o:repo.list(workspace,false)) {
            String type;
            if("WORKFLOW".equals(o.kind())&&WorkflowService.isWorkflow(o))type="WORKFLOW";
            else if("NODE".equals(o.kind())&&(RunService.isMysql(o)||RunService.isDoris(o)||SyncExecutionService.isSync(o)))type="TASK";
            else continue;
            var row=new LinkedHashMap<String,Object>();var plan=plans.get(o.id());if(plan!=null)row.putAll(plan);
            row.put("kind",type);row.put("objectId",o.id());row.put("name",o.name());row.put("nodeType",o.nodeType());row.put("scheduleId",plan==null?null:plan.get("id"));row.put("enabled",plan!=null&&Boolean.TRUE.equals(plan.get("enabled")));
            row.put("status",plan==null?"UNCONFIGURED":Boolean.TRUE.equals(plan.get("enabled"))?"ENABLED":plan.get("pauseReason")!=null?"ENDED":"PAUSED");
            var latest=latestInstances.get(o.id());if(latest!=null){row.put("latestStatus",latest.get("status"));row.put("latestReason",latest.get("reason"));}
            var releases=type.equals("TASK")?taskStore.releases(o.id()):repo.releases(workspace,o.id());
            if(!releases.isEmpty()){row.put("latestReleaseId",releases.getFirst().get("id"));row.put("latestReleaseNo",releases.getFirst().get("releaseNo"));}
            if(plan!=null&&!string(plan,"releaseId").isBlank()) {
                var release=type.equals("TASK")?tasks.release(plan.get("releaseId").toString()):repo.release(plan.get("releaseId").toString()).orElse(Map.of());row.put("retrySupported",!Boolean.TRUE.equals(release.get("containsWrites")));
            } else row.put("retrySupported",false);
            if(!query.getOrDefault("kind","").isBlank()&&!type.equals(query.get("kind")))continue;
            if(!query.getOrDefault("status","").isBlank()&&!row.get("status").equals(query.get("status")))continue;
            String search=query.getOrDefault("search","").toLowerCase(Locale.ROOT);if(!search.isBlank()&&!o.name().toLowerCase(Locale.ROOT).contains(search)&&!o.id().contains(search))continue;
            result.add(row);
        }
        result.sort(Comparator.comparing(r->r.get("name").toString()));int page=integer(query.getOrDefault("page",""),1),size=integer(query.getOrDefault("pageSize",""),20);pageCheck(page,size);
        return Map.of("items",result.subList(Math.min(result.size(),(page-1)*size),Math.min(result.size(),page*size)),"total",result.size(),"page",page,"pageSize",size);
    }
    private void pageCheck(int page,int size) { if(page<1||page>100000||size<1||size>100)throw StudioException.bad("INVALID_PAGE","分页参数无效"); }
    @SuppressWarnings("unchecked") public Map<String,Object> instances(Map<String,String> query) {
        objects.workspace(query.getOrDefault("workspaceId",""));if(!query.getOrDefault("kind","").isBlank())kind(query.get("kind"));
        for(String key:List.of("businessDate","businessDateFrom","businessDateTo"))if(!query.getOrDefault(key,"").isBlank())date(query.get(key));
        var page=new LinkedHashMap<>(store.instances(query,integer(query.getOrDefault("page",""),1),integer(query.getOrDefault("pageSize",""),20)));
        page.put("items",enrich((List<Map<String,Object>>)page.get("items")));return page;
    }
    public Map<String,Object> instance(String type,String id) { return enrich(List.of(store.instance(type,id))).getFirst(); }
    @SuppressWarnings("unchecked") private List<Map<String,Object>> enrich(List<Map<String,Object>> instances) {
        Set<String> ids=new HashSet<>();for(var t:instances) {if(t.get("runId")!=null)ids.add(t.get("runId").toString());for(var a:(List<Map<String,Object>>)t.getOrDefault("attempts",List.of()))if(a.get("runId")!=null)ids.add(a.get("runId").toString());}
        var runs=store.runs(ids);
        for(var t:instances){List<Map<String,Object>> history=new ArrayList<>();for(var a:(List<Map<String,Object>>)t.getOrDefault("attempts",List.of())) {var attempt=new LinkedHashMap<>(a);var run=runs.get(string(a,"runId"));if(run!=null){for(String key:List.of("status","errorCode","releaseId","releaseNo","sourceCutoffAt","scheduledAt","scheduleParameters","upstreamRuns"))if(run.containsKey(key))attempt.put(key,run.get(key));if(run.get("errorCode")!=null)attempt.put("reason",run.get("errorCode"));}history.add(attempt);}t.put("attempts",history);t.put("attemptCount",history.size());}
        return instances;
    }
    public Map<String,Object> state(String type,String id,Map<String,Object> input) {
        if(!(input.get("enabled") instanceof Boolean enabled)||!(input.get("expectedVersion") instanceof Number version)||version.doubleValue()!=version.intValue())throw StudioException.bad("INVALID_STATE","请携带启用状态及整数 expectedVersion");
        return kind(type).equals("TASK")?taskSchedules.setState(id,enabled,version.intValue()):workflows.setState(id,enabled,version.intValue());
    }
    public Map<String,Object> rerun(String type,String id,Map<String,Object> input) {
        var t=store.instance(type,id);String mode=Objects.toString(input.get("mode"),"LATEST");if(!Set.of("LATEST","ORIGINAL").contains(mode))throw StudioException.bad("INVALID_RERUN_MODE","重跑模式须为 LATEST 或 ORIGINAL");
        if(t.get("kind").equals("TASK"))taskSchedules.rerun(id,mode,string(input,"attemptRunId"));else workflows.rerun(id,mode,string(input,"attemptRunId"));return instance(type,id);
    }
    public Map<String,Object> stop(String type,String id) {var t=store.instance(type,id);if(t.get("kind").equals("TASK"))taskSchedules.stop(id);else workflows.stop(id);return instance(type,id);}

    private LocalDate date(String value) {try{return LocalDate.parse(value);}catch(Exception e){throw StudioException.bad("INVALID_DATE_RANGE","业务日期格式须为 yyyy-MM-dd");}}
    private Map<String,Object> request(Map<String,Object> input) {
        String workspace=string(input,"workspaceId"),type=kind(string(input,"kind")),id=string(input,"scheduleId");objects.workspace(workspace);
        var schedule=type.equals("TASK")?taskStore.schedule(id):workflows.get(id);if(!workspace.equals(schedule.get("workspaceId")))throw StudioException.bad("WORKSPACE_MISMATCH","计划不属于当前空间");
        LocalDate start=date(string(input,"startDate")),end=date(string(input,"endDate"));if(end.isBefore(start)||end.isAfter(start.plusDays(365)))throw StudioException.bad("INVALID_DATE_RANGE","补数业务日期范围须为 1–366 天");
        String from=string(input,"startTime"),to=string(input,"endTime");try {if(!from.isBlank())LocalTime.parse(from);if(!to.isBlank())LocalTime.parse(to);if(!from.isBlank()&&!to.isBlank()&&LocalTime.parse(to).isBefore(LocalTime.parse(from)))throw new IllegalArgumentException();}catch(Exception e){throw StudioException.bad("INVALID_TIME_RANGE","时点范围须为当日 hh:mm，结束不能早于开始");}
        return Map.of("workspaceId",workspace,"kind",type,"scheduleId",id,"startDate",start.toString(),"endDate",end.toString(),"startTime",from,"endTime",to,"includeDownstream",Boolean.TRUE.equals(input.get("includeDownstream")));
    }
    private record Expansion(List<Map<String,Object>> instances,String fingerprint) {}
    private List<Instant> periods(Map<String,Object> schedule,LocalDate businessDate) {
        ZoneId zone=ZoneId.of(schedule.get("timezone").toString());LocalDate local=businessDate.minusDays(((Number)schedule.get("businessDateOffset")).intValue());Instant end=local.plusDays(1).atStartOfDay(zone).toInstant(),cursor=local.atStartOfDay(zone).toInstant().minusNanos(1);List<Instant> result=new ArrayList<>();
        while(true){Instant next=workflows.next(schedule,cursor);if(next==null||!next.isBefore(end))break;result.add(next);cursor=next;}return result;
    }
    private String slotKey(String schedule,Object at) { return schedule+"|"+at; }
    @SuppressWarnings("unchecked") private Expansion expand(Map<String,Object> request,Instant cutoff,String batchKey) {
        String type=request.get("kind").toString(),workspace=request.get("workspaceId").toString();var root=type.equals("TASK")?taskStore.schedule(request.get("scheduleId").toString()):workflows.get(request.get("scheduleId").toString());
        objects.active(root.get(type.equals("TASK")?"taskId":"workflowId").toString());
        var activeIds=new HashSet<>(repo.list(workspace,false).stream().map(StudioObject::id).toList());
        List<Map<String,Object>> plans=new ArrayList<>(type.equals("TASK")?taskStore.schedules(workspace).stream().filter(p->activeIds.contains(p.get("taskId").toString())).toList():List.of(root));var selected=new LinkedHashMap<String,Map<String,Object>>();LocalDate start=date(request.get("startDate").toString()),end=date(request.get("endDate").toString());
        LocalTime from=string(request,"startTime").isBlank()?LocalTime.MIN:LocalTime.parse(string(request,"startTime")),to=string(request,"endTime").isBlank()?LocalTime.MAX:LocalTime.parse(string(request,"endTime"));
        for(LocalDate day=start;!day.isAfter(end);day=day.plusDays(1))for(Instant at:periods(root,day)) {
            LocalTime time=at.atZone(ZoneId.of(root.get("timezone").toString())).toLocalTime();if(time.isBefore(from)||time.isAfter(to))continue;
            var t=create(type,root,at,cutoff,batchKey);selected.put(slotKey(root.get("id").toString(),at),t);limit(selected.size());
        }
        if(selected.isEmpty())throw StudioException.bad("NO_BACKFILL_PERIOD","所选范围内没有计划期次");
        if(type.equals("TASK")&&Boolean.TRUE.equals(request.get("includeDownstream"))) {
            // Only enumerate schedules reachable from the selected root, then select affected periods.
            Set<String> reachable=new HashSet<>();reachable.add(root.get("taskId").toString());boolean grew;
            do {grew=false;for(var plan:plans)if(!reachable.contains(plan.get("taskId").toString())&&TaskService.dependencies(plan).stream().anyMatch(d->reachable.contains(string(d,"taskId")))){reachable.add(plan.get("taskId").toString());grew=true;}}while(grew);
            Set<String> visited=new HashSet<>();visited.add(root.get("taskId").toString());
            var remaining=new ArrayList<>(plans.stream().filter(p->!p.get("id").equals(root.get("id"))&&reachable.contains(p.get("taskId").toString())).toList());
            while(!remaining.isEmpty()) {
                var ready=remaining.stream().filter(p->TaskService.dependencies(p).stream().allMatch(d->!reachable.contains(string(d,"taskId"))||visited.contains(string(d,"taskId")))).findFirst().orElseThrow(()->StudioException.bad("DEPENDENCY_CYCLE","当前依赖存在环路"));
                for(LocalDate day=start;!day.isAfter(end);day=day.plusDays(1))for(var at:periods(ready,day)) {
                    var dependencies=taskSchedules.slotsFor(ready,at,day.toString());
                    if(dependencies.stream().anyMatch(d->selected.containsKey(slotKey(string(d,"scheduleId"),d.get("scheduledAt"))))) {var t=create(type,ready,at,cutoff,batchKey);selected.put(slotKey(ready.get("id").toString(),at),t);limit(selected.size());}
                }
                visited.add(ready.get("taskId").toString());remaining.remove(ready);
            }
        }
        var canonical=new HashMap<String,Map<String,Object>>();var queried=new HashSet<String>();
        for(var t:selected.values()) {
            List<String> missing=new ArrayList<>();for(var dep:(List<Map<String,Object>>)t.getOrDefault("dependencySlots",List.of())) {
                String key=slotKey(string(dep,"scheduleId"),dep.get("scheduledAt"));boolean internal=selected.containsKey(key);dep.put("batchKey",internal?batchKey:"");dep.put("binding",internal?"BATCH":"SCHEDULED");
                if(internal){dep.put("triggerId",selected.get(key).get("id"));continue;}
                String window=string(dep,"scheduleId")+"|"+t.get("businessDate");if(!string(dep,"scheduleId").isBlank()&&queried.add(window))for(var upper:store.window("TASK",string(dep,"scheduleId"),t.get("businessDate").toString()))canonical.put(slotKey(string(dep,"scheduleId"),upper.get("scheduledAt")),upper);
                var upper=canonical.get(key);if(upper==null){dep.put("status","MISSING");missing.add(string(dep,"name")+" "+Objects.toString(dep.get("scheduledAt"),Objects.toString(dep.get("reason"),"无匹配期次")));}else{dep.put("triggerId",upper.get("id"));dep.put("status",upper.get("status"));if(!"SUCCESS".equals(upper.get("status")))missing.add(string(dep,"name")+" "+dep.get("scheduledAt")+" "+upper.get("status"));}
            }t.put("missingUpstreams",missing);previewParameters(type,t);
        }
        var instances=new ArrayList<>(selected.values());instances.sort(Comparator.comparing((Map<String,Object> t)->t.get("businessDate").toString()).thenComparing(t->t.get("scheduledAt").toString()).thenComparing(t->t.get("kind").toString()).thenComparing(t->t.get("scheduleId").toString()).thenComparing(t->t.get("id").toString()));
        // Comparing all relevant plan versions also detects changed dependency mappings between preview and submit.
        plans.sort(Comparator.comparing(p->p.get("id").toString()));
        var versions=plans.stream().map(p->{var definition=new LinkedHashMap<String,Object>();for(String key:List.of("id","version","releaseId","cron","timezone","businessDateOffset","startDate","endDate","dependencies","retries","retryIntervalSeconds"))if(p.containsKey(key))definition.put(key,p.get(key));return definition;}).toList();
        return new Expansion(instances,digest(json.write(canonicalize(versions))));
    }
    private void limit(int count) {if(count>5000)throw StudioException.bad("BACKFILL_LIMIT","单次补数最多生成 5,000 个实例，请缩小日期或时点范围");}
    private Map<String,Object> create(String type,Map<String,Object> plan,Instant at,Instant cutoff,String batchKey) {
        if(string(plan,"releaseId").isBlank())throw StudioException.bad("TASK_RELEASE_REQUIRED","补数前请应用已发布版本");return type.equals("TASK")?taskSchedules.createInstance(plan,at,cutoff,batchKey):workflows.createInstance(plan,at,cutoff,batchKey);
    }
    @SuppressWarnings("unchecked") private void previewParameters(String type,Map<String,Object> t) {
        StudioObject snapshot;if(type.equals("TASK"))snapshot=json.read(json.write(tasks.release(t.get("releaseId").toString()).get("snapshot")),StudioObject.class);else {var release=repo.release(t.get("releaseId").toString()).orElseThrow();snapshot=json.read(json.write(((Map<String,Object>)release.get("bundle")).get("workflow")),StudioObject.class);}
        var values=new LinkedHashMap<String,Object>();ScheduleParameters.attach(values,snapshot,t,List.of());t.put("parameters",values.get("scheduleParameters"));t.put("scheduleParameters",values.get("scheduleParameters"));
    }
    public Map<String,Object> preview(Map<String,Object> input) {
        var request=request(input);return tx.execute(x->{repo.lockWorkspace(request.get("workspaceId").toString());Instant cutoff=Instant.now();var expansion=expand(request,cutoff,"PREVIEW");var payload=Map.of("requestHash",digest(json.write(canonicalize(request))),"planHash",expansion.fingerprint(),"cutoff",cutoff.toString());String encoded=Base64.getUrlEncoder().withoutPadding().encodeToString(json.write(payload).getBytes(StandardCharsets.UTF_8));
            long missing=expansion.instances().stream().filter(t->!((List<?>)t.get("missingUpstreams")).isEmpty()).count();
            return Map.of("token",encoded+"."+sign(encoded),"items",expansion.instances(),"total",expansion.instances().size(),"warnings",missing==0?List.of():List.of(missing+" 个实例需要等待或修复周期上游；同批依赖由本批实例提供"));
        });
    }
    public synchronized Map<String,Object> backfill(Map<String,Object> input) {
        String key=string(input,"requestId");try{if(!UUID.fromString(key).toString().equalsIgnoreCase(key))throw new IllegalArgumentException();}catch(Exception e){throw StudioException.bad("INVALID_REQUEST_ID","补数提交须携带 UUID requestId，同一次操作重试应复用它");}
        var request=request(input);String requestHash=digest(json.write(canonicalize(request)));
        return tx.execute(x->{repo.lockWorkspace(request.get("workspaceId").toString());var existing=store.batch(key);if(!existing.isEmpty()){if(!request.get("workspaceId").equals(existing.getFirst().get("workspaceId"))||!requestHash.equals(existing.getFirst().get("backfillRequestHash")))throw StudioException.conflict("IDEMPOTENCY_CONFLICT","此 requestId 已用于其他补数请求");return batchResult(key,existing);}
            var token=verifyToken(string(input,"previewToken"));if(!requestHash.equals(token.get("requestHash")))throw StudioException.conflict("PREVIEW_CHANGED","补数范围已变化，请重新预览");Instant cutoff=Instant.parse(token.get("cutoff").toString());if(cutoff.isBefore(Instant.now().minus(Duration.ofHours(24))))throw StudioException.conflict("PREVIEW_EXPIRED","预览已过期，请重新预览");
            var expansion=expand(request,cutoff,key);if(!expansion.fingerprint().equals(token.get("planHash")))throw StudioException.conflict("PREVIEW_CHANGED","计划或发布版本已变化，请重新预览");
            for(var t:expansion.instances()){t.put("backfillRequestHash",requestHash);t.put("backfillRequest",request);store.insert(t.get("kind").toString(),t);}return batchResult(key,expansion.instances());
        });
    }
    public Map<String,Object> batch(String key) {var instances=store.batch(key);if(instances.isEmpty())throw StudioException.missing("补数批次不存在");return batchResult(key,enrich(instances));}
    private Map<String,Object> batchResult(String key,List<Map<String,Object>> instances) {var stats=new LinkedHashMap<String,Long>();instances.forEach(t->stats.merge(t.get("status").toString(),1L,Long::sum));return Map.of("batchKey",key,"instances",instances,"total",instances.size(),"stats",stats);}
    private String digest(String input) {try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(input.getBytes(StandardCharsets.UTF_8)));}catch(Exception e){throw new IllegalStateException(e);}}
    private String sign(String input) {try{Mac mac=Mac.getInstance("HmacSHA256");mac.init(new SecretKeySpec(previewSecret,"HmacSHA256"));return Base64.getUrlEncoder().withoutPadding().encodeToString(mac.doFinal(input.getBytes(StandardCharsets.UTF_8)));}catch(Exception e){throw new IllegalStateException(e);}}
    private Map<String,Object> verifyToken(String token) {try{String[] parts=token.split("\\.");if(parts.length!=2||!MessageDigest.isEqual(sign(parts[0]).getBytes(StandardCharsets.UTF_8),parts[1].getBytes(StandardCharsets.UTF_8)))throw new IllegalArgumentException();return json.map(new String(Base64.getUrlDecoder().decode(parts[0]),StandardCharsets.UTF_8));}catch(Exception e){throw StudioException.conflict("INVALID_PREVIEW","预览无效或服务已重启，请重新预览");}}
    private Object canonicalize(Object value) {if(value instanceof Map<?,?> map){var sorted=new TreeMap<String,Object>();map.forEach((k,v)->sorted.put(k.toString(),canonicalize(v)));return sorted;}if(value instanceof List<?> list)return list.stream().map(this::canonicalize).toList();return value;}
}
