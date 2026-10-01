package com.fake.dataworks.service;

import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.*;
import java.time.*;
import java.util.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/** Persisted logical slots own immutable execution attempts and explicit upstream scopes. */
@Service
public class TaskScheduleService {
    private final TaskRepository store;private final StudioRepository repo;private final TaskService tasks;private final ScheduleService times;private final TransactionTemplate tx;private final boolean enabled;
    private final Map<String,Instant> recoveryUntil=new HashMap<>();
    private int generationRemaining=200;
    private static final Set<String> ACTIVE=Set.of("PENDING","WAITING_DEPENDENCY","WAITING_RESOURCE","RUNNING","RETRY_WAIT","PAUSED");
    private static final Set<String> DISPATCHABLE=Set.of("PENDING","WAITING_DEPENDENCY","WAITING_RESOURCE","BLOCKED");
    private static final Set<String> RETRYABLE=Set.of("DATASOURCE_UNAVAILABLE","DB_TRANSIENT","QUEUE_FULL");
    public TaskScheduleService(TaskRepository store,StudioRepository repo,TaskService tasks,ScheduleService times,TransactionTemplate tx,@Value("${studio.scheduler.enabled:true}") boolean enabled){this.store=store;this.repo=repo;this.tasks=tasks;this.times=times;this.tx=tx;this.enabled=enabled;}
    public void ready(){ready(Instant.now(),200);}
    public synchronized int ready(Instant now,int budget){if(!enabled)return 0;for(var t:store.pending(""))if("RUNNING".equals(t.get("status"))&&t.get("runId")!=null){var run=repo.run(t.get("runId").toString()).orElse(null);if(run!=null&&"QUEUED".equals(run.get("status"))&&tasks.withdrawQueued(t.get("runId").toString(),"SERVICE_RESTARTED_QUEUED")){t.put("attempt",((Number)t.get("attempt")).intValue()+1);t.put("retryBase",((Number)t.getOrDefault("retryBase",1)).intValue()+1);t.remove("runId");t.put("status","PENDING");store.updateTrigger(t);}}return resume(now,"",budget);}
    public List<Map<String,Object>> list(String workspace,String task){return store.schedules(workspace).stream().filter(s->task.isBlank()||task.equals(s.get("taskId"))).toList();}
    private List<Map<String,Object>> dependencies(String task,String workspace,Map<String,Object> input){
        List<Map<String,Object>> result=new ArrayList<>();Set<String> ids=new HashSet<>(),aliases=new HashSet<>();
        for(var d:TaskService.dependencies(input)){
            String id=Objects.toString(d.get("taskId"),""),alias=Objects.toString(d.get("alias"),"");var o=tasks.task(id);
            if(id.equals(task)||!ids.add(id))throw StudioException.bad("INVALID_DEPENDENCY","不能依赖自身或重复添加同一上游");
            if(!workspace.equals(o.workspaceId()))throw StudioException.bad("WORKSPACE_MISMATCH","上游须属于同一工作空间");
            if(!alias.matches("[A-Za-z][A-Za-z0-9_]{0,31}")||!aliases.add(alias))throw StudioException.bad("INVALID_DEPENDENCY_ALIAS","参数别名须以字母开头、最多 32 位且不能重复");
            String mode=Objects.toString(d.get("matchMode"),Objects.toString(d.get("mode"),"LATEST"));
            if(!Set.of("LATEST","ALL_DAY").contains(mode))throw StudioException.bad("INVALID_DEPENDENCY_MODE","请选择最近一期或全天依赖");
            if("ALL_DAY".equals(mode)){
                String[] cron=Objects.toString(input.get("cron"),"0 0 2 * * *").split("\\s+");
                if(cron.length!=6||!cron[1].matches("\\d+")||!cron[2].matches("\\d+")||!"*".equals(cron[3])||!"*".equals(cron[4])||!"*".equals(cron[5]))throw StudioException.bad("INVALID_ALL_DAY_DEPENDENCY","全天依赖仅支持每日执行一次的下游任务");
            }
            result.add(Map.of("taskId",id,"name",o.name(),"alias",alias,"matchMode",mode));
        }
        Map<String,List<String>> graph=new HashMap<>();for(var s:store.schedules(workspace))graph.put(s.get("taskId").toString(),TaskService.dependencies(s).stream().map(d->d.get("taskId").toString()).toList());graph.put(task,result.stream().map(d->d.get("taskId").toString()).toList());
        visit(task,graph,new HashSet<>(),new HashSet<>());return result;
    }
    private void visit(String id,Map<String,List<String>> graph,Set<String> visiting,Set<String> done){if(done.contains(id))return;if(!visiting.add(id))throw StudioException.bad("DEPENDENCY_CYCLE","上游依赖形成循环，请移除环路");for(String upstream:graph.getOrDefault(id,List.of()))visit(upstream,graph,visiting,done);visiting.remove(id);done.add(id);}
    public synchronized Map<String,Object> save(String taskId,String id,Map<String,Object> input){return tx.execute(t->{
        var object=tasks.task(taskId);repo.lockWorkspace(object.workspaceId());Map<String,Object> old=id==null?null:store.schedule(id);
        if(old!=null&&(!taskId.equals(old.get("taskId"))||!(input.get("expectedVersion") instanceof Number n)||n.doubleValue()!=n.intValue()||n.intValue()!=((Number)old.get("version")).intValue()))throw StudioException.conflict("VERSION_CONFLICT","任务调度已变化，请重新加载");
        if(old==null&&store.forTask(taskId).isPresent())throw StudioException.conflict("SCHEDULE_EXISTS","此任务已有调度配置，请重新加载");
        Map<String,Object> s=times.timeConfig(input);String releaseId=Objects.toString(input.get("releaseId"),"");
        s.put("releaseId",releaseId);if(!releaseId.isBlank()){var release=tasks.release(releaseId);if(!taskId.equals(release.get("taskId")))throw StudioException.bad("INVALID_RELEASE","请选择本任务的发布版本");s.put("releaseNo",release.get("releaseNo"));}
        if(Boolean.TRUE.equals(s.get("enabled"))&&releaseId.isBlank())throw StudioException.bad("TASK_RELEASE_REQUIRED","启用前请先发布任务并选择执行版本");
        s.put("dependencies",dependencies(taskId,object.workspaceId(),input));s.put("id",old==null?UUID.randomUUID().toString():id);s.put("taskId",taskId);s.put("workspaceId",object.workspaceId());s.put("name",object.name());s.put("version",old==null?1:((Number)old.get("version")).intValue()+1);s.put("updatedAt",ObjectService.now());
        Instant now=Instant.now(),next=times.next(s,now);if(Boolean.TRUE.equals(s.get("enabled"))&&next==null)throw StudioException.bad("NO_FUTURE_FIRE","有效期内没有未来执行时间");
        if(old!=null&&Boolean.TRUE.equals(old.get("enabled"))){generationRemaining=200;createDue(old,now,false);var advanced=store.schedule(id);if(advanced.get("nextFireAt")!=null&&!Instant.parse(advanced.get("nextFireAt").toString()).isAfter(now))throw StudioException.conflict("SCHEDULE_BACKLOG","正在逐期记录历史实例，请稍后保存配置");}
        s.put("nextFireAt",Boolean.TRUE.equals(s.get("enabled"))?next.toString():null);store.saveSchedule(s,old==null);if(old!=null)applyPause(s);return s;
    });}
    public synchronized Map<String,Object> setState(String id,boolean state,int expectedVersion){var current=store.schedule(id);var input=new LinkedHashMap<>(current);input.put("enabled",state);input.put("expectedVersion",expectedVersion);return save(current.get("taskId").toString(),id,input);}
    public String businessDate(Map<String,Object> s,Instant fire){return fire.atZone(ZoneId.of(s.get("timezone").toString())).toLocalDate().plusDays(((Number)s.get("businessDateOffset")).intValue()).toString();}
    /** Shared, pure enumeration; includes future slots and both occurrences in a DST overlap. */
    public static List<Instant> requiredDaySlots(Map<String,Object> upstream,String date){
        ZoneId zone=ZoneId.of(Objects.toString(upstream.get("timezone"),"Asia/Shanghai"));int offset=((Number)upstream.getOrDefault("businessDateOffset",-1)).intValue();LocalDate local=LocalDate.parse(date).minusDays(offset);
        String start=Objects.toString(upstream.get("startDate"),""),end=Objects.toString(upstream.get("endDate"),"");if(!start.isEmpty()&&local.isBefore(LocalDate.parse(start))||!end.isEmpty()&&local.isAfter(LocalDate.parse(end)))return List.of();
        Instant until=local.plusDays(1).atStartOfDay(zone).toInstant();var cursor=local.atStartOfDay(zone).minusNanos(1);var cron=CronExpression.parse(upstream.get("cron").toString());List<Instant> result=new ArrayList<>();
        for(int i=0;i<1600;i++){var next=cron.next(cursor);if(next==null||!next.toInstant().isBefore(until))break;result.add(next.toInstant());cursor=next;}return result;
    }
    public List<Instant> daySlots(Map<String,Object> upstream,String date){return requiredDaySlots(upstream,date);}
    public Instant matchingSlot(Map<String,Object> upstream,String date,Instant downstream){return requiredDaySlots(upstream,date).stream().filter(s->!s.isAfter(downstream)).reduce((a,b)->b).orElse(null);}
    public List<Map<String,Object>> slotsFor(Map<String,Object> schedule,Instant at,String date){List<Map<String,Object>> result=new ArrayList<>();
        for(var d:TaskService.dependencies(schedule)){
            var upstream=store.forTask(d.get("taskId").toString()).orElse(null);String mode=Objects.toString(d.get("matchMode"),Objects.toString(d.get("mode"),"LATEST"));var base=new LinkedHashMap<>(d);base.put("matchMode",mode);base.put("batchKey","");
            if(upstream==null){base.put("reason","UPSTREAM_NO_PLAN");result.add(base);continue;}base.put("scheduleId",upstream.get("id"));base.put("scheduleVersion",upstream.get("version"));base.put("cron",upstream.get("cron"));base.put("timezone",upstream.get("timezone"));base.put("businessDateOffset",upstream.get("businessDateOffset"));if(!Boolean.TRUE.equals(upstream.get("enabled")))base.put("warning","UPSTREAM_PAUSED");
            List<Instant> required="ALL_DAY".equals(mode)?requiredDaySlots(upstream,date):Optional.ofNullable(matchingSlot(upstream,date,at)).map(List::of).orElse(List.of());
            if(required.isEmpty()){base.put("reason","NO_MATCHING_UPSTREAM");result.add(base);continue;}for(Instant fire:required){var match=new LinkedHashMap<>(base);match.put("scheduledAt",fire.toString());match.put("expectedCount",required.size());result.add(match);}
        }return result;
    }
    public List<Map<String,Object>> preview(String taskId,Map<String,Object> input){var o=tasks.task(taskId);var config=times.timeConfig(input);config.put("dependencies",dependencies(taskId,o.workspaceId(),input));List<Map<String,Object>> result=new ArrayList<>();for(var item:times.preview(config)){var r=new LinkedHashMap<>(item);r.put("dependencies",slotsFor(config,Instant.parse(item.get("scheduledAt").toString()),item.get("businessDate").toString()));result.add(r);}return result;}
    public Map<String,Object> createInstance(Map<String,Object> s,Instant fire,Instant cutoff,String batchKey){
        var t=new LinkedHashMap<String,Object>();t.put("id",UUID.randomUUID().toString());for(String k:List.of("workspaceId","taskId","name","releaseId","releaseNo"))if(s.containsKey(k))t.put(k,s.get(k));t.put("kind","TASK");t.put("scheduleId",s.get("id"));t.put("scheduledAt",fire.toString());String date=businessDate(s,fire);t.put("businessDate",date);t.put("timezone",s.get("timezone"));t.put("sourceCutoffAt",cutoff.toString());t.put("scheduleSnapshot",new LinkedHashMap<>(s));t.put("dependencySlots",slotsFor(s,fire,date));t.put("attempt",1);t.put("attempts",List.of());t.put("createdAt",cutoff.toString());t.put("status","PENDING");t.put("batchKey",Objects.toString(batchKey,""));t.put("triggerType",batchKey==null||batchKey.isEmpty()?"SCHEDULED":"BACKFILL");t.put("originalReleaseId",s.get("releaseId"));t.put("scheduleParameters",parameters(s.get("releaseId").toString(),t));t.put("originalScheduleParameters",t.get("scheduleParameters"));var original=new LinkedHashMap<String,Object>();for(String key:List.of("releaseId","releaseNo","businessDate","sourceCutoffAt","scheduledAt","timezone","scheduleParameters","scheduleSnapshot","dependencySlots"))original.put(key,t.get(key));t.put("originalContext",original);return t;
    }
    @SuppressWarnings("unchecked") private Map<String,String> parameters(String releaseId,Map<String,Object> context){var release=tasks.release(releaseId);Object snapshot=release.get("snapshot");Map<String,Object> config=snapshot instanceof StudioObject o?ScheduleParameters.schedule(o):snapshot instanceof Map<?,?> m&&m.get("config") instanceof Map<?,?> c&&c.get("schedule") instanceof Map<?,?> s?(Map<String,Object>)s:Map.of();return ScheduleParameters.resolve(ScheduleParameters.rows(config.get("parameters")),ScheduleParameters.context(config,context));}
    private List<Map<String,Object>> plans(String workspace){return workspace.isBlank()?store.allSchedules():store.schedules(workspace);}
    public void resume(Instant now,String workspace){resume(now,workspace,200);}
    public synchronized int resume(Instant now,String workspace,int budget){generationRemaining=Math.max(0,Math.min(200,budget));int allowed=generationRemaining;recoveryUntil.put(workspace,now);for(var p:plans(workspace))createDue(p,now,true);return allowed-generationRemaining;}
    public void scan(Instant now,String workspace){scan(now,workspace,200);}
    public synchronized int scan(Instant now,String workspace,int budget){
        generationRemaining=Math.max(0,Math.min(200,budget));int allowed=generationRemaining;for(var t:store.pending(workspace))if("RUNNING".equals(t.get("status")))refresh(t,now);for(var p:plans(workspace))createDue(p,now,false);Set<String> admitted=new HashSet<>();
        for(var t:store.pending(workspace)){
            if("RUNNING".equals(t.get("status"))){admitted.add(t.get("taskId").toString());continue;}if("PAUSED".equals(t.get("status")))continue;
            if(automatic(t)&&planPaused(store.schedule(t.get("scheduleId").toString()))){pause(t);continue;}
            if("RETRY_WAIT".equals(t.get("status"))){if(Instant.parse(t.get("nextRetryAt").toString()).isAfter(now))continue;t.put("attempt",((Number)t.get("attempt")).intValue()+1);t.put("status","PENDING");t.remove("runId");store.updateTrigger(t);}
            if(DISPATCHABLE.contains(t.get("status"))){String task=t.get("taskId").toString();if(admitted.contains(task)){t.put("status","WAITING_RESOURCE");t.put("reason","TASK_SERIAL");store.updateTrigger(t);continue;}dispatch(t,now);if("RUNNING".equals(t.get("status")))admitted.add(task);}
        }
        return allowed-generationRemaining;
    }
    private void createDue(Map<String,Object> supplied,Instant now,boolean resumed){tx.executeWithoutResult(x->{
        store.lockSchedule(supplied.get("id").toString());var s=store.schedule(supplied.get("id").toString());if(!Boolean.TRUE.equals(s.get("enabled"))||s.get("nextFireAt")==null)return;if(repo.find(s.get("taskId").toString()).filter(o->!o.deleted()).isEmpty()){s.put("enabled",false);s.put("nextFireAt",null);s.put("pauseReason","TASK_DELETED");store.saveSchedule(s,false);return;}
        Instant fire=Instant.parse(s.get("nextFireAt").toString()),cutoff=recoveryUntil.getOrDefault(s.get("workspaceId").toString(),recoveryUntil.get(""));
        while(fire!=null&&!fire.isAfter(now)&&generationRemaining>0){generationRemaining--;var t=createInstance(s,fire,now,"");if(resumed||(cutoff!=null&&!fire.isAfter(cutoff))){t.put("status","SKIPPED");t.put("reason","MISSED_INTERVAL");t.put("missedUntil",now.toString());t.put("finishedAt",now.toString());}store.insertTrigger(t);fire=times.next(s,fire);}
        s.put("nextFireAt",fire==null?null:fire.toString());if(fire==null){s.put("enabled",false);s.put("pauseReason","END_OF_SCHEDULE");}store.saveSchedule(s,false);
    });}
    private boolean automatic(Map<String,Object> t){return "SCHEDULED".equals(Objects.toString(t.get("triggerType"),Boolean.TRUE.equals(t.get("manualRerun"))?"RERUN":"SCHEDULED"));}
    private boolean planPaused(Map<String,Object> plan){return !Boolean.TRUE.equals(plan.get("enabled"))&&!"END_OF_SCHEDULE".equals(plan.get("pauseReason"));}
    private void pause(Map<String,Object> t){if("PAUSED".equals(t.get("status")))return;t.put("pausedStatus",t.get("status"));t.put("status","PAUSED");t.put("reason","SCHEDULE_PAUSED");store.updateTrigger(t);}
    private void applyPause(Map<String,Object> plan){for(var t:store.pendingForSchedule(plan.get("id").toString())){if(!automatic(t))continue;if(Boolean.TRUE.equals(plan.get("enabled"))){if("PAUSED".equals(t.get("status"))){t.put("status",t.getOrDefault("pausedStatus","PENDING"));t.remove("pausedStatus");t.remove("reason");store.updateTrigger(t);}continue;}if("RUNNING".equals(t.get("status"))){if(tasks.pauseQueuedRun(t.get("runId").toString())){t.put("attempt",((Number)t.get("attempt")).intValue()+1);t.put("retryBase",((Number)t.getOrDefault("retryBase",1)).intValue()+1);t.remove("runId");t.put("status","PENDING");pause(t);}}else pause(t);}}
    @SuppressWarnings("unchecked") private void dispatch(Map<String,Object> t,Instant now){
        try{
            List<Map<String,Object>> inputs=new ArrayList<>();boolean waiting=false;String blocked=null;
            if(t.get("upstreamRuns") instanceof List<?> fixed)inputs.addAll((List<Map<String,Object>>)fixed);else{
                var expected=(List<Map<String,Object>>)t.getOrDefault("dependencySlots",List.of());Map<String,Map<String,Map<String,Object>>> ranges=new HashMap<>();Map<String,Map<String,Object>> upstreamPlans=new HashMap<>();var successful=new ArrayList<Map<String,Object>>();
                for(var d:expected){if(d.get("scheduledAt")==null){if(blocked==null)blocked=Objects.toString(d.get("reason"),"NO_MATCHING_UPSTREAM");d.put("status","MISSING");continue;}String scope=Objects.toString(d.get("batchKey"),""),schedule=d.get("scheduleId").toString(),key=schedule+"|"+scope;
                    var available=ranges.computeIfAbsent(key,k->{var matching=expected.stream().filter(e->Objects.equals(e.get("scheduleId"),schedule)&&scope.equals(Objects.toString(e.get("batchKey"),""))&&e.get("scheduledAt")!=null).map(e->e.get("scheduledAt").toString()).sorted().toList();Map<String,Map<String,Object>> found=new HashMap<>();for(var item:store.slotRange(schedule,scope,matching.getFirst(),matching.getLast()))found.put(item.get("scheduledAt").toString(),item);return found;});var upstream=available.get(d.get("scheduledAt").toString());
                    if(upstream==null){if(Instant.parse(d.get("scheduledAt").toString()).isAfter(now)){waiting=true;d.put("status","WAITING_TIME");continue;}var plan=upstreamPlans.computeIfAbsent(schedule,store::schedule);if(scope.isEmpty()&&Boolean.TRUE.equals(plan.get("enabled"))&&plan.get("nextFireAt")!=null&&!Instant.parse(plan.get("nextFireAt").toString()).isAfter(Instant.parse(d.get("scheduledAt").toString()))){waiting=true;d.put("status","WAITING_GENERATION");continue;}if(blocked==null)blocked="UPSTREAM_INSTANCE_MISSING";d.put("status","MISSING");d.put("reason","UPSTREAM_INSTANCE_MISSING");d.remove("triggerId");d.remove("runId");continue;}
                    d.put("triggerId",upstream.get("id"));d.put("status",upstream.get("status"));d.remove("reason");if(upstream.get("runId")!=null)d.put("runId",upstream.get("runId"));if(ACTIVE.contains(upstream.get("status"))){waiting=true;continue;}if(!"SUCCESS".equals(upstream.get("status"))){String reason="UPSTREAM_"+upstream.get("status");d.put("reason",reason);if(blocked==null)blocked=reason;continue;}if(upstream.get("runId")==null){d.put("reason","UPSTREAM_RUN_MISSING");if(blocked==null)blocked="UPSTREAM_RUN_MISSING";continue;}successful.add(d);
                }
                if(blocked==null&&!waiting){var runs=repo.runsByIds(successful.stream().map(d->d.get("runId").toString()).toList());for(var d:successful){var run=runs.get(d.get("runId").toString());if(run==null){blocked="UPSTREAM_RUN_MISSING";break;}inputs.add(tasks.input(d,run));}}
            }
            if(blocked!=null){t.put("status","BLOCKED");t.put("reason",blocked);t.put("finishedAt",now.toString());store.updateTrigger(t);return;}if(waiting){t.put("status","WAITING_DEPENDENCY");t.put("reason","WAITING_UPSTREAM");t.remove("finishedAt");store.updateTrigger(t);return;}
            t.put("upstreamRuns",inputs);t.putIfAbsent("originalUpstreamRuns",inputs);var options=new LinkedHashMap<String,Object>();for(String k:List.of("businessDate","sourceCutoffAt","scheduleId","scheduledAt","timezone","attempt","upstreamRuns","scheduleParameters","batchKey","dependencySlots","scheduleSnapshot"))if(t.containsKey(k))options.put(k,t.get(k));options.put("triggerId",t.get("id"));options.put("triggerType",t.getOrDefault("triggerType",Boolean.TRUE.equals(t.get("manualRerun"))?"RERUN":"SCHEDULED"));
            tx.executeWithoutResult(x->{store.lockTrigger(t.get("id").toString());var latest=store.trigger(t.get("id").toString());if(!DISPATCHABLE.contains(latest.get("status")))return;var run=tasks.startRelease(t.get("releaseId").toString(),options);t.put("runId",run.get("id"));t.put("status","RUNNING");t.remove("reason");t.remove("finishedAt");var attempts=new ArrayList<Object>((List<?>)t.get("attempts"));var context=new LinkedHashMap<String,Object>(options);context.put("releaseId",t.get("releaseId"));context.put("releaseNo",t.get("releaseNo"));context.put("scheduleSnapshot",t.get("scheduleSnapshot"));context.put("dependencySlots",t.get("dependencySlots"));attempts.add(Map.of("attempt",t.get("attempt"),"runId",run.get("id"),"releaseId",t.get("releaseId"),"createdAt",now.toString(),"context",context));t.put("attempts",attempts);store.updateTrigger(t);});
        }catch(Exception e){String code=e instanceof StudioException s?s.code():"SUBMISSION_FAILED";if(Set.of("MATERIALIZATION_BUSY","RECOVERY_REQUIRED","TASK_OVERLAP","QUEUE_FULL").contains(code)){t.put("status","WAITING_RESOURCE");t.put("reason","TASK_OVERLAP".equals(code)?"TASK_SERIAL":code);store.updateTrigger(t);}else failure(t,code,now);}
    }
    private void refresh(Map<String,Object> t,Instant now){var r=repo.run(t.get("runId").toString()).orElse(null);if(r==null){failure(t,"RUN_MISSING",now);return;}String status=r.get("status").toString();if(Set.of("QUEUED","RUNNING","RECOVERING").contains(status))return;if("QUEUE_FULL".equals(r.get("errorCode"))){t.put("status","WAITING_RESOURCE");t.put("reason","QUEUE_FULL");t.put("attempt",((Number)t.get("attempt")).intValue()+1);t.put("retryBase",((Number)t.getOrDefault("retryBase",1)).intValue()+1);t.remove("runId");store.updateTrigger(t);return;}if(Set.of("SUCCESS","CANCELLED").contains(status)){t.put("status",status);t.remove("reason");t.put("finishedAt",now.toString());store.updateTrigger(t);}else failure(t,Objects.toString(r.get("errorCode"),status),now);}
    @SuppressWarnings("unchecked") private void failure(Map<String,Object> t,String code,Instant now){var config=(Map<String,Object>)t.get("scheduleSnapshot");int attempt=((Number)t.get("attempt")).intValue();t.put("reason",code);int first=((Number)t.getOrDefault("retryBase",1)).intValue();if(!Boolean.TRUE.equals(t.get("cancelRequested"))&&retryAllowed(t)&&RETRYABLE.contains(code)&&attempt-first<((Number)config.get("retries")).intValue()){t.put("status","RETRY_WAIT");t.put("nextRetryAt",now.plusSeconds(((Number)config.get("retryIntervalSeconds")).longValue()).toString());store.updateTrigger(t);if(automatic(t)&&planPaused(store.schedule(t.get("scheduleId").toString())))pause(t);}else{t.put("status","FAILED");t.put("finishedAt",now.toString());store.updateTrigger(t);}}
    public Map<String,Object> triggers(String id,int page,int size,String status){store.schedule(id);return store.triggers(id,page,size,status);}
    public Map<String,Object> trigger(String id){return store.trigger(id);}
    public synchronized Map<String,Object> stop(String id){var t=store.trigger(id);if("RUNNING".equals(t.get("status"))){t.put("cancelRequested",true);store.updateTrigger(t);tasks.stop(t.get("runId").toString());refresh(t,Instant.now());}else if(ACTIVE.contains(t.get("status"))||"BLOCKED".equals(t.get("status"))){t.put("status","CANCELLED");t.put("reason","USER_CANCELLED");t.put("finishedAt",ObjectService.now());store.updateTrigger(t);}return store.trigger(id);}
    public Map<String,Object> rerun(String id){return rerun(id,"ORIGINAL",null);}
    @SuppressWarnings("unchecked") public synchronized Map<String,Object> rerun(String id,String mode,String attemptRunId){
        var t=store.trigger(id);int previousAttempt=((Number)t.get("attempt")).intValue();if(ACTIVE.contains(t.get("status"))&&!"PAUSED".equals(t.get("status")))throw StudioException.conflict("TRIGGER_ACTIVE","该实例尚未结束");mode=mode==null||mode.isBlank()?"LATEST":mode;if(!Set.of("LATEST","ORIGINAL","ATTEMPT").contains(mode))throw StudioException.bad("INVALID_RERUN_MODE","请选择最新版本、原始输入或历史尝试");t.putIfAbsent("originalReleaseId",t.get("releaseId"));t.putIfAbsent("originalScheduleParameters",t.getOrDefault("scheduleParameters",Map.of()));t.putIfAbsent("originalContext",acceptedContext(t));if(t.containsKey("upstreamRuns"))t.putIfAbsent("originalUpstreamRuns",t.get("upstreamRuns"));
        if("LATEST".equals(mode)){
            var release=store.latestRelease(t.get("taskId").toString());var applied=store.schedule(t.get("scheduleId").toString());t.put("releaseId",release.get("id"));t.put("releaseNo",release.get("releaseNo"));t.put("sourceCutoffAt",Instant.now().toString());t.put("scheduleParameters",parameters(release.get("id").toString(),t));t.remove("upstreamRuns");
            var required=slotsFor(applied,Instant.parse(t.get("scheduledAt").toString()),t.get("businessDate").toString());String batch=Objects.toString(t.get("batchKey"),"");if(!batch.isEmpty()){
                Map<String,Set<String>> local=new HashMap<>();for(var slot:required)if(slot.get("scheduleId")!=null&&slot.get("scheduledAt")!=null){String schedule=slot.get("scheduleId").toString();var keys=local.computeIfAbsent(schedule,key->{var matching=required.stream().filter(d->Objects.equals(d.get("scheduleId"),key)&&d.get("scheduledAt")!=null).map(d->d.get("scheduledAt").toString()).sorted().toList();Set<String> found=new HashSet<>();for(var row:store.slotRange(key,batch,matching.getFirst(),matching.getLast()))found.add(row.get("scheduledAt").toString());return found;});if(keys.contains(slot.get("scheduledAt").toString())){slot.put("batchKey",batch);slot.put("binding","BATCH");}}
            }t.put("dependencySlots",required);t.put("scheduleSnapshot",new LinkedHashMap<>(applied));
        }else{
            String selected=Objects.toString(attemptRunId,"");if(selected.isEmpty()&&t.get("attempts") instanceof List<?> list&&!list.isEmpty())selected=((Map<String,Object>)list.getFirst()).get("runId").toString();
            if(!selected.isEmpty())restoreAttempt(t,id,selected);
            else{if("ATTEMPT".equals(mode))throw StudioException.bad("INVALID_ATTEMPT","请选择历史尝试");if(t.get("originalContext") instanceof Map<?,?> context)t.putAll((Map<String,Object>)context);t.put("releaseId",t.get("originalReleaseId"));t.put("releaseNo",tasks.release(t.get("releaseId").toString()).get("releaseNo"));t.put("scheduleParameters",t.get("originalScheduleParameters"));if(t.containsKey("originalUpstreamRuns"))t.put("upstreamRuns",t.get("originalUpstreamRuns"));}
        }
        int attempt=previousAttempt+1;t.put("attempt",attempt);t.put("retryBase",attempt);t.put("manualRerun",true);t.put("triggerType","RERUN");t.put("status","PENDING");t.put("rerunMode",mode);for(String k:List.of("finishedAt","reason","runId","nextRetryAt","pausedStatus","cancelRequested"))t.remove(k);store.updateTrigger(t);dispatch(t,Instant.now());return store.trigger(id);
    }
    private Map<String,Object> acceptedContext(Map<String,Object> source){var result=new LinkedHashMap<String,Object>();for(String key:List.of("releaseId","releaseNo","businessDate","sourceCutoffAt","scheduledAt","timezone","scheduleParameters","upstreamRuns","scheduleSnapshot","dependencySlots"))if(source.containsKey(key))result.put(key,source.get(key));return result;}
    @SuppressWarnings("unchecked") private void restoreAttempt(Map<String,Object> target,String triggerId,String selected){
        Map<String,Object> accepted=null;var history=(List<Map<String,Object>>)target.getOrDefault("attempts",List.of());for(var prior:history)if(selected.equals(prior.get("runId"))&&prior.get("context") instanceof Map<?,?> context){accepted=(Map<String,Object>)context;break;}
        var run=repo.run(selected).orElseThrow(()->StudioException.bad("INVALID_ATTEMPT","请选择本实例的历史尝试"));if(!triggerId.equals(run.get("triggerId")))throw StudioException.bad("INVALID_ATTEMPT","运行不属于此实例");
        boolean first=!history.isEmpty()&&selected.equals(history.getFirst().get("runId"));Map<String,Object> original=(Map<String,Object>)target.getOrDefault("originalContext",Map.of());if(first)target.putAll(original);if(accepted!=null)target.putAll(accepted);target.putAll(acceptedContext(run));
        // Older runs did not save the graph: show the inputs actually accepted by that run.
        if(!run.containsKey("dependencySlots")&&(accepted==null||!accepted.containsKey("dependencySlots"))){var observed=new ArrayList<Map<String,Object>>();for(var input:(List<Map<String,Object>>)run.getOrDefault("upstreamRuns",List.of())){var slot=new LinkedHashMap<>(input);slot.put("status","SUCCESS");slot.remove("reason");observed.add(slot);}target.put("dependencySlots",observed);
            var snapshot=new LinkedHashMap<String,Object>((Map<String,Object>)target.getOrDefault("scheduleSnapshot",Map.of()));var deps=new LinkedHashMap<String,Map<String,Object>>();for(var slot:observed){var d=new LinkedHashMap<String,Object>();for(String key:List.of("taskId","alias","name","matchMode","mode"))if(slot.containsKey(key))d.put(key,slot.get(key));deps.put(Objects.toString(slot.get("taskId"),"")+"|"+Objects.toString(slot.get("alias"),""),d);}snapshot.put("dependencies",new ArrayList<>(deps.values()));target.put("scheduleSnapshot",snapshot);
        }
    }
    private boolean retryAllowed(Map<String,Object> trigger){if(trigger.get("runId")==null)return !Boolean.TRUE.equals(tasks.release(trigger.get("releaseId").toString()).get("containsWrites"));return repo.run(trigger.get("runId").toString()).map(r->!Boolean.TRUE.equals(r.get("containsWrites"))).orElse(false);}
    /** Compatibility for callers that used to own a per-service timer. */
    public void close(){}
}
