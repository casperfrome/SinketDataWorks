package com.fake.dataworks.service;

import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.*;
import jakarta.annotation.PreDestroy;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/** Persisted task instances pin the required upstream slot, never merely any older success. */
@Service
public class TaskScheduleService {
    private final TaskRepository store;private final StudioRepository repo;private final TaskService tasks;private final ScheduleService times;private final TransactionTemplate tx;private final boolean enabled;
    private final ScheduledExecutorService timer=Executors.newSingleThreadScheduledExecutor(r->new Thread(r,"task-schedule-clock"));
    private final Map<String,Instant> lastScans=new HashMap<>();
    private static final Set<String> ACTIVE=Set.of("PENDING","WAITING_DEPENDENCY","WAITING_RESOURCE","RUNNING","RETRY_WAIT");
    private static final Set<String> RETRYABLE=Set.of("DATASOURCE_UNAVAILABLE","DB_TRANSIENT","QUEUE_FULL");
    private static final org.slf4j.Logger log=org.slf4j.LoggerFactory.getLogger(TaskScheduleService.class);
    public TaskScheduleService(TaskRepository store,StudioRepository repo,TaskService tasks,ScheduleService times,TransactionTemplate tx,@Value("${studio.scheduler.enabled:true}") boolean enabled){this.store=store;this.repo=repo;this.tasks=tasks;this.times=times;this.tx=tx;this.enabled=enabled;}
    @EventListener(ApplicationReadyEvent.class) public void ready(){if(!enabled)return;
        for(var t:store.pending(""))if(!"RUNNING".equals(t.get("status"))){t.put("status","FAILED");t.put("reason","SERVICE_RESTARTED");store.updateTrigger(t);}
        resume(Instant.now(),"");timer.scheduleWithFixedDelay(()->{try{scan(Instant.now(),"");}catch(Exception e){log.warn("Task scheduling deferred: {}",e.getClass().getSimpleName());}},1,1,TimeUnit.SECONDS);
    }
    public List<Map<String,Object>> list(String workspace,String task){return store.schedules(workspace).stream().filter(s->task.isBlank()||task.equals(s.get("taskId"))).toList();}
    private List<Map<String,Object>> dependencies(String task,String workspace,Map<String,Object> input){
        List<Map<String,Object>> result=new ArrayList<>();Set<String> ids=new HashSet<>(),aliases=new HashSet<>();
        for(var d:TaskService.dependencies(input)){
            String id=Objects.toString(d.get("taskId"),""),alias=Objects.toString(d.get("alias"),"");var o=tasks.task(id);
            if(id.equals(task)||!ids.add(id))throw StudioException.bad("INVALID_DEPENDENCY","不能依赖自身或重复添加同一上游");
            if(!workspace.equals(o.workspaceId()))throw StudioException.bad("WORKSPACE_MISMATCH","上游须属于同一工作空间");
            if(!alias.matches("[A-Za-z][A-Za-z0-9_]{0,31}")||!aliases.add(alias))throw StudioException.bad("INVALID_DEPENDENCY_ALIAS","参数别名须以字母开头、最多 32 位且不能重复");
            result.add(Map.of("taskId",id,"name",o.name(),"alias",alias));
        }
        Map<String,List<String>> graph=new HashMap<>();for(var s:store.schedules(workspace))graph.put(s.get("taskId").toString(),TaskService.dependencies(s).stream().map(d->d.get("taskId").toString()).toList());graph.put(task,result.stream().map(d->d.get("taskId").toString()).toList());
        visit(task,graph,new HashSet<>(),new HashSet<>());return result;
    }
    private void visit(String id,Map<String,List<String>> graph,Set<String> visiting,Set<String> done){if(done.contains(id))return;if(!visiting.add(id))throw StudioException.bad("DEPENDENCY_CYCLE","上游依赖形成循环，请移除环路");for(String upstream:graph.getOrDefault(id,List.of()))visit(upstream,graph,visiting,done);visiting.remove(id);done.add(id);}
    public Map<String,Object> save(String taskId,String id,Map<String,Object> input){return tx.execute(t->{
        var object=tasks.task(taskId);repo.lockWorkspace(object.workspaceId());Map<String,Object> old=id==null?null:store.schedule(id);
        if(old!=null&&(!taskId.equals(old.get("taskId"))||!(input.get("expectedVersion") instanceof Number n)||n.doubleValue()!=n.intValue()||n.intValue()!=((Number)old.get("version")).intValue()))throw StudioException.conflict("VERSION_CONFLICT","任务调度已变化，请重新加载");
        if(old==null&&store.forTask(taskId).isPresent())throw StudioException.conflict("SCHEDULE_EXISTS","此任务已有调度配置，请重新加载");
        Map<String,Object> s=times.timeConfig(input);String releaseId=Objects.toString(input.get("releaseId"),"");
        s.put("releaseId",releaseId);if(!releaseId.isBlank()){var release=tasks.release(releaseId);if(!taskId.equals(release.get("taskId")))throw StudioException.bad("INVALID_RELEASE","请选择本任务的发布版本");s.put("releaseNo",release.get("releaseNo"));}
        if(Boolean.TRUE.equals(s.get("enabled"))&&releaseId.isBlank())throw StudioException.bad("TASK_RELEASE_REQUIRED","启用前请先发布任务并选择执行版本");
        s.put("dependencies",dependencies(taskId,object.workspaceId(),input));s.put("id",old==null?UUID.randomUUID().toString():id);s.put("taskId",taskId);s.put("workspaceId",object.workspaceId());s.put("name",object.name());s.put("version",old==null?1:((Number)old.get("version")).intValue()+1);s.put("updatedAt",ObjectService.now());
        Instant next=times.next(s,Instant.now());if(Boolean.TRUE.equals(s.get("enabled"))&&next==null)throw StudioException.bad("NO_FUTURE_FIRE","有效期内没有未来执行时间");s.put("nextFireAt",Boolean.TRUE.equals(s.get("enabled"))?next.toString():null);store.saveSchedule(s,old==null);return s;
    });}
    public String businessDate(Map<String,Object> s,Instant fire){return fire.atZone(ZoneId.of(s.get("timezone").toString())).toLocalDate().plusDays(((Number)s.get("businessDateOffset")).intValue()).toString();}
    /** At most one business day's minute slots, including a 25-hour DST day. */
    public Instant matchingSlot(Map<String,Object> upstream,String date,Instant downstream){
        ZoneId zone=ZoneId.of(upstream.get("timezone").toString());LocalDate local=LocalDate.parse(date).minusDays(((Number)upstream.get("businessDateOffset")).intValue());Instant end=local.plusDays(1).atStartOfDay(zone).toInstant();Instant cursor=local.atStartOfDay(zone).minusNanos(1).toInstant(),last=null;
        for(int i=0;i<1600;i++){Instant next=times.next(upstream,cursor);if(next==null||!next.isBefore(end)||next.isAfter(downstream))break;if(businessDate(upstream,next).equals(date))last=next;cursor=next;}return last;
    }
    private List<Map<String,Object>> slots(Map<String,Object> schedule,Instant at,String date){List<Map<String,Object>> result=new ArrayList<>();
        for(var d:TaskService.dependencies(schedule)){var match=new LinkedHashMap<>(d);var upstream=store.forTask(d.get("taskId").toString()).orElse(null);
            if(upstream==null){match.put("reason","UPSTREAM_NO_PLAN");}
            else {match.put("scheduleId",upstream.get("id"));match.put("scheduleVersion",upstream.get("version"));match.put("cron",upstream.get("cron"));Instant slot=matchingSlot(upstream,date,at);if(slot==null)match.put("reason","NO_MATCHING_UPSTREAM");else match.put("scheduledAt",slot.toString());if(!Boolean.TRUE.equals(upstream.get("enabled")))match.put("warning","UPSTREAM_PAUSED");}
            result.add(match);
        }return result;
    }
    public List<Map<String,Object>> preview(String taskId,Map<String,Object> input){var o=tasks.task(taskId);var config=times.timeConfig(input);config.put("dependencies",dependencies(taskId,o.workspaceId(),input));List<Map<String,Object>> result=new ArrayList<>();for(var item:times.preview(config)){var r=new LinkedHashMap<>(item);r.put("dependencies",slots(config,Instant.parse(item.get("scheduledAt").toString()),item.get("businessDate").toString()));result.add(r);}return result;}
    private List<Map<String,Object>> plans(String workspace){return workspace.isBlank()?store.allSchedules():store.schedules(workspace);}
    public synchronized void resume(Instant now,String workspace){for(var p:plans(workspace))createDue(p,now,true);lastScans.put(workspace,now);}
    public synchronized void scan(Instant now,String workspace){
        Instant before=lastScans.put(workspace,now);boolean resumed=before!=null&&Duration.between(before,now).getSeconds()>5;
        for(var t:store.pending(workspace))if("RUNNING".equals(t.get("status")))refresh(t,now);
        for(var p:plans(workspace))createDue(p,now,resumed);
        for(var t:store.pending(workspace)){
            if("RETRY_WAIT".equals(t.get("status"))){if(Instant.parse(t.get("nextRetryAt").toString()).isAfter(now))continue;t.put("attempt",((Number)t.get("attempt")).intValue()+1);t.put("status","PENDING");store.updateTrigger(t);}
            if(Set.of("PENDING","WAITING_DEPENDENCY","WAITING_RESOURCE").contains(t.get("status")))dispatch(t,now);
        }
    }
    private void createDue(Map<String,Object> supplied,Instant now,boolean resumed){tx.executeWithoutResult(x->{store.lockSchedule(supplied.get("id").toString());var s=store.schedule(supplied.get("id").toString());if(!Boolean.TRUE.equals(s.get("enabled"))||s.get("nextFireAt")==null)return;Instant fire=Instant.parse(s.get("nextFireAt").toString());if(fire.isAfter(now))return;
        if(repo.find(s.get("taskId").toString()).filter(o->!o.deleted()).isEmpty()){s.put("enabled",false);s.put("nextFireAt",null);s.put("pauseReason","TASK_DELETED");store.saveSchedule(s,false);return;}
        var t=new LinkedHashMap<String,Object>();t.put("id",UUID.randomUUID().toString());for(String k:List.of("workspaceId","taskId","name","releaseId","releaseNo"))if(s.containsKey(k))t.put(k,s.get(k));t.put("scheduleId",s.get("id"));t.put("scheduledAt",fire.toString());String date=businessDate(s,fire);t.put("businessDate",date);t.put("timezone",s.get("timezone"));t.put("sourceCutoffAt",now.toString());t.put("scheduleSnapshot",s);t.put("dependencySlots",slots(s,fire,date));t.put("attempt",1);t.put("attempts",List.of());t.put("createdAt",now.toString());t.put("status","PENDING");
        if(resumed||Duration.between(fire,now).getSeconds()>60){t.put("status","SKIPPED");t.put("reason","MISSED_INTERVAL");t.put("missedUntil",now.toString());}
        else if(store.pending(s.get("workspaceId").toString()).stream().anyMatch(p->Objects.equals(p.get("taskId"),s.get("taskId")))||repo.topRuns(s.get("workspaceId").toString()).stream().anyMatch(r->Objects.equals(r.get("objectId"),s.get("taskId"))&&Set.of("QUEUED","RUNNING","RECOVERING").contains(r.get("status")))){t.put("status","SKIPPED");t.put("reason","OVERLAP");}
        store.insertTrigger(t);Instant next=times.next(s,now);s.put("nextFireAt",next==null?null:next.toString());if(next==null)s.put("enabled",false);store.saveSchedule(s,false);
    });}
    @SuppressWarnings("unchecked") private void dispatch(Map<String,Object> t,Instant now){
        try{
            List<Map<String,Object>> inputs=new ArrayList<>();boolean waiting=false;String blocked=null;
            if(t.get("upstreamRuns") instanceof List<?> fixed)inputs.addAll((List<Map<String,Object>>)fixed);
            else for(var d:(List<Map<String,Object>>)t.get("dependencySlots")){
                if(d.get("scheduledAt")==null){blocked=Objects.toString(d.get("reason"),"NO_MATCHING_UPSTREAM");break;}
                var upstream=store.at(d.get("scheduleId").toString(),d.get("scheduledAt").toString()).orElse(null);
                if(upstream==null){blocked="UPSTREAM_INSTANCE_MISSING";break;}
                d.put("triggerId",upstream.get("id"));d.put("status",upstream.get("status"));
                if(ACTIVE.contains(upstream.get("status"))){waiting=true;continue;}
                if(!"SUCCESS".equals(upstream.get("status"))){blocked="UPSTREAM_"+upstream.get("status");break;}
                var run=repo.run(upstream.get("runId").toString()).orElseThrow();inputs.add(tasks.input(d,run));
            }
            if(blocked!=null){t.put("status","BLOCKED");t.put("reason",blocked);t.put("finishedAt",now.toString());store.updateTrigger(t);return;}
            if(waiting){t.put("status","WAITING_DEPENDENCY");t.put("reason","WAITING_UPSTREAM");store.updateTrigger(t);return;}
            t.put("upstreamRuns",inputs);var options=new LinkedHashMap<String,Object>();for(String k:List.of("businessDate","sourceCutoffAt","scheduleId","scheduledAt","timezone","attempt","upstreamRuns"))options.put(k,t.get(k));options.put("triggerId",t.get("id"));options.put("triggerType",Boolean.TRUE.equals(t.get("manualRerun"))?"RERUN":"SCHEDULED");
            tx.executeWithoutResult(x->{var run=tasks.startRelease(t.get("releaseId").toString(),options);t.put("runId",run.get("id"));t.put("status","RUNNING");t.remove("reason");var attempts=new ArrayList<Object>((List<?>)t.get("attempts"));attempts.add(Map.of("attempt",t.get("attempt"),"runId",run.get("id"),"createdAt",now.toString()));t.put("attempts",attempts);store.updateTrigger(t);});
        }catch(Exception e){String code=e instanceof StudioException s?s.code():"SUBMISSION_FAILED";
            if(Set.of("MATERIALIZATION_BUSY","RECOVERY_REQUIRED").contains(code)){t.put("status","WAITING_RESOURCE");t.put("reason",code);store.updateTrigger(t);}
            else if("TASK_OVERLAP".equals(code)){t.put("status","SKIPPED");t.put("reason","OVERLAP");store.updateTrigger(t);}
            else failure(t,code,now);
        }
    }
    private void refresh(Map<String,Object> t,Instant now){var r=repo.run(t.get("runId").toString()).orElse(null);if(r==null){failure(t,"RUN_MISSING",now);return;}String status=r.get("status").toString();if(Set.of("QUEUED","RUNNING","RECOVERING").contains(status))return;if("SUCCESS".equals(status)){t.put("status","SUCCESS");t.remove("reason");t.put("finishedAt",now.toString());store.updateTrigger(t);}else failure(t,Objects.toString(r.get("errorCode"),status),now);}
    @SuppressWarnings("unchecked") private void failure(Map<String,Object> t,String code,Instant now){var config=(Map<String,Object>)t.get("scheduleSnapshot");int attempt=((Number)t.get("attempt")).intValue();t.put("reason",code);int first=((Number)t.getOrDefault("retryBase",1)).intValue();if(retryAllowed(t)&&RETRYABLE.contains(code)&&attempt-first<((Number)config.get("retries")).intValue()){t.put("status","RETRY_WAIT");t.put("nextRetryAt",now.plusSeconds(((Number)config.get("retryIntervalSeconds")).longValue()).toString());}else {t.put("status","FAILED");t.put("finishedAt",now.toString());}store.updateTrigger(t);}
    public Map<String,Object> triggers(String id,int page,int size,String status){store.schedule(id);return store.triggers(id,page,size,status);}
    public synchronized Map<String,Object> rerun(String id){var t=store.trigger(id);if(ACTIVE.contains(t.get("status")))throw StudioException.conflict("TRIGGER_ACTIVE","该实例尚未结束");int attempt=((Number)t.get("attempt")).intValue()+1;t.put("attempt",attempt);t.put("retryBase",attempt);t.put("manualRerun",true);t.put("status","PENDING");t.remove("finishedAt");store.updateTrigger(t);dispatch(t,Instant.now());return store.trigger(id);}
    private boolean retryAllowed(Map<String,Object> trigger) {
        if(trigger.get("runId")==null) {
            return trigger.get("releaseId")==null||!Boolean.TRUE.equals(tasks.release(trigger.get("releaseId").toString()).get("containsWrites"));
        }
        return repo.run(trigger.get("runId").toString()).map(r->!Boolean.TRUE.equals(r.get("containsWrites"))).orElse(false);
    }
    @PreDestroy public void close(){timer.shutdownNow();}
}
