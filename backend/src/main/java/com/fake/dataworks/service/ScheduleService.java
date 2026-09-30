package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.StudioRepository;
import jakarta.annotation.PreDestroy;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/** Durable, single-host minute scheduling. A trigger and every attempt retain their original context. */
@Service
public class ScheduleService {
    private final JdbcTemplate jdbc;private final JsonCodec json;private final WorkflowService workflows;private final ObjectService objects;private final StudioRepository repo;private final InventoryExecutionService inventory;private final TransactionTemplate tx;private final boolean enabled;
    private final ScheduledExecutorService timer=Executors.newSingleThreadScheduledExecutor(r->new Thread(r,"schedule-clock"));
    private final Map<String,Instant> lastScans=new HashMap<>();
    private static final Set<String> RETRYABLE=Set.of("DATASOURCE_UNAVAILABLE","DB_TRANSIENT","QUEUE_FULL","WORKFLOW_LIMIT");
    private static final org.slf4j.Logger log=org.slf4j.LoggerFactory.getLogger(ScheduleService.class);
    @Value("${studio.inventory.recover-on-start:true}") private boolean recoveryEnabled;
    public ScheduleService(JdbcTemplate jdbc,JsonCodec json,WorkflowService workflows,ObjectService objects,StudioRepository repo,InventoryExecutionService inventory,TransactionTemplate tx,@Value("${studio.scheduler.enabled:true}") boolean enabled){this.jdbc=jdbc;this.json=json;this.workflows=workflows;this.objects=objects;this.repo=repo;this.inventory=inventory;this.tx=tx;this.enabled=enabled;}
    @EventListener(ApplicationReadyEvent.class) public void ready(){
        if(recoveryEnabled)inventory.recover();
        if(!enabled)return;
        for(var trigger:pending()){
            if("RETRY_WAIT".equals(trigger.get("status"))||"PENDING".equals(trigger.get("status"))){trigger.put("status","FAILED");trigger.put("reason","SERVICE_RESTARTED");updateTrigger(trigger);}
        }
        recordMissedOnResume(Instant.now(),"");
        if(enabled)timer.scheduleWithFixedDelay(()->{try{scan(Instant.now());}catch(Exception e){log.warn("Schedule scan deferred: {}",e.getClass().getSimpleName());}},1,1,TimeUnit.SECONDS);
    }
    private int integer(Map<String,Object> input,String key,int fallback,int min,int max){Object value=input.getOrDefault(key,fallback);if(!(value instanceof Number n)||n.doubleValue()!=n.intValue()||n.intValue()<min||n.intValue()>max)throw StudioException.bad("INVALID_SCHEDULE",key+" 须在 "+min+"–"+max+" 范围内");return n.intValue();}
    public Map<String,Object> timeConfig(Map<String,Object> input){
        Map<String,Object> c=new LinkedHashMap<>();String cron=Objects.toString(input.get("cron"),"0 0 2 * * *").trim();
        try{String[] parts=cron.split("\\s+");if(parts.length!=6||!Set.of("0","00").contains(parts[0]))throw new IllegalArgumentException();CronExpression.parse(cron);}catch(Exception e){throw StudioException.bad("INVALID_CRON","请输入六字段 Cron，秒须为 0，最小周期为一分钟");}
        String zone=Objects.toString(input.get("timezone"),"Asia/Shanghai");try{ZoneId.of(zone);}catch(Exception e){throw StudioException.bad("INVALID_TIMEZONE","请选择有效时区");}
        String start=Objects.toString(input.get("startDate"),""),end=Objects.toString(input.get("endDate"),"");
        try{if(!start.isEmpty())LocalDate.parse(start);if(!end.isEmpty()&&(!start.isEmpty()&&LocalDate.parse(end).isBefore(LocalDate.parse(start))))throw new IllegalArgumentException();if(!end.isEmpty())LocalDate.parse(end);}catch(Exception e){throw StudioException.bad("INVALID_DATE_RANGE","请检查调度生效及结束日期");}
        c.put("cron",cron);c.put("timezone",zone);c.put("startDate",start);c.put("endDate",end);c.put("businessDateOffset",integer(input,"businessDateOffset",-1,-365,0));c.put("retries",integer(input,"retries",0,0,10));c.put("retryIntervalSeconds",integer(input,"retryIntervalSeconds",60,60,1800));
        if(((Number)c.get("retryIntervalSeconds")).intValue()%60!=0)throw StudioException.bad("INVALID_SCHEDULE","重试间隔须为 1–30 整分钟");
        c.put("cycle",Objects.toString(input.get("cycle"),"CUSTOM"));c.put("enabled",Boolean.TRUE.equals(input.get("enabled")));return c;
    }
    public Instant next(Map<String,Object> config,Instant after){
        ZoneId zone=ZoneId.of(config.get("timezone").toString());ZonedDateTime base=after.atZone(zone);
        String start=config.get("startDate").toString(),end=config.get("endDate").toString();
        if(!start.isEmpty()){var first=LocalDate.parse(start).atStartOfDay(zone);if(base.isBefore(first))base=first.minusNanos(1);}
        ZonedDateTime next=CronExpression.parse(config.get("cron").toString()).next(base);
        if(next==null||(!end.isEmpty()&&next.toLocalDate().isAfter(LocalDate.parse(end))))return null;
        return next.toInstant();
    }
    public List<Map<String,Object>> preview(Map<String,Object> input){var config=timeConfig(input);List<Map<String,Object>> result=new ArrayList<>();Instant at=Instant.now();for(int i=0;i<5;i++){at=next(config,at);if(at==null)break;result.add(Map.of("scheduledAt",at.toString(),"localTime",at.atZone(ZoneId.of(config.get("timezone").toString())).toString(),"businessDate",businessDate(config,at)));}return result;}
    private String businessDate(Map<String,Object> config,Instant instant){return instant.atZone(ZoneId.of(config.get("timezone").toString())).toLocalDate().plusDays(((Number)config.get("businessDateOffset")).intValue()).toString();}
    public List<Map<String,Object>> list(String workspace,String workflow){objects.workspace(workspace);return jdbc.query("SELECT data_json FROM dw_workflow_schedule WHERE workspace_id=? AND (?='' OR workflow_id=?) ORDER BY workflow_id",(r,n)->json.map(r.getString(1)),workspace,workflow,workflow);}
    public Map<String,Object> get(String id){return jdbc.query("SELECT data_json FROM dw_workflow_schedule WHERE id=?",(r,n)->json.map(r.getString(1)),id).stream().findFirst().orElseThrow(()->StudioException.missing("调度计划不存在"));}
    public Map<String,Object> save(String workflowId,String id,Map<String,Object> input){
        return tx.execute(t->{
            Map<String,Object> old=null;
            if(id!=null){jdbc.queryForObject("SELECT id FROM dw_workflow_schedule WHERE id=? FOR UPDATE",String.class,id);old=get(id);workflowIdCheck(workflowId,old);}
            String workflow=old==null?workflowId:old.get("workflowId").toString();var object=objects.active(workflow);
            repo.lockWorkspace(object.workspaceId());
            if(old!=null&&(!(input.get("expectedVersion") instanceof Number n)||n.doubleValue()!=((Number)old.get("version")).intValue()))throw StudioException.conflict("VERSION_CONFLICT","调度配置已变化，请刷新后重试");
            if(old==null&&!list(object.workspaceId(),workflow).isEmpty())throw StudioException.conflict("SCHEDULE_EXISTS","此工作流已有调度计划");
            var release=workflows.release(Objects.toString(input.get("releaseId"),""));
            if(!workflow.equals(release.get("workflowId")))throw StudioException.bad("INVALID_RELEASE","发布版本不属于该工作流");
            Map<String,Object> config=timeConfig(input);String key=old==null?UUID.randomUUID().toString():id;
            config.put("id",key);config.put("workspaceId",object.workspaceId());config.put("workflowId",workflow);config.put("name",object.name());config.put("releaseId",release.get("id"));config.put("releaseNo",release.get("releaseNo"));config.put("version",old==null?1:((Number)old.get("version")).intValue()+1);config.put("updatedAt",ObjectService.now());
            Instant next=next(config,Instant.now());if(Boolean.TRUE.equals(config.get("enabled"))&&next==null)throw StudioException.bad("NO_FUTURE_FIRE","有效期内没有未来执行时间");config.put("nextFireAt",Boolean.TRUE.equals(config.get("enabled"))?next.toString():null);
            if(old==null)jdbc.update("INSERT INTO dw_workflow_schedule(id,workspace_id,workflow_id,release_id,enabled,version,next_fire_at,data_json) VALUES(?,?,?,?,?,?,?,?)",key,object.workspaceId(),workflow,release.get("id"),config.get("enabled"),config.get("version"),config.get("nextFireAt"),json.write(config));
            else updateSchedule(config);return config;
        });
    }
    private void workflowIdCheck(String workflow,Map<String,Object> old){if(workflow!=null&&!workflow.equals(old.get("workflowId")))throw StudioException.bad("WORKSPACE_MISMATCH","调度归属不能修改");}
    private void updateSchedule(Map<String,Object> s){jdbc.update("UPDATE dw_workflow_schedule SET release_id=?,enabled=?,version=?,next_fire_at=?,data_json=? WHERE id=?",s.get("releaseId"),s.get("enabled"),s.get("version"),s.get("nextFireAt"),json.write(s),s.get("id"));}
    public Map<String,Object> triggers(String id,int page,int size,String status){get(id);if(page<1||page>100000||size<1||size>100)throw StudioException.bad("INVALID_PAGE","分页参数无效");String where=" WHERE schedule_id=? AND (?='' OR status=?)";return Map.of("items",jdbc.query("SELECT data_json FROM dw_schedule_trigger"+where+" ORDER BY scheduled_at DESC LIMIT ? OFFSET ?",(r,n)->json.map(r.getString(1)),id,status,status,size,(page-1)*size),"total",jdbc.queryForObject("SELECT COUNT(*) FROM dw_schedule_trigger"+where,Long.class,id,status,status),"page",page,"pageSize",size);}
    private List<Map<String,Object>> pending(){return pending("");}
    private List<Map<String,Object>> pending(String workspace){return jdbc.query("SELECT data_json FROM dw_schedule_trigger WHERE status IN ('PENDING','RUNNING','RETRY_WAIT') AND (?='' OR JSON_UNQUOTE(JSON_EXTRACT(data_json,'$.workspaceId'))=?) ORDER BY scheduled_at",(r,n)->json.map(r.getString(1)),workspace,workspace);}
    private Map<String,Object> trigger(String id){return jdbc.query("SELECT data_json FROM dw_schedule_trigger WHERE id=?",(r,n)->json.map(r.getString(1)),id).stream().findFirst().orElseThrow(()->StudioException.missing("调度实例不存在"));}
    private void updateTrigger(Map<String,Object> r){jdbc.update("UPDATE dw_schedule_trigger SET status=?,run_id=?,data_json=? WHERE id=?",r.get("status"),r.get("runId"),json.write(r),r.get("id"));}
    public synchronized void scan(Instant now){scan(now,"");}
    private List<String> enabledPlans(String workspace){return jdbc.query("SELECT id FROM dw_workflow_schedule WHERE enabled=TRUE AND next_fire_at IS NOT NULL AND (?='' OR workspace_id=?)",(r,n)->r.getString(1),workspace,workspace);}
    synchronized void recordMissedOnResume(Instant now,String workspace){for(String id:enabledPlans(workspace))tx.executeWithoutResult(t->createDue(id,now,true));lastScans.put(workspace,now);}
    synchronized void scan(Instant now,String workspace){
        Instant previous=lastScans.put(workspace,now);boolean resumed=previous!=null&&Duration.between(previous,now).getSeconds()>5;
        if(recoveryEnabled)inventory.recover();
        for(String id:enabledPlans(workspace))tx.executeWithoutResult(t->createDue(id,now,resumed));
        for(var r:pending(workspace)){
            String status=r.get("status").toString();
            if(status.equals("RUNNING")){
                var run=repo.run(r.get("runId").toString()).orElse(null);
                if(run==null){completeFailure(r,"RUN_MISSING",now);continue;}
                String state=run.get("status").toString();
                if(state.equals("SUCCESS")||state.equals("CANCELLED")){r.put("status",state);r.put("finishedAt",now.toString());updateTrigger(r);}
                else if(state.equals("FAILED"))completeFailure(r,Objects.toString(run.get("errorCode"),"WORKFLOW_FAILED"),now);
            }else if(status.equals("RETRY_WAIT")&&!Instant.parse(r.get("nextRetryAt").toString()).isAfter(now)){r.put("status","PENDING");r.put("attempt",((Number)r.get("attempt")).intValue()+1);updateTrigger(r);dispatch(r.get("id").toString(),now);}
            else if(status.equals("PENDING"))dispatch(r.get("id").toString(),now);
        }
    }
    private void createDue(String id,Instant now,boolean resumed){
        jdbc.queryForObject("SELECT id FROM dw_workflow_schedule WHERE id=? FOR UPDATE",String.class,id);var schedule=get(id);
        if(!Boolean.TRUE.equals(schedule.get("enabled"))||schedule.get("nextFireAt")==null)return;
        var object=repo.find(schedule.get("workflowId").toString());if(object.isEmpty()||object.get().deleted()){schedule.put("enabled",false);schedule.put("nextFireAt",null);schedule.put("pauseReason","WORKFLOW_DELETED");updateSchedule(schedule);return;}
        Instant fire=Instant.parse(schedule.get("nextFireAt").toString());if(fire.isAfter(now))return;
        boolean missed=resumed||Duration.between(fire,now).getSeconds()>60;
        Map<String,Object> r=new LinkedHashMap<>();r.put("id",UUID.randomUUID().toString());r.put("scheduleId",id);r.put("workspaceId",schedule.get("workspaceId"));r.put("workflowId",schedule.get("workflowId"));r.put("releaseId",schedule.get("releaseId"));r.put("releaseNo",schedule.get("releaseNo"));r.put("scheduledAt",fire.toString());r.put("businessDate",businessDate(schedule,fire));r.put("timezone",schedule.get("timezone"));r.put("sourceCutoffAt",now.toString());r.put("attempt",1);r.put("scheduleSnapshot",new LinkedHashMap<>(schedule));r.put("status",missed?"SKIPPED":"PENDING");r.put("createdAt",now.toString());r.put("attempts",List.of());
        if(missed){r.put("reason","MISSED_INTERVAL");r.put("missedUntil",now.toString());}
        jdbc.update("INSERT IGNORE INTO dw_schedule_trigger(id,schedule_id,scheduled_at,status,data_json) VALUES(?,?,?,?,?)",r.get("id"),id,fire.toString(),r.get("status"),json.write(r));
        Instant next=next(schedule,now);schedule.put("nextFireAt",next==null?null:next.toString());if(next==null)schedule.put("enabled",false);updateSchedule(schedule);
    }
    @SuppressWarnings("unchecked")
    private void dispatch(String id,Instant now){
        try{tx.executeWithoutResult(t->{jdbc.queryForObject("SELECT id FROM dw_schedule_trigger WHERE id=? FOR UPDATE",String.class,id);var r=trigger(id);if(!"PENDING".equals(r.get("status")))return;
            repo.lockWorkspace(r.get("workspaceId").toString());
            if(repo.topRuns(r.get("workspaceId").toString()).stream().anyMatch(run->Objects.equals(run.get("objectId"),r.get("workflowId"))&&Set.of("QUEUED","RUNNING","RECOVERING").contains(run.get("status"))))throw StudioException.conflict("MATERIALIZATION_BUSY","上一轮工作流未结束");
            Map<String,Object> options=options(r);options.put("triggerType","SCHEDULED");options.put("scheduleId",r.get("scheduleId"));options.put("triggerId",id);options.put("scheduledAt",r.get("scheduledAt"));options.put("attempt",r.get("attempt"));
            var run=workflows.startRelease(r.get("releaseId").toString(),options);r.put("runId",run.get("id"));r.put("status","RUNNING");List<Object> attempts=new ArrayList<>((List<?>)r.get("attempts"));attempts.add(Map.of("attempt",r.get("attempt"),"runId",run.get("id"),"createdAt",now.toString()));r.put("attempts",attempts);updateTrigger(r);
        });}catch(Exception e){var r=trigger(id);String code=e instanceof StudioException s?s.code():"SUBMISSION_FAILED";if(code.equals("MATERIALIZATION_BUSY")){r.put("status","SKIPPED");r.put("reason","OVERLAP");updateTrigger(r);}else completeFailure(r,code,now);}
    }
    @SuppressWarnings("unchecked")
    private void completeFailure(Map<String,Object> r,String code,Instant now){
        var config=(Map<String,Object>)r.get("scheduleSnapshot");int attempt=((Number)r.get("attempt")).intValue();r.put("reason",code);
        if(retryAllowed(r)&&RETRYABLE.contains(code)&&attempt<=((Number)config.get("retries")).intValue()){r.put("status","RETRY_WAIT");r.put("nextRetryAt",now.plusSeconds(((Number)config.get("retryIntervalSeconds")).longValue()).toString());}
        else {r.put("status","FAILED");r.put("finishedAt",now.toString());}updateTrigger(r);
    }
    private Map<String,Object> options(Map<String,Object> source){Map<String,Object> result=new LinkedHashMap<>();for(String key:List.of("businessDate","sourceCutoffAt","scheduledAt","timezone"))if(source.containsKey(key))result.put(key,source.get(key));return result;}
    public Map<String,Object> rerunTrigger(String id){var r=trigger(id);var input=options(r);input.put("triggerType","RERUN");if(r.get("runId")!=null)input.put("retryOfRunId",r.get("runId"));return workflows.startRelease(r.get("releaseId").toString(),input);}
    public Map<String,Object> rerun(String id){var run=repo.run(id).orElseThrow(()->StudioException.missing("运行不存在"));if(run.get("parentRunId")!=null||run.get("releaseId")==null)throw StudioException.bad("RELEASE_RUN_REQUIRED","请选择已发布工作流的顶层运行");var input=options(run);input.put("triggerType","RERUN");input.put("retryOfRunId",id);return workflows.startRelease(run.get("releaseId").toString(),input);}
    private boolean retryAllowed(Map<String,Object> trigger) {
        if(trigger.get("runId")==null) {
            return trigger.get("releaseId")==null||!Boolean.TRUE.equals(workflows.release(trigger.get("releaseId").toString()).get("containsWrites"));
        }
        return repo.run(trigger.get("runId").toString()).map(r->!Boolean.TRUE.equals(r.get("containsWrites"))).orElse(false);
    }
    @PreDestroy public void close(){timer.shutdownNow();}
}
