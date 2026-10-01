package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.StudioRepository;
import java.time.*;
import java.util.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/** Durable, single-host minute scheduling. A trigger and every attempt retain their original context. */
@Service
public class ScheduleService {
    private final JdbcTemplate jdbc;private final JsonCodec json;private final WorkflowService workflows;private final ObjectService objects;private final StudioRepository repo;private final InventoryExecutionService inventory;private final TransactionTemplate tx;private final boolean enabled;
    private final Map<String,Instant> recoveryUntil=new HashMap<>();
    private int generationRemaining=200;
    private static final Set<String> RETRYABLE=Set.of("DATASOURCE_UNAVAILABLE","DB_TRANSIENT","QUEUE_FULL","WORKFLOW_LIMIT");
    private static final org.slf4j.Logger log=org.slf4j.LoggerFactory.getLogger(ScheduleService.class);
    @Value("${studio.inventory.recover-on-start:true}") private boolean recoveryEnabled;
    public ScheduleService(JdbcTemplate jdbc,JsonCodec json,WorkflowService workflows,ObjectService objects,StudioRepository repo,InventoryExecutionService inventory,TransactionTemplate tx,@Value("${studio.scheduler.enabled:true}") boolean enabled){this.jdbc=jdbc;this.json=json;this.workflows=workflows;this.objects=objects;this.repo=repo;this.inventory=inventory;this.tx=tx;this.enabled=enabled;}
    public void ready(){ready(Instant.now(),200);}
    public synchronized int ready(Instant now,int budget){
        if(enabled)for(var r:pending())if("RUNNING".equals(r.get("status"))&&r.get("runId")!=null){var run=repo.run(r.get("runId").toString()).orElse(null);if(run!=null&&"QUEUED".equals(run.get("status"))&&workflows.withdrawQueued(r.get("runId").toString(),"SERVICE_RESTARTED_QUEUED")){r.put("attempt",((Number)r.get("attempt")).intValue()+1);r.put("retryBase",((Number)r.getOrDefault("retryBase",1)).intValue()+1);r.remove("runId");r.put("status","PENDING");updateTrigger(r);}}
        if(recoveryEnabled)inventory.recover();
        if(!enabled)return 0;
        return recordMissedOnResume(now,"",budget);
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
    public synchronized Map<String,Object> save(String workflowId,String id,Map<String,Object> input){
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
            Instant now=Instant.now(),next=next(config,now);if(Boolean.TRUE.equals(config.get("enabled"))&&next==null)throw StudioException.bad("NO_FUTURE_FIRE","有效期内没有未来执行时间");if(old!=null&&Boolean.TRUE.equals(old.get("enabled"))){generationRemaining=200;createDue(key,now,false);var advanced=get(key);if(advanced.get("nextFireAt")!=null&&!Instant.parse(advanced.get("nextFireAt").toString()).isAfter(now))throw StudioException.conflict("SCHEDULE_BACKLOG","正在逐期记录历史实例，请稍后保存配置");}config.put("nextFireAt",Boolean.TRUE.equals(config.get("enabled"))?next.toString():null);
            if(old==null)jdbc.update("INSERT INTO dw_workflow_schedule(id,workspace_id,workflow_id,release_id,enabled,version,next_fire_at,data_json) VALUES(?,?,?,?,?,?,?,?)",key,object.workspaceId(),workflow,release.get("id"),config.get("enabled"),config.get("version"),config.get("nextFireAt"),json.write(config));
            else {updateSchedule(config);applyPause(config);}return config;
        });
    }
    private void workflowIdCheck(String workflow,Map<String,Object> old){if(workflow!=null&&!workflow.equals(old.get("workflowId")))throw StudioException.bad("WORKSPACE_MISMATCH","调度归属不能修改");}
    private void updateSchedule(Map<String,Object> s){jdbc.update("UPDATE dw_workflow_schedule SET release_id=?,enabled=?,version=?,next_fire_at=?,data_json=? WHERE id=?",s.get("releaseId"),s.get("enabled"),s.get("version"),s.get("nextFireAt"),json.write(s),s.get("id"));}
    public Map<String,Object> triggers(String id,int page,int size,String status){get(id);if(page<1||page>100000||size<1||size>100)throw StudioException.bad("INVALID_PAGE","分页参数无效");String where=" WHERE schedule_id=? AND (?='' OR status=?)";return Map.of("items",jdbc.query("SELECT data_json FROM dw_schedule_trigger"+where+" ORDER BY scheduled_at DESC LIMIT ? OFFSET ?",(r,n)->json.map(r.getString(1)),id,status,status,size,(page-1)*size),"total",jdbc.queryForObject("SELECT COUNT(*) FROM dw_schedule_trigger"+where,Long.class,id,status,status),"page",page,"pageSize",size);}
    private List<Map<String,Object>> pending(){return pending("");}
    private List<Map<String,Object>> pending(String workspace){return jdbc.query("SELECT data_json FROM dw_schedule_trigger WHERE status IN ('PENDING','WAITING_RESOURCE','RUNNING','RETRY_WAIT','PAUSED') AND (?='' OR JSON_UNQUOTE(JSON_EXTRACT(data_json,'$.workspaceId'))=?) ORDER BY scheduled_at,id",(r,n)->json.map(r.getString(1)),workspace,workspace);}
    public Map<String,Object> trigger(String id){return jdbc.query("SELECT data_json FROM dw_schedule_trigger WHERE id=?",(r,n)->json.map(r.getString(1)),id).stream().findFirst().orElseThrow(()->StudioException.missing("调度实例不存在"));}
    private void updateTrigger(Map<String,Object> r){jdbc.update("UPDATE dw_schedule_trigger SET status=?,run_id=?,data_json=? WHERE id=?",r.get("status"),r.get("runId"),json.write(r),r.get("id"));}
    public synchronized void scan(Instant now){scan(now,"");}
    private List<String> enabledPlans(String workspace){return jdbc.query("SELECT id FROM dw_workflow_schedule WHERE enabled=TRUE AND next_fire_at IS NOT NULL AND (?='' OR workspace_id=?)",(r,n)->r.getString(1),workspace,workspace);}
    void recordMissedOnResume(Instant now,String workspace){recordMissedOnResume(now,workspace,200);}
    public synchronized int recordMissedOnResume(Instant now,String workspace,int budget){generationRemaining=Math.max(0,Math.min(200,budget));int allowed=generationRemaining;recoveryUntil.put(workspace,now);for(String id:enabledPlans(workspace))tx.executeWithoutResult(t->createDue(id,now,true));return allowed-generationRemaining;}
    void scan(Instant now,String workspace){scan(now,workspace,200);}
    public synchronized int scan(Instant now,String workspace,int budget){
        generationRemaining=Math.max(0,Math.min(200,budget));int allowed=generationRemaining;
        if(recoveryEnabled)inventory.recover();
        for(String id:enabledPlans(workspace))tx.executeWithoutResult(t->createDue(id,now,false));
        for(var r:pending(workspace)){
            String status=r.get("status").toString();
            if(status.equals("RUNNING")){
                var run=repo.run(r.get("runId").toString()).orElse(null);
                if(run==null){completeFailure(r,"RUN_MISSING",now);continue;}
                String state=run.get("status").toString();
                if(state.equals("SUCCESS")||state.equals("CANCELLED")){r.put("status",state);r.put("finishedAt",now.toString());updateTrigger(r);}
                else if(state.equals("FAILED"))completeFailure(r,Objects.toString(run.get("errorCode"),"WORKFLOW_FAILED"),now);
            }else if(!status.equals("PAUSED")){
                if(automatic(r)&&planPaused(get(r.get("scheduleId").toString()))){pause(r);continue;}
                if(status.equals("RETRY_WAIT")&&!Instant.parse(r.get("nextRetryAt").toString()).isAfter(now)){r.put("status","PENDING");r.put("attempt",((Number)r.get("attempt")).intValue()+1);r.remove("runId");updateTrigger(r);dispatch(r.get("id").toString(),now);}
                else if(Set.of("PENDING","WAITING_RESOURCE").contains(status))dispatch(r.get("id").toString(),now);
            }
        }
        return allowed-generationRemaining;
    }
    private void createDue(String id,Instant now,boolean resumed){
        jdbc.queryForObject("SELECT id FROM dw_workflow_schedule WHERE id=? FOR UPDATE",String.class,id);var schedule=get(id);
        if(!Boolean.TRUE.equals(schedule.get("enabled"))||schedule.get("nextFireAt")==null)return;
        var object=repo.find(schedule.get("workflowId").toString());if(object.isEmpty()||object.get().deleted()){schedule.put("enabled",false);schedule.put("nextFireAt",null);schedule.put("pauseReason","WORKFLOW_DELETED");updateSchedule(schedule);return;}
        Instant fire=Instant.parse(schedule.get("nextFireAt").toString()),cutoff=recoveryUntil.getOrDefault(schedule.get("workspaceId").toString(),recoveryUntil.get(""));
        while(fire!=null&&!fire.isAfter(now)&&generationRemaining>0){generationRemaining--;
            var r=createInstance(schedule,fire,now,"");if(resumed||(cutoff!=null&&!fire.isAfter(cutoff))){r.put("status","SKIPPED");r.put("reason","MISSED_INTERVAL");r.put("missedUntil",now.toString());r.put("finishedAt",now.toString());}
            jdbc.update("INSERT IGNORE INTO dw_schedule_trigger(id,schedule_id,scheduled_at,batch_key,business_date,status,data_json) VALUES(?,?,?,?,?,?,?)",r.get("id"),id,fire.toString(),"",r.get("businessDate"),r.get("status"),json.write(r));fire=next(schedule,fire);
        }
        schedule.put("nextFireAt",fire==null?null:fire.toString());if(fire==null){schedule.put("enabled",false);schedule.put("pauseReason","END_OF_SCHEDULE");}updateSchedule(schedule);
    }
    @SuppressWarnings("unchecked")
    private void dispatch(String id,Instant now){
        try{tx.executeWithoutResult(t->{jdbc.queryForObject("SELECT id FROM dw_schedule_trigger WHERE id=? FOR UPDATE",String.class,id);var r=trigger(id);if(!Set.of("PENDING","WAITING_RESOURCE").contains(r.get("status")))return;
            repo.lockWorkspace(r.get("workspaceId").toString());
            if(repo.hasActiveTaskRun(r.get("workspaceId").toString(),r.get("workflowId").toString(),null))throw StudioException.conflict("MATERIALIZATION_BUSY","上一轮工作流未结束");
            Map<String,Object> options=options(r);options.put("triggerType",r.getOrDefault("triggerType","SCHEDULED"));options.put("scheduleId",r.get("scheduleId"));options.put("triggerId",id);options.put("scheduledAt",r.get("scheduledAt"));options.put("attempt",r.get("attempt"));
            var run=workflows.startRelease(r.get("releaseId").toString(),options);r.put("runId",run.get("id"));r.put("status","RUNNING");List<Object> attempts=new ArrayList<>((List<?>)r.get("attempts"));var context=new LinkedHashMap<String,Object>(options);context.put("releaseId",r.get("releaseId"));context.put("releaseNo",r.get("releaseNo"));context.put("scheduleSnapshot",r.get("scheduleSnapshot"));attempts.add(Map.of("attempt",r.get("attempt"),"runId",run.get("id"),"createdAt",now.toString(),"context",context));r.put("attempts",attempts);updateTrigger(r);
        });}catch(Exception e){var r=trigger(id);String code=e instanceof StudioException s?s.code():"SUBMISSION_FAILED";if(Set.of("MATERIALIZATION_BUSY","RECOVERY_REQUIRED","TASK_OVERLAP","WORKFLOW_LIMIT","QUEUE_FULL").contains(code)){r.put("status","WAITING_RESOURCE");r.put("reason",code);updateTrigger(r);}else completeFailure(r,code,now);}
    }
    @SuppressWarnings("unchecked")
    private void completeFailure(Map<String,Object> r,String code,Instant now){
        if("QUEUE_FULL".equals(code)){r.put("status","WAITING_RESOURCE");r.put("reason",code);r.put("attempt",((Number)r.get("attempt")).intValue()+1);r.put("retryBase",((Number)r.getOrDefault("retryBase",1)).intValue()+1);r.remove("runId");updateTrigger(r);return;}
        var config=(Map<String,Object>)r.get("scheduleSnapshot");int attempt=((Number)r.get("attempt")).intValue();r.put("reason",code);
        int first=((Number)r.getOrDefault("retryBase",1)).intValue();if(!Boolean.TRUE.equals(r.get("cancelRequested"))&&retryAllowed(r)&&RETRYABLE.contains(code)&&attempt-first<((Number)config.get("retries")).intValue()){r.put("status","RETRY_WAIT");r.put("nextRetryAt",now.plusSeconds(((Number)config.get("retryIntervalSeconds")).longValue()).toString());}
        else {r.put("status","FAILED");r.put("finishedAt",now.toString());}updateTrigger(r);if("RETRY_WAIT".equals(r.get("status"))&&automatic(r)&&planPaused(get(r.get("scheduleId").toString())))pause(r);
    }
    private Map<String,Object> options(Map<String,Object> source){Map<String,Object> result=new LinkedHashMap<>();for(String key:List.of("businessDate","sourceCutoffAt","scheduledAt","timezone","scheduleParameters","nodeScheduleParameters","batchKey","scheduleSnapshot"))if(source.containsKey(key))result.put(key,source.get(key));return result;}
    public synchronized Map<String,Object> setState(String id,boolean state,int expectedVersion){var plan=get(id);var input=new LinkedHashMap<>(plan);input.put("enabled",state);input.put("expectedVersion",expectedVersion);return save(null,id,input);}
    public Map<String,Object> createInstance(Map<String,Object> schedule,Instant fire,Instant cutoff,String batchKey){
        var r=new LinkedHashMap<String,Object>();r.put("id",UUID.randomUUID().toString());for(String key:List.of("workspaceId","workflowId","name","releaseId","releaseNo"))r.put(key,schedule.get(key));r.put("kind","WORKFLOW");r.put("scheduleId",schedule.get("id"));r.put("scheduledAt",fire.toString());r.put("businessDate",businessDate(schedule,fire));r.put("timezone",schedule.get("timezone"));r.put("sourceCutoffAt",cutoff.toString());r.put("attempt",1);r.put("scheduleSnapshot",new LinkedHashMap<>(schedule));r.put("status","PENDING");r.put("createdAt",cutoff.toString());r.put("attempts",List.of());r.put("batchKey",Objects.toString(batchKey,""));r.put("triggerType",batchKey==null||batchKey.isEmpty()?"SCHEDULED":"BACKFILL");r.put("originalReleaseId",schedule.get("releaseId"));freezeParameters(r);r.put("originalContext",new LinkedHashMap<>(options(r)));return r;
    }
    @SuppressWarnings("unchecked") private void freezeParameters(Map<String,Object> instance){
        var release=workflows.release(instance.get("releaseId").toString());var bundle=(Map<String,Object>)release.get("bundle");if(bundle==null)return;
        var workflow=json.read(json.write(bundle.get("workflow")),com.fake.dataworks.domain.StudioObject.class);var context=ScheduleParameters.context(ScheduleParameters.schedule(workflow),instance);var inherited=ScheduleParameters.definitions(workflow);instance.put("scheduleParameters",ScheduleParameters.resolve(inherited,context));
        var perNode=new LinkedHashMap<String,Map<String,String>>();for(var entry:(List<Map<String,Object>>)bundle.getOrDefault("nodes",List.of())){var node=json.read(json.write(entry.get("object")),com.fake.dataworks.domain.StudioObject.class);perNode.put(entry.get("graphNodeId").toString(),ScheduleParameters.resolve(ScheduleParameters.merge(inherited,ScheduleParameters.definitions(node)),context));}instance.put("nodeScheduleParameters",perNode);
    }
    private boolean automatic(Map<String,Object> r){return "SCHEDULED".equals(Objects.toString(r.get("triggerType"),"SCHEDULED"));}
    private boolean planPaused(Map<String,Object> plan){return !Boolean.TRUE.equals(plan.get("enabled"))&&!"END_OF_SCHEDULE".equals(plan.get("pauseReason"));}
    private void pause(Map<String,Object> r){if("PAUSED".equals(r.get("status")))return;r.put("pausedStatus",r.get("status"));r.put("status","PAUSED");r.put("reason","SCHEDULE_PAUSED");updateTrigger(r);}
    private void applyPause(Map<String,Object> schedule){for(var r:pending(schedule.get("workspaceId").toString())){if(!Objects.equals(r.get("scheduleId"),schedule.get("id"))||!automatic(r))continue;if(Boolean.TRUE.equals(schedule.get("enabled"))){if("PAUSED".equals(r.get("status"))){r.put("status",r.getOrDefault("pausedStatus","PENDING"));r.remove("pausedStatus");r.remove("reason");updateTrigger(r);}}else if("RUNNING".equals(r.get("status"))){if(workflows.pauseQueuedRun(r.get("runId").toString())){r.put("attempt",((Number)r.get("attempt")).intValue()+1);r.put("retryBase",((Number)r.getOrDefault("retryBase",1)).intValue()+1);r.remove("runId");r.put("status","PENDING");pause(r);}}else pause(r);}}
    public synchronized Map<String,Object> stop(String id){var r=trigger(id);if("RUNNING".equals(r.get("status"))){r.put("cancelRequested",true);updateTrigger(r);workflows.stop(r.get("runId").toString());}else if(Set.of("PENDING","WAITING_RESOURCE","RETRY_WAIT","PAUSED","BLOCKED").contains(r.get("status"))){r.put("status","CANCELLED");r.put("reason","USER_CANCELLED");r.put("finishedAt",ObjectService.now());updateTrigger(r);}return trigger(id);}
    @SuppressWarnings("unchecked") public synchronized Map<String,Object> rerun(String id,String mode,String attemptRunId){
        var r=trigger(id);int previousAttempt=((Number)r.get("attempt")).intValue();if(Set.of("PENDING","WAITING_RESOURCE","RETRY_WAIT","RUNNING").contains(r.get("status")))throw StudioException.conflict("TRIGGER_ACTIVE","该实例尚未结束");mode=mode==null||mode.isBlank()?"LATEST":mode;if(!Set.of("LATEST","ORIGINAL","ATTEMPT").contains(mode))throw StudioException.bad("INVALID_RERUN_MODE","请选择最新版本或原始输入");r.putIfAbsent("originalReleaseId",r.get("releaseId"));r.putIfAbsent("originalContext",options(r));
        if("LATEST".equals(mode)){var release=repo.releases(r.get("workspaceId").toString(),r.get("workflowId").toString()).stream().max(Comparator.comparingInt(e->((Number)e.get("releaseNo")).intValue())).orElseThrow(()->StudioException.bad("RELEASE_RUN_REQUIRED","请先发布工作流"));r.put("releaseId",release.get("id"));r.put("releaseNo",release.get("releaseNo"));r.put("sourceCutoffAt",Instant.now().toString());r.put("scheduleSnapshot",new LinkedHashMap<>(get(r.get("scheduleId").toString())));freezeParameters(r);}
        else {
            String selected=Objects.toString(attemptRunId,"");if(selected.isEmpty()&&r.get("attempts") instanceof List<?> list&&!list.isEmpty())selected=((Map<String,Object>)list.getFirst()).get("runId").toString();
            if(!selected.isEmpty()){var history=(List<Map<String,Object>>)r.getOrDefault("attempts",List.of());if(!history.isEmpty()&&selected.equals(history.getFirst().get("runId")))r.putAll((Map<String,Object>)r.get("originalContext"));for(var prior:history)if(selected.equals(prior.get("runId"))&&prior.get("context") instanceof Map<?,?> accepted)r.putAll((Map<String,Object>)accepted);var run=repo.run(selected).orElseThrow(()->StudioException.bad("INVALID_ATTEMPT","运行不存在"));if(!id.equals(run.get("triggerId")))throw StudioException.bad("INVALID_ATTEMPT","运行不属于此实例");for(String key:List.of("releaseId","releaseNo","businessDate","sourceCutoffAt","scheduledAt","timezone","scheduleParameters","nodeScheduleParameters","scheduleSnapshot"))if(run.containsKey(key))r.put(key,run.get(key));}
            else {r.put("releaseId",r.get("originalReleaseId"));r.put("releaseNo",workflows.release(r.get("releaseId").toString()).get("releaseNo"));r.putAll((Map<String,Object>)r.get("originalContext"));}
        }
        int attempt=previousAttempt+1;r.put("attempt",attempt);r.put("retryBase",attempt);r.put("triggerType","RERUN");r.put("status","PENDING");r.put("rerunMode",mode);for(String key:List.of("runId","reason","finishedAt","nextRetryAt","pausedStatus","cancelRequested"))r.remove(key);updateTrigger(r);dispatch(id,Instant.now());return trigger(id);
    }
    public Map<String,Object> rerunTrigger(String id){var r=trigger(id);var input=options(r);input.put("triggerType","RERUN");if(r.get("runId")!=null)input.put("retryOfRunId",r.get("runId"));return workflows.startRelease(r.get("releaseId").toString(),input);}
    public Map<String,Object> rerun(String id){var run=repo.run(id).orElseThrow(()->StudioException.missing("运行不存在"));if(run.get("parentRunId")!=null||run.get("releaseId")==null)throw StudioException.bad("RELEASE_RUN_REQUIRED","请选择已发布工作流的顶层运行");var input=options(run);input.put("triggerType","RERUN");input.put("retryOfRunId",id);return workflows.startRelease(run.get("releaseId").toString(),input);}
    private boolean retryAllowed(Map<String,Object> trigger) {
        if(trigger.get("runId")==null) {
            return trigger.get("releaseId")==null||!Boolean.TRUE.equals(workflows.release(trigger.get("releaseId").toString()).get("containsWrites"));
        }
        return repo.run(trigger.get("runId").toString()).map(r->!Boolean.TRUE.equals(r.get("containsWrites"))).orElse(false);
    }
    public void close(){}
}
