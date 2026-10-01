package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.repository.*;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** In-memory scheduler checks: no Spring context, JDBC connection, or business writes. */
class TaskInstanceSchedulerTest {
    @Test void restartRecordsEveryMissedSlotWithinOneGlobalGenerationBudget(){
        var f=new Fixture();Instant now=Instant.parse("2026-09-28T10:00:00Z");f.plan("a",now.minusSeconds(300*60));f.plan("b",now.minusSeconds(300*60));
        assertEquals(200,f.service.resume(now,"ws",200));assertEquals(200,f.store.instances.size());
        assertEquals(200,f.service.scan(now,"ws",200));assertEquals(400,f.store.instances.size());
        assertTrue(f.store.instances.values().stream().allMatch(t->"SKIPPED".equals(t.get("status"))&&"MISSED_INTERVAL".equals(t.get("reason"))));
        f.service.scan(now,"ws",200);f.service.scan(now,"ws",200);int total=f.store.instances.size();f.service.scan(now,"ws",200);assertEquals(602,total);assertEquals(total,f.store.instances.size());verify(f.tasks,never()).startRelease(anyString(),anyMap());
    }
    @Test void ordinaryScanDelayQueuesEachSlotInsteadOfTreatingDelayAsDowntime(){
        var f=new Fixture();Instant now=Instant.parse("2026-09-28T10:00:00Z");f.plan("a",now.minusSeconds(180));f.service.scan(now,"ws");
        assertEquals(4,f.store.instances.size());assertEquals(1,f.store.instances.values().stream().filter(t->"RUNNING".equals(t.get("status"))).count());assertEquals(3,f.store.instances.values().stream().filter(t->"WAITING_RESOURCE".equals(t.get("status"))).count());assertFalse(f.store.instances.values().stream().anyMatch(t->"SKIPPED".equals(t.get("status"))));
        f.service.scan(now,"ws");assertEquals(4,f.store.instances.size());verify(f.tasks,times(1)).startRelease(anyString(),anyMap());
    }
    @Test void allDayWaitsForFutureSlotsThenRepairsBlockedDependencyWithBoundedQueries(){
        var f=new Fixture();Instant early=Instant.parse("2026-09-28T02:05:00Z");var up=f.plan("up",early.plusSeconds(3600));up.put("cron","0 0 * * * *");f.store.saveSchedule(up,false);
        var down=f.plan("down",early.plusSeconds(86400));down.put("cron","0 5 2 * * *");down.put("dependencies",List.of(Map.of("taskId","up","alias","up","matchMode","ALL_DAY")));f.store.saveSchedule(down,false);
        var instance=f.service.createInstance(down,early,early,"");f.store.insertTrigger(instance);for(int i=0;i<3;i++)f.upstream(up,Instant.parse("2026-09-28T00:00:00Z").plusSeconds(i*3600),"SUCCESS","");
        f.service.scan(early,"ws");assertEquals("WAITING_DEPENDENCY",f.store.trigger(instance.get("id").toString()).get("status"));verify(f.tasks,never()).startRelease(anyString(),anyMap());
        for(int i=3;i<24;i++)f.upstream(up,Instant.parse("2026-09-28T00:00:00Z").plusSeconds(i*3600),i==17?"FAILED":"SUCCESS","");Instant late=Instant.parse("2026-09-29T00:00:00Z");up.put("nextFireAt",late.plusSeconds(3600).toString());down.put("nextFireAt",late.plusSeconds(3600).toString());f.store.saveSchedule(up,false);f.store.saveSchedule(down,false);
        f.service.scan(late,"ws");assertEquals("BLOCKED",f.store.trigger(instance.get("id").toString()).get("status"));
        var repaired=f.store.at(up.get("id").toString(),"2026-09-28T17:00:00Z").orElseThrow();repaired.put("status","SUCCESS");f.store.updateTrigger(repaired);f.store.rangeCalls=0;clearInvocations(f.repo);f.service.scan(late,"ws");
        var done=f.store.trigger(instance.get("id").toString());assertEquals("RUNNING",done.get("status"));assertEquals(24,((List<?>)done.get("upstreamRuns")).size());assertEquals(1,f.store.rangeCalls);verify(f.repo,times(1)).runsByIds(anyCollection());verify(f.repo,never()).run(anyString());
    }
    @Test void backfillNeverFallsBackToCanonicalOrAnotherBatch(){
        var f=new Fixture();Instant fire=Instant.parse("2026-09-28T02:00:00Z");var up=f.plan("up",fire.plusSeconds(86400));up.put("cron","0 0 2 * * *");f.store.saveSchedule(up,false);var down=f.plan("down",fire.plusSeconds(86400));down.put("dependencies",List.of(Map.of("taskId","up","alias","up")));f.store.saveSchedule(down,false);
        f.upstream(up,fire,"SUCCESS","");f.upstream(up,fire,"SUCCESS","other");var d=f.service.createInstance(down,fire,fire,"wanted");((List<Map<String,Object>>)d.get("dependencySlots")).getFirst().put("batchKey","wanted");f.store.insertTrigger(d);f.service.scan(fire,"ws");assertEquals("BLOCKED",f.store.trigger(d.get("id").toString()).get("status"));verify(f.tasks,never()).startRelease(anyString(),anyMap());
        f.upstream(up,fire,"SUCCESS","wanted");f.service.scan(fire,"ws");assertEquals("RUNNING",f.store.trigger(d.get("id").toString()).get("status"));
    }
    @Test void pauseWithdrawsQueuedAttemptAndResumePreservesContextWhileBackfillRemainsRunnable(){
        var f=new Fixture();Instant now=Instant.now().truncatedTo(ChronoUnit.MINUTES);var plan=f.plan("a",now.plusSeconds(3600));var a=f.service.createInstance(plan,now,now,"");a.put("status","RUNNING");a.put("runId","old");f.store.insertTrigger(a);f.runs.put("old",new LinkedHashMap<>(Map.of("id","old","status","QUEUED")));when(f.tasks.pauseQueuedRun("old")).thenReturn(true);
        f.service.setState(plan.get("id").toString(),false,1);var paused=f.store.trigger(a.get("id").toString());assertEquals("PAUSED",paused.get("status"));assertEquals(2,((Number)paused.get("attempt")).intValue());assertEquals(a.get("sourceCutoffAt"),paused.get("sourceCutoffAt"));assertNull(paused.get("runId"));
        var backfill=f.service.createInstance(plan,now,now,"batch");f.store.insertTrigger(backfill);f.service.scan(now,"ws",0);assertEquals("RUNNING",f.store.trigger(backfill.get("id").toString()).get("status"));assertEquals("PAUSED",f.store.trigger(a.get("id").toString()).get("status"));
        f.service.setState(plan.get("id").toString(),true,2);assertEquals("PENDING",f.store.trigger(a.get("id").toString()).get("status"));assertEquals(a.get("scheduleParameters"),f.store.trigger(a.get("id").toString()).get("scheduleParameters"));
    }
    @Test void latestRerunFreezesNewCutoffAndOriginalCanReplaySelectedHistoricalAttempt(){
        assertOriginalRestoresAcceptedDependencyGraph(false);
    }
    @Test void legacyOriginalRebuildsObservationsFromAcceptedInputsRatherThanLatestGraph(){assertOriginalRestoresAcceptedDependencyGraph(true);}
    @SuppressWarnings("unchecked") private void assertOriginalRestoresAcceptedDependencyGraph(boolean legacy){
        var f=new Fixture();Instant fire=Instant.now().minusSeconds(3600).truncatedTo(ChronoUnit.MINUTES),next=Instant.now().plusSeconds(3600);var upA=f.plan("up-a",next);var upB=f.plan("up-b",next);f.upstream(upA,fire,"SUCCESS","");f.upstream(upB,fire,"SUCCESS","");var plan=f.plan("a",next);plan.put("dependencies",List.of(Map.of("taskId","up-a","alias","source","matchMode","LATEST")));plan.put("retries",1);f.store.saveSchedule(plan,false);
        var t=f.service.createInstance(plan,fire,fire,"");String id=t.get("id").toString();f.store.insertTrigger(t);f.service.scan(Instant.now(),"ws",0);String first=f.store.trigger(id).get("runId").toString();f.finish(first,id);
        assertEquals("up-a",((List<Map<String,Object>>)f.runs.get(first).get("dependencySlots")).getFirst().get("taskId"));assertEquals(1,((Number)((Map<String,Object>)f.runs.get(first).get("scheduleSnapshot")).get("retries")).intValue());
        if(legacy){var historical=f.store.trigger(id);historical.remove("originalContext");for(var attempt:(List<Map<String,Object>>)historical.get("attempts"))attempt.remove("context");f.store.updateTrigger(historical);f.runs.get(first).remove("dependencySlots");f.runs.get(first).remove("scheduleSnapshot");}
        plan.put("dependencies",List.of(Map.of("taskId","up-b","alias","source","matchMode","LATEST")));plan.put("retries",5);f.store.saveSchedule(plan,false);f.store.latest.put("a",Map.of("id","r2-a","taskId","a","releaseNo",2));var latest=f.service.rerun(id,"LATEST",null);assertEquals("r2-a",latest.get("releaseId"));assertNotEquals(t.get("sourceCutoffAt"),latest.get("sourceCutoffAt"));assertEquals(t.get("scheduledAt"),latest.get("scheduledAt"));assertEquals("up-b",((List<Map<String,Object>>)latest.get("dependencySlots")).getFirst().get("taskId"));String second=latest.get("runId").toString();f.finish(second,id);
        var original=f.service.rerun(id,"ORIGINAL",first);assertEquals("r1-a",original.get("releaseId"));assertEquals(t.get("sourceCutoffAt"),original.get("sourceCutoffAt"));assertEquals(t.get("scheduleParameters"),original.get("scheduleParameters"));assertEquals(t.get("businessDate"),original.get("businessDate"));assertEquals(3,((Number)original.get("attempt")).intValue());assertEquals("up-a",((List<Map<String,Object>>)original.get("dependencySlots")).getFirst().get("taskId"));assertEquals("SUCCESS",((List<Map<String,Object>>)original.get("dependencySlots")).getFirst().get("status"));assertEquals("up-a",((List<Map<String,Object>>)original.get("upstreamRuns")).getFirst().get("taskId"));var restored=(Map<String,Object>)original.get("scheduleSnapshot");assertEquals(1,((Number)restored.get("retries")).intValue());assertEquals("up-a",((List<Map<String,Object>>)restored.get("dependencies")).getFirst().get("taskId"));
    }
    @Test void queueFullWaitsForCapacityWithoutConsumingRetry(){
        var f=new Fixture();Instant now=Instant.now();var p=f.plan("a",now.plusSeconds(3600));p.put("retries",1);f.store.saveSchedule(p,false);var t=f.service.createInstance(p,now,now,"");f.store.insertTrigger(t);when(f.tasks.startRelease(anyString(),anyMap())).thenThrow(new com.fake.dataworks.exception.StudioException("QUEUE_FULL","full",429));f.service.scan(now,"ws",0);var waiting=f.store.trigger(t.get("id").toString());assertEquals("WAITING_RESOURCE",waiting.get("status"));assertEquals(1,((Number)waiting.get("attempt")).intValue());assertNull(waiting.get("nextRetryAt"));
    }
    @Test void restartWithdrawsUnstartedExplicitBatchWithoutLosingItsInputs(){
        var f=new Fixture();Instant now=Instant.now();var p=f.plan("a",now.plusSeconds(3600));var t=f.service.createInstance(p,now,now,"batch");t.put("status","RUNNING");t.put("runId","old");t.put("upstreamRuns",List.of());f.store.insertTrigger(t);f.runs.put("old",new LinkedHashMap<>(Map.of("id","old","status","QUEUED")));when(f.tasks.withdrawQueued("old","SERVICE_RESTARTED_QUEUED")).thenReturn(true);
        var enabled=new TaskScheduleService(f.store,f.repo,f.tasks,f.times,f.tx,true);enabled.ready(now,0);var restored=f.store.trigger(t.get("id").toString());assertEquals("PENDING",restored.get("status"));assertEquals("batch",restored.get("batchKey"));assertEquals(t.get("sourceCutoffAt"),restored.get("sourceCutoffAt"));assertEquals(List.of(),restored.get("upstreamRuns"));assertNull(restored.get("runId"));
    }
    static class Fixture {
        final MemoryStore store=new MemoryStore();final StudioRepository repo=mock(StudioRepository.class);final TaskService tasks=mock(TaskService.class);final Map<String,Map<String,Object>> runs=new HashMap<>();final ScheduleService times=new ScheduleService(null,null,null,null,null,null,null,false);
        final TransactionTemplate tx=new TransactionTemplate(){@Override public <T> T execute(TransactionCallback<T> action){return action.doInTransaction(null);}};
        final TaskScheduleService service=new TaskScheduleService(store,repo,tasks,times,tx,false);
        Fixture(){
            when(repo.find(anyString())).thenAnswer(c->Optional.of(object(c.getArgument(0))));when(tasks.task(anyString())).thenAnswer(c->object(c.getArgument(0)));
            when(tasks.release(anyString())).thenAnswer(c->{String id=c.getArgument(0);String task=id.substring(id.indexOf('-')+1);var snapshot=object(task);return Map.of("id",id,"taskId",task,"releaseNo",id.startsWith("r2")?2:1,"snapshot",snapshot,"containsWrites",false);});
            when(repo.run(anyString())).thenAnswer(c->Optional.ofNullable(runs.get(c.getArgument(0))));when(repo.runsByIds(anyCollection())).thenAnswer(c->{Map<String,Map<String,Object>> result=new HashMap<>();for(String id:(Collection<String>)c.getArgument(0))if(runs.containsKey(id))result.put(id,runs.get(id));return result;});
            when(tasks.input(anyMap(),anyMap())).thenAnswer(c->{var d=new LinkedHashMap<String,Object>(c.getArgument(0));Map<String,Object> r=c.getArgument(1);d.put("runId",r.get("id"));return d;});
            when(tasks.startRelease(anyString(),anyMap())).thenAnswer(c->{String release=c.getArgument(0);var run=new LinkedHashMap<String,Object>((Map<String,Object>)c.getArgument(1));run.put("id",UUID.randomUUID().toString());run.put("status","QUEUED");run.put("releaseId",release);run.put("releaseNo",release.startsWith("r2")?2:1);runs.put(run.get("id").toString(),run);return run;});
        }
        StudioObject object(String id){return new StudioObject(id,"ws",null,"NODE","MySQL",id,"","SELECT 1",Map.of(),List.of(),false,false,1,"owner","");}
        Map<String,Object> plan(String task,Instant next){var p=times.timeConfig(Map.of("cron","0 * * * * *","timezone","UTC","businessDateOffset",0,"enabled",true));p.put("id","s-"+task);p.put("taskId",task);p.put("workspaceId","ws");p.put("name",task);p.put("releaseId","r1-"+task);p.put("releaseNo",1);p.put("version",1);p.put("dependencies",List.of());p.put("nextFireAt",next.toString());store.saveSchedule(p,true);return p;}
        void upstream(Map<String,Object> plan,Instant fire,String status,String batch){String id=UUID.randomUUID().toString();var t=new LinkedHashMap<String,Object>();t.put("id",id);t.put("scheduleId",plan.get("id"));t.put("taskId",plan.get("taskId"));t.put("workspaceId","ws");t.put("scheduledAt",fire.toString());t.put("status",status);t.put("batchKey",batch);t.put("runId","run-"+id);store.insertTrigger(t);runs.put("run-"+id,new LinkedHashMap<>(Map.of("id","run-"+id,"status","SUCCESS","businessDate",fire.atZone(ZoneOffset.UTC).toLocalDate().toString(),"workspaceId","ws")));}
        void finish(String run,String trigger){runs.get(run).put("status","SUCCESS");var t=store.trigger(trigger);t.put("status","SUCCESS");store.updateTrigger(t);}
    }
    static class MemoryStore extends TaskRepository {
        final JsonCodec json=new JsonCodec();final Map<String,Map<String,Object>> plans=new LinkedHashMap<>(),instances=new LinkedHashMap<>(),latest=new HashMap<>();int rangeCalls;
        MemoryStore(){super(null,null);}
        Map<String,Object> copy(Map<String,Object> m){return json.map(json.write(m));}
        @Override public List<Map<String,Object>> schedules(String w){return plans.values().stream().filter(p->w.equals(p.get("workspaceId"))).map(this::copy).toList();}
        @Override public List<Map<String,Object>> allSchedules(){return plans.values().stream().map(this::copy).toList();}
        @Override public Optional<Map<String,Object>> forTask(String t){return plans.values().stream().filter(p->t.equals(p.get("taskId"))).map(this::copy).findFirst();}
        @Override public Map<String,Object> schedule(String id){return copy(plans.get(id));}
        @Override public void lockSchedule(String id){}
        @Override public void lockTrigger(String id){}
        @Override public void saveSchedule(Map<String,Object> p,boolean insert){plans.put(p.get("id").toString(),copy(p));}
        @Override public Map<String,Object> latestRelease(String task){return copy(latest.getOrDefault(task,Map.of("id","r1-"+task,"releaseNo",1,"taskId",task)));}
        @Override public boolean insertTrigger(Map<String,Object> t){if(at(t.get("scheduleId").toString(),t.get("scheduledAt").toString(),Objects.toString(t.get("batchKey"),"")).isPresent())return false;instances.put(t.get("id").toString(),copy(t));return true;}
        @Override public void updateTrigger(Map<String,Object> t){instances.put(t.get("id").toString(),copy(t));}
        @Override public Map<String,Object> trigger(String id){return copy(instances.get(id));}
        @Override public Optional<Map<String,Object>> at(String s,String at,String b){return instances.values().stream().filter(t->s.equals(t.get("scheduleId"))&&at.equals(t.get("scheduledAt"))&&b.equals(Objects.toString(t.get("batchKey"),""))).map(this::copy).findFirst();}
        @Override public List<Map<String,Object>> pending(String w){return instances.values().stream().filter(t->(w.isEmpty()||w.equals(t.get("workspaceId")))&&Set.of("PENDING","WAITING_DEPENDENCY","WAITING_RESOURCE","RUNNING","RETRY_WAIT","PAUSED","BLOCKED").contains(t.get("status"))).sorted(Comparator.comparing(t->t.get("scheduledAt").toString())).map(this::copy).toList();}
        @Override public List<Map<String,Object>> pendingForSchedule(String s){return pending("").stream().filter(t->s.equals(t.get("scheduleId"))).toList();}
        @Override public List<Map<String,Object>> slotRange(String s,String b,String from,String to){rangeCalls++;return instances.values().stream().filter(t->s.equals(t.get("scheduleId"))&&b.equals(Objects.toString(t.get("batchKey"),""))&&t.get("scheduledAt").toString().compareTo(from)>=0&&t.get("scheduledAt").toString().compareTo(to)<=0).map(this::copy).toList();}
    }
}
