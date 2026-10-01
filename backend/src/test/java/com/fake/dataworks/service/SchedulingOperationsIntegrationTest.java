package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.dto.ObjectInput;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties={"spring.main.web-application-type=none","studio.scheduler.enabled=false","studio.inventory.recover-on-start=false","studio.simulation.recover-on-start=false"})
@EnabledIfEnvironmentVariable(named="INVENTORY_TEST_ROOT_PASSWORD",matches=".+")
class SchedulingOperationsIntegrationTest {
    @Autowired SchedulingOperationsService operations;
    @Autowired ObjectService objects;
    @Autowired DatasourceService sources;
    @Autowired TaskService tasks;
    @Autowired TaskScheduleService schedules;
    @Autowired ScheduleService times;
    @Autowired WorkflowService workflows;
    @Autowired TaskRepository taskStore;
    @Autowired StudioRepository repo;
    @Autowired SchedulingRepository instances;
    @Autowired JdbcTemplate jdbc;
    @Autowired JsonCodec json;
    String workspace,schema,user,source,password;
    final List<String> additionalWorkspaces=new ArrayList<>();
    JdbcTemplate admin;
    @BeforeEach void setup() {
        String suffix=UUID.randomUUID().toString().replace("-","");workspace="sched-it-"+suffix;schema="scheduler_it_"+suffix;user="si_"+suffix.substring(0,20);password=UUID.randomUUID().toString();
        admin=new JdbcTemplate(new DriverManagerDataSource("jdbc:mysql://127.0.0.1:3307/?allowPublicKeyRetrieval=true","root",System.getenv("INVENTORY_TEST_ROOT_PASSWORD")));
        admin.execute("CREATE DATABASE "+schema);admin.execute("CREATE USER '"+user+"'@'%' IDENTIFIED BY '"+password+"'");admin.execute("GRANT SELECT ON "+schema+".* TO '"+user+"'@'%'");
        jdbc.update("INSERT INTO dw_workspace(id,name,code,region,workspace_type) VALUES(?,?,?,?,'TEST')",workspace,"调度验收",workspace,"local");
        source=sources.save(null,Map.of("workspaceId",workspace,"name","隔离业务库","host","127.0.0.1","port",3307,"database",schema,"username",user,"password",password)).get("id").toString();
    }
    @AfterEach void cleanup() throws Exception {
        if(workspace==null)return;var workspaces=new ArrayList<>(additionalWorkspaces);workspaces.add(workspace);for(String space:workspaces)for(var run:repo.runs(space))if(Set.of("QUEUED","RUNNING","RECOVERING").contains(run.get("status")))try{tasks.stop(run.get("id").toString());}catch(Exception ignored){}
        Thread.sleep(100);
        for(String space:workspaces)cleanupWorkspace(space);
        if(schema!=null&&schema.matches("scheduler_it_[a-f0-9]{32}")){admin.execute("DROP DATABASE "+schema);admin.execute("DROP USER '"+user+"'@'%'");}
    }
    private void cleanupWorkspace(String space){
        jdbc.update("DELETE FROM dw_task_trigger WHERE schedule_id IN (SELECT id FROM dw_task_schedule WHERE workspace_id=?)",space);jdbc.update("DELETE FROM dw_task_schedule WHERE workspace_id=?",space);jdbc.update("DELETE FROM dw_task_release WHERE workspace_id=?",space);jdbc.update("DELETE FROM dw_run WHERE workspace_id=?",space);
        jdbc.update("DELETE FROM dw_schedule_trigger WHERE schedule_id IN (SELECT id FROM dw_workflow_schedule WHERE workspace_id=?)",space);jdbc.update("DELETE FROM dw_workflow_schedule WHERE workspace_id=?",space);jdbc.update("DELETE FROM dw_workflow_release WHERE workspace_id=?",space);
        jdbc.update("DELETE FROM dw_version WHERE object_id IN (SELECT id FROM dw_object WHERE workspace_id=?)",space);jdbc.update("DELETE FROM dw_object WHERE workspace_id=?",space);jdbc.update("DELETE FROM dw_datasource WHERE workspace_id=?",space);jdbc.update("DELETE FROM dw_workspace WHERE id=?",space);
    }
    StudioObject node(String name,String version) {return objects.create(new ObjectInput(workspace,null,"NODE","MySQL",name,"","SELECT '${version}' AS v, :bizdate AS d, :source_cutoff AS cutoff",Map.of("run",Map.of("provider","MYSQL","dataSourceId",source,"timeoutSeconds",5),"schedule",Map.of("parameters",List.of(Map.of("name","version","value",version)))),List.of(),false,null));}
    Map<String,Object> plan(StudioObject node,String cron,String zone,List<Map<String,Object>> deps) {var release=tasks.publish(node.id(),node.version(),"");var input=new LinkedHashMap<String,Object>();input.putAll(Map.of("releaseId",release.get("id"),"cron",cron,"timezone",zone,"businessDateOffset",0,"enabled",false,"dependencies",deps));return schedules.save(node.id(),null,input);}
    Map<String,Object> request(Map<String,Object> plan,String day,boolean downstream) {return new LinkedHashMap<>(Map.of("workspaceId",workspace,"kind","TASK","scheduleId",plan.get("id"),"startDate",day,"endDate",day,"includeDownstream",downstream));}
    @SuppressWarnings("unchecked") List<Map<String,Object>> items(Map<String,Object> result,String key) {return (List<Map<String,Object>>)result.get(key);}
    Map<String,Object> submit(Map<String,Object> request) {var preview=operations.preview(request);var input=new LinkedHashMap<>(request);input.put("previewToken",preview.get("token"));input.put("requestId",UUID.randomUUID().toString());return operations.backfill(input);}
    void complete(String batch) throws Exception {for(int i=0;i<400;i++){schedules.scan(Instant.now(),workspace);times.scan(Instant.now(),workspace);var state=operations.batch(batch);if(items(state,"instances").stream().allMatch(t->Set.of("SUCCESS","FAILED","CANCELLED","SKIPPED").contains(t.get("status")))){assertTrue(items(state,"instances").stream().allMatch(t->"SUCCESS".equals(t.get("status"))),state.toString());return;}Thread.sleep(25);}fail("Batch did not complete: "+operations.batch(batch));}
    Map<String,Object> periodic(Map<String,Object> plan,String at) {var t=schedules.createInstance(plan,Instant.parse(at),Instant.now(),"");t.put("triggerType","RERUN");instances.insert("TASK",t);return t;}
    void completeInstances(List<Map<String,Object>> targets) throws Exception {for(int i=0;i<200;i++){schedules.scan(Instant.now(),workspace);var actual=targets.stream().map(t->taskStore.trigger(t.get("id").toString())).toList();if(actual.stream().allMatch(t->"SUCCESS".equals(t.get("status"))))return;if(actual.stream().anyMatch(t->"FAILED".equals(t.get("status"))))fail(actual.toString());Thread.sleep(25);}fail("Instances did not complete");}
    @Test void batchDependenciesAreIsolatedAndRepeatedRequestsReturnSameInstances() throws Exception {
        var upper=node("上游","up");var up=plan(upper,"0 0 * * * *","Asia/Shanghai",List.of());var lower=node("下游","down");plan(lower,"0 0 2 * * *","Asia/Shanghai",List.of(Map.of("taskId",upper.id(),"alias","upper","matchMode","ALL_DAY")));
        var request=request(up,LocalDate.now().minusDays(2).toString(),true);var preview=operations.preview(request);assertEquals(25,((Number)preview.get("total")).intValue());
        String key=UUID.randomUUID().toString();request.put("previewToken",preview.get("token"));request.put("requestId",key);var first=operations.backfill(request);var again=operations.backfill(request);assertEquals(items(first,"instances").stream().map(t->t.get("id")).toList(),items(again,"instances").stream().map(t->t.get("id")).toList());
        var downstream=items(first,"instances").stream().filter(t->lower.id().equals(t.get("taskId"))).findFirst().orElseThrow();assertEquals(24,items(downstream,"dependencySlots").size());assertTrue(items(downstream,"dependencySlots").stream().allMatch(d->key.equals(d.get("batchKey"))));
        complete(key);assertEquals(0,jdbc.queryForObject("SELECT COUNT(*) FROM dw_task_trigger WHERE schedule_id=? AND batch_key=''",Integer.class,up.get("id")));
        assertEquals(25L,operations.instances(Map.of("workspaceId",workspace,"batchKey",key,"pageSize","100")).get("total"));
        operations.rerun("TASK",downstream.get("id").toString(),Map.of("mode","LATEST"));complete(key);assertEquals(24,items(instances.instance("TASK",downstream.get("id").toString()),"upstreamRuns").size());
    }
    @Test void concurrentSameRequestIdAcrossWorkspacesCreatesExactlyOneUnmixedBatch() throws Exception {
        String secondWorkspace="sched-it-"+UUID.randomUUID().toString().replace("-","");additionalWorkspaces.add(secondWorkspace);jdbc.update("INSERT INTO dw_workspace(id,name,code,region,workspace_type) VALUES(?,?,?,?,'TEST')",secondWorkspace,"并发调度验收",secondWorkspace,"local");
        String secondSource=sources.save(null,Map.of("workspaceId",secondWorkspace,"name","隔离业务库","host","127.0.0.1","port",3307,"database",schema,"username",user,"password",password)).get("id").toString();
        var secondNode=objects.create(new ObjectInput(secondWorkspace,null,"NODE","MySQL","另一空间任务","","SELECT 1",Map.of("run",Map.of("provider","MYSQL","dataSourceId",secondSource,"timeoutSeconds",5)),List.of(),false,null));
        var firstPlan=plan(node("本空间任务","v1"),"0 * * * * *","Asia/Shanghai",List.of());var secondPlan=plan(secondNode,"0 * * * * *","Asia/Shanghai",List.of());String key=UUID.randomUUID().toString();
        var firstRequest=request(firstPlan,"2026-09-21",false);var secondRequest=request(secondPlan,"2026-09-21",false);secondRequest.put("workspaceId",secondWorkspace);
        for(var input:List.of(firstRequest,secondRequest)){input.put("startTime","01:00");input.put("endTime","01:02");input.put("previewToken",operations.preview(input).get("token"));input.put("requestId",key);}
        var ready=new CountDownLatch(2);var start=new CountDownLatch(1);var workers=Executors.newFixedThreadPool(2);
        try{
            List<Future<Object>> submissions=new ArrayList<>();for(var input:List.of(firstRequest,secondRequest))submissions.add(workers.submit(()->{ready.countDown();if(!start.await(5,TimeUnit.SECONDS))throw new IllegalStateException("Concurrent submissions did not start");try{return operations.backfill(input);}catch(StudioException e){return e.code();}}));assertTrue(ready.await(5,TimeUnit.SECONDS));start.countDown();
            List<Object> results=new ArrayList<>();for(var submission:submissions)results.add(submission.get(30,TimeUnit.SECONDS));assertEquals(1,results.stream().filter(Map.class::isInstance).count());assertEquals(1,results.stream().filter("IDEMPOTENCY_CONFLICT"::equals).count());
            var stored=items(operations.batch(key),"instances");assertEquals(3,stored.size());String winner=stored.getFirst().get("workspaceId").toString();assertTrue(Set.of(workspace,secondWorkspace).contains(winner));assertTrue(stored.stream().allMatch(t->winner.equals(t.get("workspaceId"))&&key.equals(t.get("batchKey"))));assertEquals(3,jdbc.queryForObject("SELECT COUNT(*) FROM dw_task_trigger WHERE batch_key=?",Integer.class,key));
            String loser=winner.equals(workspace)?secondWorkspace:workspace;assertEquals(0,jdbc.queryForObject("SELECT COUNT(*) FROM dw_task_trigger t JOIN dw_task_schedule s ON s.id=t.schedule_id WHERE t.batch_key=? AND s.workspace_id=?",Integer.class,key,loser));
        }finally{start.countDown();workers.shutdownNow();assertTrue(workers.awaitTermination(5,TimeUnit.SECONDS));}
    }
    @Test void previewChecksDefinitionButAllowsCursorToAdvanceAndEnforcesLimit() {
        var p=plan(node("分钟","v1"),"0 * * * * *","Asia/Shanghai",List.of());var request=request(p,"2026-09-20",false);request.put("startTime","01:00");request.put("endTime","01:02");var preview=operations.preview(request);assertEquals(3,((Number)preview.get("total")).intValue());
        p.put("nextFireAt",Instant.now().toString());taskStore.saveSchedule(p,false);request.put("previewToken",preview.get("token"));request.put("requestId",UUID.randomUUID().toString());assertEquals(3,((Number)operations.backfill(request).get("total")).intValue());
        var stale=request(p,"2026-09-20",false);stale.put("startTime","01:00");stale.put("endTime","01:01");var old=operations.preview(stale);p.put("version",((Number)p.get("version")).intValue()+1);taskStore.saveSchedule(p,false);stale.put("previewToken",old.get("token"));stale.put("requestId",UUID.randomUUID().toString());assertEquals("PREVIEW_CHANGED",assertThrows(StudioException.class,()->operations.backfill(stale)).code());
        var large=request(p,"2026-09-20",false);large.put("endDate","2026-09-24");assertEquals("BACKFILL_LIMIT",assertThrows(StudioException.class,()->operations.preview(large)).code());
    }
    @Test void daylightSavingDaysEnumerateEveryMinuteAndHour() {
        var p=plan(node("夏令时","v1"),"0 0 * * * *","America/New_York",List.of());assertEquals(25,((Number)operations.preview(request(p,"2026-11-01",false)).get("total")).intValue());assertEquals(23,((Number)operations.preview(request(p,"2026-03-08",false)).get("total")).intValue());
        p.put("cron","0 * * * * *");taskStore.saveSchedule(p,false);assertEquals(1500,((Number)operations.preview(request(p,"2026-11-01",false)).get("total")).intValue());
    }
    @Test void downstreamClosureDoesNotResurrectDeletedTasks() {
        var upper=node("活动上游","up");var up=plan(upper,"0 0 2 * * *","Asia/Shanghai",List.of());var lower=node("已删除下游","down");plan(lower,"0 0 3 * * *","Asia/Shanghai",List.of(Map.of("taskId",upper.id(),"alias","upper")));
        var request=request(up,"2026-09-21",true);var preview=operations.preview(request);assertEquals(2,((Number)preview.get("total")).intValue());objects.delete(lower.id());assertEquals(1,((Number)operations.preview(request).get("total")).intValue());
        request.put("previewToken",preview.get("token"));request.put("requestId",UUID.randomUUID().toString());assertEquals("PREVIEW_CHANGED",assertThrows(StudioException.class,()->operations.backfill(request)).code());
    }
    @Test void latestRerunRefreshesParametersAndOriginalReproducesSelectedAttempt() throws Exception {
        var n=node("版本","v1");var p=plan(n,"0 0 2 * * *","Asia/Shanghai",List.of());var batch=submit(request(p,LocalDate.now().minusDays(2).toString(),false));String key=batch.get("batchKey").toString();complete(key);var first=items(operations.batch(key),"instances").getFirst();String oldRun=first.get("runId").toString();
        var config=new LinkedHashMap<>(n.config());config.put("schedule",Map.of("parameters",List.of(Map.of("name","version","value","v2"))));var updated=objects.update(n.id(),new ObjectInput(workspace,null,n.kind(),n.nodeType(),n.name(),"",n.content(),config,List.of(),false,n.version()));var r2=tasks.publish(n.id(),updated.version(),"");Thread.sleep(5);
        operations.rerun("TASK",first.get("id").toString(),Map.of("mode","LATEST"));complete(key);var second=instances.instance("TASK",first.get("id").toString());var latest=repo.run(second.get("runId").toString()).orElseThrow();assertEquals(r2.get("id"),latest.get("releaseId"));assertEquals("v2",((Map<?,?>)latest.get("scheduleParameters")).get("version"));assertNotEquals(repo.run(oldRun).orElseThrow().get("sourceCutoffAt"),latest.get("sourceCutoffAt"));assertEquals(first.get("scheduledAt"),latest.get("scheduledAt"));
        operations.rerun("TASK",first.get("id").toString(),Map.of("mode","ORIGINAL","attemptRunId",oldRun));complete(key);var third=instances.instance("TASK",first.get("id").toString());var original=repo.run(third.get("runId").toString()).orElseThrow();assertEquals("v1",((Map<?,?>)original.get("scheduleParameters")).get("version"));assertEquals(repo.run(oldRun).orElseThrow().get("sourceCutoffAt"),original.get("sourceCutoffAt"));assertEquals(3,items(operations.instance("TASK",first.get("id").toString()),"attempts").size());
    }
    @Test void latestRerunRefreshesUpstreamAndAppliedDependencyWhileOriginalRestoresItsObservation() throws Exception {
        var a=node("上游A","A");var pa=plan(a,"0 0 2 * * *","Asia/Shanghai",List.of());var b=node("上游B","B");var pb=plan(b,"0 0 2 * * *","Asia/Shanghai",List.of());var lower=node("依赖切换","lower");var pl=plan(lower,"0 0 3 * * *","Asia/Shanghai",List.of(Map.of("taskId",a.id(),"alias","source")));
        var ta=periodic(pa,"2026-09-20T18:00:00Z");var tb=periodic(pb,"2026-09-20T18:00:00Z");var tl=periodic(pl,"2026-09-20T19:00:00Z");completeInstances(List.of(ta,tb,tl));String original=taskStore.trigger(tl.get("id").toString()).get("runId").toString();String firstUpper=taskStore.trigger(ta.get("id").toString()).get("runId").toString();
        operations.rerun("TASK",ta.get("id").toString(),Map.of("mode","LATEST"));completeInstances(List.of(ta));String newUpper=taskStore.trigger(ta.get("id").toString()).get("runId").toString();assertNotEquals(firstUpper,newUpper);
        operations.rerun("TASK",tl.get("id").toString(),Map.of("mode","LATEST"));completeInstances(List.of(tl));assertEquals(newUpper,items(taskStore.trigger(tl.get("id").toString()),"upstreamRuns").getFirst().get("runId"));
        var changed=new LinkedHashMap<>(pl);changed.put("expectedVersion",pl.get("version"));changed.put("dependencies",List.of(Map.of("taskId",b.id(),"alias","source")));schedules.save(lower.id(),pl.get("id").toString(),changed);
        operations.rerun("TASK",tl.get("id").toString(),Map.of("mode","LATEST"));completeInstances(List.of(tl));assertEquals(b.id(),items(taskStore.trigger(tl.get("id").toString()),"upstreamRuns").getFirst().get("taskId"));
        operations.rerun("TASK",tl.get("id").toString(),Map.of("mode","ORIGINAL","attemptRunId",original));completeInstances(List.of(tl));var restored=operations.instance("TASK",tl.get("id").toString());assertEquals(firstUpper,items(restored,"upstreamRuns").getFirst().get("runId"));assertEquals(a.id(),items(restored,"dependencySlots").getFirst().get("taskId"));
        assertEquals(3L,operations.instances(Map.of("workspaceId",workspace,"source","RERUN")).get("total"));assertEquals(0L,operations.instances(Map.of("workspaceId",workspace,"source","SCHEDULED")).get("total"));
    }
    @Test void backfillWhilePausedAndInstanceWithoutRunRemainVisible() throws Exception {
        var p=plan(node("已暂停","v1"),"0 0 2 * * *","Asia/Shanghai",List.of());var periodic=schedules.createInstance(p,Instant.parse("2026-09-20T18:00:00Z"),Instant.now(),"");periodic.put("status","SKIPPED");periodic.put("reason","MISSED_INTERVAL");instances.insert("TASK",periodic);
        assertEquals("MISSED_INTERVAL",operations.instance("TASK",periodic.get("id").toString()).get("reason"));assertNull(operations.instance("TASK",periodic.get("id").toString()).get("runId"));
        var backfill=submit(request(p,"2026-09-21",false));complete(backfill.get("batchKey").toString());assertFalse(Boolean.TRUE.equals(taskStore.schedule(p.get("id").toString()).get("enabled")));assertEquals("SKIPPED",operations.instance("TASK",periodic.get("id").toString()).get("status"));
    }
    @Test void workflowUsesTheSameBackfillAndInstanceOperations() throws Exception {
        var n=node("组合子节点","v1");var w=objects.create(new ObjectInput(workspace,null,"WORKFLOW","手动工作流","组合任务","","",Map.of("run",Map.of("provider","WORKFLOW"),"graph",Map.of("nodes",List.of(Map.of("id","n","objectId",n.id(),"label",n.name(),"nodeType","MySQL","x",0,"y",0)),"edges",List.of())),List.of(),false,null));
        var release=workflows.publish(w.id(),w.version(),Map.of(n.id(),n.version()),"");var plan=times.save(w.id(),null,Map.of("releaseId",release.get("id"),"cron","0 0 2 * * *","timezone","Asia/Shanghai","businessDateOffset",0,"enabled",false));
        var request=request(plan,"2026-09-21",false);request.put("kind","WORKFLOW");var result=submit(request);String batch=result.get("batchKey").toString();complete(batch);var first=items(operations.batch(batch),"instances").getFirst();assertEquals("WORKFLOW",first.get("kind"));
        var r2=workflows.publish(w.id(),w.version(),Map.of(n.id(),n.version()),"new");operations.rerun("WORKFLOW",first.get("id").toString(),Map.of("mode","LATEST"));complete(batch);var latest=operations.instance("WORKFLOW",first.get("id").toString());assertEquals(r2.get("id"),latest.get("releaseId"));assertEquals(2,items(latest,"attempts").size());
    }
}
