package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.dto.ObjectInput;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.StudioRepository;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.math.BigDecimal;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.transaction.support.TransactionTemplate;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

@SpringBootTest(properties={"spring.main.web-application-type=none","studio.simulation.recover-on-start=false","studio.scheduler.enabled=false","studio.inventory.recover-on-start=false"})
@EnabledIfEnvironmentVariable(named="INVENTORY_TEST_ROOT_PASSWORD",matches=".+")
class InventoryIntegrationTest {
    @Autowired WorkflowService workflows; @Autowired RunService runs; @Autowired ObjectService objects;
    @Autowired DatasourceService sources; @Autowired JdbcTemplate jdbc; @Autowired JsonCodec json;
    @MockitoSpyBean StudioRepository repo; @Autowired ScheduleService schedules; @Autowired TransactionTemplate tx;
    @MockitoSpyBean InventoryExecutionService inventory;
    @Autowired TaskService tasks; @Autowired TaskScheduleService taskSchedules; @Autowired com.fake.dataworks.repository.TaskRepository taskStore;
    JdbcTemplate business; String schema,workspace,sourceId,user; LocalDate day1,day2; StudioObject workflow; List<StudioObject> nodes;
    @BeforeEach void setup() throws Exception {
        String suffix=UUID.randomUUID().toString().replace("-","");schema="studio_inventory_test_"+suffix;user="it_"+suffix.substring(0,20);workspace="inventory-it-"+suffix;
        var root=new DriverManagerDataSource("jdbc:mysql://127.0.0.1:3307/?allowPublicKeyRetrieval=true&characterEncoding=UTF-8","root",System.getenv("INVENTORY_TEST_ROOT_PASSWORD"));
        String password=UUID.randomUUID().toString();
        try(Connection c=root.getConnection()){
            for(String file:List.of("inventory-schema.sql","inventory-data.sql"))ScriptUtils.executeSqlScript(c,new org.springframework.core.io.support.EncodedResource(new ByteArrayResource(Files.readString(Path.of("../scripts",file)).replace("studio_inventory",schema).getBytes(StandardCharsets.UTF_8)),StandardCharsets.UTF_8));
            try(Statement s=c.createStatement()){
                s.execute("CREATE USER '"+user+"'@'%' IDENTIFIED BY '"+password+"'");s.execute("GRANT SELECT ON "+schema+".* TO '"+user+"'@'%'");
                for(String target:InventorySql.TARGETS)for(String table:List.of(target,"etl_stage_"+target))s.execute("GRANT INSERT,DELETE ON "+schema+"."+table+" TO '"+user+"'@'%'");
                s.execute("GRANT INSERT ON "+schema+".etl_publish_receipt TO '"+user+"'@'%'");s.execute("GRANT INSERT,UPDATE ON "+schema+".etl_partition_publication TO '"+user+"'@'%'");
            }
        }
        business=new JdbcTemplate(new DriverManagerDataSource("jdbc:mysql://127.0.0.1:3307/"+schema+"?allowPublicKeyRetrieval=true&characterEncoding=UTF-8","root",System.getenv("INVENTORY_TEST_ROOT_PASSWORD")));
        day1=business.queryForObject("SELECT first_day FROM inventory_demo_config",LocalDate.class);day2=day1.plusDays(1);
        jdbc.update("INSERT INTO dw_workspace VALUES(?,?,?,?)",workspace,"库存集成测试",workspace,"local");
        var config=new LinkedHashMap<String,Object>(Map.of("workspaceId",workspace,"name","库存测试源","host","127.0.0.1","port",3307,"database",schema,"username",user,"password",password));config.put("materializationEnabled",true);config.put("materializationTargets",InventorySql.TARGETS);
        sourceId=sources.save(null,config).get("id").toString();nodes=new ArrayList<>();
        var sql=List.of(InventorySql.dwd(),InventorySql.dws(),InventorySql.ads());
        for(int i=0;i<3;i++)nodes.add(objects.create(new ObjectInput(workspace,null,"NODE","MySQL",InventorySql.TARGETS.get(i),"",sql.get(i),Map.of("run",Map.of("provider","MYSQL","executionMode","MATERIALIZE","dataSourceId",sourceId,"targetTable",InventorySql.TARGETS.get(i),"timeoutSeconds",10)),List.of(),false,null)));
        List<Map<String,Object>> graphNodes=new ArrayList<>();for(int i=0;i<3;i++)graphNodes.add(Map.of("id","n"+i,"objectId",nodes.get(i).id(),"label",nodes.get(i).name(),"nodeType","MySQL","x",i*220,"y",0));
        workflow=objects.create(new ObjectInput(workspace,null,"WORKFLOW","手动工作流","库存测试流程","","",Map.of("run",Map.of("provider","WORKFLOW"),"graph",Map.of("nodes",graphNodes,"edges",List.of(Map.of("id","e1","source","n0","target","n1"),Map.of("id","e2","source","n1","target","n2")))),List.of(),false,null));
    }
    @AfterEach void cleanup() throws Exception {
        reset(inventory,repo);
        for(var run:repo.topRuns(workspace))try{runs.stop(run.get("id").toString());}catch(Exception ignored){}
        Thread.sleep(300);
        jdbc.update("DELETE FROM dw_task_trigger WHERE schedule_id IN (SELECT id FROM dw_task_schedule WHERE workspace_id=?)",workspace);jdbc.update("DELETE FROM dw_task_schedule WHERE workspace_id=?",workspace);jdbc.update("DELETE FROM dw_task_release WHERE workspace_id=?",workspace);
        jdbc.update("DELETE FROM dw_schedule_trigger WHERE schedule_id IN (SELECT id FROM dw_workflow_schedule WHERE workspace_id=?)",workspace);
        jdbc.update("DELETE FROM dw_workflow_schedule WHERE workspace_id=?",workspace);jdbc.update("DELETE FROM dw_workflow_release WHERE workspace_id=?",workspace);jdbc.update("DELETE FROM dw_run WHERE workspace_id=?",workspace);
        jdbc.update("DELETE FROM dw_version WHERE object_id IN (SELECT id FROM dw_object WHERE workspace_id=?)",workspace);jdbc.update("DELETE FROM dw_object WHERE workspace_id=?",workspace);jdbc.update("DELETE FROM dw_datasource WHERE workspace_id=?",workspace);jdbc.update("DELETE FROM dw_workspace WHERE id=?",workspace);
        if(schema.matches("studio_inventory_test_[a-f0-9]{32}")){var admin=new JdbcTemplate(new DriverManagerDataSource("jdbc:mysql://127.0.0.1:3307/?allowPublicKeyRetrieval=true","root",System.getenv("INVENTORY_TEST_ROOT_PASSWORD")));admin.execute("DROP DATABASE `"+schema+"`");admin.execute("DROP USER '"+user+"'@'%'");}
    }
    Map<String,Object> versions(){Map<String,Object> v=new HashMap<>();nodes.forEach(n->v.put(n.id(),n.version()));return v;}
    String submit(LocalDate day){return workflows.startDevelopment(workflow.id(),workflow.version(),versions(),Map.of("businessDate",day.toString())).get("id").toString();}
    Map<String,Object> terminal(String id) throws Exception {for(int i=0;i<500;i++){var r=runs.required(id);if(Set.of("SUCCESS","FAILED","CANCELLED").contains(r.get("status"))){Thread.sleep(60);return r;}Thread.sleep(30);}throw new AssertionError("Not terminal: "+runs.required(id));}
    void success(LocalDate day)throws Exception{var r=terminal(submit(day));assertEquals("SUCCESS",r.get("status"),r.toString());assertEquals("PUBLISHED",r.get("publicationStatus"));}
    void edit(int i,String sql,int timeout){var n=nodes.get(i);var config=new LinkedHashMap<>(n.config());var run=new LinkedHashMap<String,Object>();RunService.config(n).forEach((k,v)->run.put(k.toString(),v));run.put("timeoutSeconds",timeout);config.put("run",run);nodes.set(i,objects.update(n.id(),new ObjectInput(workspace,null,n.kind(),n.nodeType(),n.name(),"",sql,config,List.of(),false,n.version())));}
    void amount(LocalDate date,String wh,String field,String expected){assertEquals(0,new BigDecimal(expected).compareTo(business.queryForObject("SELECT "+field+" FROM "+InventorySql.ADS+" WHERE business_date=? AND warehouse_id=?",BigDecimal.class,date,wh)));}
    List<List<Map<String,Object>>> snapshot(){return InventorySql.TARGETS.stream().map(t->business.queryForList("SELECT * FROM "+t+" ORDER BY 1,2")).toList();}
    Map<String,Object> release(){return workflows.publish(workflow.id(),workflow.version(),versions(),"test inventory release");}
    Map<String,Object> plan(String release,boolean enabled,int retries){return schedules.save(workflow.id(),null,Map.of("releaseId",release,"cron","0 * * * * *","enabled",enabled,"retries",retries));}
    void due(Map<String,Object> plan,Instant fire){plan.put("nextFireAt",fire.toString());jdbc.update("UPDATE dw_workflow_schedule SET next_fire_at=?,data_json=? WHERE id=?",fire.toString(),json.write(plan),plan.get("id"));}
    @SuppressWarnings("unchecked") List<Map<String,Object>> triggers(Map<String,Object> plan){return (List<Map<String,Object>>)schedules.triggers(plan.get("id").toString(),1,100,"").get("items");}
    @Test void independentDaysReconcileAndRerunsDoNotDuplicate() throws Exception {
        success(day2);amount(day2,"W1","closing_qty","162");amount(day2,"W1","closing_amount","1960");amount(day2,"W2","closing_qty","26");amount(day2,"W2","closing_amount","410");amount(day2,"W3","closing_qty","-2");amount(day2,"W3","negative_sku_count","1");
        var second=snapshot();success(day1);amount(day1,"W1","closing_qty","155");amount(day1,"W2","closing_qty","33");amount(day1,"W3","closing_qty","4");var before=snapshot();success(day2);assertEquals(before,snapshot());
        for(int i=0;i<3;i++){String table=InventorySql.TARGETS.get(i);assertEquals(second.get(i),business.queryForList("SELECT * FROM "+table+" WHERE business_date=? ORDER BY 1,2",day2));}
        assertEquals(0,business.queryForObject("SELECT COUNT(*) FROM "+InventorySql.DWS+" WHERE closing_qty<>opening_qty+inbound_qty-outbound_qty+transfer_in_qty-transfer_out_qty+adjustment_qty",Integer.class));
        assertEquals(0,new BigDecimal("0").compareTo(business.queryForObject("SELECT SUM(quantity) FROM "+InventorySql.DWD+" WHERE movement_type IN ('TRANSFER_IN','TRANSFER_OUT')",BigDecimal.class)));
        assertEquals(0,business.queryForObject("SELECT COUNT(*) FROM "+InventorySql.DWD+" WHERE source_doc='IN-CANCEL'",Integer.class));
        assertEquals(1,business.queryForObject("SELECT COUNT(*) FROM "+InventorySql.DWD+" WHERE source_doc='IN-1' AND business_date=?",Integer.class,day1));
        success(day2.plusDays(1));amount(day2.plusDays(1),"W1","opening_qty","162");amount(day2.plusDays(1),"W1","closing_qty","162");
        assertEquals(0,business.queryForObject("SELECT COUNT(*) FROM "+InventorySql.DWD+" WHERE business_date=? AND row_kind='MOVEMENT'",Integer.class,day2.plusDays(1)));
    }
    @Test void fixedCutoffRerunKeepsOriginalDataAndRecomputeSeesLateRevision() throws Exception {
        var release=release();var first=terminal(workflows.startRelease(release.get("id").toString(),Map.of("businessDate",day2.toString())).get("id").toString());String old=first.get("sourceCutoffAt").toString();
        Thread.sleep(20);business.update("INSERT INTO ods_inventory_inbound VALUES('late','IN-2',1,2,?,'W1',NULL,'S1',30,'VALID',UTC_TIMESTAMP(6))",day2);
        var rerun=terminal(schedules.rerun(first.get("id").toString()).get("id").toString());assertEquals("SUCCESS",rerun.get("status"));assertEquals(old,rerun.get("sourceCutoffAt"));amount(day2,"W1","closing_qty","162");
        assertNotEquals(first.get("buildId"),rerun.get("buildId"));success(day2);amount(day2,"W1","closing_qty","182");
        business.update("INSERT INTO ods_inventory_inbound VALUES('late-cancel','IN-2',1,3,?,'W1',NULL,'S1',30,'CANCELLED',UTC_TIMESTAMP(6))",day2);success(day2);amount(day2,"W1","closing_qty","152");
    }
    @Test void missingDimensionsAndIllegalQuantityProtectPublishedResults() throws Exception {
        success(day2);var before=snapshot();business.update("INSERT INTO ods_inventory_outbound VALUES('bad','BAD',1,1,?,'MISSING',NULL,'S1',-1,'VALID',UTC_TIMESTAMP(6))",day2);
        var failed=terminal(submit(day2));assertEquals("INVALID_INVENTORY_SOURCE",failed.get("errorCode"));assertEquals(before,snapshot());
        business.update("UPDATE ods_inventory_outbound SET warehouse_id='W1' WHERE record_id='bad'");assertEquals("INVALID_INVENTORY_SOURCE",terminal(submit(day2)).get("errorCode"));assertEquals(before,snapshot());
    }
    @Test void eachLayerFailureLeavesAllOfficialDatesUntouched() throws Exception {
        success(day1);success(day2);var before=snapshot();
        for(int i=0;i<3;i++){String sql=nodes.get(i).content();edit(i,"SELECT nonexistent FROM dim_sku",10);assertEquals("FAILED",terminal(submit(day2)).get("status"));assertEquals(before,snapshot());edit(i,sql,10);}
    }
    @Test void cancellationTimeoutAndOverlapDoNotPublishPartialWork() throws Exception {
        success(day2);var before=snapshot();edit(1,InventorySql.dws()+" HAVING SLEEP(8)=0",10);String id=submit(day2);
        for(int i=0;i<150&&workflows.nodes(id).stream().noneMatch(r->"n1".equals(r.get("graphNodeId"))&&"RUNNING".equals(r.get("status")));i++)Thread.sleep(20);
        assertEquals("MATERIALIZATION_BUSY",assertThrows(StudioException.class,()->submit(day2)).code());runs.stop(id);assertEquals("CANCELLED",terminal(id).get("status"));Thread.sleep(250);assertEquals(before,snapshot());
        edit(1,InventorySql.dws()+" HAVING SLEEP(8)=0",1);assertEquals("QUERY_TIMEOUT",terminal(submit(day2)).get("errorCode"));assertEquals(before,snapshot());
    }
    @Test void finalCommitAcknowledgementLossRecoversWithoutSecondPublication() throws Exception {
        doAnswer(call->{boolean result=(boolean)call.callRealMethod();var batch=(InventoryExecutionService.Batch)call.getArgument(0);batch.recovering=true;throw new SQLTransientConnectionException("lost commit acknowledgement");}).doCallRealMethod().when(inventory).publish(any());
        var completed=terminal(submit(day2));assertEquals("SUCCESS",completed.get("status"));assertEquals("PUBLISHED",completed.get("publicationStatus"));assertEquals(1,business.queryForObject("SELECT COUNT(*) FROM etl_publish_receipt",Integer.class));amount(day2,"W1","closing_qty","162");
    }
    @Test void finalCommitFailureRollsBackAllLayers() throws Exception {
        success(day2);var before=snapshot();
        // A failure on the third official INSERT forces rollback of deletes/inserts on the first two layers too.
        business.execute("CREATE TRIGGER fail_ads BEFORE INSERT ON "+InventorySql.ADS+" FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected final failure'");
        var failed=terminal(submit(day2));assertEquals("FAILED",failed.get("status"));assertEquals("NOT_PUBLISHED",failed.get("publicationStatus"));assertEquals(before,snapshot());
    }
    @Test void cancelAfterDurableCommitCannotMarkPublishedResultsCancelled() throws Exception {
        var failMetadata=new java.util.concurrent.atomic.AtomicBoolean(true);
        doAnswer(call->{Map<?,?> run=call.getArgument(0);if(failMetadata.get()&&Boolean.TRUE.equals(run.get("materialization"))&&"SUCCESS".equals(run.get("status")))throw new IllegalStateException("metadata acknowledgement fault");return call.callRealMethod();}).when(repo).transitionRun(anyMap(),anyString());
        String id=submit(day2);for(int i=0;i<250&&business.queryForObject("SELECT COUNT(*) FROM etl_publish_receipt",Integer.class)==0;i++)Thread.sleep(20);
        assertEquals(1,business.queryForObject("SELECT COUNT(*) FROM etl_publish_receipt",Integer.class));assertThrows(IllegalStateException.class,()->workflows.stop(id));assertNotEquals("CANCELLED",runs.required(id).get("status"));
        failMetadata.set(false);workflows.stop(id);assertEquals("SUCCESS",terminal(id).get("status"));amount(day2,"W1","closing_qty","162");
    }
    @Test void restartChecksReceiptAndDoesNotOpenWritesWhileDatabaseIsUncertain() throws Exception {
        var committed=terminal(submit(day2));String committedId=committed.get("id").toString();var pending=new LinkedHashMap<>(committed);pending.put("status","RUNNING");pending.put("publicationStatus","RECOVERING");assertTrue(repo.transitionRun(pending,"SUCCESS"));
        sources.save(sourceId,Map.of("port",1));inventory.recover();assertEquals("RECOVERING",runs.required(committedId).get("status"));assertEquals("RECOVERY_REQUIRED",assertThrows(StudioException.class,()->submit(day2)).code());
        sources.save(sourceId,Map.of("port",3307));inventory.recover();assertEquals("SUCCESS",runs.required(committedId).get("status"));
        var interrupted=new LinkedHashMap<>(pending);String id=UUID.randomUUID().toString();interrupted.put("id",id);interrupted.put("buildId",id);interrupted.put("status","RUNNING");repo.insertRun(interrupted,workflow);inventory.recover();assertEquals("FAILED",runs.required(id).get("status"));assertEquals("SERVICE_RESTARTED",runs.required(id).get("errorCode"));
    }
    @Test void outerTransactionRollbackNeverStartsOrRetainsTheDatabaseLease() throws Exception {
        tx.executeWithoutResult(t->{submit(day2);t.setRollbackOnly();});Thread.sleep(400);assertTrue(repo.topRuns(workspace).isEmpty());assertTrue(snapshot().stream().allMatch(List::isEmpty));success(day2);
    }
    @Test void schedulerFreezesVersionsAndParametersAndDeduplicatesPolls() throws Exception {
        String r1=release().get("id").toString();var plan=plan(r1,true,0);edit(2,"SELECT nonexistent FROM dim_sku",10);String r2=release().get("id").toString();
        Instant now=Instant.now();due(plan,now.truncatedTo(ChronoUnit.MINUTES));schedules.scan(now,workspace);schedules.scan(now,workspace);
        var trigger=triggers(plan).getFirst();assertEquals(1,triggers(plan).size());var done=terminal(trigger.get("runId").toString());assertEquals("SUCCESS",done.get("status"));assertEquals(r1,done.get("releaseId"));schedules.scan(Instant.now(),workspace);assertEquals("SUCCESS",triggers(plan).getFirst().get("status"));
        assertEquals(r1,schedules.get(plan.get("id").toString()).get("releaseId"));var update=new LinkedHashMap<>(plan);update.put("releaseId",r2);update.put("expectedVersion",plan.get("version"));update.put("enabled",false);var switched=schedules.save(workflow.id(),plan.get("id").toString(),update);assertEquals(r2,switched.get("releaseId"));
        assertEquals("SUCCESS",terminal(schedules.rerunTrigger(trigger.get("id").toString()).get("id").toString()).get("status"));
        assertEquals("FAILED",terminal(workflows.startRelease(r2,Map.of("businessDate",day2.toString())).get("id").toString()).get("status"));
        assertEquals("VERSION_CONFLICT",assertThrows(StudioException.class,()->schedules.save(workflow.id(),plan.get("id").toString(),update)).code());
    }
    @Test void pauseMisfireAndOverlapAreRecordedWithoutCatchup() throws Exception {
        var plan=plan(release().get("id").toString(),false,0);schedules.scan(Instant.now(),workspace);assertTrue(triggers(plan).isEmpty());var enabled=new LinkedHashMap<>(plan);enabled.put("enabled",true);enabled.put("expectedVersion",plan.get("version"));plan=schedules.save(workflow.id(),plan.get("id").toString(),enabled);
        Instant now=Instant.now();due(plan,now.minusSeconds(300).truncatedTo(ChronoUnit.MINUTES));schedules.scan(now,workspace);assertEquals("MISSED_INTERVAL",triggers(plan).getFirst().get("reason"));assertTrue(repo.topRuns(workspace).isEmpty());
        edit(0,InventorySql.dwd()+" WHERE SLEEP(8)=0",10);String active=submit(day2);due(plan,now.truncatedTo(ChronoUnit.MINUTES));schedules.scan(now,workspace);assertEquals("OVERLAP",triggers(plan).getFirst().get("reason"));runs.stop(active);terminal(active);
        assertTrue(Instant.parse(schedules.get(plan.get("id").toString()).get("nextFireAt").toString()).isAfter(now));
    }
    @Test void restartSkipsEvenARecentlyMissedMinuteAndKeepsFutureSchedule() {
        var plan=plan(release().get("id").toString(),true,0);Instant now=Instant.now();due(plan,now.truncatedTo(ChronoUnit.MINUTES));schedules.recordMissedOnResume(now,workspace);var skipped=triggers(plan).getFirst();assertEquals("SKIPPED",skipped.get("status"));assertEquals("MISSED_INTERVAL",skipped.get("reason"));assertTrue(repo.topRuns(workspace).isEmpty());assertTrue(Instant.parse(schedules.get(plan.get("id").toString()).get("nextFireAt").toString()).isAfter(now));
    }
    @Test void transientRetryKeepsReleaseDateAndCutoffButCreatesNewRun() throws Exception {
        var plan=plan(release().get("id").toString(),true,1);Instant now=Instant.now();due(plan,now.truncatedTo(ChronoUnit.MINUTES));
        doThrow(new StudioException("DATASOURCE_UNAVAILABLE","temporary unavailable",502)).doCallRealMethod().when(inventory).begin(anyString(),any(),anyMap());
        schedules.scan(now,workspace);var waiting=triggers(plan).getFirst();assertEquals("RETRY_WAIT",waiting.get("status"));schedules.scan(now.plusSeconds(61),workspace);var attempted=triggers(plan).stream().filter(t->t.get("id").equals(waiting.get("id"))).findFirst().orElseThrow();assertEquals(2,((Number)attempted.get("attempt")).intValue());
        var run=terminal(attempted.get("runId").toString());assertEquals("SUCCESS",run.get("status"));assertEquals(waiting.get("sourceCutoffAt"),run.get("sourceCutoffAt"));assertEquals(waiting.get("businessDate"),run.get("businessDate"));assertEquals(waiting.get("releaseId"),run.get("releaseId"));
    }
    List<Map<String,Object>> taskPlans(boolean enabled)throws Exception{
        List<Map<String,Object>> result=new ArrayList<>();
        for(int i=0;i<3;i++){
            if(i>0)edit(i,nodes.get(i).content().replace(":build_id",i==1?":upstream_dwd_build_id":":upstream_dws_build_id"),10);
            var release=tasks.publish(nodes.get(i).id(),nodes.get(i).version(),"task test");
            result.add(taskSchedules.save(nodes.get(i).id(),null,Map.of("releaseId",release.get("id"),"enabled",enabled,"cron","0 * * * * *","dependencies",i==0?List.of():List.of(Map.of("taskId",nodes.get(i-1).id(),"alias",i==1?"dwd":"dws")))));
        }return result;
    }
    Map<String,Object> taskDone(String id)throws Exception{for(int i=0;i<500;i++){inventory.recover();var r=runs.required(id);if(Set.of("SUCCESS","FAILED","CANCELLED").contains(r.get("status"))){Thread.sleep(50);return r;}Thread.sleep(20);}throw new AssertionError(runs.required(id));}
    Map<String,Object> runTask(Map<String,Object> p)throws Exception{return taskDone(tasks.startRelease(p.get("releaseId").toString(),Map.of("businessDate",day2.toString())).get("id").toString());}
    void taskDue(Map<String,Object> plan,Instant at){plan.put("nextFireAt",at.toString());taskStore.saveSchedule(plan,false);}
    @SuppressWarnings("unchecked") List<Map<String,Object>> taskTriggers(Map<String,Object> p){return (List<Map<String,Object>>)taskSchedules.triggers(p.get("id").toString(),1,100,"").get("items");}
    @Test void taskNodesPublishIndependentlyAndKeepPinnedInputsForRerun()throws Exception{
        var plans=taskPlans(false);var dwd=runTask(plans.get(0));assertEquals("SUCCESS",dwd.get("status"),dwd.toString());assertEquals("PUBLISHED",dwd.get("publicationStatus"));assertEquals(0,business.queryForObject("SELECT COUNT(*) FROM "+InventorySql.ADS,Integer.class));
        var dws=runTask(plans.get(1));assertEquals("SUCCESS",dws.get("status"),dws.toString());var ads=runTask(plans.get(2));assertEquals("SUCCESS",ads.get("status"),ads.toString());amount(day2,"W1","closing_qty","162");
        String adsBuild=ads.get("buildId").toString();Thread.sleep(25);business.update("INSERT INTO ods_inventory_inbound VALUES('task-late','IN-2',1,2,?,'W1',NULL,'S1',30,'VALID',UTC_TIMESTAMP(6))",day2);assertEquals("SUCCESS",runTask(plans.get(0)).get("status"));
        var rerun=taskDone(tasks.rerun(dws.get("id").toString()).get("id").toString());assertEquals("SUCCESS",rerun.get("status"));assertEquals(dws.get("upstreamRuns"),rerun.get("upstreamRuns"));
        assertEquals(0,new BigDecimal("162").compareTo(business.queryForObject("SELECT closing_qty FROM "+InventorySql.DWS+" WHERE warehouse_id='W1'",BigDecimal.class)));
        assertEquals(adsBuild,business.queryForObject("SELECT build_id FROM etl_partition_publication WHERE target_table=?",String.class,InventorySql.ADS));
        runTask(plans.get(1));runTask(plans.get(2));amount(day2,"W1","closing_qty","182");
    }
    @Test void taskFailuresDoNotRollbackUpstreamAndVersionSwitchIsExplicit()throws Exception{
        var plans=taskPlans(false);for(var p:plans)assertEquals("SUCCESS",runTask(p).get("status"));var oldDws=business.queryForList("SELECT * FROM "+InventorySql.DWS);var oldAds=business.queryForList("SELECT * FROM "+InventorySql.ADS);
        edit(1,"SELECT nonexistent FROM dim_sku",10);var r2=tasks.publish(nodes.get(1).id(),nodes.get(1).version(),"broken R2");assertEquals("SUCCESS",runTask(plans.get(1)).get("status"));assertEquals("FAILED",taskDone(tasks.startRelease(r2.get("id").toString(),Map.of("businessDate",day2.toString())).get("id").toString()).get("status"));assertEquals(oldDws,business.queryForList("SELECT * FROM "+InventorySql.DWS));assertEquals(oldAds,business.queryForList("SELECT * FROM "+InventorySql.ADS));
        assertEquals(plans.get(1).get("releaseId"),taskStore.forTask(nodes.get(1).id()).orElseThrow().get("releaseId"));
    }
    @Test void taskDependenciesRejectCyclesAndDraftsNeedNoRelease()throws Exception{
        var draft=taskSchedules.save(nodes.get(0).id(),null,Map.of());assertFalse((boolean)draft.get("enabled"));var enable=new LinkedHashMap<>(draft);enable.put("enabled",true);enable.put("expectedVersion",draft.get("version"));assertEquals("TASK_RELEASE_REQUIRED",assertThrows(StudioException.class,()->taskSchedules.save(nodes.get(0).id(),draft.get("id").toString(),enable)).code());
        var second=taskSchedules.save(nodes.get(1).id(),null,Map.of("dependencies",List.of(Map.of("taskId",nodes.get(0).id(),"alias","dwd"))));
        var update=new LinkedHashMap<>(draft);update.put("expectedVersion",draft.get("version"));update.put("dependencies",List.of(Map.of("taskId",nodes.get(1).id(),"alias","dws")));assertEquals("DEPENDENCY_CYCLE",assertThrows(StudioException.class,()->taskSchedules.save(nodes.get(0).id(),draft.get("id").toString(),update)).code());
        update.put("dependencies",List.of(Map.of("taskId",nodes.get(0).id(),"alias","self")));assertEquals("INVALID_DEPENDENCY",assertThrows(StudioException.class,()->taskSchedules.save(nodes.get(0).id(),draft.get("id").toString(),update)).code());
        update.put("dependencies",List.of());update.put("expectedVersion",99);assertEquals("VERSION_CONFLICT",assertThrows(StudioException.class,()->taskSchedules.save(nodes.get(0).id(),draft.get("id").toString(),update)).code());
    }
    @Test void taskSchedulerWaitsForEachNodeAndDeduplicatesTheMinute()throws Exception{
        var plans=taskPlans(true);var multi=new LinkedHashMap<>(plans.get(2));multi.put("expectedVersion",multi.get("version"));multi.put("dependencies",List.of(Map.of("taskId",nodes.get(1).id(),"alias","dws"),Map.of("taskId",nodes.get(0).id(),"alias","dwd")));plans.set(2,taskSchedules.save(nodes.get(2).id(),multi.get("id").toString(),multi));Instant now=Instant.now();Instant fire=now.truncatedTo(ChronoUnit.MINUTES);for(var p:plans)taskDue(p,fire);
        taskSchedules.scan(now,workspace);taskSchedules.scan(now,workspace);assertEquals("WAITING_DEPENDENCY",taskTriggers(plans.get(2)).getFirst().get("status"));
        // Keep this deduplication test in one logical minute even when real execution crosses a minute boundary.
        for(int i=0;i<250;i++){taskSchedules.scan(now,workspace);if("SUCCESS".equals(taskTriggers(plans.get(2)).getFirst().get("status")))break;Thread.sleep(30);}
        for(var p:plans){assertEquals(1,taskTriggers(p).size());assertEquals("SUCCESS",taskTriggers(p).getFirst().get("status"),taskTriggers(p).toString());}amount(day2,"W1","closing_qty","162");
        var last=taskTriggers(plans.get(2)).getFirst();assertEquals(2,((List<?>)last.get("upstreamRuns")).size());
    }
    @Test void taskLatestFailedSlotBlocksInsteadOfReusingOlderSuccess()throws Exception{
        var plans=taskPlans(true);var old=runTask(plans.get(0));Instant fire=Instant.now().truncatedTo(ChronoUnit.MINUTES);var upstream=plans.get(0);
        for(int i=0;i<2;i++){var t=new LinkedHashMap<String,Object>();t.put("id",UUID.randomUUID().toString());t.put("scheduleId",upstream.get("id"));t.put("scheduledAt",fire.minusSeconds(i==0?60:0).toString());t.put("status",i==0?"SUCCESS":"FAILED");t.put("runId",old.get("id"));taskStore.insertTrigger(t);}
        taskDue(plans.get(1),fire);taskSchedules.scan(Instant.now(),workspace);var blocked=taskTriggers(plans.get(1)).getFirst();assertEquals("BLOCKED",blocked.get("status"));assertEquals("UPSTREAM_FAILED",blocked.get("reason"));assertNull(blocked.get("runId"));
    }
    @Test void taskSingleCommitFailureAndAcknowledgementLossRecoverByReceipt()throws Exception{
        var plans=taskPlans(false);var first=runTask(plans.get(0));assertEquals("SUCCESS",first.get("status"));var before=business.queryForList("SELECT * FROM "+InventorySql.DWD);
        business.execute("CREATE TRIGGER fail_dwd BEFORE INSERT ON "+InventorySql.DWD+" FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='single failure'");assertEquals("FAILED",runTask(plans.get(0)).get("status"));assertEquals(before,business.queryForList("SELECT * FROM "+InventorySql.DWD));business.execute("DROP TRIGGER fail_dwd");
        doAnswer(call->{call.callRealMethod();var b=(InventoryExecutionService.Batch)call.getArgument(0);b.recovering=true;throw new SQLTransientConnectionException("lost ack");}).when(inventory).publish(any());var recovered=runTask(plans.get(0));assertEquals("SUCCESS",recovered.get("status"));assertEquals("PUBLISHED",recovered.get("publicationStatus"));assertTrue(((Number)recovered.get("writtenRows")).intValue()>0);
    }
    @Test void taskCancelAndTimeoutPreserveLastSuccessfulPartition()throws Exception{
        var plans=taskPlans(false);runTask(plans.get(0));var before=business.queryForList("SELECT * FROM "+InventorySql.DWD);edit(0,InventorySql.dwd()+" WHERE SLEEP(8)=0",1);var release=tasks.publish(nodes.get(0).id(),nodes.get(0).version(),"timeout");String id=tasks.startRelease(release.get("id").toString(),Map.of("businessDate",day2.toString())).get("id").toString();
        var timedOut=taskDone(id);assertEquals("QUERY_TIMEOUT",timedOut.get("errorCode"),"Run "+id+" status="+timedOut.get("status")+", publicationStatus="+timedOut.get("publicationStatus")+", elapsedMs="+timedOut.get("elapsedMs"));assertEquals(before,business.queryForList("SELECT * FROM "+InventorySql.DWD));
        id=tasks.startRelease(release.get("id").toString(),Map.of("businessDate",day2.toString())).get("id").toString();tasks.stop(id);assertEquals("CANCELLED",taskDone(id).get("status"));assertEquals(before,business.queryForList("SELECT * FROM "+InventorySql.DWD));
    }

    @Test void taskRetryPauseResumeAndHistoryRemainDurable()throws Exception{
        var n=nodes.get(0);var parameterConfig=new LinkedHashMap<String,Object>(n.config());parameterConfig.put("schedule",Map.of("parameters",List.of(Map.of("name","fixed_time","value","$cyctime","source","MANUAL"))));
        nodes.set(0,objects.update(n.id(),new ObjectInput(workspace,null,n.kind(),n.nodeType(),n.name(),"",n.content(),parameterConfig,List.of(),false,n.version())));
        var plans=taskPlans(false);Map<String,Object> p=new LinkedHashMap<>(plans.get(0));p.put("enabled",true);p.put("retries",1);p.put("expectedVersion",p.get("version"));p=taskSchedules.save(nodes.get(0).id(),p.get("id").toString(),p);
        doThrow(new StudioException("DATASOURCE_UNAVAILABLE","transient",503)).doCallRealMethod().when(inventory).beginTask(anyString(),any(),anyMap(),anyMap());Instant now=Instant.now();taskDue(p,now.truncatedTo(ChronoUnit.MINUTES));taskSchedules.scan(now,workspace);var t=taskTriggers(p).getFirst();assertEquals("RETRY_WAIT",t.get("status"));String cutoff=t.get("sourceCutoffAt").toString();t.put("nextRetryAt",now.toString());taskStore.updateTrigger(t);taskSchedules.scan(now,workspace);var running=taskTriggers(p).getFirst();assertEquals("SUCCESS",taskDone(running.get("runId").toString()).get("status"));taskSchedules.scan(Instant.now(),workspace);var done=taskTriggers(p).getFirst();assertEquals("SUCCESS",done.get("status"));assertEquals(cutoff,done.get("sourceCutoffAt"));assertEquals(2,((Number)done.get("attempt")).intValue());
        var actualRun=runs.required(done.get("runId").toString());assertEquals(t.get("scheduledAt"),actualRun.get("scheduledAt"));
        var fixed=ScheduleParameters.context(Map.of(),Map.of("businessDate",t.get("businessDate"),"scheduledAt",t.get("scheduledAt"),"timezone",t.get("timezone")));
        assertEquals(Map.of("fixed_time",ScheduleParameters.evaluate("$cyctime",fixed)),actualRun.get("scheduleParameters"));
        p=taskStore.schedule(p.get("id").toString());p.put("enabled",false);p.put("expectedVersion",p.get("version"));p=taskSchedules.save(nodes.get(0).id(),p.get("id").toString(),p);taskSchedules.scan(Instant.now(),workspace);assertEquals(1,taskTriggers(p).size());
        p.put("enabled",true);p.put("expectedVersion",p.get("version"));p=taskSchedules.save(nodes.get(0).id(),p.get("id").toString(),p);taskDue(p,now.minusSeconds(120).truncatedTo(ChronoUnit.MINUTES));taskSchedules.resume(Instant.now(),workspace);assertTrue(taskTriggers(p).stream().anyMatch(x->"MISSED_INTERVAL".equals(x.get("reason"))));assertTrue(Instant.parse(taskStore.schedule(p.get("id").toString()).get("nextFireAt").toString()).isAfter(now));
        var rerun=taskSchedules.rerun(done.get("id").toString());assertEquals("SUCCESS",taskDone(rerun.get("runId").toString()).get("status"));assertEquals(cutoff,rerun.get("sourceCutoffAt"));assertEquals(actualRun.get("scheduleParameters"),runs.required(rerun.get("runId").toString()).get("scheduleParameters"));
    }
    @Test void newWorkflowManuallyOrchestratesIndependentTaskCommits()throws Exception{
        taskPlans(false);
        for(int i=0;i<3;i++){var n=nodes.get(i);var config=new LinkedHashMap<>(n.config());var run=new LinkedHashMap<String,Object>();RunService.config(n).forEach((k,v)->run.put(k.toString(),v));run.put("taskExecutionVersion",1);config.put("run",run);nodes.set(i,objects.update(n.id(),new ObjectInput(workspace,null,n.kind(),n.nodeType(),n.name(),"",n.content(),config,List.of(),false,n.version())));}
        String id=submit(day2);assertEquals("SUCCESS",terminal(id).get("status"));for(var child:workflows.nodes(id))assertEquals("PUBLISHED",child.get("publicationStatus"));assertEquals(3,business.queryForObject("SELECT COUNT(*) FROM etl_publish_receipt",Integer.class));
        edit(2,"SELECT nonexistent FROM dim_sku",10);id=submit(day2);assertEquals("FAILED",terminal(id).get("status"));assertEquals("PUBLISHED",workflows.nodes(id).stream().filter(r->"n0".equals(r.get("graphNodeId"))).findFirst().orElseThrow().get("publicationStatus"));
    }

    @Test void standaloneTaskRejectsLegacySharedInputBeforeReplacingResults(){
        var n=nodes.get(1);var release=tasks.publish(n.id(),n.version(),"legacy SQL");assertEquals("LEGACY_SHARED_BATCH_SQL",assertThrows(StudioException.class,()->tasks.startRelease(release.get("id").toString(),Map.of("businessDate",day2.toString()))).code());assertTrue(repo.topRuns(workspace).isEmpty());
    }

}
