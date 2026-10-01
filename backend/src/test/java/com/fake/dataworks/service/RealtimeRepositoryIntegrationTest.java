package com.fake.dataworks.service;

import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.RealtimeRepository;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import static org.junit.jupiter.api.Assertions.*;

/** Owns only TEST workspaces; does not submit SQL or touch the running Flink cluster. */
@SpringBootTest(properties={"spring.main.web-application-type=none","studio.realtime.polling-enabled=false","studio.simulation.recover-on-start=false","studio.inventory.recover-on-start=false"})
class RealtimeRepositoryIntegrationTest {
    @Autowired RealtimeService realtime;
    @Autowired RealtimeRepository repo;
    @Autowired DatasourceService sources;
    @Autowired JdbcTemplate jdbc;
    @Autowired TransactionTemplate tx;
    String workspace,other;
    final Set<String> cdcOwners=new HashSet<>(),cdcEndpoints=new HashSet<>();
    @BeforeEach void setup(){workspace=workspace();other=workspace();}
    String workspace(){String id="test-rlt-"+UUID.randomUUID();jdbc.update("INSERT INTO dw_workspace(id,name,code,region,workspace_type) VALUES(?,?,?,?,'TEST')",id,"实时元数据测试",id,"local");return id;}
    @AfterEach void cleanup(){for(String owner:cdcOwners)jdbc.update("DELETE FROM dw_realtime_cdc_reservation WHERE owner_id=?",owner);for(String endpoint:cdcEndpoints)jdbc.update("DELETE FROM dw_realtime_cdc_endpoint WHERE endpoint=?",endpoint);for(String id:List.of(workspace,other)){jdbc.update("DELETE FROM dw_realtime_active WHERE task_id IN (SELECT id FROM dw_realtime_task WHERE workspace_id=?)",id);for(String table:List.of("dw_realtime_preview","dw_realtime_operation","dw_realtime_job","dw_realtime_release","dw_realtime_draft","dw_realtime_task","dw_realtime_folder","dw_realtime_import","dw_datasource"))jdbc.update("DELETE FROM "+table+" WHERE workspace_id=?",id);jdbc.update("DELETE FROM dw_workspace WHERE id=?",id);}}
    Map<String,Object> task(String name){return new LinkedHashMap<>(Map.of("name",name,"description","","sql","INSERT INTO target SELECT 1","bindings",List.of(),"runtime",Map.of("parallelism",1,"checkpointSeconds",60,"restartAttempts",3,"restartDelaySeconds",10)));}
    @Test void persistsDraftsVersionsAndWorkspaceIsolation(){var task=realtime.createTask(workspace,task("任务"));String id=task.get("id").toString();var draft=new LinkedHashMap<>(task);draft.put("sql","SELECT 2");realtime.saveDraft(workspace,id,draft,1);assertEquals("SELECT 2",((Map<?,?>)repo.drafts(workspace).get(id)).get("sql"));var saved=realtime.saveTask(workspace,id,draft,1);assertEquals(2,saved.get("revision"));assertTrue(repo.drafts(workspace).isEmpty());assertEquals("VERSION_CONFLICT",assertThrows(StudioException.class,()->realtime.saveTask(workspace,id,task("丢失修改"),1)).code());assertEquals("SELECT 2",repo.task(id,workspace).get("sql"));assertEquals("NOT_FOUND",assertThrows(StudioException.class,()->repo.task(id,other)).code());}
    @Test void folderNamesAndTaskNamesAreUniqueAndNonemptyFoldersCannotBeDeleted(){var folder=realtime.folder(workspace,null,"目录");var input=task("任务");input.put("folderId",folder.get("id"));realtime.createTask(workspace,input);assertThrows(org.springframework.dao.DuplicateKeyException.class,()->realtime.createTask(workspace,input));assertEquals("FOLDER_NOT_EMPTY",assertThrows(StudioException.class,()->realtime.deleteFolder(workspace,folder.get("id").toString())).code());}
    @Test void activityLockSurvivesTerminalOldJobAndTransfersWithinUpgrade(){var task=realtime.createTask(workspace,task("锁"));String id=task.get("id").toString();tx.executeWithoutResult(t->{repo.lockWorkspace(workspace);repo.acquire(id,"old-job","upgrade-op");});assertEquals("REALTIME_TASK_ACTIVE",assertThrows(StudioException.class,()->tx.executeWithoutResult(t->{repo.lockWorkspace(workspace);repo.acquire(id,"other-job","other-op");})).code());repo.transfer(id,"new-job","upgrade-op");repo.releaseActive(id,"old-job");assertEquals("new-job",repo.active(id).orElseThrow().get("job_id"));assertEquals("REALTIME_TASK_ACTIVE",assertThrows(StudioException.class,()->realtime.deleteTask(workspace,id)).code());repo.releaseActive(id,"new-job");realtime.deleteTask(workspace,id);assertTrue(repo.tasks(workspace).isEmpty());}
    @Test void releasesKeepImmutableTaskSnapshotsAfterSavingAnotherRevision(){var task=realtime.createTask(workspace,task("发布快照"));String id=task.get("id").toString();var release=new LinkedHashMap<String,Object>();release.put("id",UUID.randomUUID().toString());release.put("taskId",id);release.put("workspaceId",workspace);release.put("releaseNo",1);release.put("createdAt",RealtimeService.now());release.put("snapshot",task);repo.insertRelease(release);var changed=new LinkedHashMap<>(task);changed.put("sql","INSERT INTO target SELECT 2");realtime.saveTask(workspace,id,changed,1);assertEquals("INSERT INTO target SELECT 1",((Map<?,?>)repo.release(release.get("id").toString(),workspace).get("snapshot")).get("sql"));assertEquals(2,repo.nextRelease(id));}
    @Test void localImportIsIdempotentAndNeverImportsMockJobs(){String taskId="legacy-task-"+UUID.randomUUID();var old=task("旧任务");old.put("id",taskId);old.put("workspaceId",workspace);var state=Map.<String,Object>of("tasks",List.of(old),"folders",List.of(),"releases",List.of(),"jobs",List.of(Map.of("id","mock-job","taskId",taskId,"status","RUNNING")),"kafkaSources",List.of(),"drafts",Map.of());realtime.importState(workspace,"legacy-v1",state);realtime.importState(workspace,"legacy-v1",state);assertEquals(1,repo.tasks(workspace).size());assertTrue(repo.jobs(workspace).isEmpty());var changed=new LinkedHashMap<>(state);changed.put("tasks",List.of());assertEquals("IMPORT_CONFLICT",assertThrows(StudioException.class,()->realtime.importState(workspace,"legacy-v1",changed)).code());}
    @Test void legacyKafkaImportRejectsEveryChangedPublicTargetIdentity(){var kafka=new LinkedHashMap<String,Object>(Map.of("id","legacy-kafka","workspaceId",workspace,"type","KAFKA","name","同名连接","bootstrapServers","broker:9092","flinkBootstrapServers","runtime-broker:9092","securityProtocol","SASL_PLAINTEXT","saslMechanism","PLAIN","username","studio"));sources.importKafka(kafka);for(var change:Map.of("bootstrapServers","other:9092","flinkBootstrapServers","other-runtime:9092","securityProtocol","SASL_SSL","saslMechanism","SCRAM-SHA-256","username","other-user").entrySet()){var changed=new LinkedHashMap<>(kafka);changed.put(change.getKey(),change.getValue());String importId="conflict-"+change.getKey();assertEquals("IMPORT_SOURCE_CONFLICT",assertThrows(StudioException.class,()->realtime.importState(workspace,importId,Map.of("kafkaSources",List.of(changed)))).code());assertTrue(repo.imported(workspace,importId).isEmpty());}assertEquals(1,sources.list(workspace).size());}
    @Test void legacyKafkaImportReusesSameTargetAndWarnsAboutMissingSaslPassword(){var kafka=new LinkedHashMap<String,Object>(Map.of("id","legacy-kafka","workspaceId",workspace,"type","KAFKA","name","缺凭据连接","bootstrapServers","broker:9092","flinkBootstrapServers","runtime-broker:9092","securityProtocol","SASL_PLAINTEXT","saslMechanism","PLAIN","username","studio"));var source=sources.importKafka(kafka);var result=realtime.importState(workspace,"same-target",Map.of("kafkaSources",List.of(kafka)));assertEquals(source.get("id"),RealtimeService.map(result.get("idMap")).get("legacy-kafka"));assertEquals(1,sources.list(workspace).size());assertEquals(false,sources.list(workspace).getFirst().get("passwordSet"));var warning=RealtimeService.maps(result.get("importWarnings")).getFirst();assertEquals("KAFKA_CREDENTIALS_REQUIRED",warning.get("errorCode"));assertEquals(source.get("id"),warning.get("datasourceId"));assertFalse(warning.containsKey("password"));assertEquals(result.get("idMap"),realtime.importState(workspace,"same-target",Map.of("kafkaSources",List.of(kafka))).get("idMap"));}
    @Test void terminalGatewayCleanupScanIncludesConfirmedAndSafeUnsubmittedHandlesButNeverUnknownSubmissions(){var task=realtime.createTask(workspace,task("网关清理"));String taskId=task.get("id").toString();String confirmed=UUID.randomUUID().toString(),unknown=UUID.randomUUID().toString(),initializing=UUID.randomUUID().toString();var job=new LinkedHashMap<String,Object>(Map.of("id",confirmed,"taskId",taskId,"workspaceId",workspace,"status","FINISHED","updatedAt",RealtimeService.now(),"flinkJobId","a".repeat(32),"sessionHandle","session","operationHandle","handle"));repo.insertJob(job);var pending=new LinkedHashMap<>(job);pending.put("id",unknown);pending.put("status","FAILED");pending.remove("flinkJobId");pending.put("submissionAttempted",true);repo.insertJob(pending);var safe=new LinkedHashMap<>(pending);safe.put("id",initializing);safe.put("submissionAttempted",false);repo.insertJob(safe);var ids=repo.terminalGatewayCleanupJobs().stream().map(row->row.get("id")).toList();assertTrue(ids.contains(confirmed));assertTrue(ids.contains(initializing));assertFalse(ids.contains(unknown));job.remove("sessionHandle");job.remove("operationHandle");job.put("gatewayCleanupStatus","COMPLETED");repo.saveJob(job);assertFalse(repo.terminalGatewayCleanupJobs().stream().anyMatch(row->confirmed.equals(row.get("id"))));}
    Map<String,Object> cdcRange(String endpoint,long first,long last,String table){cdcEndpoints.add(endpoint);return Map.of("endpoint",endpoint,"first",first,"last",last,"tableName",table);}
    String cdcOwner(String prefix){String owner=prefix+UUID.randomUUID();cdcOwners.add(owner);return owner;}
    @Test void concurrentCdcOwnersLockEndpointsInStableOrderAndKeepReleasedHistory() throws Exception {
        String endpointA="a-"+UUID.randomUUID()+":3306",endpointB="b-"+UUID.randomUUID()+":3306",job=cdcOwner("job-"),preview=cdcOwner("preview-");
        var a=cdcRange(endpointA,5401,5656,"source_a");var b=cdcRange(endpointB,7001,7256,"source_b");var gate=new CountDownLatch(1);
        String jobResult,previewResult;
        try(var workers=Executors.newFixedThreadPool(2)){
            var first=workers.submit(()->{gate.await();try{repo.reserveCdcServerIds("JOB",job,"job-attempt",List.of(a,b));return "SUCCESS";}catch(StudioException e){return e.code();}});
            var second=workers.submit(()->{gate.await();try{repo.reserveCdcServerIds("PREVIEW",preview,"preview-attempt",List.of(b,a));return "SUCCESS";}catch(StudioException e){return e.code();}});
            gate.countDown();jobResult=first.get(10,TimeUnit.SECONDS);previewResult=second.get(10,TimeUnit.SECONDS);
        }
        assertEquals(Set.of("SUCCESS","CDC_SERVER_ID_IN_USE"),Set.of(jobResult,previewResult));
        assertEquals(2,jdbc.queryForObject("SELECT COUNT(*) FROM dw_realtime_cdc_reservation WHERE endpoint IN (?,?) AND released=FALSE",Integer.class,endpointA,endpointB));
        String winnerType="SUCCESS".equals(jobResult)?"JOB":"PREVIEW",winner="SUCCESS".equals(jobResult)?job:preview,winnerAttempt="SUCCESS".equals(jobResult)?"job-attempt":"preview-attempt";
        repo.releaseCdcServerIds(winnerType,winner,winnerAttempt);
        repo.reserveCdcServerIds("SUCCESS".equals(jobResult)?"PREVIEW":"JOB","SUCCESS".equals(jobResult)?preview:job,"SUCCESS".equals(jobResult)?"preview-attempt":"job-attempt",List.of(a,b));
        assertEquals(4,jdbc.queryForObject("SELECT COUNT(*) FROM dw_realtime_cdc_reservation WHERE endpoint IN (?,?)",Integer.class,endpointA,endpointB));
        assertEquals(2,jdbc.queryForObject("SELECT COUNT(*) FROM dw_realtime_cdc_reservation WHERE endpoint IN (?,?) AND released=TRUE AND released_at IS NOT NULL",Integer.class,endpointA,endpointB));
    }
    @Test void cdcReservationSurvivesRepositoryRecreationAndRejectsAttemptRangeChanges(){
        String endpoint="cdc-"+UUID.randomUUID()+":3306",owner=cdcOwner("job-");var range=cdcRange(endpoint,5401,5656,"source");
        repo.reserveCdcServerIds("JOB",owner,"attempt",List.of(range));
        var recreated=new RealtimeRepository(jdbc,new com.fake.dataworks.config.JsonCodec());
        tx.executeWithoutResult(t->recreated.reserveCdcServerIds("JOB",owner,"attempt",List.of(range)));
        assertEquals(1,jdbc.queryForObject("SELECT COUNT(*) FROM dw_realtime_cdc_reservation WHERE owner_id=?",Integer.class,owner));
        assertEquals("CDC_RESERVATION_CHANGED",assertThrows(StudioException.class,()->repo.reserveCdcServerIds("JOB",owner,"attempt",List.of(cdcRange(endpoint,5402,5657,"source")))).code());
        String otherOwner=cdcOwner("preview-");
        assertEquals("CDC_SERVER_ID_IN_USE",assertThrows(StudioException.class,()->repo.reserveCdcServerIds("PREVIEW",otherOwner,"other-attempt",List.of(cdcRange(endpoint,5656,5900,"other")))).code());
        repo.reserveCdcServerIds("PREVIEW",otherOwner,"other-attempt",List.of(cdcRange(endpoint,5657,5900,"other")));
        repo.releaseCdcServerIds("JOB",owner,"attempt");repo.reserveCdcServerIds("JOB",owner,"attempt",List.of(range));
        assertEquals(2,jdbc.queryForObject("SELECT COUNT(*) FROM dw_realtime_cdc_reservation WHERE owner_id=?",Integer.class,owner));
        assertEquals(1,jdbc.queryForObject("SELECT COUNT(*) FROM dw_realtime_cdc_reservation WHERE owner_id=? AND released=TRUE",Integer.class,owner));
    }
    @Test void multiEndpointCdcConflictRollsBackEveryNewReservation(){
        String a="a-"+UUID.randomUUID()+":3306",b="b-"+UUID.randomUUID()+":3306",first=cdcOwner("job-"),second=cdcOwner("preview-");
        repo.reserveCdcServerIds("JOB",first,"attempt",List.of(cdcRange(b,5401,5656,"source")));
        assertEquals("CDC_SERVER_ID_IN_USE",assertThrows(StudioException.class,()->repo.reserveCdcServerIds("PREVIEW",second,"attempt",List.of(cdcRange(a,7001,7256,"free"),cdcRange(b,5500,5600,"busy")))).code());
        assertEquals(0,jdbc.queryForObject("SELECT COUNT(*) FROM dw_realtime_cdc_reservation WHERE owner_id=?",Integer.class,second));
    }
    @Test void terminalCdcScanFindsReservationsWithoutGatewayHandlesAndKeepsUnknownSubmissionsForReconciliation(){
        String endpoint="cdc-"+UUID.randomUUID()+":3306",owner=cdcOwner("job-");
        var job=new LinkedHashMap<String,Object>(Map.of("id",owner,"taskId","test-task","workspaceId",workspace,"status","FAILED","updatedAt",RealtimeService.now(),"submissionAttempted",true,"attemptId","attempt"));repo.insertJob(job);
        repo.reserveCdcServerIds("JOB",owner,"attempt",List.of(cdcRange(endpoint,5401,5656,"source")));
        assertTrue(repo.terminalCdcJobs().stream().anyMatch(value->owner.equals(value.get("id"))));
        assertFalse(repo.terminalGatewayCleanupJobs().stream().anyMatch(value->owner.equals(value.get("id"))));
        repo.releaseCdcServerIds("JOB",owner,"attempt");assertFalse(repo.terminalCdcJobs().stream().anyMatch(value->owner.equals(value.get("id"))));
    }
}
