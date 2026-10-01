package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.*;
import java.time.Instant;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** The execution admission, snapshots and cancellation checks never connect to a database. */
class ExecutionConsistencyTest {
    final StudioRepository repo=mock(StudioRepository.class);
    final TaskRepository store=mock(TaskRepository.class);
    final ObjectService objects=mock(ObjectService.class);
    final DatasourceService sources=mock(DatasourceService.class);
    final MysqlExecutionProvider mysql=mock(MysqlExecutionProvider.class);
    final InventoryExecutionService inventory=mock(InventoryExecutionService.class);
    final SyncExecutionService sync=mock(SyncExecutionService.class);
    final JsonCodec json=new JsonCodec();
    final TransactionTemplate tx=new TransactionTemplate(new MemoryTransactions());
    final Map<String,Map<String,Object>> records=new ConcurrentHashMap<>();
    TaskService tasks;
    final String date="2026-09-28",cutoff="2026-09-29T00:00:00Z";
    final DatasourceService.ConnectionSpec original=new DatasourceService.ConnectionSpec("source","workspace","original","127.0.0.1",3307,"original_db","user","cipher","MYSQL",Map.of());

    @BeforeEach void setup(){
        when(repo.run(anyString())).thenAnswer(i->Optional.ofNullable(records.get(i.getArgument(0))).map(LinkedHashMap::new));
        when(repo.runsByIds(anyCollection())).thenAnswer(i->{Collection<String> ids=i.getArgument(0);Map<String,Map<String,Object>> found=new HashMap<>();for(String id:ids)if(records.containsKey(id))found.put(id,new LinkedHashMap<>(records.get(id)));return found;});
        doAnswer(i->{Map<String,Object> value=i.getArgument(0);records.put(value.get("id").toString(),new LinkedHashMap<>(value));return null;}).when(repo).insertRun(anyMap(),any());
        doAnswer(i->{Map<String,Object> value=i.getArgument(0);records.put(value.get("id").toString(),new LinkedHashMap<>(value));return null;}).when(repo).updateRun(anyMap());
        when(repo.transitionRun(anyMap(),anyString())).thenAnswer(i->{Map<String,Object> value=i.getArgument(0);synchronized(records){var before=records.get(value.get("id"));if(before==null||!Objects.equals(before.get("status"),i.getArgument(1)))return false;records.put(value.get("id").toString(),new LinkedHashMap<>(value));return true;}});
        when(repo.childRuns(anyString())).thenAnswer(i->records.values().stream().filter(r->Objects.equals(r.get("parentRunId"),i.getArgument(0))).map(r->(Map<String,Object>)new LinkedHashMap<>(r)).toList());
        when(repo.topRuns(anyString())).thenAnswer(i->records.values().stream().filter(r->r.get("parentRunId")==null&&Objects.equals(r.get("workspaceId"),i.getArgument(0))).map(r->(Map<String,Object>)new LinkedHashMap<>(r)).toList());
        when(repo.hasActiveTaskRun(anyString(),anyString(),anyString())).thenAnswer(i->records.values().stream().anyMatch(r->Objects.equals(r.get("workspaceId"),i.getArgument(0))&&Objects.equals(r.get("objectId"),i.getArgument(1))&&!Objects.equals(r.get("id"),i.getArgument(2))&&Set.of("QUEUED","RUNNING","RECOVERING").contains(r.get("status"))));
        when(store.forTask(anyString())).thenReturn(Optional.empty());
        when(sources.forWorkspace(anyString(),anyString())).thenReturn(original);
        when(mysql.prepare(any(),any())).thenAnswer(i->query(i.getArgument(0),i.getArgument(1)));
        when(mysql.newRun(any(),anyString())).thenCallRealMethod();
        when(mysql.tryEnqueueExisting(anyString(),any())).thenReturn(true);
        when(mysql.reserve(anyString())).thenReturn(true);
        when(inventory.parameters(anyMap(),anyString())).thenCallRealMethod();
        tasks=new TaskService(store,repo,objects,sources,mysql,inventory,json,tx,sync);
    }

    StudioObject node(String id,String code){return new StudioObject(id,"workspace",null,"NODE","MySQL",id,"",code,Map.of("run",Map.of("provider","MYSQL","dataSourceId","source","taskExecutionVersion",1)),List.of(),false,false,1,"admin",Instant.now().toString());}
    MysqlExecutionProvider.PreparedQuery query(StudioObject node,DatasourceService.ConnectionSpec source){return new MysqlExecutionProvider.PreparedQuery(node,source,30,SqlScript.prepare(node.content(),source.database(),new SqlGuard(),"MYSQL"));}
    Map<String,Object> child(StudioObject node){return new LinkedHashMap<>(Map.of("id","child","objectId",node.id(),"workspaceId","workspace","parentRunId","parent","graphNodeId","n0","status","WAITING","businessDate",date,"sourceCutoffAt",cutoff,"createdAt",Instant.now().toString(),"releaseId","workflow-release"));}
    Map<String,Object> upstream(String id,String build){return new LinkedHashMap<>(Map.of("id",id,"objectId","upstream","objectName","upstream","workspaceId","workspace","businessDate",date,"status","SUCCESS","buildId",build,"publicationStatus","PUBLISHED","targetTable","dwd_inventory_ledger_di","lineage",Map.of("dwd_inventory_ledger_di",build)));}
    Map<String,Object> release(StudioObject node){return Map.of("id","release","taskId",node.id(),"snapshot",node,"releaseNo",1,"dataSource",original.publicView());}

    @Test void workflowUsesFrozenQueryConnectionAndAliasWithoutReadingEditablePlans(){
        var node=node("downstream","SELECT :upstream_original_build_id AS build");var prepared=query(node,original);var child=child(node);records.put("child",child);var upstream=upstream("upstream-run","fixed-build");records.put("upstream-run",upstream);
        var run=tasks.startWorkflowNode(node,child,List.of(upstream),prepared,null,null,List.of(Map.of("taskId","upstream","alias","original")));
        assertEquals("fixed-build",((Map<?,?>)run.get("parameters")).get("upstream_original_build_id"));
        assertEquals("workflow-release",run.get("releaseId"));verify(mysql).tryEnqueueExisting(eq("child"),same(prepared));verify(sources,never()).forWorkspace(anyString(),anyString());verify(store,never()).forTask(anyString());verify(mysql,never()).prepare(any(),any());
    }

    @Test void ordinaryWorkflowFanInUsesGraphIdentityWhenCompletionOnlyParentsShareTheSameTaskAlias(){
        var node=node("downstream","SELECT 1");var child=child(node);records.put("child",child);List<Map<String,Object>> upstream=new ArrayList<>();
        for(String graphId:List.of("copy-a","copy-b")){var run=upstream(graphId,"unused");run.remove("buildId");run.remove("targetTable");run.remove("lineage");run.put("graphNodeId",graphId);records.put(graphId,run);upstream.add(run);}
        var run=tasks.startWorkflowNode(node,child,upstream,query(node,original),null,null,List.of(Map.of("taskId","upstream","alias","same")));
        var inputs=(List<Map<String,Object>>)run.get("upstreamRuns");assertEquals(2,inputs.stream().map(input->input.get("alias")).distinct().count());assertEquals(Map.of(),run.get("lineage"));
        records.put("child",child);var scalar=node("downstream","SELECT :upstream_same_build_id");assertEquals("AMBIGUOUS_DEPENDENCY_PARAMETER",assertThrows(StudioException.class,()->tasks.startWorkflowNode(scalar,child,upstream,query(scalar,original),null,null,List.of(Map.of("taskId","upstream","alias","same")))).code());
    }

    @Test void allDayValidatesEveryRunButDoesNotChooseOneBuildOrMergeSnapshotLineage(){
        var node=node("downstream","SELECT 1");when(store.release("release")).thenReturn(release(node));records.put("first",upstream("first","build-a"));records.put("second",upstream("second","build-b"));
        var inputs=List.of(Map.<String,Object>of("taskId","upstream","alias","day","runId","first","matchMode","ALL_DAY"),Map.<String,Object>of("taskId","upstream","alias","day","runId","second","matchMode","ALL_DAY"));
        var run=tasks.startRelease("release",Map.of("businessDate",date,"sourceCutoffAt",cutoff,"upstreamRuns",inputs));
        assertEquals(2,((List<?>)run.get("upstreamRuns")).size());assertEquals(Map.of(),run.get("lineage"));assertFalse(((Map<?,?>)run.get("parameters")).containsKey("upstream_day_build_id"));
    }

    @Test void allDayScalarFailsBeforeAnyExecutionOrRunIsInserted(){
        var node=node("downstream","INSERT INTO target_table VALUES (:upstream_day_build_id)");when(store.release("release")).thenReturn(release(node));records.put("first",upstream("first","build-a"));
        var input=Map.<String,Object>of("taskId","upstream","alias","day","runId","first","matchMode","ALL_DAY");
        var error=assertThrows(StudioException.class,()->tasks.startRelease("release",Map.of("businessDate",date,"sourceCutoffAt",cutoff,"upstreamRuns",List.of(input))));
        assertEquals("AMBIGUOUS_DEPENDENCY_PARAMETER",error.code());verify(repo,never()).insertRun(anyMap(),any());verify(mysql,never()).enqueueExisting(anyString(),any());
    }

    @Test void queueCapacityFailureCreatesNoAttemptAndRollbackReleasesItsReservation(){
        var node=node("queued","SELECT 1");when(store.release("release")).thenReturn(release(node));when(mysql.reserve(anyString())).thenReturn(false);
        assertEquals("QUEUE_FULL",assertThrows(StudioException.class,()->tasks.startRelease("release",Map.of("businessDate",date))).code());verify(repo,never()).insertRun(anyMap(),any());
        when(mysql.reserve(anyString())).thenReturn(true);doThrow(new IllegalStateException("metadata unavailable")).when(repo).insertRun(anyMap(),any());assertThrows(IllegalStateException.class,()->tasks.startRelease("release",Map.of("businessDate",date)));verify(mysql).releaseReservation(anyString());verify(mysql,never()).enqueueDeferred(anyString(),any());
    }

    @Test void standaloneSqlAdmissionRollsBackItsCapacityWhenMetadataCannotBeSaved() throws Exception {
        var provider=new MysqlExecutionProvider(repo,sources,new SqlGuard(),json,tx,1,1);doThrow(new IllegalStateException("metadata unavailable")).when(repo).insertRun(anyMap(),any());
        try{assertThrows(IllegalStateException.class,()->provider.start(node("standalone","SELECT 1"),"MANUAL",false));assertTrue(provider.reserve("after-one"));assertTrue(provider.reserve("after-two"));assertFalse(provider.reserve("no-capacity"));verify(sources,never()).open(any(),anyInt(),anyBoolean());}finally{provider.shutdown();}
    }

    @Test void recoveryInAnotherBusinessDatabaseOrSyncDoesNotBlockInventory() throws Exception {
        var service=new InventoryExecutionService(sources,new SqlGuard(),repo,tx);var lease=mock(Connection.class);var lock=mock(PreparedStatement.class);var result=mock(ResultSet.class);when(sources.open(original,300,true)).thenReturn(lease);when(lease.prepareStatement(anyString())).thenReturn(lock);when(lock.executeQuery()).thenReturn(result);when(result.getInt(1)).thenReturn(1);
        when(repo.unfinishedRuns()).thenReturn(List.of(Map.of("status","RECOVERING","provider","SYNC"),Map.of("status","RECOVERING","materialization",true,"dataSource",Map.of("host","127.0.0.1","port",3307,"database","other_db"))));
        try{var batch=service.begin("new",original,Map.of());service.release(batch);clearInvocations(sources);when(repo.unfinishedRuns()).thenReturn(List.of(Map.of("status","RECOVERING","materialization",true,"dataSource",original.publicView())));assertEquals("RECOVERY_REQUIRED",assertThrows(StudioException.class,()->service.begin("blocked",original,Map.of())).code());verify(sources,never()).open(any(),anyInt(),anyBoolean());}finally{service.shutdown();}
    }

    @Test void workflowChildBlocksManualAndPublishedEntrypointsForTheSameTask(){
        var node=node("shared","SELECT 1");var running=child(node);running.put("status","RUNNING");records.put("child",running);when(store.release("release")).thenReturn(release(node));
        assertEquals("TASK_OVERLAP",assertThrows(StudioException.class,()->tasks.startDevelopment(node,Map.of("businessDate",date))).code());
        assertEquals("TASK_OVERLAP",assertThrows(StudioException.class,()->tasks.startRelease("release",Map.of("businessDate",date))).code());verify(mysql,never()).prepare(any(),any());
    }

    @Test void manualDateUsesAppliedScheduleTimeUnlessAnExplicitContextIsProvided(){
        var node=node("manual","SELECT 1");when(store.forTask("manual")).thenReturn(Optional.of(Map.of("cron","0 0 6 * * *","timezone","Asia/Shanghai","businessDateOffset",-1)));
        var run=tasks.startDevelopment(node,Map.of("businessDate",date));assertEquals("2026-09-28T22:00:00Z",run.get("scheduledAt"));
        records.clear();var explicit=tasks.startDevelopment(node,Map.of("businessDate",date,"scheduledAt","2026-09-30T01:00:00Z","timezone","UTC"));assertEquals("2026-09-30T01:00:00Z",explicit.get("scheduledAt"));assertEquals("UTC",explicit.get("timezone"));
    }

    @Test void workflowRunsTwoIndependentNodesAndWaitsForCancellationAcknowledgement(){
        var a=node("a","SELECT 1");var b=node("b","SELECT 2");var c=node("c","SELECT 3");
        var graph=Map.of("nodes",List.of(Map.of("id","a","objectId","a"),Map.of("id","b","objectId","b"),Map.of("id","c","objectId","c")),"edges",List.of(Map.of("id","ac","source","a","target","c")));
        var workflow=new StudioObject("workflow","workspace",null,"WORKFLOW","手动工作流","workflow","","",Map.of("run",Map.of("provider","WORKFLOW"),"graph",graph),List.of(),false,false,1,"admin",Instant.now().toString());
        when(objects.active("workflow")).thenReturn(workflow);when(objects.active("a")).thenReturn(a);when(objects.active("b")).thenReturn(b);when(objects.active("c")).thenReturn(c);
        var tasks=mock(TaskService.class);when(tasks.dependencyBindings(anyString())).thenReturn(List.of());
        when(tasks.startWorkflowNode(any(),anyMap(),anyList(),any(),any(),any(),anyList())).thenAnswer(i->{Map<String,Object> child=i.getArgument(1);child.put("status","RUNNING");repo.updateRun(child);return child;});
        for(boolean cancel:List.of(false,true)){records.clear();var workflows=new WorkflowService(repo,objects,sources,new GraphValidator(),mysql,inventory,tasks,json,tx,100,8,sync);try{
            var parent=workflows.startDevelopment("workflow",1,Map.of("a",1,"b",1,"c",1),Map.of("businessDate",date));String id=parent.get("id").toString();assertNotNull(parent.get("sourceCutoffAt"));assertTrue(repo.childRuns(id).stream().allMatch(child->Objects.equals(parent.get("sourceCutoffAt"),child.get("sourceCutoffAt"))));ReflectionTestUtils.invokeMethod(workflows,"tick");
            assertEquals(2,repo.childRuns(id).stream().filter(r->"RUNNING".equals(r.get("status"))).count());
            if(cancel){var stopping=workflows.stop(id);assertEquals("RUNNING",stopping.get("status"));assertEquals(true,stopping.get("cancelRequested"));}
            for(var child:repo.childRuns(id))if("RUNNING".equals(child.get("status"))){child.put("status","FAILED");child.put("errorCode","b".equals(child.get("graphNodeId"))?"COMMIT_UNKNOWN":"QUERY_FAILED");repo.updateRun(child);}
            ReflectionTestUtils.invokeMethod(workflows,"tick");assertEquals("FAILED",repo.run(id).orElseThrow().get("status"));assertEquals("COMMIT_UNKNOWN",repo.run(id).orElseThrow().get("errorCode"));assertEquals(true,repo.run(id).orElseThrow().get("commitUnknown"));
        }finally{workflows.shutdown();}}
    }

    @Test void queuedSqlWithdrawalFreesExecutorCapacityAndNeverStartsWithdrawnAttempt() throws Exception {
        var provider=new MysqlExecutionProvider(repo,sources,new SqlGuard(),json,tx,1,1);var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var first=node("first","SELECT 1");var second=node("second","SELECT 2");var third=node("third","SELECT 3");
        var p1=query(first,original);var p2=query(second,original);var p3=query(third,original);var r1=provider.newRun(p1,"MANUAL");var r2=provider.newRun(p2,"SCHEDULED");var r3=provider.newRun(p3,"MANUAL");for(var run:List.of(r1,r2,r3))records.put(run.get("id").toString(),run);
        when(repo.transitionRun(anyMap(),eq("QUEUED"))).thenAnswer(i->{Map<String,Object> value=i.getArgument(0);if(value.get("id").equals(r1.get("id"))&&"RUNNING".equals(value.get("status"))){entered.countDown();assertTrue(release.await(5,TimeUnit.SECONDS));}synchronized(records){var before=records.get(value.get("id"));if(!"QUEUED".equals(before.get("status")))return false;records.put(value.get("id").toString(),new LinkedHashMap<>(value));return true;}});
        try{assertTrue(provider.tryEnqueueExisting(r1.get("id").toString(),p1));assertTrue(entered.await(5,TimeUnit.SECONDS));assertTrue(provider.tryEnqueueExisting(r2.get("id").toString(),p2));assertTrue(provider.withdrawQueued(r2.get("id").toString(),"SCHEDULE_PAUSED"));assertTrue(provider.tryEnqueueExisting(r3.get("id").toString(),p3));assertEquals("CANCELLED",repo.run(r2.get("id").toString()).orElseThrow().get("status"));verify(sources,never()).open(eq(original),anyInt(),eq(true));}finally{release.countDown();provider.shutdown();}
    }

    static class MemoryTransactions extends AbstractPlatformTransactionManager {
        protected Object doGetTransaction(){return new Object();}
        protected void doBegin(Object transaction,TransactionDefinition definition){}
        protected void doCommit(DefaultTransactionStatus status){}
        protected void doRollback(DefaultTransactionStatus status){}
    }
}
