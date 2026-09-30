package com.fake.dataworks;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.dto.ObjectInput;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.StudioRepository;
import com.fake.dataworks.service.*;
import java.util.*;
import java.sql.*;
import java.net.*;
import java.net.http.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT,properties={"studio.simulation.recover-on-start=false","studio.mysql.workers=1","studio.mysql.queue-size=1","studio.workflow.max-nodes=10","studio.workflow.max-active=2"})
class WorkflowIntegrationTest {
    @Autowired WorkflowService workflows;
    @Autowired RunService runs;
    @Autowired ObjectService objects;
    @Autowired DatasourceService sources;
    @Autowired MysqlExecutionProvider mysql;
    @Autowired JdbcTemplate jdbc;
    @Autowired JsonCodec json;
    @MockitoSpyBean StudioRepository repo;
    @Value("${studio.demo.password}") String password;
    @LocalServerPort int port;
    String workspace,sourceId;
    @BeforeEach void setup() {
        workspace="workflow-test-"+UUID.randomUUID();jdbc.update("INSERT INTO dw_workspace VALUES(?,?,?,?)",workspace,"工作流测试",workspace,"local");
        sourceId=sources.save(null,Map.of("workspaceId",workspace,"name","Test source","host","127.0.0.1","port",3307,"database","studio_demo","username","studio_reader","password",password)).get("id").toString();
    }
    @AfterEach void cleanup() throws Exception {
        reset(repo);
        for(var r:repo.topRuns(workspace))runs.stop(r.get("id").toString());
        for(var r:repo.runs(workspace))if("MYSQL".equals(r.get("provider")))mysql.stop(r.get("id").toString());
        Thread.sleep(200);
        jdbc.update("DELETE FROM dw_workflow_release WHERE workspace_id=?",workspace);
        jdbc.update("DELETE FROM dw_run WHERE workspace_id=?",workspace);
        jdbc.update("DELETE FROM dw_version WHERE object_id IN (SELECT id FROM dw_object WHERE workspace_id=?)",workspace);
        jdbc.update("DELETE FROM dw_object WHERE workspace_id=?",workspace);jdbc.update("DELETE FROM dw_datasource WHERE workspace_id=?",workspace);jdbc.update("DELETE FROM dw_workspace WHERE id=?",workspace);
    }
    StudioObject node(String sql) {return objects.create(new ObjectInput(workspace,null,"NODE","MySQL",UUID.randomUUID().toString(),"",sql,Map.of("run",Map.of("provider","MYSQL","dataSourceId",sourceId,"timeoutSeconds",10)),List.of(),false,null));}
    StudioObject workflow(List<StudioObject> nodes,int[][] edges) {
        List<Map<String,Object>> graphNodes=new ArrayList<>(),graphEdges=new ArrayList<>();
        for(int i=0;i<nodes.size();i++)graphNodes.add(Map.of("id","n"+i,"objectId",nodes.get(i).id(),"label","Node "+i,"nodeType","MySQL","x",i*100,"y",0));
        for(int[] edge:edges)graphEdges.add(Map.of("id","e"+edge[0]+"-"+edge[1],"source","n"+edge[0],"target","n"+edge[1]));
        return objects.create(new ObjectInput(workspace,null,"WORKFLOW","手动工作流",UUID.randomUUID().toString(),"","",Map.of("run",Map.of("provider","WORKFLOW"),"graph",Map.of("nodes",graphNodes,"edges",graphEdges)),List.of(),false,null));
    }
    Map<String,Object> versions(StudioObject... nodes) {Map<String,Object> result=new HashMap<>();for(var n:nodes)result.put(n.id(),n.version());return result;}
    String submit(StudioObject w,StudioObject... nodes) {return runs.submit(w.id(),"MANUAL",false,w.version(),versions(nodes)).get("id").toString();}
    Map<String,Object> publish(StudioObject w,StudioObject... nodes) {return workflows.publish(w.id(),w.version(),versions(nodes),"release test");}
    Map<String,Object> terminal(String id) throws Exception {
        for(int i=0;i<400;i++) {var r=runs.required(id);if(!Set.of("WAITING","QUEUED","RUNNING").contains(r.get("status")))return r;Thread.sleep(30);}throw new AssertionError("No terminal state: "+id);
    }
    Map<String,Object> runningChild(String id) throws Exception {
        for(int i=0;i<200;i++) {var r=workflows.nodes(id).stream().filter(n->"RUNNING".equals(n.get("status"))).findFirst();if(r.isPresent())return r.get();Thread.sleep(20);}throw new AssertionError("No running child");
    }
    Map<String,Map<String,Object>> children(String id) {Map<String,Map<String,Object>> result=new HashMap<>();for(var r:workflows.nodes(id))result.put(r.get("graphNodeId").toString(),r);return result;}
    StudioObject edit(StudioObject o,String content) {return objects.update(o.id(),new ObjectInput(workspace,null,o.kind(),o.nodeType(),o.name(),o.description(),content,o.config(),o.tags(),false,o.version()));}
    @Test void releaseRetainsWriteFlagForSchedulerRetryPolicy() {
        var read=node("SELECT 1");var readWorkflow=workflow(List.of(read),new int[][]{});
        var readRelease=publish(readWorkflow,read);
        assertEquals(false,workflows.release(readRelease.get("id").toString()).get("containsWrites"));
        var write=node("DELETE FROM orders WHERE 1=0");var writeWorkflow=workflow(List.of(write),new int[][]{});
        var writeRelease=publish(writeWorkflow,write);
        assertEquals(true,workflows.release(writeRelease.get("id").toString()).get("containsWrites"));
    }
    @Test void workflowParametersInheritOverrideAndStayInReleaseSnapshots() throws Exception {
        var a=node("SELECT '${region}' AS region, '${bizdate}' AS custom_day, :bizdate AS legacy_day");
        var nodeConfig=new LinkedHashMap<String,Object>(a.config());nodeConfig.put("schedule",Map.of("parameters",List.of(Map.of("name","region","value","child","source","CODE"))));
        a=objects.update(a.id(),new ObjectInput(workspace,null,a.kind(),a.nodeType(),a.name(),"",a.content(),nodeConfig,List.of(),false,a.version()));
        var b=node(a.content());var w=workflow(List.of(a,b),new int[][]{{0,1}});
        var config=new LinkedHashMap<String,Object>(w.config());config.put("schedule",Map.of("cron","0 0 2 * * *","timezone","Asia/Shanghai","businessDateOffset",-1,"parameters",List.of(Map.of("name","region","value","parent","source","MANUAL"),Map.of("name","bizdate","value","$[yyyymmdd-1]","source","MANUAL"))));
        w=objects.update(w.id(),new ObjectInput(workspace,null,w.kind(),w.nodeType(),w.name(),"","",config,List.of(),false,w.version()));
        var release=publish(w,a,b);
        String id=workflows.startRelease(release.get("id").toString(),Map.of("businessDate","2026-09-28")).get("id").toString();
        assertEquals("SUCCESS",terminal(id).get("status"));var children=children(id);
        assertEquals(Map.of("region","child","bizdate","20260928"),children.get("n0").get("scheduleParameters"));
        assertEquals(Map.of("region","parent","bizdate","20260928"),children.get("n1").get("scheduleParameters"));
        for(var child:children.values())assertEquals("2026-09-28T18:00:00Z",child.get("scheduledAt"));
        assertEquals(List.of("child","20260928","2026-09-28"),((List<?>)runs.results(children.get("n0").get("id").toString(),1,100).get("rows")).getFirst());
        nodeConfig.put("schedule",Map.of("parameters",List.of(Map.of("name","region","value","changed","source","CODE"))));
        objects.update(a.id(),new ObjectInput(workspace,null,a.kind(),a.nodeType(),a.name(),"",a.content(),nodeConfig,List.of(),false,a.version()));
        String repeated=workflows.startRelease(release.get("id").toString(),Map.of("businessDate","2026-09-28")).get("id").toString();
        assertEquals("SUCCESS",terminal(repeated).get("status"));assertEquals(children.get("n0").get("scheduleParameters"),children(repeated).get("n0").get("scheduleParameters"));
    }
    @Test void executesForkJoinAndRepeatedObjectWithIsolatedHistory() throws Exception {
        var a=node("SELECT 1 AS n");var b=node("SELECT 2 AS n");var w=workflow(List.of(a,b,a),new int[][]{{0,2},{1,2}});
        String id=submit(w,a,b);assertEquals("SUCCESS",terminal(id).get("status"));var children=children(id);assertEquals(3,children.size());
        for(var child:children.values()) {assertEquals("SUCCESS",child.get("status"));assertEquals(1,runs.results(child.get("id").toString(),1,100).get("total"));}
        assertEquals("1",((List<?>)((List<?>)runs.results(children.get("n2").get("id").toString(),1,100).get("rows")).getFirst()).getFirst());
        assertEquals(1,repo.topRuns(workspace).size());assertEquals(1L,((Number)repo.runSummaries(workspace,1,12,"","").get("total")).longValue());
        assertEquals(4,repo.runs(workspace).size());
    }
    @Test void failureSkipsDescendantsButContinuesIndependentBranch() throws Exception {
        var bad=node("SELECT absent_column FROM orders");var good=node("SELECT 2");var child=node("SELECT 3");var join=node("SELECT 4");
        var w=workflow(List.of(bad,good,child,join),new int[][]{{0,2},{2,3},{1,3}});String id=submit(w,bad,good,child,join);
        assertEquals("FAILED",terminal(id).get("status"));var c=children(id);
        assertEquals("FAILED",c.get("n0").get("status"));assertEquals("SUCCESS",c.get("n1").get("status"));assertEquals("SKIPPED",c.get("n2").get("status"));assertEquals("SKIPPED",c.get("n3").get("status"));
    }
    @Test void releasesRemainExecutableAfterEditsAndDeletion() throws Exception {
        var a=node("SELECT 'R1' AS version");var w=workflow(List.of(a),new int[][]{});var r1=publish(w,a);
        assertEquals(0,repo.runs(workspace).size());a=edit(a,"SELECT 'R2' AS version");var r2=publish(w,a);
        assertEquals(1,r1.get("releaseNo"));assertEquals(2,r2.get("releaseNo"));objects.delete(a.id());objects.delete(w.id());
        for(var release:List.of(r1,r2)) {
            String id=workflows.startRelease(release.get("id").toString()).get("id").toString();assertEquals("SUCCESS",terminal(id).get("status"));
            var child=workflows.nodes(id).getFirst();var rows=(List<?>)runs.results(child.get("id").toString(),1,100).get("rows");assertEquals("R"+release.get("releaseNo"),((List<?>)rows.getFirst()).getFirst());
            assertEquals("RELEASE",child.get("executionSource"));assertEquals(release.get("id"),child.get("releaseId"));
        }
        assertEquals(2,workflows.releases(workspace,"").size());
    }
    @Test void preflightAndVersionConflictsLeaveNoPartialExecutionOrRelease() {
        var a=node("SELECT 1");var bad=node("SET autocommit=0");var w=workflow(List.of(a,bad),new int[][]{{0,1}});
        assertThrows(StudioException.class,()->submit(w,a,bad));assertThrows(StudioException.class,()->publish(w,a,bad));
        assertTrue(repo.runs(workspace).isEmpty());assertTrue(workflows.releases(workspace,"").isEmpty());
        var valid=workflow(List.of(a),new int[][]{});edit(a,"SELECT 2");
        assertEquals("VERSION_CONFLICT",assertThrows(StudioException.class,()->submit(valid,a)).code());
        assertEquals("VERSION_CONFLICT",assertThrows(StudioException.class,()->workflows.publish(valid.id(),999,versions(a),"")).code());
        assertTrue(repo.runs(workspace).isEmpty());
    }
    @Test void transactionFailureDoesNotScheduleAnyChild() {
        var a=node("SELECT 1");var w=workflow(List.of(a,a),new int[][]{{0,1}});
        doAnswer(invocation->{Map<?,?> run=invocation.getArgument(0);if("n1".equals(run.get("graphNodeId")))throw new IllegalStateException("transaction failure");return invocation.callRealMethod();}).when(repo).insertRun(anyMap(),any(StudioObject.class));
        assertThrows(IllegalStateException.class,()->submit(w,a));reset(repo);assertTrue(repo.runs(workspace).isEmpty());
    }
    @Test void freezesAllNodesBeforeTheyStart() throws Exception {
        var slow=node("SELECT SLEEP(1) AS delay");var a=node("SELECT 'original' AS value");var w=workflow(List.of(slow,a),new int[][]{{0,1}});
        String id=submit(w,slow,a);runningChild(id);edit(a,"SELECT 'changed' AS value");sources.save(sourceId,Map.of("port",1));
        assertEquals("SUCCESS",terminal(id).get("status"));var child=children(id).get("n1");assertEquals("SELECT 'original' AS value",((Map<?,?>)runs.detail(child.get("id").toString()).get("snapshot")).get("content"));
        assertEquals("original",((List<?>)((List<?>)runs.results(child.get("id").toString(),1,100).get("rows")).getFirst()).getFirst());
    }
    @Test void cancellationStopsRealQueryAndNeverStartsDownstream() throws Exception {
        var slow=node("SELECT SLEEP(8) AS workflow_cancel_probe");var a=node("SELECT 1");var w=workflow(List.of(slow,a),new int[][]{{0,1}});
        String id=submit(w,slow,a);var child=runningChild(id);Thread.sleep(100);
        assertEquals("STOP_PARENT_WORKFLOW",assertThrows(StudioException.class,()->runs.stop(child.get("id").toString())).code());
        assertEquals("CANCELLED",runs.stop(id).get("status"));assertEquals("CANCELLED",runs.stop(id).get("status"));Thread.sleep(300);
        for(var r:workflows.nodes(id))assertEquals("CANCELLED",r.get("status"));
        try(Connection c=sources.open(sources.get(sourceId),5);Statement statement=c.createStatement();ResultSet rs=statement.executeQuery("SHOW PROCESSLIST")) {
            while(rs.next())assertFalse(Objects.toString(rs.getString("Info"),"").contains("workflow_cancel_probe"));
        }
    }
    @Test void credentialsRotateButConnectionIdentityCannotChangeSilently() throws Exception {
        var a=node("SELECT 1");var w=workflow(List.of(a),new int[][]{});var release=publish(w,a);String id=release.get("id").toString();
        sources.save(sourceId,Map.of("password",password,"name","renamed source"));assertEquals("SUCCESS",terminal(workflows.startRelease(id).get("id").toString()).get("status"));
        sources.save(sourceId,Map.of("port",1));assertEquals("DATASOURCE_BINDING_CHANGED",assertThrows(StudioException.class,()->workflows.startRelease(id)).code());
        String cipher=jdbc.queryForObject("SELECT password_cipher FROM dw_datasource WHERE id=?",String.class,sourceId);
        String publicJson=json.write(workflows.release(id))+json.write(repo.runs(workspace));assertFalse(publicJson.contains(password));assertFalse(publicJson.contains(cipher));
    }
    @Test void queueRejectionAndTimeoutStillContinueIndependentNodes() throws Exception {
        var slow=node("SELECT SLEEP(8) AS occupied");String first=runs.submit(slow.id(),"MANUAL",false,slow.version()).get("id").toString();
        for(int i=0;i<100&&!"RUNNING".equals(runs.required(first).get("status"));i++)Thread.sleep(20);
        String queued=runs.submit(slow.id(),"MANUAL",false,slow.version()).get("id").toString();
        var a=node("SELECT 1");var w=workflow(List.of(a,a),new int[][]{{0,1}});String id=submit(w,a);
        assertEquals("FAILED",terminal(id).get("status"));assertEquals("QUEUE_FULL",children(id).get("n0").get("errorCode"));assertEquals("SKIPPED",children(id).get("n1").get("status"));
        runs.stop(first);runs.stop(queued);
        var timeout=objects.update(slow.id(),new ObjectInput(workspace,null,"NODE","MySQL",slow.name(),"",slow.content(),Map.of("run",Map.of("provider","MYSQL","dataSourceId",sourceId,"timeoutSeconds",1)),List.of(),false,slow.version()));
        var tw=workflow(List.of(timeout,a),new int[][]{});String tid=submit(tw,timeout,a);assertEquals("FAILED",terminal(tid).get("status"));assertEquals("QUERY_TIMEOUT",children(tid).get("n0").get("errorCode"));assertEquals("SUCCESS",children(tid).get("n1").get("status"));
    }
    @Test void validatesBindingsLimitsAndLegacyReleaseIsolation() throws Exception {
        var a=node("SELECT 1");var empty=workflow(List.of(),new int[][]{});assertEquals("EMPTY_WORKFLOW",assertThrows(StudioException.class,()->submit(empty)).code());
        var oversized=workflow(Collections.nCopies(11,a),new int[][]{});assertEquals("WORKFLOW_TOO_LARGE",assertThrows(StudioException.class,()->submit(oversized,a)).code());
        var simulation=objects.create(new ObjectInput(workspace,null,"NODE","MySQL","simulated","","SELECT 1",Map.of(),List.of(),false,null));var sw=workflow(List.of(simulation),new int[][]{});assertThrows(StudioException.class,()->submit(sw,simulation));
        var w=workflow(List.of(a),new int[][]{});var input=new HashMap<>(w.config());input.put("graph",Map.of("nodes",List.of(Map.of("id","unbound","label","Unbound")),"edges",List.of()));
        var unbound=objects.update(w.id(),new ObjectInput(workspace,null,"WORKFLOW",w.nodeType(),w.name(),"","",input,List.of(),false,w.version()));assertEquals("UNBOUND_WORKFLOW_NODE",assertThrows(StudioException.class,()->submit(unbound,a)).code());
        var release=publish(workflow(List.of(a),new int[][]{}),a);var client=HttpClient.newHttpClient();String base="http://127.0.0.1:"+port+"/api/v1";
        assertEquals(404,client.send(HttpRequest.newBuilder(URI.create(base+"/records/"+release.get("id"))).header("Content-Type","application/json").method("PATCH",HttpRequest.BodyPublishers.ofString("{\"payload\":{}}")).build(),HttpResponse.BodyHandlers.ofString()).statusCode());
        var response=client.send(HttpRequest.newBuilder(URI.create(base+"/workflow-releases/"+release.get("id")+"/runs")).POST(HttpRequest.BodyPublishers.noBody()).build(),HttpResponse.BodyHandlers.ofString());assertEquals(201,response.statusCode());
        String id=json.map(response.body()).get("id").toString();assertEquals("SUCCESS",terminal(id).get("status"));
        assertEquals(200,client.send(HttpRequest.newBuilder(URI.create(base+"/runs/"+id+"/nodes")).build(),HttpResponse.BodyHandlers.ofString()).statusCode());
    }
    @Test void rejectsCrossWorkspaceAndDeletedReferencesBeforeAnyRun() {
        var foreign=objects.create(new ObjectInput("local-workspace",null,"NODE","MySQL","foreign-"+UUID.randomUUID(),"","SELECT 1",Map.of(),List.of(),false,null));var invalid=workflow(List.of(foreign),new int[][]{});
        assertEquals("WORKSPACE_MISMATCH",assertThrows(StudioException.class,()->submit(invalid,foreign)).code());
        jdbc.update("DELETE FROM dw_version WHERE object_id=?",foreign.id());jdbc.update("DELETE FROM dw_object WHERE id=?",foreign.id());
        var a=node("SELECT 1");var w=workflow(List.of(a),new int[][]{});objects.delete(a.id());
        assertEquals("OBJECT_DELETED",assertThrows(StudioException.class,()->submit(w,a)).code());assertTrue(repo.runs(workspace).isEmpty());
    }
    @Test void cancelsAResourceQueuedWorkflowWithoutStartingItsSql() throws Exception {
        var slow=node("SELECT SLEEP(8) AS occupied_for_cancel");String occupied=runs.submit(slow.id(),"MANUAL",false,slow.version()).get("id").toString();
        for(int i=0;i<100&&!"RUNNING".equals(runs.required(occupied).get("status"));i++)Thread.sleep(20);
        var a=node("SELECT 1");var w=workflow(List.of(a,a),new int[][]{{0,1}});String id=submit(w,a);
        for(int i=0;i<100&&!"QUEUED".equals(children(id).get("n0").get("status"));i++)Thread.sleep(20);
        assertEquals("QUEUED",children(id).get("n0").get("status"));assertEquals("CANCELLED",runs.stop(id).get("status"));runs.stop(occupied);Thread.sleep(300);
        for(var child:workflows.nodes(id)) {assertEquals("CANCELLED",child.get("status"));assertNull(child.get("startedAt"));}
    }
    @Test void enforcesActiveLimitAndCancellationReleasesCapacity() throws Exception {
        var slow=node("SELECT SLEEP(8) AS admission_probe");var w=workflow(List.of(slow),new int[][]{});
        String first=submit(w,slow),second=submit(w,slow);
        assertEquals("WORKFLOW_LIMIT",assertThrows(StudioException.class,()->submit(w,slow)).code());assertEquals(2,repo.topRuns(workspace).size());
        workflows.stop(first);String third=submit(w,slow);assertNotNull(third);workflows.stop(second);workflows.stop(third);
    }
    @Test void completionAndRepeatedCancellationNeverRewriteTerminalStates() throws Exception {
        var a=node("SELECT SLEEP(0.03) AS short_query");var b=node("SELECT 1");var w=workflow(List.of(a,b),new int[][]{{0,1}});
        var terminalStates=Set.of("SUCCESS","FAILED","CANCELLED","SKIPPED");
        for(int i=0;i<3;i++) {
            String id=submit(w,a,b);runningChild(id);
            var first=java.util.concurrent.CompletableFuture.supplyAsync(()->workflows.stop(id));
            var second=java.util.concurrent.CompletableFuture.supplyAsync(()->workflows.stop(id));
            first.get();second.get();
            // The worker settles an interrupted query after the parent cancellation returns.
            var before=children(id);
            for(int attempt=0;attempt<250&&before.values().stream().anyMatch(child->!terminalStates.contains(child.get("status")));attempt++) {
                Thread.sleep(20);before=children(id);
            }
            assertTrue(before.values().stream().allMatch(child->terminalStates.contains(child.get("status"))),"Children did not settle after cancellation: "+id);
            String state=terminal(id).get("status").toString();Thread.sleep(250);
            assertTrue(Set.of("SUCCESS","CANCELLED").contains(state));assertEquals(state,runs.required(id).get("status"));
            var after=children(id);for(String node:before.keySet())assertEquals(before.get(node).get("status"),after.get(node).get("status"));
        }
    }
}
