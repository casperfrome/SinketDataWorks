package com.fake.dataworks;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.dto.ObjectInput;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.StudioRepository;
import com.fake.dataworks.service.*;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import java.net.*;
import java.net.http.*;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT,properties={"studio.simulation.recover-on-start=false","studio.mysql.workers=1","studio.mysql.queue-size=2"})
class MysqlQueryIntegrationTest {
    @Autowired DatasourceService sources;@Autowired ObjectService objects;@Autowired RunService runs;@Autowired StudioRepository repo;@Autowired JdbcTemplate jdbc;@Autowired JsonCodec json;
    @Autowired TaskService tasks; @Autowired TaskScheduleService schedules; @Autowired com.fake.dataworks.repository.TaskRepository taskStore;
    @Value("${studio.demo.password}") String password;
    @LocalServerPort int port;
    String workspace,sourceId;
    @BeforeEach void setup() {
        workspace="query-test-"+UUID.randomUUID();jdbc.update("INSERT INTO dw_workspace VALUES(?,?,?,?)",workspace,"查询测试",workspace,"local");
        sourceId=sources.save(null,sourceInput(password)).get("id").toString();
    }
    @AfterEach void cleanup() throws Exception {
        for(var run:repo.runs(workspace))runs.stop(run.get("id").toString());
        // stop cancels the database statement; wait for worker cleanup before deleting its records.
        Thread.sleep(150);
        jdbc.update("DELETE FROM dw_task_trigger WHERE schedule_id IN (SELECT id FROM dw_task_schedule WHERE workspace_id=?)",workspace);
        jdbc.update("DELETE FROM dw_task_schedule WHERE workspace_id=?",workspace);jdbc.update("DELETE FROM dw_task_release WHERE workspace_id=?",workspace);
        jdbc.update("DELETE FROM dw_run WHERE workspace_id=?",workspace);
        jdbc.update("DELETE FROM dw_version WHERE object_id IN (SELECT id FROM dw_object WHERE workspace_id=?)",workspace);
        jdbc.update("DELETE FROM dw_object WHERE workspace_id=?",workspace);jdbc.update("DELETE FROM dw_datasource WHERE workspace_id=?",workspace);jdbc.update("DELETE FROM dw_workspace WHERE id=?",workspace);
    }
    Map<String,Object> sourceInput(String secret) {return Map.of("workspaceId",workspace,"name","Test source","host","127.0.0.1","port",3307,"database","studio_demo","username","studio_reader","password",secret);}
    StudioObject node(String sql,int seconds) {return objects.create(new ObjectInput(workspace,null,"NODE","MySQL",UUID.randomUUID().toString(),"",sql,Map.of("run",Map.of("provider","MYSQL","dataSourceId",sourceId,"timeoutSeconds",seconds)),List.of(),false,null));}
    String submit(StudioObject node) {return runs.submit(node.id(),"MANUAL",false,node.version()).get("id").toString();}
    Map<String,Object> waitTerminal(String id) throws Exception {
        long end=System.currentTimeMillis()+12000;while(System.currentTimeMillis()<end) {var r=runs.required(id);if(!Set.of("RUNNING","QUEUED").contains(r.get("status")))return r;Thread.sleep(30);}throw new AssertionError("Run did not finish: "+id);
    }
    void waitRunning(String id) throws Exception {for(int i=0;i<100;i++){if("RUNNING".equals(runs.required(id).get("status")))return;Thread.sleep(30);}fail("not running");}
    @SuppressWarnings("unchecked") @Test void customParametersMatchPreviewAndPublishedScheduledReruns() throws Exception {
        var n=node("SELECT '${bizdate}' AS custom_day, '${region}' AS region, :bizdate AS legacy_day, 'prefix_${region}' AS combined",30);
        String day=java.time.LocalDate.now(java.time.ZoneId.of("Asia/Shanghai")).minusDays(2).toString();
        var definitions=List.of(Map.of("name","bizdate","value","$[yyyymmdd-1]","source","CODE"),Map.of("name","region","value","O'Reilly","source","MANUAL"));
        var config=new LinkedHashMap<String,Object>(n.config());config.put("schedule",Map.of("parameters",definitions,"cron","0 0 2 * * *","timezone","Asia/Shanghai","businessDateOffset",-1));
        n=objects.update(n.id(),new ObjectInput(workspace,null,"NODE","MySQL",n.name(),"",n.content(),config,List.of(),false,n.version()));
        var previewInput=new LinkedHashMap<String,Object>((Map<String,Object>)config.get("schedule"));previewInput.put("businessDate",day);previewInput.put("code",n.content());previewInput.put("count",1);
        var request=HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+"/api/v1/schedule-parameters/preview")).header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString(json.write(previewInput))).build();
        var response=HttpClient.newHttpClient().send(request,HttpResponse.BodyHandlers.ofString());assertEquals(200,response.statusCode(),response.body());
        var expected=((List<Map<String,Object>>)json.read(response.body(),List.class)).getFirst();
        var development=runs.submit(n.id(),"MANUAL",false,n.version(),Map.of(),Map.of("businessDate",day));
        assertEquals("SUCCESS",waitTerminal(development.get("id").toString()).get("status"));assertEquals(expected.get("values"),development.get("scheduleParameters"));
        assertEquals(List.of(day.replace("-",""),"O'Reilly",day,"prefix_O'Reilly"),((List<?>)runs.results(development.get("id").toString(),1,100).get("rows")).getFirst());
        var release=tasks.publish(n.id(),n.version(),"fixed params");
        var planInput=new LinkedHashMap<String,Object>(previewInput);planInput.put("releaseId",release.get("id"));planInput.put("enabled",true);planInput.put("dependencies",List.of());
        var plan=schedules.save(n.id(),null,planInput);
        config.put("schedule",Map.of("parameters",List.of(Map.of("name","bizdate","value","changed","source","CODE"),Map.of("name","region","value","changed","source","MANUAL"))));
        objects.update(n.id(),new ObjectInput(workspace,null,"NODE","MySQL",n.name(),"",n.content(),config,List.of(),false,n.version()));
        plan.put("nextFireAt",expected.get("scheduledAt"));taskStore.saveSchedule(plan,false);
        org.springframework.test.util.ReflectionTestUtils.invokeMethod(schedules,"scan",java.time.Instant.parse(expected.get("scheduledAt").toString()),workspace);
        var trigger=((List<Map<String,Object>>)schedules.triggers(plan.get("id").toString(),1,20,"").get("items")).getFirst();
        var actual=waitTerminal(trigger.get("runId").toString());assertEquals("SUCCESS",actual.get("status"));assertEquals(expected.get("values"),actual.get("scheduleParameters"));
        var repeat=tasks.rerun(actual.get("id").toString());assertEquals("SUCCESS",waitTerminal(repeat.get("id").toString()).get("status"));assertEquals(actual.get("scheduleParameters"),repeat.get("scheduleParameters"));assertEquals(actual.get("scheduledAt"),repeat.get("scheduledAt"));
        var fresh=objects.active(n.id());var manual=runs.submit(fresh.id(),"MANUAL",false,fresh.version());assertEquals("SUCCESS",waitTerminal(manual.get("id").toString()).get("status"));assertEquals("changed",((Map<?,?>)manual.get("scheduleParameters")).get("bizdate"));
    }
    @Test void realQueryPreservesValuesAndExecutionSnapshot() throws Exception {
        var node=node("SELECT '中文' AS same, NULL AS same, CAST(9007199254740993 AS UNSIGNED) AS big, CAST(123456789012345.1234 AS DECIMAL(20,4)) AS precise",30);
        String id=submit(node);assertEquals("SUCCESS",waitTerminal(id).get("status"));
        var result=runs.results(id,1,100);assertEquals(List.of("same","same","big","precise"),result.get("columns"));
        assertEquals(Arrays.asList("中文",null,"9007199254740993","123456789012345.1234"),((List<?>)result.get("rows")).getFirst());
        assertEquals(node.content(),((Map<?,?>)runs.detail(id).get("snapshot")).get("content"));
        assertFalse(json.write(runs.detail(id)).contains(password));assertFalse(json.write(sources.list(workspace)).contains(password));
        assertFalse(jdbc.queryForObject("SELECT password_cipher FROM dw_datasource WHERE id=?",String.class,sourceId).contains(password));
        assertEquals("SUCCESS",waitTerminal(submit(node("WITH x AS (SELECT city FROM users) SELECT COUNT(*) AS n FROM x",30))).get("status"));
    }
    @Test void dataSourceEditKeepsPasswordAndReadsMetadata() {
        assertEquals(true,sources.test(sources.get(sourceId)).get("success"));
        sources.save(sourceId,Map.of("name","Renamed"));assertEquals(true,sources.test(sources.get(sourceId)).get("success"));
        assertTrue(sources.tables(sourceId).stream().anyMatch(t->"orders".equals(t.get("name"))));
        assertTrue(sources.columns(sourceId,"orders").stream().anyMatch(t->"total_amount".equals(t.get("name"))));
        assertEquals("DATASOURCE_AUTH_FAILED",assertThrows(StudioException.class,()->sources.test(sources.input(null,sourceInput("incorrect-secret")))).code());
        var input=new HashMap<>(sourceInput(password));input.put("port",1);assertThrows(StudioException.class,()->sources.test(sources.input(null,input)));
    }
    @Test void enforcesSourceAndVersionBoundariesAndDatabasePrivileges() throws Exception {
        var input=new HashMap<>(sourceInput(password));input.put("username","root");assertThrows(StudioException.class,()->sources.save(null,input));
        input.put("username","studio_reader");input.put("database","fake_dataworks_260927");assertThrows(StudioException.class,()->sources.save(null,input));
        assertThrows(StudioException.class,()->sources.forWorkspace(sourceId,"local-workspace"));
        var node=node("SELECT 1",30);assertThrows(StudioException.class,()->runs.submit(node.id(),"MANUAL",false,null));assertThrows(StudioException.class,()->runs.submit(node.id(),"MANUAL",false,999));
        assertThrows(StudioException.class,()->submit(node("SELECT * FROM fake_dataworks_260927.dw_object",30)));
        try(Connection c=sources.open(sources.get(sourceId),5);Statement statement=c.createStatement()) {
            assertThrows(SQLException.class,()->statement.executeUpdate("UPDATE orders SET status='BROKEN' WHERE id=-1"));
            assertThrows(SQLException.class,()->statement.executeQuery("SELECT * FROM mysql.user"));
        }
    }
    @Test void paginatesStoredResultsAndTruncatesWithoutReexecuting() throws Exception {
        String id=submit(node("SELECT a.id FROM orders a CROSS JOIN orders b CROSS JOIN orders c CROSS JOIN orders d",30));
        assertEquals("SUCCESS",waitTerminal(id).get("status"));
        assertEquals(1000,runs.results(id,1,100).get("total"));assertEquals(true,runs.results(id,1,100).get("truncated"));assertEquals(100,((List<?>)runs.results(id,10,100).get("rows")).size());assertEquals(0,((List<?>)runs.results(id,11,100).get("rows")).size());
        String large=submit(node("SELECT REPEAT('中',30000) AS large_cell",30));waitTerminal(large);
        var result=runs.results(large,1,100);String cell=((List<?>)((List<?>)result.get("rows")).getFirst()).getFirst().toString();
        assertTrue(cell.getBytes(java.nio.charset.StandardCharsets.UTF_8).length<=65536);assertEquals(true,result.get("truncated"));
        String empty=submit(node("SELECT id FROM orders WHERE 1=0",30));waitTerminal(empty);assertEquals(0,runs.results(empty,1,100).get("total"));assertEquals(List.of("id"),runs.results(empty,1,100).get("columns"));
    }
    @Test void terminatesTimedOutDatabaseQuery() throws Exception {
        String id=submit(node("SELECT SLEEP(8) AS timeout_probe",1));var result=waitTerminal(id);
        assertEquals("FAILED",result.get("status"));assertEquals("QUERY_TIMEOUT",result.get("errorCode"));assertNoQuery("timeout_probe");
    }
    @Test void capsTotalStoredBytesAndReturnsUsefulQueryErrors() throws Exception {
        String id=submit(node("SELECT REPEAT('x',60000) AS payload FROM orders a CROSS JOIN orders b CROSS JOIN orders c",30));
        assertEquals("SUCCESS",waitTerminal(id).get("status"));var result=repo.result(id).orElseThrow();
        assertTrue(json.write(result).getBytes(java.nio.charset.StandardCharsets.UTF_8).length<=5*1024*1024);assertEquals(true,runs.results(id,1,100).get("truncated"));assertTrue(((List<?>)runs.results(id,1,100).get("rows")).size()<100);
        String binary=submit(node("SELECT UNHEX(REPEAT('41',60000)) AS binary_value",30));assertEquals("SUCCESS",waitTerminal(binary).get("status"));
        var binaryResult=runs.results(binary,1,100);String encoded=((List<?>)((List<?>)binaryResult.get("rows")).getFirst()).getFirst().toString();
        assertTrue(encoded.startsWith("base64:"));assertTrue(encoded.length()<=65536);assertEquals(true,binaryResult.get("truncated"));
        var missing=waitTerminal(submit(node("SELECT missing_column FROM orders",30)));assertEquals("FAILED",missing.get("status"));assertEquals("QUERY_FAILED",missing.get("errorCode"));assertTrue(missing.get("logs").toString().contains("字段不存在"));
    }
    @Test void cancelsRunningAndQueuedQueriesAndDoesNotOverwriteTerminalState() throws Exception {
        String running=submit(node("SELECT SLEEP(8) AS cancel_probe",30));waitRunning(running);Thread.sleep(150);
        String queued=submit(node("SELECT 42",30));assertEquals("QUEUED",runs.required(queued).get("status"));runs.stop(queued);assertEquals("CANCELLED",runs.required(queued).get("status"));
        runs.stop(running);assertEquals("CANCELLED",waitTerminal(running).get("status"));assertNoQuery("cancel_probe");Thread.sleep(150);assertEquals("CANCELLED",runs.required(running).get("status"));
        var stale=new HashMap<>(runs.required(running));stale.put("status","SUCCESS");assertFalse(repo.transitionRun(stale,"RUNNING"));
        // Capacity is returned after queued cancellation.
        assertEquals("SUCCESS",waitTerminal(submit(node("SELECT 7",30))).get("status"));
    }
    @Test void rejectsFullQueue() throws Exception {
        String running=submit(node("SELECT SLEEP(8) AS queue_probe",30));waitRunning(running);
        submit(node("SELECT 1",30));submit(node("SELECT 2",30));
        assertEquals("QUEUE_FULL",assertThrows(StudioException.class,()->submit(node("SELECT 3",30))).code());runs.stop(running);
    }
    @Test void recordsConnectionLossDuringARealQuery() throws Exception {
        String id=submit(node("SELECT SLEEP(8) AS disconnect_probe",30));waitRunning(id);
        boolean killed=false;
        try(Connection c=sources.open(sources.get(sourceId),5);Statement control=c.createStatement()) {
            for(int i=0;i<60&&!killed;i++) {
                long connectionId=-1;
                try(ResultSet process=control.executeQuery("SHOW PROCESSLIST")) {while(process.next())if(Objects.toString(process.getString("Info"),"").contains("disconnect_probe"))connectionId=process.getLong("Id");}
                if(connectionId>0) {control.execute("KILL CONNECTION "+connectionId);killed=true;}else Thread.sleep(30);
            }
        }
        assertTrue(killed,"The isolated query connection must be interrupted");
        var result=waitTerminal(id);assertEquals("FAILED",result.get("status"));assertEquals("DATASOURCE_UNAVAILABLE",result.get("errorCode"));
    }
    void assertNoQuery(String marker) throws Exception {
        try(Connection c=sources.open(sources.get(sourceId),5);Statement statement=c.createStatement()) {
            for(int i=0;i<40;i++) {boolean found=false;try(ResultSet rs=statement.executeQuery("SHOW PROCESSLIST")) {while(rs.next())if(Objects.toString(rs.getString("Info"),"").contains(marker))found=true;}if(!found)return;Thread.sleep(50);}fail("Database query still active: "+marker);
        }
    }
    @Test void exposesHttpDetailsSummaryResultsAndValidation() throws Exception {
        var node=node("SELECT COUNT(*) AS order_count FROM orders",30);
        var response=http("POST","/runs",Map.of("objectId",node.id(),"expectedVersion",node.version()));assertEquals(201,response.statusCode());
        String id=json.map(response.body()).get("id").toString();waitTerminal(id);
        var summary=http("GET","/runs?workspaceId="+workspace+"&summary=true",null);assertEquals(200,summary.statusCode());assertFalse(summary.body().contains("\"rows\""));
        assertTrue(http("GET","/runs/"+id,null).body().contains("snapshot"));assertTrue(http("GET","/runs/"+id+"/results",null).body().contains("order_count"));
        assertEquals(400,http("GET","/runs/"+id+"/results?page=0",null).statusCode());assertEquals(409,http("POST","/runs",Map.of("objectId",node.id())).statusCode());
        assertFalse(http("GET","/datasources/"+sourceId,null).body().contains(password));
        var update=http("PUT","/datasources/"+sourceId,Map.of("name","HTTP edited"));assertEquals(200,update.statusCode());assertEquals(200,http("POST","/datasources/"+sourceId+"/test",Map.of()).statusCode());
    }
    HttpResponse<String> http(String method,String path,Object body) throws Exception {
        var builder=HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+"/api/v1"+path)).header("Content-Type","application/json");
        return HttpClient.newHttpClient().send(builder.method(method,body==null?HttpRequest.BodyPublishers.noBody():HttpRequest.BodyPublishers.ofString(json.write(body))).build(),HttpResponse.BodyHandlers.ofString());
    }
}
