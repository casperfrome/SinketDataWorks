package com.fake.dataworks;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.dto.ObjectInput;
import com.fake.dataworks.repository.StudioRepository;
import com.fake.dataworks.service.*;
import java.net.URI;
import java.net.http.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.*;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

/** Every test owns a random workspace; the build should use a dedicated metadata schema. */
@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT,properties={
    "studio.simulation.queue-ms=30","studio.simulation.run-ms=50",
    "studio.simulation.recover-on-start=false","studio.scheduler.enabled=false",
    "studio.inventory.recover-on-start=false","studio.storage-path=./target/test-storage"
})
class NodeDebugIntegrationTest {
    @Autowired ObjectService objects;
    @Autowired RunService runs;
    @Autowired DatasourceService sources;
    @Autowired TaskService tasks;
    @Autowired JdbcTemplate jdbc;
    @Autowired JsonCodec json;
    @MockitoSpyBean StudioRepository repo;
    @Value("${studio.demo.password:}") String demoPassword;
    @Value("${NODE_DEBUG_DORIS_DATABASE:}") String dorisDatabase;
    @Value("${NODE_DEBUG_DORIS_USER:}") String dorisUser;
    @Value("${NODE_DEBUG_DORIS_PASSWORD:}") String dorisPassword;
    @LocalServerPort int port;
    String workspace;

    @BeforeEach void setup() {
        workspace="debug-test-"+UUID.randomUUID();
        jdbc.update("INSERT INTO dw_workspace(id,name,code,region,workspace_type) VALUES(?,?,?,?,'TEST')",workspace,"节点调试集成验证",workspace,"local");
    }
    @AfterEach void cleanup() throws Exception {
        reset(repo);
        for(var run:repo.topRuns(workspace))runs.stop(run.get("id").toString());
        Thread.sleep(120);
        jdbc.update("DELETE FROM dw_task_trigger WHERE schedule_id IN (SELECT id FROM dw_task_schedule WHERE workspace_id=?)",workspace);
        jdbc.update("DELETE FROM dw_task_schedule WHERE workspace_id=?",workspace);
        jdbc.update("DELETE FROM dw_task_release WHERE workspace_id=?",workspace);
        jdbc.update("DELETE FROM dw_run WHERE workspace_id=?",workspace);
        jdbc.update("DELETE FROM dw_version WHERE object_id IN (SELECT id FROM dw_object WHERE workspace_id=?)",workspace);
        jdbc.update("DELETE FROM dw_object WHERE workspace_id=?",workspace);
        jdbc.update("DELETE FROM dw_datasource WHERE workspace_id=?",workspace);
        jdbc.update("DELETE FROM dw_workspace WHERE id=?",workspace);
    }

    @Test void prepareReadsCapturedDraftAndDeduplicatesWithoutSavingOrRunning() throws Exception {
        var stored=node("SELECT 1",Map.of());
        var captured=draft(stored,"SELECT '${region}', '${day}', '${region}'",schedule(Map.of("day","$[yyyymmdd]")));
        var prepared=prepare(captured);
        assertEquals(Set.of("region","day"),new HashSet<>(parameterNames(prepared)));
        assertEquals(List.of("region"),prepared.get("missingParameters"));
        assertEquals("MISSING",parameter(prepared,"region").get("source"));
        assertEquals("SCHEDULE",parameter(prepared,"day").get("source"));
        assertEquals(parameter(prepared,"day").get("defaultValue"),parameter(prepared,"day").get("value"));
        assertEquals(Map.of(),prepared.get("debugParameters"));
        assertEquals(stored,objects.get(stored.id()));
        assertEquals(1,repo.versions(stored.id()).size());
        assertEquals(0,runCount());
        assertEquals(0,profileCount(stored.id()));
    }

    @Test void noParametersCanRunAndPrepareReturnsEmptyState() throws Exception {
        var node=node("SELECT 1",Map.of());
        var prepared=prepare(node);
        assertEquals(List.of(),prepared.get("parameters"));
        assertEquals(List.of(),prepared.get("missingParameters"));
        var run=submit(node,null,Map.of());
        assertEquals("SUCCESS",terminal(run).get("status"));
        assertEquals(Map.of(),run.get("scheduleParameters"));
        assertEquals(node,objects.get(node.id()));
    }

    @Test void syncPreparationReadsFiltersAndConstantPartitionAssignmentsWithoutConnections() throws Exception {
        var config=new LinkedHashMap<String,Object>(schedule(Map.of("day","${yyyymmdd}")));
        config.put("run",Map.of("provider","SYNC"));
        config.put("sync",Map.of("where","region = '${region}' AND ds = '${day}'",
            "sourcePartitionFilter",Map.of("where","region = '${region}' AND zone = '${zone}'"),
            "targetPartitionAssignments",List.of(Map.of("target","day","mode","value","value","${day}"),Map.of("target","part","mode","value","value","${partition}"),Map.of("target","source","mode","column","source","${ignored}"))));
        var node=objects.create(new ObjectInput(workspace,null,"NODE","数据集成","sync draft","","",config,List.of(),false,null));
        var prepared=prepare(node);
        assertEquals(Set.of("region","day","zone","partition"),new HashSet<>(parameterNames(prepared)));
        assertEquals(Set.of("region","zone","partition"),new HashSet<>((List<?>)prepared.get("missingParameters")));
        assertEquals(0,runCount());assertEquals(0,profileCount(node.id()));
        assertEquals(node,objects.get(node.id()));
    }

    @Test void remembersOnlyLiteralOverridesAndExplicitMappingsReplaceOrClearMemory() throws Exception {
        var node=node("SELECT '${region}', '${day}'",schedule(Map.of("region","default","day","$[yyyymmdd]")));
        String literal="O'Reilly = ${yyyy} with spaces";
        var first=submit(node,Map.of("region",literal),Map.of("simulateFailure",true));
        assertEquals("FAILED",terminal(first).get("status"));
        assertEquals(literal,values(first).get("region"));
        assertEquals(Map.of("region",literal),profile(node.id()));
        var prepared=prepare(node);
        assertEquals("DEBUG",parameter(prepared,"region").get("source"));
        assertEquals("default",parameter(prepared,"region").get("defaultValue"));
        assertEquals("SCHEDULE",parameter(prepared,"day").get("source"));
        assertEquals(literal,values(submit(node,null,Map.of())).get("region"));
        var replacement=submit(node,Map.of("day","manual = day"),Map.of());
        assertEquals("default",values(replacement).get("region"));
        assertEquals("manual = day",values(replacement).get("day"));
        assertEquals(Map.of("day","manual = day"),profile(node.id()));
        var restored=submit(node,Map.of(),Map.of());
        assertEquals("default",values(restored).get("region"));
        assertEquals(Map.of(),profile(node.id()));
        assertEquals(node,objects.get(node.id()));
        assertEquals(1,repo.versions(node.id()).size());
    }

    @Test void missingAndInvalidOverridesNeverChangeMemoryOrAdmitRuns() throws Exception {
        var missing=node("SELECT '${required}'",Map.of());
        assertEquals(400,http("POST","/runs",request(missing,null,Map.of())).statusCode());
        assertEquals(0,runCount());
        var node=node("SELECT '${region}'",schedule(Map.of("region","default")));
        terminal(submit(node,Map.of("region","saved"),Map.of()));
        int count=runCount();
        var nullValue=new LinkedHashMap<String,Object>();nullValue.put("region",null);
        for(Object invalid:List.of(Map.of("region",""),Map.of("region","x".repeat(4097)),Map.of("unused","value"),Map.of("region",42),nullValue,"not a map")) {
            assertEquals(400,http("POST","/runs",request(node,invalid,Map.of())).statusCode());
            assertEquals(Map.of("region","saved"),profile(node.id()));
            assertEquals(count,runCount());
        }
        var tooMany=new LinkedHashMap<String,Object>();for(int i=0;i<101;i++)tooMany.put("p"+i,"value");
        assertEquals(400,http("POST","/runs",request(node,tooMany,Map.of())).statusCode());
        assertEquals(Map.of("region","saved"),profile(node.id()));
        assertEquals(count,runCount());
    }

    @Test void prepareAndSubmitEnforceIdentityAndVersionsIncludingSimulation() throws Exception {
        var original=node("SELECT '${region}'",schedule(Map.of("region","default")));
        var changed=objects.update(original.id(),input(original,"SELECT '${region}', 2",original.config()));
        assertEquals(409,http("POST","/runs/parameters/prepare",Map.of("object",original)).statusCode());
        assertEquals(409,http("POST","/runs",request(original,Map.of("region","lost"),Map.of())).statusCode());
        var forged=new StudioObject(changed.id(),"local-workspace",changed.parentId(),changed.kind(),changed.nodeType(),changed.name(),changed.description(),changed.content(),changed.config(),changed.tags(),changed.favorite(),changed.deleted(),changed.version(),changed.owner(),changed.updatedAt());
        assertEquals(400,http("POST","/runs/parameters/prepare",Map.of("object",forged)).statusCode());
        var sql=objects.create(new ObjectInput(workspace,null,"NODE","MySQL","version required","","SELECT 1",Map.of("run",Map.of("provider","MYSQL","dataSourceId","missing")),List.of(),false,null));
        var noVersion=request(sql,null,Map.of());noVersion.remove("expectedVersion");
        assertEquals(409,http("POST","/runs",noVersion).statusCode());
        assertEquals(0,runCount());assertEquals(0,profileCount(changed.id()));
        assertEquals(changed,objects.get(changed.id()));
    }

    @Test void profileAndRunInsertionRollBackTogetherBeforeDispatch() throws Exception {
        var node=node("SELECT '${region}'",schedule(Map.of("region","default")));
        terminal(submit(node,Map.of("region","saved"),Map.of()));
        int count=runCount();
        doAnswer(call->{call.callRealMethod();throw new IllegalStateException("injected insertion acknowledgement failure");}).when(repo).insertRun(anyMap(),any(StudioObject.class));
        assertThrows(IllegalStateException.class,()->runs.submit(node.id(),"MANUAL",false,node.version(),Map.of(),Map.of("debugParameters",Map.of("region","rolled back"))));
        reset(repo);
        Thread.sleep(150);
        assertEquals(count,runCount());
        assertEquals(Map.of("region","saved"),profile(node.id()));
        assertEquals(node,objects.get(node.id()));
    }

    @Test void profileSurvivesSoftDeleteButDoesNotFollowCopiesAndCascadesOnHardDelete() throws Exception {
        var node=node("SELECT '${region}'",schedule(Map.of("region","default")));
        terminal(submit(node,Map.of("region","remembered"),Map.of()));
        var copy=objects.copy(node.id(),"copied",null,false);
        assertEquals(0,profileCount(copy.id()));
        assertEquals("SCHEDULE",parameter(prepare(copy),"region").get("source"));
        objects.delete(node.id());assertEquals(Map.of("region","remembered"),profile(node.id()));
        var restored=objects.restore(node.id(),null);
        assertEquals("remembered",parameter(prepare(restored),"region").get("value"));
        jdbc.update("DELETE FROM dw_run WHERE object_id=?",node.id());
        jdbc.update("DELETE FROM dw_version WHERE object_id=?",node.id());
        jdbc.update("DELETE FROM dw_object WHERE id=?",node.id());
        assertEquals(0,profileCount(node.id()));
    }

    @Test void defaultDateExpressionsResolveAgainForEveryAcceptedRun() throws Exception {
        var node=node("SELECT '${region}', '${day}'",schedule(Map.of("region","default","day","${yyyymmdd}")));
        var first=submit(node,Map.of("region","remembered"),Map.of("businessDate","2026-09-27"));
        var second=submit(node,null,Map.of("businessDate","2026-09-28"));
        assertEquals("20260927",values(first).get("day"));
        assertEquals("20260928",values(second).get("day"));
        assertEquals("remembered",values(second).get("region"));
        assertEquals(Map.of("region","remembered"),profile(node.id()));
        assertEquals(values(first),values(runs.detail(first.get("id").toString())));
        assertEquals(first.get("scheduledAt"),runs.detail(first.get("id").toString()).get("scheduledAt"));
    }

    @Test void mysqlUsesBoundLiteralValuesAndPublishedRunsRemainIndependent() throws Exception {
        Assumptions.assumeFalse(demoPassword.isEmpty(),"A configured demo reader is required for the real MySQL check");
        var source=sources.save(null,Map.of("workspaceId",workspace,"name","debug MySQL","host","127.0.0.1","port",3307,"database","studio_demo","username","studio_reader","password",demoPassword));
        var config=new LinkedHashMap<String,Object>(schedule(Map.of("region","schedule-default")));
        config.put("run",Map.of("provider","MYSQL","dataSourceId",source.get("id"),"timeoutSeconds",10));
        var node=objects.create(new ObjectInput(workspace,null,"NODE","MySQL","bound literals","","SELECT '${region}' AS value, :bizdate AS builtin, 'prefix_${region}' AS combined",config,List.of(),false,null));
        var persisted=objects.get(node.id());
        String literal="O'Reilly = '${yyyy}'; SELECT 999 --";
        var development=submit(node,Map.of("region",literal),Map.of("businessDate","2026-09-27"));
        assertEquals("SUCCESS",terminal(development).get("status"));
        assertEquals(List.of(literal,"2026-09-27","prefix_"+literal),((List<?>)runs.results(development.get("id").toString(),1,100).get("rows")).getFirst());
        var release=tasks.publish(node.id(),node.version(),"debug values remain independent");
        var releaseSnapshot=(Map<?,?>)tasks.release(release.get("id").toString()).get("snapshot");
        assertFalse(json.write(releaseSnapshot).contains(literal));
        var published=tasks.startRelease(release.get("id").toString(),Map.of("businessDate","2026-09-28"));
        assertEquals("SUCCESS",terminal(published).get("status"));
        assertEquals("schedule-default",values(published).get("region"));
        var repeated=tasks.rerun(published.get("id").toString());
        assertEquals("SUCCESS",terminal(repeated).get("status"));
        assertEquals(values(published),values(repeated));
        assertEquals(published.get("scheduledAt"),repeated.get("scheduledAt"));
        assertEquals(Map.of("region",literal),profile(node.id()));
        assertEquals(persisted,objects.get(node.id()));
    }

    @Test @EnabledIfEnvironmentVariable(named="NODE_DEBUG_DORIS_PASSWORD",matches=".+")
    void dorisUsesBoundLiteralValuesInAnIndependentReadOnlyFixture() throws Exception {
        var source=sources.save(null,Map.of("workspaceId",workspace,"name","debug Doris","type","DORIS","host","127.0.0.1","port",9030,"database",dorisDatabase,"username",dorisUser,"password",dorisPassword));
        var config=Map.<String,Object>of("run",Map.of("provider","DORIS","dataSourceId",source.get("id"),"timeoutSeconds",10));
        var node=objects.create(new ObjectInput(workspace,null,"NODE","Doris","Doris literals","","SELECT '${value}' AS literal, 'prefix_${value}' AS combined",config,List.of(),false,null));
        String literal="O'Reilly = ${yyyy}; SELECT 999 --";
        var run=submit(node,Map.of("value",literal),Map.of());
        assertEquals("SUCCESS",terminal(run).get("status"));
        assertEquals(List.of(literal,"prefix_"+literal),((List<?>)runs.results(run.get("id").toString(),1,100).get("rows")).getFirst());
        assertEquals(Map.of("value",literal),profile(node.id()));
    }

    StudioObject node(String content,Map<String,Object> config) {
        return objects.create(new ObjectInput(workspace,null,"NODE","MAXCOMPUTE_SQL",UUID.randomUUID().toString(),"",content,config,List.of(),false,null));
    }
    ObjectInput input(StudioObject node,String content,Map<String,Object> config) {
        return new ObjectInput(workspace,node.parentId(),node.kind(),node.nodeType(),node.name(),node.description(),content,config,node.tags(),node.favorite(),node.version(),node.owner());
    }
    StudioObject draft(StudioObject node,String content,Map<String,Object> config) {
        return new StudioObject(node.id(),node.workspaceId(),node.parentId(),node.kind(),node.nodeType(),node.name(),node.description(),content,config,node.tags(),node.favorite(),node.deleted(),node.version(),node.owner(),node.updatedAt());
    }
    Map<String,Object> schedule(Map<String,String> defaults) {
        return Map.of("schedule",Map.of("parameters",defaults.entrySet().stream().map(e->Map.of("name",e.getKey(),"value",e.getValue(),"source","CODE")).toList()));
    }
    Map<String,Object> request(StudioObject node,Object overrides,Map<String,Object> extra) {
        var body=new LinkedHashMap<String,Object>(extra);body.put("objectId",node.id());body.put("expectedVersion",node.version());
        if(overrides!=null)body.put("debugParameters",overrides);return body;
    }
    Map<String,Object> submit(StudioObject node,Object overrides,Map<String,Object> extra) throws Exception {
        var response=http("POST","/runs",request(node,overrides,extra));assertEquals(201,response.statusCode(),response.body());return json.map(response.body());
    }
    Map<String,Object> prepare(StudioObject node) throws Exception {
        var response=http("POST","/runs/parameters/prepare",Map.of("object",node));assertEquals(200,response.statusCode(),response.body());return json.map(response.body());
    }
    @SuppressWarnings("unchecked") List<Map<String,Object>> parameters(Map<String,Object> prepared) {return (List<Map<String,Object>>)prepared.get("parameters");}
    List<String> parameterNames(Map<String,Object> prepared) {return parameters(prepared).stream().map(p->p.get("name").toString()).toList();}
    Map<String,Object> parameter(Map<String,Object> prepared,String name) {return parameters(prepared).stream().filter(p->name.equals(p.get("name"))).findFirst().orElseThrow();}
    Map<?,?> values(Map<String,Object> run) {return (Map<?,?>)run.get("scheduleParameters");}
    int runCount() {return jdbc.queryForObject("SELECT COUNT(*) FROM dw_run WHERE workspace_id=?",Integer.class,workspace);}
    int profileCount(String node) {return jdbc.queryForObject("SELECT COUNT(*) FROM dw_node_debug_parameters WHERE object_id=?",Integer.class,node);}
    Map<String,Object> profile(String node) {
        var rows=jdbc.queryForList("SELECT parameters_json FROM dw_node_debug_parameters WHERE object_id=?",String.class,node);
        return rows.isEmpty()?Map.of():json.map(rows.getFirst());
    }
    Map<String,Object> terminal(Map<String,Object> run) throws Exception {
        String id=run.get("id").toString();long until=System.currentTimeMillis()+10000;
        while(System.currentTimeMillis()<until){var found=runs.required(id);if(Set.of("SUCCESS","FAILED","CANCELLED").contains(found.get("status")))return found;Thread.sleep(20);}
        fail("Run did not finish: "+id);return Map.of();
    }
    HttpResponse<String> http(String method,String path,Object body) throws Exception {
        var request=HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+"/api/v1"+path)).header("Content-Type","application/json");
        return HttpClient.newHttpClient().send(request.method(method,body==null?HttpRequest.BodyPublishers.noBody():HttpRequest.BodyPublishers.ofString(json.write(body))).build(),HttpResponse.BodyHandlers.ofString());
    }
}
