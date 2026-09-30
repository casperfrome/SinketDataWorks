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
import org.springframework.jdbc.core.JdbcTemplate;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties={"studio.simulation.recover-on-start=false","studio.mysql.workers=2"})
class MysqlScriptIntegrationTest {
    @Autowired DatasourceService sources;@Autowired ObjectService objects;@Autowired RunService runs;@Autowired StudioRepository repo;@Autowired JdbcTemplate jdbc;@Autowired JsonCodec json;
    @Autowired TaskService tasks;@Autowired WorkflowService workflows;
    @Autowired TaskScheduleService schedules;@Autowired com.fake.dataworks.repository.TaskRepository taskStore;
    @Value("${studio.demo.password}") String password;
    String workspace,sourceId,table,renamed,view;
    @BeforeEach void setup() {
        String token=UUID.randomUUID().toString().replace("-","");workspace="script-test-"+token;table="rw_"+token;renamed=table+"_r";view=table+"_v";
        jdbc.update("INSERT INTO dw_workspace VALUES(?,?,?,?)",workspace,"SQL脚本测试",workspace,"local");
        var input=new LinkedHashMap<String,Object>(Map.of("workspaceId",workspace,"name","Unified source","host","127.0.0.1","port",3307,"database","studio_demo","username","studio_reader","password",password));
        input.put("materializationEnabled",false);input.put("materializationTargets",List.of());sourceId=sources.save(null,input).get("id").toString();
        assertFalse(sources.get(sourceId).publicView().containsKey("materializationEnabled"));
    }
    @AfterEach void cleanup() throws Exception {
        for(var run:repo.topRuns(workspace))runs.stop(run.get("id").toString());
        for(var run:repo.runs(workspace))terminal(run.get("id").toString());
        try(Connection c=connection();Statement s=c.createStatement()){s.execute("DROP VIEW IF EXISTS "+view);s.execute("DROP TABLE IF EXISTS "+table+", "+renamed);}
        jdbc.update("DELETE FROM dw_task_trigger WHERE schedule_id IN (SELECT id FROM dw_task_schedule WHERE workspace_id=?)",workspace);jdbc.update("DELETE FROM dw_task_schedule WHERE workspace_id=?",workspace);
        jdbc.update("DELETE FROM dw_task_release WHERE workspace_id=?",workspace);jdbc.update("DELETE FROM dw_workflow_release WHERE workspace_id=?",workspace);
        jdbc.update("DELETE FROM dw_run WHERE workspace_id=?",workspace);jdbc.update("DELETE FROM dw_version WHERE object_id IN (SELECT id FROM dw_object WHERE workspace_id=?)",workspace);
        jdbc.update("DELETE FROM dw_object WHERE workspace_id=?",workspace);jdbc.update("DELETE FROM dw_datasource WHERE workspace_id=?",workspace);jdbc.update("DELETE FROM dw_workspace WHERE id=?",workspace);
    }
    Connection connection() throws SQLException {var c=sources.open(sources.get(sourceId),10,true);c.setAutoCommit(true);return c;}
    StudioObject node(String sql,int timeout) {return objects.create(new ObjectInput(workspace,null,"NODE","MySQL",UUID.randomUUID().toString(),"",sql,Map.of("run",Map.of("provider","MYSQL","dataSourceId",sourceId,"timeoutSeconds",timeout)),List.of(),false,null));}
    String submit(String sql,int timeout) {var n=node(sql,timeout);return runs.submit(n.id(),"MANUAL",false,n.version()).get("id").toString();}
    Map<String,Object> terminal(String id) throws Exception {for(int i=0;i<500;i++){var r=runs.required(id);if(!Set.of("QUEUED","RUNNING","WAITING").contains(r.get("status")))return r;Thread.sleep(30);}throw new AssertionError("Still active: "+id);}
    long count() throws Exception {try(Connection c=connection();Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT COUNT(*) FROM "+table)){r.next();return r.getLong(1);}}
    void table() throws Exception {try(Connection c=connection();Statement s=c.createStatement()){s.execute("CREATE TABLE "+table+"(id INT PRIMARY KEY, label VARCHAR(100)) ENGINE=InnoDB");}}
    void awaitQuery(String marker) throws Exception {
        try(Connection c=connection();Statement s=c.createStatement()){for(int i=0;i<200;i++){try(ResultSet r=s.executeQuery("SHOW PROCESSLIST")){while(r.next())if(Objects.toString(r.getString("Info"),"").contains(marker))return;}Thread.sleep(20);}}fail("No active query: "+marker);
    }
    @Test void executesDdlCrudAndMultipleResultsWithLegacySourceFlagsIgnored() throws Exception {
        String id=submit("CREATE TABLE "+table+"(id INT PRIMARY KEY, label VARCHAR(30));INSERT INTO "+table+" VALUES(1,'first;value');UPDATE "+table+" SET label='updated' WHERE id=1;SELECT * FROM "+table+";ALTER TABLE "+table+" ADD COLUMN extra INT;CREATE INDEX rw_index ON "+table+"(extra);DROP INDEX rw_index ON studio_demo."+table+";CREATE VIEW "+view+" AS SELECT id FROM "+table+";ALTER VIEW "+view+" AS SELECT label FROM "+table+";DROP VIEW "+view+";REPLACE INTO "+table+"(id,label) VALUES(2,'two');DELETE FROM "+table+" WHERE id=1;SELECT COUNT(*) AS n FROM "+table+";ALTER TABLE "+table+" RENAME TO studio_demo."+renamed+";RENAME TABLE "+renamed+" TO "+table+";TRUNCATE TABLE "+table+";DROP TABLE "+table,30);
        var done=terminal(id);assertEquals("SUCCESS",done.get("status"),done.toString());assertEquals(true,done.get("containsWrites"));
        assertEquals(17,SqlResults.items(repo.result(id).orElseThrow()).size());assertEquals(4,runs.results(id,1,100).get("statementIndex"));
        assertEquals(List.of(List.of("1","updated")),runs.results(id,1,100,4).get("rows"));assertEquals(1,((Number)runs.results(id,1,100,2).get("affectedRows")).intValue());
        assertEquals("COMMITTED",runs.results(id,1,100,17).get("commitStatus"));
        assertFalse(sources.tables(sourceId).stream().anyMatch(t->table.equals(t.get("name"))));
    }
    @Test void failureKeepsEarlierCommitsAndSkipsLaterStatements() throws Exception {
        table();String id=submit("INSERT INTO "+table+" VALUES(1,'one');INSERT INTO "+table+" VALUES(1,'duplicate');INSERT INTO "+table+" VALUES(2,'never')",30);
        assertEquals("FAILED",terminal(id).get("status"));assertEquals(1,count());
        assertEquals("COMMITTED",runs.results(id,1,100,1).get("commitStatus"));assertEquals("FAILED",runs.results(id,1,100,2).get("status"));assertEquals("SKIPPED",runs.results(id,1,100,3).get("status"));
    }
    @Test void validatesEntireScriptBeforeAnyWriteAndBindsAcrossCommands() throws Exception {
        table();assertThrows(StudioException.class,()->submit("INSERT INTO "+table+" VALUES(1,'one');DROP DATABASE studio_demo",30));assertEquals(0,count());
        var n=node("INSERT INTO "+table+" VALUES(1,'${region}');SELECT '${region}', :bizdate FROM "+table,30);
        var config=new LinkedHashMap<String,Object>(n.config());config.put("schedule",Map.of("parameters",List.of(Map.of("name","region","value","O'Reilly;quoted","source","CODE"))));
        n=objects.update(n.id(),new ObjectInput(workspace,null,"NODE","MySQL",n.name(),"",n.content(),config,List.of(),false,n.version()));
        String id=runs.submit(n.id(),"MANUAL",false,n.version()).get("id").toString();assertEquals("SUCCESS",terminal(id).get("status"));
        assertEquals("O'Reilly;quoted",((List<?>)((List<?>)runs.results(id,1,100,2).get("rows")).getFirst()).getFirst());assertEquals(1,count());
    }
    @Test void timeoutAndCancellationPreserveCompletedWrites() throws Exception {
        table();String id=submit("INSERT INTO "+table+" VALUES(1,'one');SELECT SLEEP(8) AS stop_after_write;INSERT INTO "+table+" VALUES(2,'never')",30);
        awaitQuery("stop_after_write");runs.stop(id);assertEquals("CANCELLED",terminal(id).get("status"));assertEquals(1,count());assertEquals("COMMITTED",runs.results(id,1,100,1).get("commitStatus"));assertEquals("SKIPPED",runs.results(id,1,100,3).get("status"));
        String timed=submit("SELECT SLEEP(8);INSERT INTO "+table+" VALUES(3,'never')",1);assertEquals("QUERY_TIMEOUT",terminal(timed).get("errorCode"));assertEquals(1,count());
    }
    @Test void connectionLossDuringWriteIsUnknownAndNeverContinues() throws Exception {
        table();String id=submit("INSERT INTO "+table+" SELECT 1, IF(SLEEP(8)=0,'write_disconnect_probe','x');INSERT INTO "+table+" VALUES(2,'never')",30);
        awaitQuery("write_disconnect_probe");
        try(Connection c=connection();Statement s=c.createStatement()) {
            long target=-1;try(ResultSet r=s.executeQuery("SHOW PROCESSLIST")){while(r.next())if(Objects.toString(r.getString("Info"),"").contains("write_disconnect_probe"))target=r.getLong("Id");}
            assertTrue(target>0);s.execute("KILL CONNECTION "+target);
        }
        assertEquals("COMMIT_UNKNOWN",terminal(id).get("errorCode"));assertEquals("UNKNOWN",runs.results(id,1,100,1).get("commitStatus"));assertEquals("SKIPPED",runs.results(id,1,100,2).get("status"));
    }
    @Test void sharedPreviewBudgetDoesNotStopFollowingWrites() throws Exception {
        table();String large="SELECT REPEAT('x',60000) FROM orders a CROSS JOIN orders b CROSS JOIN orders c";
        String id=submit(large+";"+large+";INSERT INTO "+table+" VALUES(1,'after previews');SELECT 42",30);
        assertEquals("SUCCESS",terminal(id).get("status"));assertEquals(1,count());assertEquals(true,runs.results(id,1,100,2).get("truncated"));
        assertTrue(json.write(repo.result(id).orElseThrow()).getBytes(java.nio.charset.StandardCharsets.UTF_8).length<=5*1024*1024);
        assertEquals("SUCCESS",runs.results(id,1,100,4).get("status"));
    }
    @Test void taskReleaseAndWorkflowUseTheSameWriterAndPinSnapshots() throws Exception {
        table();var n=node("INSERT INTO "+table+" VALUES(1,'release');SELECT * FROM "+table,30);var release=tasks.publish(n.id(),n.version(),"script release");assertEquals(true,release.get("containsWrites"));
        objects.update(n.id(),new ObjectInput(workspace,null,"NODE","MySQL",n.name(),"","SELECT 'changed'",n.config(),List.of(),false,n.version()));
        var run=tasks.startRelease(release.get("id").toString(),Map.of());assertEquals("SUCCESS",terminal(run.get("id").toString()).get("status"));assertEquals(1,count());
        var writer=node("INSERT INTO "+table+" VALUES(2,'workflow')",30);var reader=node("SELECT COUNT(*) FROM "+table,30);
        var nodes=List.of(Map.of("id","w","objectId",writer.id(),"label","writer","nodeType","MySQL","x",0,"y",0),Map.of("id","r","objectId",reader.id(),"label","reader","nodeType","MySQL","x",100,"y",0));
        var workflow=objects.create(new ObjectInput(workspace,null,"WORKFLOW","手动工作流","write-workflow","","",Map.of("run",Map.of("provider","WORKFLOW"),"graph",Map.of("nodes",nodes,"edges",List.of(Map.of("id","e","source","w","target","r")))),List.of(),false,null));
        var parent=runs.submit(workflow.id(),"MANUAL",false,workflow.version(),Map.of(writer.id(),writer.version(),reader.id(),reader.version()));
        assertEquals(true,parent.get("containsWrites"));assertEquals("SUCCESS",terminal(parent.get("id").toString()).get("status"));assertEquals(2,count());
    }
    @Test void scheduledScriptPersistsWritesAndDoesNotRetryTransientFailure() throws Exception {
        table();var n=node("INSERT INTO "+table+" VALUES(1,'scheduled');SELECT missing_column FROM "+table,30);var release=tasks.publish(n.id(),n.version(),"schedule");
        var plan=schedules.save(n.id(),null,Map.of("releaseId",release.get("id"),"cron","0 * * * * *","enabled",true,"retries",3,"dependencies",List.of()));
        var now=java.time.Instant.now();plan.put("nextFireAt",now.truncatedTo(java.time.temporal.ChronoUnit.MINUTES).toString());taskStore.saveSchedule(plan,false);
        org.springframework.test.util.ReflectionTestUtils.invokeMethod(schedules,"scan",now,workspace);
        @SuppressWarnings("unchecked") var triggers=(List<Map<String,Object>>)schedules.triggers(plan.get("id").toString(),1,20,"").get("items");
        var trigger=triggers.getFirst();var completed=terminal(trigger.get("runId").toString());assertEquals("FAILED",completed.get("status"));assertEquals(1,count());
        completed.put("errorCode","DB_TRANSIENT");repo.updateRun(completed); // Exercise the scheduler's retryable-failure branch.
        org.springframework.test.util.ReflectionTestUtils.invokeMethod(schedules,"scan",now.plusMillis(100),workspace);
        var stored=taskStore.trigger(trigger.get("id").toString());assertEquals("FAILED",stored.get("status"));assertEquals(1,((Number)stored.get("attempt")).intValue());assertEquals(1,count());
    }
    @Test @org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable(named="INVENTORY_TEST_ROOT_PASSWORD",matches=".+")
    void databaseReadOnlyAccountReportsWritePermissionFailure() throws Exception {
        table();String user="rw_limited_"+UUID.randomUUID().toString().replace("-","").substring(0,16);String secret=UUID.randomUUID().toString();
        try(Connection admin=DriverManager.getConnection("jdbc:mysql://127.0.0.1:3307/?allowPublicKeyRetrieval=true","root",System.getenv("INVENTORY_TEST_ROOT_PASSWORD"));Statement control=admin.createStatement()) {
            control.execute("CREATE USER '"+user+"'@'%' IDENTIFIED BY '"+secret+"'");
            try {
                control.execute("GRANT SELECT ON studio_demo.* TO '"+user+"'@'%'");
                String limited=sources.save(null,Map.of("workspaceId",workspace,"name","Limited source","host","127.0.0.1","port",3307,"database","studio_demo","username",user,"password",secret)).get("id").toString();
                var n=objects.create(new ObjectInput(workspace,null,"NODE","MySQL","permission-test","","SELECT 1;INSERT INTO "+table+" VALUES(1,'denied');SELECT 2",Map.of("run",Map.of("provider","MYSQL","dataSourceId",limited)),List.of(),false,null));
                String id=runs.submit(n.id(),"MANUAL",false,n.version()).get("id").toString();var done=terminal(id);
                assertEquals("FAILED",done.get("status"));assertTrue(done.get("logs").toString().contains("权限"));assertEquals("SUCCESS",runs.results(id,1,100,1).get("status"));assertEquals("SKIPPED",runs.results(id,1,100,3).get("status"));assertEquals(0,count());
            } finally {control.execute("DROP USER '"+user+"'@'%'");}
        }
    }
}
