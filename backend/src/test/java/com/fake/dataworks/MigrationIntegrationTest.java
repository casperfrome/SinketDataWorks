package com.fake.dataworks;

import java.sql.*;
import java.util.*;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.boot.flyway.autoconfigure.FlywayMigrationStrategy;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties={"spring.main.web-application-type=none","studio.simulation.queue-ms=100","studio.simulation.run-ms=160","studio.simulation.recover-on-start=false","studio.storage-path=./target/test-storage"})
class MigrationIntegrationTest {
    @Autowired JdbcTemplate jdbc;
    @Value("${spring.datasource.url}") String url;
    @Value("${spring.datasource.username}") String user;
    @Value("${spring.datasource.password}") String password;
    @Test void migratesFreshSchemaAndLeavesExistingDataOnRepeatedMigration() throws Exception {
        withSchema(ds->{
            var flyway=Flyway.configure().dataSource(ds).cleanDisabled(true).load();
            assertEquals(12,flyway.migrate().migrationsExecuted);
            JdbcTemplate isolated=new JdbcTemplate(ds);isolated.update("INSERT INTO dw_workspace(id,name,code,region) VALUES('preserved','保留数据','keep','local')");
            assertEquals(0,flyway.migrate().migrationsExecuted);
            assertEquals("保留数据",isolated.queryForObject("SELECT name FROM dw_workspace WHERE id='preserved'",String.class));
            assertTrue(flyway.getConfiguration().isCleanDisabled());
        });
    }
    @Test void explicitBaselinePreservesV1Content() throws Exception {
        withSchema(ds->{
            try(Connection c=ds.getConnection()) {ScriptUtils.executeSqlScript(c,new ClassPathResource("db/migration/V1__initial_metadata.sql"));}
            JdbcTemplate isolated=new JdbcTemplate(ds);isolated.update("INSERT INTO dw_workspace VALUES('old','旧库记录','keep','local')");
            var flyway=Flyway.configure().dataSource(ds).cleanDisabled(true).load();
            assertThrows(Exception.class,flyway::migrate);
            flyway.baseline();assertEquals(11,flyway.migrate().migrationsExecuted);
            assertEquals("旧库记录",isolated.queryForObject("SELECT name FROM dw_workspace WHERE id='old'",String.class));
        });
    }
    interface CheckedAction {void run(DataSource source) throws Exception;}
    @Test void upgradesV2WithoutChangingExistingRuns() throws Exception {
        withSchema(ds->{
            Flyway.configure().dataSource(ds).target("2").cleanDisabled(true).load().migrate();
            var isolated=new JdbcTemplate(ds);
            isolated.update("INSERT INTO dw_workspace VALUES('v2','原工作空间','v2','local')");
            isolated.update("INSERT INTO dw_run VALUES('old-run','v2','old-object','SUCCESS','{}','{}','2026-09-28T00:00:00Z')");
            assertEquals(10,Flyway.configure().dataSource(ds).cleanDisabled(true).load().migrate().migrationsExecuted);
            assertEquals("SUCCESS",isolated.queryForObject("SELECT status FROM dw_run WHERE id='old-run'",String.class));
            assertNull(isolated.queryForObject("SELECT parent_run_id FROM dw_run WHERE id='old-run'",String.class));
            assertEquals(0,isolated.queryForObject("SELECT COUNT(*) FROM dw_workflow_release",Integer.class));
        });
    }
    @Test void cleanupRunsOnlyOnceAndKeepsRecoverableFilesConnectionsAndVersions() throws Exception {
        withSchema(ds->{
            Flyway.configure().dataSource(ds).target("5").load().migrate();
            var db=new JdbcTemplate(ds);var codec=new com.fake.dataworks.config.JsonCodec();
            db.update("INSERT INTO dw_workspace VALUES('clean','Cleanup','clean','local')");
            var repository=new com.fake.dataworks.repository.StudioRepository(db,codec);
            var objects=new com.fake.dataworks.service.ObjectService(repository,new com.fake.dataworks.service.GraphValidator());
            var folder=objects.create(new com.fake.dataworks.dto.ObjectInput("clean",null,"FOLDER","FOLDER","parent","","",Map.of(),List.of(),false,null));
            var child=objects.create(new com.fake.dataworks.dto.ObjectInput("clean",folder.id(),"NODE","MySQL","child","","SELECT 1",Map.of(),List.of(),false,null));
            db.update("INSERT INTO dw_datasource(id,workspace_id,name,host,port,database_name,username,password_cipher,updated_at) VALUES('keep','clean','keep','localhost',3306,'business','reader','encrypted','now')");
            db.update("INSERT INTO dw_task_schedule(id,workspace_id,task_id,enabled,version,next_fire_at,data_json) VALUES('plan','clean',?,TRUE,1,'future',?)",child.id(),codec.write(Map.of("enabled",true,"version",1,"nextFireAt","future")));
            db.update("INSERT INTO dw_preference VALUES('local_admin',?)",codec.write(Map.of("openTabs",List.of(child.id()),"activeId",child.id(),"theme","dark")));
            var flyway=Flyway.configure().dataSource(ds).load();assertEquals(7,flyway.migrate().migrationsExecuted);
            assertTrue(objects.list("clean",false).isEmpty());assertEquals(2,objects.list("clean",true).size());
            assertEquals(1,repository.versions(child.id()).size());assertEquals(1,db.queryForObject("SELECT COUNT(*) FROM dw_datasource",Integer.class));
            assertFalse(db.queryForObject("SELECT enabled FROM dw_task_schedule",Boolean.class));
            var plan=codec.map(db.queryForObject("SELECT data_json FROM dw_task_schedule",String.class));assertEquals(2,((Number)plan.get("version")).intValue());assertFalse((Boolean)plan.get("enabled"));
            var preference=codec.map(db.queryForObject("SELECT data_json FROM dw_preference",String.class));assertEquals(List.of(),preference.get("openTabs"));assertEquals("dark",preference.get("theme"));
            objects.restore(folder.id(),null);assertEquals(2,objects.list("clean",false).size());
            assertFalse(objects.get(child.id()).deleted());assertEquals("SELECT 1",objects.get(child.id()).content());
            objects.create(new com.fake.dataworks.dto.ObjectInput("clean",null,"NODE","MySQL","new file","","SELECT 2",Map.of(),List.of(),false,null));
            assertEquals(0,flyway.migrate().migrationsExecuted);assertEquals(3,objects.list("clean",false).size());
        });
    }
    @Test void retiresOnlyBuiltInSandboxAndSyncFixturesAndPreservesTheirData() throws Exception {
        withSchema(ds->{
            Flyway.configure().dataSource(ds).target("7").load().migrate();
            var db=new JdbcTemplate(ds);var codec=new com.fake.dataworks.config.JsonCodec();
            db.update("INSERT INTO dw_workspace VALUES('local-workspace','默认','dataworks_local','old'),('sandbox','沙箱','dataworks_sandbox','old'),('sync-accept-1234abcd','同步验收','1234abcd','local'),('business','业务项目','business','local')");
            db.update("INSERT INTO dw_object(id,workspace_id,kind,node_type,name,description,content,config_json,tags_json,owner,updated_at) VALUES('keep-file','sandbox','NODE','MySQL','保留文件','','SELECT 1','{}','[]','admin','now')");
            db.update("INSERT INTO dw_datasource(id,workspace_id,name,host,port,database_name,username,password_cipher,updated_at) VALUES('keep-source','sandbox','业务连接','localhost',3306,'business','reader','encrypted','now')");
            for(String space:List.of("sandbox","sync-accept-1234abcd","business")) {
                db.update("INSERT INTO dw_task_schedule(id,workspace_id,task_id,enabled,version,next_fire_at,data_json) VALUES(?,?,?,TRUE,1,'future',?)",space,space,"task-"+space,codec.write(Map.of("enabled",true,"version",1,"nextFireAt","future")));
                db.update("INSERT INTO dw_task_trigger(id,schedule_id,scheduled_at,status,data_json) VALUES(?,?,?,'PENDING','{}')",space,space,"future");
                db.update("INSERT INTO dw_workflow_schedule(id,workspace_id,workflow_id,release_id,enabled,version,next_fire_at,data_json) VALUES(?,?,?,?,TRUE,1,'future',?)",space,space,"workflow-"+space,"release-"+space,codec.write(Map.of("enabled",true,"version",1,"nextFireAt","future")));
                db.update("INSERT INTO dw_schedule_trigger(id,schedule_id,scheduled_at,status,data_json) VALUES(?,?,?,'PENDING','{}')",space,space,"future");
            }
            db.update("INSERT INTO dw_preference VALUES('local_admin',?)",codec.write(Map.of("workspaceId","sync-accept-1234abcd","openTabs",List.of("keep-file"),"activeId","keep-file","theme","dark")));
            var flyway=Flyway.configure().dataSource(ds).load();assertEquals(5,flyway.migrate().migrationsExecuted);
            var repository=new com.fake.dataworks.repository.StudioRepository(db,codec);
            assertEquals(Set.of("local-workspace","business"),new HashSet<>(repository.workspaces().stream().map(w->w.get("id")).toList()));
            assertEquals("local-workspace",repository.workspaces().getFirst().get("id"));
            assertEquals("SELECT 1",repository.find("keep-file").orElseThrow().content());
            assertFalse(repository.find("keep-file").orElseThrow().deleted());
            assertEquals("encrypted",db.queryForObject("SELECT password_cipher FROM dw_datasource WHERE id='keep-source'",String.class));
            for(String table:List.of("dw_task_schedule","dw_workflow_schedule")) {
                assertEquals(0,db.queryForObject("SELECT COUNT(*) FROM "+table+" WHERE enabled=TRUE AND workspace_id<>'business'",Integer.class));
                assertTrue(db.queryForObject("SELECT enabled FROM "+table+" WHERE workspace_id='business'",Boolean.class));
            }
            for(String table:List.of("dw_task_trigger","dw_schedule_trigger")) {
                assertEquals("CANCELLED",db.queryForObject("SELECT status FROM "+table+" WHERE id='sandbox'",String.class));
                assertEquals("PENDING",db.queryForObject("SELECT status FROM "+table+" WHERE id='business'",String.class));
            }
            assertEquals("local-workspace",repository.preferences().get("workspaceId"));
            assertEquals(List.of(),repository.preferences().get("openTabs"));
            assertEquals("dark",repository.preferences().get("theme"));
            assertEquals(0,flyway.migrate().migrationsExecuted);
        });
    }
    @Test void upgradesV8WithIndependentDebugParametersAndCascadeCleanup() throws Exception {
        withSchema(ds->{
            Flyway.configure().dataSource(ds).target("8").cleanDisabled(true).load().migrate();
            var db=new JdbcTemplate(ds);
            db.update("INSERT INTO dw_workspace(id,name,code,region,workspace_type) VALUES('debug','Debug','debug','local','TEST')");
            var repository=new com.fake.dataworks.repository.StudioRepository(db,new com.fake.dataworks.config.JsonCodec());
            var objects=new com.fake.dataworks.service.ObjectService(repository,new com.fake.dataworks.service.GraphValidator());
            var node=objects.create(new com.fake.dataworks.dto.ObjectInput("debug",null,"NODE","MAXCOMPUTE_SQL","preserved","","SELECT '${region}'",Map.of(),List.of(),false,null));
            var flyway=Flyway.configure().dataSource(ds).cleanDisabled(true).load();
            assertEquals(4,flyway.migrate().migrationsExecuted);
            assertEquals(node,repository.find(node.id()).orElseThrow());
            assertEquals(0,db.queryForObject("SELECT COUNT(*) FROM dw_node_debug_parameters",Integer.class));
            db.update("INSERT INTO dw_node_debug_parameters(object_id,parameters_json,updated_at) VALUES(?,?,?)",node.id(),"{\"region\":\"debug value\"}","2026-09-30T00:00:00Z");
            objects.delete(node.id());
            assertEquals(1,db.queryForObject("SELECT COUNT(*) FROM dw_node_debug_parameters WHERE object_id=?",Integer.class,node.id()));
            db.update("DELETE FROM dw_version WHERE object_id=?",node.id());
            db.update("DELETE FROM dw_object WHERE id=?",node.id());
            assertEquals(0,db.queryForObject("SELECT COUNT(*) FROM dw_node_debug_parameters",Integer.class));
            assertEquals(0,flyway.migrate().migrationsExecuted);
        });
    }
    void withSchema(CheckedAction action) throws Exception {
        String schema="studio_migration_test_"+UUID.randomUUID().toString().replace("-","");
        jdbc.execute("CREATE DATABASE `"+schema+"` CHARACTER SET utf8mb4");
        String base=url.split("\\?",2)[0],query=url.contains("?")?url.substring(url.indexOf('?')):"";
        DataSource isolated=new DriverManagerDataSource(base.substring(0,base.lastIndexOf('/')+1)+schema+query,user,password);
        try {action.run(isolated);}finally {jdbc.execute("DROP DATABASE `"+schema+"`");}
    }
}
