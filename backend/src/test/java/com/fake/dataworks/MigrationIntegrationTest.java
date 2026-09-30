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
            assertEquals(7,flyway.migrate().migrationsExecuted);
            JdbcTemplate isolated=new JdbcTemplate(ds);isolated.update("INSERT INTO dw_workspace VALUES('preserved','保留数据','keep','local')");
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
            flyway.baseline();assertEquals(6,flyway.migrate().migrationsExecuted);
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
            assertEquals(5,Flyway.configure().dataSource(ds).cleanDisabled(true).load().migrate().migrationsExecuted);
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
            var flyway=Flyway.configure().dataSource(ds).load();assertEquals(2,flyway.migrate().migrationsExecuted);
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
    void withSchema(CheckedAction action) throws Exception {
        String schema="studio_migration_test_"+UUID.randomUUID().toString().replace("-","");
        jdbc.execute("CREATE DATABASE `"+schema+"` CHARACTER SET utf8mb4");
        String base=url.split("\\?",2)[0],query=url.contains("?")?url.substring(url.indexOf('?')):"";
        DataSource isolated=new DriverManagerDataSource(base.substring(0,base.lastIndexOf('/')+1)+schema+query,user,password);
        try {action.run(isolated);}finally {jdbc.execute("DROP DATABASE `"+schema+"`");}
    }
}
