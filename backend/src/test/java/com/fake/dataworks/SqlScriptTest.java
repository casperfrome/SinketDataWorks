package com.fake.dataworks;

import com.fake.dataworks.service.*;
import com.fake.dataworks.exception.StudioException;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SqlScriptTest {
    private SqlScript parse(String sql) {return SqlScript.prepare(sql,"studio_demo",new SqlGuard());}
    @Test void supportsMysqlDataAndSchemaStatements() {
        for(String sql:List.of("SELECT 1", "WITH c AS (SELECT 1) SELECT * FROM c", "INSERT INTO t(id) VALUES(1)","INSERT INTO t SELECT id FROM a", "REPLACE INTO t VALUES(1)", "UPDATE t SET id=2", "DELETE FROM t WHERE id=1", "CREATE TABLE t(id INT PRIMARY KEY)","CREATE TABLE t LIKE studio_demo.a", "CREATE TABLE t AS SELECT * FROM a", "ALTER TABLE t ADD COLUMN name VARCHAR(30)","ALTER TABLE t RENAME TO studio_demo.a", "RENAME TABLE t TO a, b TO c", "TRUNCATE TABLE t", "CREATE INDEX idx ON studio_demo.t(id)", "DROP INDEX idx ON studio_demo.t", "CREATE VIEW v AS SELECT * FROM t", "ALTER VIEW v AS SELECT * FROM t", "DROP TABLE IF EXISTS t, studio_demo.a", "DROP VIEW v")) {
            assertDoesNotThrow(()->parse(sql),sql);
        }
    }
    @Test void splitsOnlySqlDelimitersAndBindsParametersIndependently() {
        var script=parse("# ignored ;\n INSERT INTO t VALUES('${region}'); SELECT ';it''s', 'x\\\';y', :bizdate /* ; */; -- ;\n SELECT 3;;");
        assertEquals(3,script.commands().size());assertTrue(script.writes());
        assertEquals(1,script.commands().get(0).parameters().names().size());
        assertEquals(List.of("bizdate"),script.commands().get(1).parameters().names());
        assertFalse(parse("SELECT 1;SELECT 2").writes());
    }
    @Test void validatesAllTargetsAndSourcesIncludingDdlReferences() {
        for(String sql:List.of("CREATE TABLE t(id INT REFERENCES other.a(id))", "CREATE TABLE t(id INT DEFAULT (other.f()))", "ALTER TABLE t ADD COLUMN id INT REFERENCES other.a(id)"))assertThrows(StudioException.class,()->parse(sql),sql);
        for(String sql:List.of("INSERT INTO other.t VALUES(1)", "INSERT INTO t SELECT * FROM other.a", "UPDATE other.t SET id=1", "DELETE FROM other.t", "CREATE TABLE other.t(id INT)", "CREATE TABLE t LIKE other.a", "CREATE TABLE t AS SELECT * FROM other.a", "CREATE TABLE t(id INT, FOREIGN KEY(id) REFERENCES other.a(id))", "ALTER TABLE t ADD FOREIGN KEY(id) REFERENCES other.a(id)", "ALTER TABLE t RENAME TO other.a", "RENAME TABLE t TO other.a", "CREATE INDEX idx ON other.t(id)", "DROP INDEX idx ON other.t", "CREATE VIEW v AS SELECT * FROM other.t", "ALTER VIEW other.v AS SELECT * FROM t", "ALTER VIEW v AS SELECT * FROM other.t", "DROP TABLE t, other.a", "TRUNCATE TABLE other.t"))assertThrows(StudioException.class,()->parse(sql),sql);
    }
    @Test void rejectsWholeScriptBeforeRunningAnyUnsupportedCommand() {
        for(String sql:List.of("INSERT INTO t VALUES(1);GRANT SELECT ON t TO x", "SELECT 1;COMMIT", "BEGIN", "SET autocommit=0", "CREATE DATABASE db", "DROP DATABASE studio_demo", "CALL p()", "LOAD DATA INFILE 'x' INTO TABLE t", "SELECT 1 INTO OUTFILE '/tmp/x'", "INSERT INTO t VALUES(GET_LOCK('a',1))", "UPDATE t SET id=other.f()", "CREATE TABLE t(id INT) DATA DIRECTORY='/tmp'", "SELECT /*!50000 1*/", "SELECT 'unclosed", "SELECT 1 /* unclosed", "SELECT @x", "SELECT 1; bad syntax", "SELECT * FROM ${table}"))assertThrows(StudioException.class,()->parse(sql),sql);
    }
    @Test void resultsPageAndRecoveryPreserveAcknowledgedWrites() {
        var items=SqlResults.pending(parse("INSERT INTO t VALUES(1);SELECT 1;UPDATE t SET id=2"));
        items.get(0).put("status","SUCCESS");items.get(0).put("commitStatus","COMMITTED");items.get(0).put("affectedRows",1);
        items.get(1).put("status","SUCCESS");items.get(1).put("columns",List.of("n"));items.get(1).put("rows",List.of(List.of("1")));
        items.get(2).put("status","RUNNING");items.get(2).put("commitStatus","IN_PROGRESS");
        var result=SqlResults.envelope(items);assertTrue(SqlResults.recover(result));
        assertEquals("COMMITTED",items.get(0).get("commitStatus"));assertEquals("UNKNOWN",items.get(2).get("commitStatus"));
        assertEquals(2,SqlResults.page(result,1,100,null).get("statementIndex"));assertEquals(1,SqlResults.page(result,1,100,1).get("affectedRows"));
        assertThrows(StudioException.class,()->SqlResults.page(result,1,100,0));assertThrows(StudioException.class,()->SqlResults.page(result,1,100,4));
        assertEquals(List.of(List.of("legacy")),SqlResults.page(Map.of("columns",List.of("x"),"rows",List.of(List.of("legacy"))),1,100,null).get("rows"));
    }
}
