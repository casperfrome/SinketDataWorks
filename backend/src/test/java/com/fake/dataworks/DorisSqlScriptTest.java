package com.fake.dataworks;

import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.service.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DorisSqlScriptTest {
    private SqlScript parse(String sql){return SqlScript.prepare(sql,"test_ods",new SqlGuard(),"DORIS");}
    @Test void supportsEmptyDailyAutoPartitionWithDefaultsAndRecognizesReadOnlyMetadata(){
        var script=parse("CREATE TABLE test_ods.ods_orders_di(ds DATE NOT NULL,id BIGINT NOT NULL,user_id BIGINT NOT NULL,total_amount DECIMAL(18,2) NOT NULL,status VARCHAR(20) NOT NULL,ordered_at DATETIME NOT NULL) AUTO PARTITION BY RANGE(date_trunc(ds,'day')) () PROPERTIES('partition.retention_count'='400','replication_num'='1');SHOW CREATE TABLE test_ods.ods_orders_di;SHOW PARTITIONS FROM ods_orders_di;DESC ods_orders_di;WITH c AS(SELECT id FROM ods_orders_di) SELECT * FROM c");
        assertEquals(List.of("DDL","QUERY","QUERY","QUERY","QUERY"),script.commands().stream().map(SqlScript.Command::kind).toList());assertTrue(script.writes());
        assertFalse(parse("SHOW PARTITIONS FROM ods_orders_di;DESCRIBE test_ods.ods_orders_di;SHOW CREATE TABLE ods_orders_di").writes());
        var results=SqlResults.pending(script);assertEquals(2,SqlResults.page(SqlResults.envelope(results),1,100,null).get("statementIndex"));
    }
    @Test void supportsOwnSchemaCrudAndPartitionOperations(){
        for(String sql:List.of(
                "CREATE TABLE IF NOT EXISTS `test_ods`.`t`(ds DATE NOT NULL,id BIGINT NOT NULL) ENGINE=OLAP DUPLICATE KEY(ds,id) AUTO PARTITION BY RANGE(date_trunc(ds,'day')) () DISTRIBUTED BY RANDOM BUCKETS AUTO PROPERTIES('replication_num'='1')",
                "CREATE TABLE t(ds DATE NOT NULL,id BIGINT) DUPLICATE KEY(ds,id) PARTITION BY RANGE(ds)(PARTITION p1 VALUES [('2026-09-20'),('2026-09-21'))) DISTRIBUTED BY HASH(id) BUCKETS 3",
                "CREATE TABLE t(ds DATE NOT NULL) AUTO PARTITION BY LIST(ds) () DISTRIBUTED BY RANDOM BUCKETS 3",
                "CREATE TABLE t LIKE test_ods.orders", "CREATE TABLE t AS SELECT * FROM test_ods.orders WHERE id != 1",
                "INSERT INTO t VALUES('2026-09-20',1)", "INSERT INTO test_ods.t SELECT ds,id FROM orders", "UPDATE test_ods.t SET id=2 WHERE id=1", "DELETE FROM test_ods.t WHERE id=1", "SELECT * FROM orders PARTITION(p1) WHERE id != 1",
                "ALTER TABLE t ADD PARTITION p1 VALUES LESS THAN('2026-09-21') DISTRIBUTED BY HASH(id) BUCKETS AUTO PROPERTIES('replication_num'='1')",
                "ALTER TABLE t ADD TEMPORARY PARTITION p1 VALUES [('2026-09-20'),('2026-09-21'))", "ALTER TABLE t DROP PARTITION IF EXISTS p1 FORCE",
                "ALTER TABLE t DROP TEMPORARY PARTITION p1", "ALTER TABLE t REPLACE PARTITION(p1) WITH TEMPORARY PARTITION(tmp1) PROPERTIES('strict_range'='false')",
                "ALTER TABLE t MODIFY PARTITION(*) SET('replication_num'='1')", "ALTER TABLE t SET('partition.retention_count'='400')",
                "ALTER TABLE t ADD COLUMN label VARCHAR(20) NULL DEFAULT 'hello' AFTER id, MODIFY COLUMN id BIGINT NOT NULL, DROP COLUMN label",
                "ALTER TABLE t RENAME TO test_ods.orders", "ALTER TABLE t RENAME PARTITION p1 TO p2", "ALTER TABLE t RENAME COLUMN id TO order_id",
                "TRUNCATE TABLE t PARTITION(p1,p2)", "DROP TABLE IF EXISTS test_ods.t FORCE", "SHOW TABLES FROM test_ods LIKE 'ods_%'", "SHOW COLUMNS FROM test_ods.t"))assertDoesNotThrow(()->parse(sql),sql);
    }
    @Test void neverForwardsUnrecognizedOrPartiallyParsedDorisDdl(){
        for(String sql:List.of(
                "CREATE TABLE t(ds DATE NOT NULL) AUTO PARTITION BY RANGE(date_trunc(ds,'day')) () BUCKETS AUTO",
                "CREATE TABLE t(ds DATE NOT NULL) ENGINE=MYSQL PROPERTIES('host'='somewhere')", "CREATE EXTERNAL TABLE t(id INT)",
                "CREATE TABLE t(ds DATE NOT NULL) AUTO PARTITION BY RANGE(other.f(ds)) ()", "CREATE TABLE t(ds DATE NOT NULL DEFAULT other.f())", "CREATE TABLE t(id INT REFERENCES other.secret(id))",
                "CREATE TABLE t(ds DATE NOT NULL) AUTO PARTITION BY RANGE(date_trunc(ds,'day')) () garbage", "ALTER TABLE t ENABLE FEATURE 'BATCH_DELETE'",
                "ALTER TABLE t ADD COLUMN id BIGINT;CALL p()", "SHOW CREATE DATABASE test_ods", "SHOW BACKENDS", "SET group_commit='async_mode'", "BEGIN", "COMMIT",
                "CREATE TABLE t(id INT) AS SELECT * FROM other.orders", "CREATE TABLE other.t(id INT)", "CREATE TABLE t LIKE other.orders",
                "ALTER TABLE other.t ADD COLUMN id BIGINT", "ALTER TABLE t RENAME TO other.orders", "DROP TABLE t,other.orders", "TRUNCATE TABLE other.t",
                "SHOW CREATE TABLE other.orders", "SHOW PARTITIONS FROM internal.test_ods.orders", "DESC other.orders", "SHOW TABLES FROM other", "SHOW COLUMNS FROM t FROM other",
                "INSERT INTO t SELECT * FROM other.orders", "SELECT * FROM other.orders PARTITION(p1)", "SELECT other.f()", "SELECT @x", "SELECT 1 INTO OUTFILE '/tmp/x'",
                "SELECT * FROM S3('uri'='https://example.test/file.csv')", "SELECT * FROM query('catalog'='jdbc','query'='select * from other.orders')",
                "INSERT INTO t SELECT * FROM S3('uri'='https://example.test/file.csv')", "CREATE TABLE t AS SELECT * FROM query('catalog'='jdbc','query'='select * from other.orders')",
                "CREATE TABLE t(id INT /*+ HINT */)", "CREATE TABLE t(id INT) PROPERTIES('x'='unclosed)", "CREATE TABLE t(id INT) PROPERTIES('x'='y') INSERT INTO t VALUES(1)"))assertThrows(StudioException.class,()->parse(sql),sql);
    }
    @Test void metadataQueriesRecoverWithoutUnknownCommit(){
        var items=SqlResults.pending(parse("SHOW CREATE TABLE t;SHOW PARTITIONS FROM t;ALTER TABLE t SET('partition.retention_count'='400')"));
        items.get(0).put("status","SUCCESS");items.get(0).put("commitStatus","NOT_APPLICABLE");items.get(1).put("status","RUNNING");items.get(1).put("commitStatus","NOT_APPLICABLE");
        assertFalse(SqlResults.recover(SqlResults.envelope(items)));assertEquals("NOT_APPLICABLE",items.get(1).get("commitStatus"));assertEquals("SKIPPED",items.get(2).get("status"));
        items.get(2).put("status","RUNNING");items.get(2).put("commitStatus","IN_PROGRESS");assertTrue(SqlResults.recover(SqlResults.envelope(items)));assertEquals("UNKNOWN",items.get(2).get("commitStatus"));
    }
}
