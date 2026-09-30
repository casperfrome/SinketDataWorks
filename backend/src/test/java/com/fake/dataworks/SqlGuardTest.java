package com.fake.dataworks;

import com.fake.dataworks.service.SqlGuard;
import com.fake.dataworks.exception.StudioException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SqlGuardTest {
    private final SqlGuard guard=new SqlGuard();
    @Test void acceptsSelectCteCommentsAndQuotedSemicolons() {
        for(String sql:new String[]{"SELECT 1;","SELECT 'DROP; UPDATE INTO' AS text","-- comment\n SELECT name FROM studio_demo.users", "WITH x AS (SELECT id FROM users) SELECT * FROM x", "SELECT * FROM `studio_demo`.`orders`", "SELECT 1 UNION ALL SELECT 2", "SELECT ';' /* ; DELETE */"}) assertDoesNotThrow(()->guard.validate(sql,"studio_demo"),sql);
    }
    @Test void refusesNonQueriesMultipleStatementsMacrosAndSideEffects() {
        for(String sql:new String[]{"DELETE FROM users","DROP TABLE users","INSERT INTO users VALUES(1)","SELECT 1; SELECT 2", "SELECT * FROM users FOR UPDATE", "SELECT * FROM users LOCK IN SHARE MODE", "SELECT 1 INTO OUTFILE '/tmp/test'", "SELECT @a:=1", "SELECT GET_LOCK('a',1)", "SELECT LOAD_FILE('/etc/passwd')", "SELECT /*!50000 1 */", "SELECT /*+ MAX_EXECUTION_TIME(0) */ 1", "SELECT '${bizdate}'", "WITH x AS (DELETE FROM users RETURNING id) SELECT * FROM x"}) assertThrows(StudioException.class,()->guard.validate(sql,"studio_demo"),sql);
    }
    @Test void refusesCrossDatabaseIncludingNestedAndQuotedTables() {
        for(String sql:new String[]{"SELECT * FROM mysql.user", "SELECT * FROM `fake_dataworks_260927`.`dw_object`", "SELECT (SELECT count(*) FROM other.secret)", "WITH x AS (SELECT * FROM other.secret) SELECT * FROM x"}) assertThrows(StudioException.class,()->guard.validate(sql,"studio_demo"),sql);
    }
    @Test void refusesQuotedSideEffectFunctionsAndQualifiedStoredFunctions() {
        for(String sql:new String[]{"SELECT `GET_LOCK`('a',1)","SELECT other.my_function()", "SELECT LAST_INSERT_ID(2)"}) assertThrows(StudioException.class,()->guard.validate(sql,"studio_demo"),sql);
    }
}
