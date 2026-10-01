package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DatasourceCdcTest {
    DatasourceService sources;
    DatasourceService.ConnectionSpec spec=new DatasourceService.ConnectionSpec("source","workspace","MySQL","localhost",3307,"business","reader","cipher","MYSQL",Map.of());
    Connection connection;
    Statement statement;
    static final String VARIABLES="SHOW GLOBAL VARIABLES WHERE Variable_name IN ('log_bin','binlog_format','binlog_row_image','server_id','time_zone','system_time_zone')";
    static final String SELECT="SELECT * FROM `business`.`orders` LIMIT 0";
    static final String GRANTS="SHOW GRANTS FOR CURRENT_USER";
    @BeforeEach void init() throws Exception {
        sources=spy(new DatasourceService(mock(JdbcTemplate.class),mock(ObjectService.class),mock(SecretCipher.class),new JsonCodec()));
        connection=mock(Connection.class);statement=mock(Statement.class);when(connection.createStatement()).thenReturn(statement);
        doReturn(spec).when(sources).get("source");doReturn(Map.of("columns",List.of(Map.of("name","id")))).when(sources).syncMetadata(spec,"orders");doReturn(connection).when(sources).open(spec,5);
        var variables=mock(ResultSet.class);when(variables.next()).thenReturn(true,true,true,true,false);when(variables.getString(1)).thenReturn("log_bin","binlog_format","binlog_row_image","server_id");when(variables.getString(2)).thenReturn("ON","ROW","FULL","1");when(statement.executeQuery(VARIABLES)).thenReturn(variables);
        var empty=mock(ResultSet.class);when(statement.executeQuery(SELECT)).thenReturn(empty);stubQuery("SELECT CURRENT_ROLE()","NONE");
        stubQuery(GRANTS,"GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `reader`@`%`");
    }
    static ResultSet rows(String... values) throws Exception {
        var rows=mock(ResultSet.class);var cursor=new java.util.concurrent.atomic.AtomicInteger(-1);when(rows.next()).thenAnswer(call->cursor.incrementAndGet()<values.length);when(rows.getString(1)).thenAnswer(call->values[cursor.get()]);return rows;
    }
    void stubQuery(String query,String... values) throws Exception {var result=rows(values);when(statement.executeQuery(query)).thenReturn(result);}
    @Test void readsNoBusinessRowsAndProvesSelectWithBoundedQueries() throws Exception {
        var result=sources.cdcMetadata("source","orders");assertEquals(true,result.get("valid"));assertEquals(List.of(),result.get("issues"));verify(statement).executeQuery(SELECT);verify(statement,times(4)).setQueryTimeout(5);verify(connection).close();
    }
    @Test void metadataVisibilityCannotSubstituteForSelectPrivilege() throws Exception {
        when(statement.executeQuery(SELECT)).thenThrow(new SQLException("sensitive upstream message","42000",1142));var result=sources.cdcMetadata("source","orders");assertEquals(false,result.get("valid"));assertTrue(result.get("issues").toString().contains("SELECT"));assertFalse(result.toString().contains("sensitive upstream"));verify(connection).close();
    }
    @Test void currentActiveRolesAreExpandedWithoutActivatingAdditionalRoles() throws Exception {
        String roles="`cdc_read`@`%`,`cdc_replication`@`localhost`";stubQuery("SELECT CURRENT_ROLE()",roles);stubQuery(GRANTS,"GRANT USAGE ON *.* TO `reader`@`%`","GRANT `cdc_replication`@`localhost` TO `reader`@`%`");
        stubQuery(GRANTS+" USING "+roles,"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `reader`@`%`");assertEquals(true,sources.cdcMetadata("source","orders").get("valid"));verify(statement).executeQuery(GRANTS+" USING "+roles);verify(statement,never()).executeQuery("SET ROLE ALL");
    }
    @Test void untrustedOrUnsupportedRoleTextCannotBecomeAnExecutableClause() throws Exception {
        String roles="`cdc`@`%`; DROP TABLE orders";stubQuery("SELECT CURRENT_ROLE()",roles);var result=sources.cdcMetadata("source","orders");assertEquals(false,result.get("valid"));verify(statement,never()).executeQuery(GRANTS+" USING "+roles);
        for(String invalid:List.of("cdc@%","`cdc`@`%`,","`cdc`@`%` UNION SELECT 1","'cdc\\'@'%'","`cdc`@`%`\n"))assertThrows(IllegalArgumentException.class,()->DatasourceService.validatedCdcRoles(invalid));
        assertEquals("`cdc``role`@`%`,'second'@'localhost'",DatasourceService.validatedCdcRoles("`cdc``role`@`%`, 'second'@'localhost'"));
    }
    @Test void replicationChecksRequireExactGlobalPrivilegeTokens() {
        var invalid=DatasourceService.cdcGlobalPrivileges(List.of("GRANT REPLICATION SLAVE ADMIN, SELECT ON *.* TO `REPLICATION SLAVE REPLICATION CLIENT`@`%`","GRANT REPLICATION SLAVE, REPLICATION CLIENT ON `business`.* TO `reader`@`%`","GRANT `REPLICATION SLAVE`@`%`,`REPLICATION CLIENT`@`%` TO `reader`@`%`"));assertFalse(invalid.contains("REPLICATION SLAVE"));assertFalse(invalid.contains("REPLICATION CLIENT"));
        assertTrue(DatasourceService.cdcGlobalPrivileges(List.of("GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `reader`@`%` WITH GRANT OPTION")).containsAll(Set.of("REPLICATION SLAVE","REPLICATION CLIENT")));
        assertTrue(DatasourceService.cdcGlobalPrivileges(List.of("GRANT ALL PRIVILEGES ON *.* TO `reader`@`%`")).containsAll(Set.of("REPLICATION SLAVE","REPLICATION CLIENT")));
    }
    @Test void serverIdentityMustBeNonzeroAndWithinMysqlRange() {
        for(String id:List.of("0","-1","4294967296","not-a-number")){var config=new HashMap<>(Map.of("log_bin","ON","binlog_format","ROW","binlog_row_image","FULL","server_id",id));assertTrue(DatasourceService.cdcVariableIssues(config).stream().anyMatch(issue->issue.contains("server_id")));}
        assertEquals(List.of(),DatasourceService.cdcVariableIssues(Map.of("log_bin","ON","binlog_format","ROW","binlog_row_image","FULL","server_id","4294967295")));
    }
}
