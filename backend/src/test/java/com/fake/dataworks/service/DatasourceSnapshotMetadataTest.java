package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import java.sql.*;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class DatasourceSnapshotMetadataTest {
    @Test void metadataUsesOneConnectionSnapshotForColumnsDdlAndPartitions() throws Exception {
        var sources=spy(new DatasourceService(mock(JdbcTemplate.class),mock(ObjectService.class),mock(SecretCipher.class),new JsonCodec()));
        var original=new DatasourceService.ConnectionSpec("doris","workspace","Doris","original-host",9030,"original_schema","writer","cipher","DORIS",Map.of());
        var changed=new DatasourceService.ConnectionSpec("doris","workspace","Doris","changed-host",9030,"changed_schema","writer","cipher","DORIS",Map.of());
        doReturn(original,changed).when(sources).get("doris");
        var connection=mock(Connection.class);doReturn(connection).when(sources).open(same(original),anyInt());
        var columnStatement=mock(PreparedStatement.class);var partitionStatement=mock(PreparedStatement.class);var ddlStatement=mock(Statement.class);
        when(connection.prepareStatement(startsWith("SELECT COLUMN_NAME"))).thenReturn(columnStatement);
        when(connection.prepareStatement("SHOW PARTITIONS FROM `original_schema`.`orders`")).thenReturn(partitionStatement);
        when(connection.createStatement()).thenReturn(ddlStatement);
        var columnRows=mock(ResultSet.class);var columnMeta=mock(ResultSetMetaData.class);when(columnStatement.executeQuery()).thenReturn(columnRows);when(columnRows.next()).thenReturn(true,false);when(columnRows.getMetaData()).thenReturn(columnMeta);
        when(columnMeta.getColumnCount()).thenReturn(2);when(columnMeta.getColumnLabel(1)).thenReturn("name");when(columnMeta.getColumnLabel(2)).thenReturn("type");when(columnRows.getString(1)).thenReturn("ds");when(columnRows.getString(2)).thenReturn("date");
        var ddlRows=mock(ResultSet.class);when(ddlStatement.executeQuery("SHOW CREATE TABLE `original_schema`.`orders`")).thenReturn(ddlRows);when(ddlRows.next()).thenReturn(true);
        when(ddlRows.getString(2)).thenReturn("CREATE TABLE orders(ds DATE) DUPLICATE KEY(ds) AUTO PARTITION BY RANGE(date_trunc(ds,'day')) () DISTRIBUTED BY HASH(ds) BUCKETS AUTO");
        var partitionRows=mock(ResultSet.class);when(partitionStatement.executeQuery()).thenReturn(partitionRows);when(partitionRows.getMetaData()).thenReturn(mock(ResultSetMetaData.class));when(partitionRows.next()).thenReturn(false);

        var metadata=sources.syncMetadata("doris","orders");

        assertEquals("DUPLICATE",metadata.get("model"));assertTrue(SyncPartitionMetadata.from(metadata).automatic());
        verify(sources,times(1)).get("doris");verify(sources,times(2)).open(same(original),eq(5));verify(sources).open(same(original),eq(10));verify(sources,never()).open(same(changed),anyInt());
        verify(columnStatement).setString(1,"original_schema");verify(columnStatement).setString(2,"orders");
    }
}
