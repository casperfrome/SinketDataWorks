package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.exception.StudioException;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DatasourceRealtimeSchemaTest {
    DatasourceService sources;
    final DatasourceService.ConnectionSpec mysql=new DatasourceService.ConnectionSpec("mysql","workspace","MySQL","localhost",3307,"business","reader","cipher","MYSQL",Map.of());
    final DatasourceService.ConnectionSpec doris=new DatasourceService.ConnectionSpec("doris","workspace","Doris","localhost",9030,"warehouse","writer","cipher","DORIS",Map.of());
    @BeforeEach void init(){
        sources=spy(new DatasourceService(mock(JdbcTemplate.class),mock(ObjectService.class),mock(SecretCipher.class),new JsonCodec()));
        doReturn(mysql).when(sources).forWorkspace("mysql","workspace");doReturn(doris).when(sources).forWorkspace("doris","workspace");
    }
    static Map<String,Object> field(String name,String type,boolean key){return Map.of("name",name,"type",type,"primaryKey",key,"nullable",false);}
    static Map<String,Object> column(String name,String type,String key){return Map.of("name",name,"type",type,"columnKey",key,"nullable","NO");}
    static Map<String,Object> binding(String connector,List<Map<String,Object>> fields){
        return new LinkedHashMap<>(Map.of("connector",connector,"datasourceId",connector.equals("DORIS")?"doris":"mysql","physicalTable","orders","fields",fields,"writeMode","upsert","dorisModel","UNIQUE","syncDeletes",true));
    }
    void metadata(DatasourceService.ConnectionSpec spec,List<Map<String,Object>> columns,List<List<String>> keys){doReturn(Map.of("columns",columns,"uniqueKeys",keys,"model",spec.type().equals("MYSQL")?"INNODB":"UNIQUE_MOW")).when(sources).syncMetadata(spec,"orders");}
    void dorisDdl(String ddl) throws Exception {
        var connection=mock(Connection.class);var statement=mock(Statement.class);var rows=mock(ResultSet.class);
        doReturn(connection).when(sources).open(doris,10);when(connection.createStatement()).thenReturn(statement);when(statement.executeQuery("SHOW CREATE TABLE `warehouse`.`orders`")).thenReturn(rows);when(rows.next()).thenReturn(true);when(rows.getString(2)).thenReturn(ddl);
    }
    static void mismatch(String code,Runnable action,String detail){var failure=assertThrows(StudioException.class,action::run);assertEquals(code,failure.code());assertTrue(failure.getMessage().contains(detail),failure.getMessage());}
    @Test void liveMissingColumnAndIncompatibleTypeHaveConnectorSpecificErrors(){
        metadata(mysql,List.of(column("id","int(11)","PRI"),column("payload","varchar(255)","")),List.of(List.of("id")));
        var valid=binding("MYSQL_JDBC",List.of(field("id","INT",true),field("payload","STRING",false)));assertDoesNotThrow(()->sources.validateRealtimeTable(valid,"workspace"));
        mismatch("TARGET_SCHEMA_MISMATCH",()->sources.validateRealtimeTable(binding("MYSQL_JDBC",List.of(field("gone","INT",true))),"workspace"),"字段不存在：gone");
        mismatch("CDC_SCHEMA_MISMATCH",()->sources.validateRealtimeTable(binding("MYSQL_CDC",List.of(field("id","STRING",true))),"workspace"),"类型不兼容");
        verify(sources,atLeastOnce()).syncMetadata(mysql,"orders");
    }
    @Test void cdcPrimaryKeyAndJdbcUniqueKeyMustMatchWholeCompositeKeys(){
        metadata(mysql,List.of(column("id","int(11)","PRI"),column("tenant","int(11)","PRI")),List.of(List.of("id","tenant")));
        for(String connector:List.of("MYSQL_CDC","MYSQL_JDBC")){
            mismatch(connector.equals("MYSQL_CDC")?"CDC_SCHEMA_MISMATCH":"TARGET_SCHEMA_MISMATCH",()->sources.validateRealtimeTable(binding(connector,List.of(field("id","INT",true),field("tenant","INT",false))),"workspace"),"主键");
            assertDoesNotThrow(()->sources.validateRealtimeTable(binding(connector,List.of(field("TENANT","INT",true),field("ID","INT",true))),"workspace"));
        }
    }
    @Test void integerCapacityIsCheckedInDataDirectionIncludingUnsigned(){
        metadata(mysql,List.of(column("id","bigint(20)","PRI")),List.of(List.of("id")));
        mismatch("CDC_SCHEMA_MISMATCH",()->sources.validateRealtimeTable(binding("MYSQL_CDC",List.of(field("id","INT",true))),"workspace"),"整数范围");
        metadata(mysql,List.of(column("id","tinyint(4)","PRI")),List.of(List.of("id")));
        mismatch("TARGET_SCHEMA_MISMATCH",()->sources.validateRealtimeTable(binding("MYSQL_JDBC",List.of(field("id","BIGINT",true))),"workspace"),"整数范围");
        assertDoesNotThrow(()->sources.validateRealtimeTable(binding("MYSQL_CDC",List.of(field("id","BIGINT",true))),"workspace"));
        metadata(mysql,List.of(column("id","int(10) unsigned","PRI")),List.of(List.of("id")));
        mismatch("CDC_SCHEMA_MISMATCH",()->sources.validateRealtimeTable(binding("MYSQL_CDC",List.of(field("id","INT",true))),"workspace"),"整数范围");
        assertDoesNotThrow(()->sources.validateRealtimeTable(binding("MYSQL_CDC",List.of(field("id","BIGINT",true))),"workspace"));
    }
    @Test void floatWideningIsAllowedAndNarrowingRejected(){
        metadata(mysql,List.of(column("id","int(11)","PRI"),column("amount","float","")),List.of(List.of("id")));
        var fields=List.of(field("id","INT",true),field("amount","DOUBLE",false));assertDoesNotThrow(()->sources.validateRealtimeTable(binding("MYSQL_CDC",fields),"workspace"));
        mismatch("TARGET_SCHEMA_MISMATCH",()->sources.validateRealtimeTable(binding("MYSQL_JDBC",fields),"workspace"),"浮点精度");
        metadata(mysql,List.of(column("id","int(11)","PRI"),column("amount","double","")),List.of(List.of("id")));
        mismatch("CDC_SCHEMA_MISMATCH",()->sources.validateRealtimeTable(binding("MYSQL_CDC",List.of(field("id","INT",true),field("amount","FLOAT",false))),"workspace"),"浮点精度");
    }
    @Test void decimalPrecisionAndScaleMustMatchAndTemporalPrecisionCannotBeLost(){
        metadata(mysql,List.of(column("id","int(11)","PRI"),column("amount","decimal(18,2)",""),column("created_at","datetime(6)","")),List.of(List.of("id")));
        mismatch("TARGET_SCHEMA_MISMATCH",()->sources.validateRealtimeTable(binding("MYSQL_JDBC",List.of(field("id","INT",true),field("amount","DECIMAL(20,4)",false))),"workspace"),"DECIMAL 精度");
        assertDoesNotThrow(()->sources.validateRealtimeTable(binding("MYSQL_JDBC",List.of(field("id","INT",true),field("amount","NUMERIC(18,2)",false))),"workspace"));
        mismatch("CDC_SCHEMA_MISMATCH",()->sources.validateRealtimeTable(binding("MYSQL_CDC",List.of(field("id","INT",true),field("created_at","TIMESTAMP(3)",false))),"workspace"),"时间精度");
        metadata(mysql,List.of(column("id","int(11)","PRI"),column("created_at","datetime(0)","")),List.of(List.of("id")));
        mismatch("TARGET_SCHEMA_MISMATCH",()->sources.validateRealtimeTable(binding("MYSQL_JDBC",List.of(field("id","INT",true),field("created_at","TIMESTAMP(3)",false))),"workspace"),"时间精度");
    }
    @Test void nullableBindingCannotWriteToNonNullableTarget(){
        metadata(mysql,List.of(column("id","int(11)","PRI"),column("payload","varchar(255)","")),List.of(List.of("id")));
        var nullable=new LinkedHashMap<>(field("payload","STRING",false));nullable.put("nullable",true);
        mismatch("TARGET_SCHEMA_MISMATCH",()->sources.validateRealtimeTable(binding("MYSQL_JDBC",List.of(field("id","INT",true),nullable)),"workspace"),"NOT NULL");
    }
    @Test void dorisPhysicalModelAndWholeUniqueKeyAreVerifiedFromActualCreateTable() throws Exception {
        metadata(doris,List.of(column("id","int(11)","UNI"),column("payload","varchar(255)","")),List.of());
        String ddl="CREATE TABLE orders (`id` int NOT NULL, `payload` varchar(255) COMMENT 'DUPLICATE KEY(fake)') ENGINE=OLAP UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES (\"enable_unique_key_merge_on_write\" = \"true\")";dorisDdl(ddl);
        assertDoesNotThrow(()->sources.validateRealtimeTable(binding("DORIS",List.of(field("id","INT",true),field("payload","STRING",false))),"workspace"));
        mismatch("TARGET_SCHEMA_MISMATCH",()->sources.validateRealtimeTable(binding("DORIS",List.of(field("id","INT",false),field("payload","STRING",true))),"workspace"),"Unique Key");
        dorisDdl(ddl.replace("UNIQUE KEY(`id`)","DUPLICATE KEY(`id`)"));mismatch("TARGET_SCHEMA_MISMATCH",()->sources.validateRealtimeTable(binding("DORIS",List.of(field("id","INT",true))),"workspace"),"表模型不一致");
    }
    @Test void dorisMergeOnReadCannotAcceptDeletesButCanAcceptNonDeletingWrites() throws Exception {
        metadata(doris,List.of(column("id","int(11)","UNI")),List.of());dorisDdl("CREATE TABLE orders(id INT) ENGINE=OLAP UNIQUE KEY(`id`) PROPERTIES (\"enable_unique_key_merge_on_write\" = \"false\")");
        var target=binding("DORIS",List.of(field("id","INT",true)));mismatch("TARGET_SCHEMA_MISMATCH",()->sources.validateRealtimeTable(target,"workspace"),"Merge-on-Write");target.put("syncDeletes",false);assertDoesNotThrow(()->sources.validateRealtimeTable(target,"workspace"));
    }
    @Test void kafkaAndUnboundSqlRequireNoDatabaseSchemaMetadata(){sources.validateRealtimeTable(Map.of("connector","KAFKA"),"workspace");verify(sources,never()).forWorkspace(anyString(),anyString());verify(sources,never()).syncMetadata(any(DatasourceService.ConnectionSpec.class),anyString());}
}
