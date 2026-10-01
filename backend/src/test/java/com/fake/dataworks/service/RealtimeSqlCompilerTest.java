package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.exception.StudioException;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RealtimeSqlCompilerTest {
    DatasourceService sources;RealtimeSqlCompiler compiler;JsonCodec json=new JsonCodec();
    DatasourceService.ConnectionSpec mysql;
    @BeforeEach void init(){sources=mock(DatasourceService.class);compiler=new RealtimeSqlCompiler(sources,json);mysql=new DatasourceService.ConnectionSpec("mysql","workspace","db","127.0.0.1",3307,"business","reader","cipher","MYSQL",Map.of());
        when(sources.forWorkspace("mysql","workspace")).thenReturn(mysql);when(sources.credentials(mysql)).thenReturn(Map.of("username","reader","password","secret'quote"));}
    Map<String,Object> task(String sql,List<Map<String,Object>> bindings){return new LinkedHashMap<>(Map.of("id","task","name","task","workspaceId","workspace","sql",sql,"runtime",Map.of("parallelism",1,"checkpointSeconds",60,"restartAttempts",3,"restartDelaySeconds",10),"bindings",bindings));}
    Map<String,Object> binding(String role,String connector){var b=new LinkedHashMap<String,Object>();b.putAll(Map.of("id","binding-"+role,"role",role,"connector",connector,"datasourceId","mysql","tableName",role.equals("SOURCE")?"source":"sink","physicalTable","orders","fields",List.of(Map.of("id","field1","name","id","type","INT","nullable",false,"primaryKey",true))));b.put("writeMode","upsert");b.put("serverId","");b.put("timezone","UTC");b.put("cdcStartupMode","initial");b.put("eventTimeField","");b.put("watermarkSeconds",5);return b;}
    @Test void sqlLexerProtectsStringsCommentsAndStatementSet(){
        var result=RealtimeSqlCompiler.split("CREATE TABLE t (id INT) WITH ('connector'='print'); -- ; END\nEXECUTE STATEMENT SET BEGIN INSERT INTO t SELECT ';END'; INSERT INTO t SELECT 'it''s'; END;/*;*/");
        assertEquals(2,result.size());assertTrue(result.get(1).contains("SELECT ';END'"));assertTrue(result.get(1).contains("INSERT INTO t SELECT 'it''s'"));
        assertEquals(1,RealtimeSqlCompiler.split("BEGIN STATEMENT SET; INSERT INTO t SELECT 1; END;").size());
        assertEquals(1,RealtimeSqlCompiler.split("EXECUTE\nSTATEMENT  SET\nBEGIN INSERT INTO t SELECT 1; END;").size());
        assertEquals(2,RealtimeSqlCompiler.split("SELECT 'backslash\\'; SELECT 2;").size());
        assertTrue(RealtimeSqlCompiler.split("SELECT /*+ BROADCAST(t) */ * FROM t").getFirst().contains("BROADCAST(t)"));
        assertThrows(StudioException.class,()->RealtimeSqlCompiler.split("SELECT * FROM t /*+ OPTIONS('password'='secret') */"));
        for(String bad:List.of("SELECT 'unclosed","/*unclosed","EXECUTE STATEMENT SET BEGIN INSERT INTO t SELECT 1;","END;"))assertThrows(StudioException.class,()->RealtimeSqlCompiler.split(bad));
    }
    @Test void bindingsHydrateSecretsAndContainerAddressesAndCombineInserts(){
        var source=binding("SOURCE","MYSQL_CDC");var sink=binding("SINK","MYSQL_JDBC");var p=compiler.compile(task("INSERT INTO sink SELECT id FROM source; INSERT INTO sink SELECT id FROM source WHERE id=9",List.of(source,sink)),"attempt-one");
        assertEquals(2,p.statements().size());assertTrue(p.statements().getFirst().contains("'hostname' = 'mysql-rlt'"));assertTrue(p.statements().getFirst().contains("'port' = '3306'"));assertTrue(p.statements().getFirst().contains("'server-id'"));assertTrue(p.statements().getFirst().contains("secret''quote"));
        assertTrue(p.execution().startsWith("EXECUTE STATEMENT SET"));assertEquals(2,p.execution().split("INSERT INTO",-1).length-1);assertEquals("false",p.properties().get("execution.attached"));
        assertFalse(p.toString().contains("secret"));assertFalse(p.redact(String.join("\n",p.statements())).contains("secret"));assertFalse(json.write(p.datasourceBindings()).contains("cipher"));
        assertEquals(p.fingerprint(),compiler.compile(task("INSERT INTO sink SELECT id FROM source; INSERT INTO sink SELECT id FROM source WHERE id=9",List.of(source,sink)),"attempt-two").fingerprint());
        assertNotEquals(p.statements().getFirst(),compiler.compile(task("INSERT INTO sink SELECT id FROM source",List.of(source,sink)),"attempt-two").statements().getFirst());
        var explicit=new DatasourceService.ConnectionSpec("mysql","workspace","db","127.0.0.1",3307,"business","reader","cipher","MYSQL",Map.of("flinkPort",3308));when(sources.forWorkspace("mysql","workspace")).thenReturn(explicit);when(sources.credentials(explicit)).thenReturn(Map.of("username","reader","password","secret'quote"));
        assertTrue(compiler.compile(task("INSERT INTO sink SELECT id FROM source",List.of(source,sink)),"attempt-three").statements().getFirst().contains("'port' = '3308'"));
    }
    @Test void managedBlocksMustMatchCurrentBindingAndCannotDuplicateOrNest(){
        var sink=binding("SINK","MYSQL_JDBC");String ddl=compiler.previewDDL(sink,"workspace","task");String marked="-- @realtime-binding:binding-SINK:begin\n"+ddl+"\n-- @realtime-binding:binding-SINK:end\nINSERT INTO sink VALUES(1)";
        var p=compiler.compile(task(marked,List.of(sink)),"run");assertEquals(1,p.statements().size());assertTrue(p.statements().getFirst().contains("secret''quote"));
        assertEquals("REALTIME_DDL_STALE",assertThrows(StudioException.class,()->compiler.compile(task(marked.replace("orders","other"),List.of(sink)),"run")).code());
        assertThrows(StudioException.class,()->compiler.compile(task(marked+"\n"+marked,List.of(sink)),"run"));
        assertThrows(StudioException.class,()->compiler.compile(task(marked.replace("binding-SINK:end","other:end"),List.of(sink)),"run"));
        assertEquals("REALTIME_DDL_CONFLICT",assertThrows(StudioException.class,()->compiler.compile(task(ddl+"\nINSERT INTO sink VALUES(1)",List.of(sink)),"run")).code());
    }
    @Test void manualDatasourceDirectiveHydratesAndPreviewKeepsInitializationFromProduction(){
        String sql="CREATE TABLE t(id INT) WITH ('connector'='jdbc','studio.datasource-id'='mysql','table-name'='orders','url'='jdbc:mysql://wrong:3306/other'); CREATE VIEW v AS SELECT * FROM t; INSERT INTO t SELECT * FROM v;";
        var p=compiler.compilePreview(task(sql,List.of()),"preview-run","SELECT id FROM v WHERE id>0");assertEquals(2,p.statements().size());assertTrue(p.statements().getFirst().contains("jdbc:mysql://mysql-rlt:3306/business"));assertFalse(p.statements().getFirst().contains("studio.datasource-id"));assertEquals("EXPLAIN SELECT id FROM v WHERE id>0",p.explain());assertEquals("true",p.properties().get("execution.attached"));
        assertThrows(StudioException.class,()->compiler.compilePreview(task(sql,List.of()),"run","INSERT INTO t VALUES(1)"));
        assertEquals("REALTIME_INLINE_SECRET",assertThrows(StudioException.class,()->compiler.compile(task(sql.replace("'table-name'='orders'","'password'='leaked','table-name'='orders'"),List.of()),"run")).code());
        assertThrows(StudioException.class,()->compiler.compile(task(sql.replace("'studio.datasource-id'='mysql',",""),List.of()),"run"));
    }
    @Test void snapshotNumericTypesSurviveJsonRoundtripAndCredentialRotation(){
        var p=compiler.compilePreview(task("CREATE TABLE t(id INT) WITH ('connector'='jdbc','studio.datasource-id'='mysql','table-name'='orders');SELECT * FROM t",List.of()),"p");
        @SuppressWarnings("unchecked") var snapshots=(List<Map<String,Object>>)(List<?>)json.read(json.write(p.datasourceBindings()),List.class);assertDoesNotThrow(()->compiler.verifyDatasourceBindings("workspace",snapshots));
        var changed=new DatasourceService.ConnectionSpec("mysql","workspace","db","other",3307,"business","reader","rotated","MYSQL",Map.of());when(sources.forWorkspace("mysql","workspace")).thenReturn(changed);assertEquals("DATASOURCE_BINDING_CHANGED",assertThrows(StudioException.class,()->compiler.verifyDatasourceBindings("workspace",snapshots)).code());
    }
    @Test void reservedConfigInvalidTypesAndQueryPublicationAreRejected(){
        String ddl="CREATE TABLE sink(id INT) WITH('connector'='print');";
        for(String property:List.of("pipeline.fixed-job-id","execution.target","execution.state-recovery.path","pipeline.name","table.exec.uid.generation"))assertThrows(StudioException.class,()->compiler.compile(task("SET '"+property+"'='value';"+ddl+"INSERT INTO sink VALUES(1)",List.of()),"run"));
        var b=binding("SOURCE","MYSQL_CDC");b.put("serverId","100-100");var t=task("INSERT INTO sink VALUES(1)",List.of(b));t.put("runtime",Map.of("parallelism",2));assertThrows(StudioException.class,()->compiler.compile(t,"run"));
        assertThrows(StudioException.class,()->compiler.compile(task("SELECT 1",List.of()),"run"));assertThrows(StudioException.class,()->compiler.compilePreview(task("SELECT 1;SELECT 2",List.of()),"run"));
        var invalid=binding("SOURCE","MYSQL_CDC");invalid.put("fields",List.of(Map.of("name","id","type","INT); DROP TABLE x;--","nullable",false,"primaryKey",true)));assertThrows(StudioException.class,()->compiler.compile(task("INSERT INTO sink VALUES(1)",List.of(invalid)),"run"));
    }
    @Test void setupCannotSubmitCtasOrOverridePublishedConnectionWithAlter(){
        for(String sql:List.of("CREATE TABLE t(id INT) WITH('connector'='blackhole') AS SELECT 1", "CREATE TABLE t(id INT) AS VALUES (1)")){
            var error=assertThrows(StudioException.class,()->compiler.compilePreview(task(sql+";SELECT 1",List.of()),"preview"));assertEquals("REALTIME_CTAS_UNSUPPORTED",error.code());
        }
        for(String sql:List.of("ALTER TABLE sink SET ('connector'='jdbc','url'='jdbc:mysql://other:3306/db')", "ALTER TABLE sink RESET ('password')", "ALTER TABLE sink ADD id INT")){
            var error=assertThrows(StudioException.class,()->compiler.compile(task("CREATE TABLE sink(id INT) WITH('connector'='print');"+sql+";INSERT INTO sink VALUES(1)",List.of()),"run"));assertEquals("REALTIME_ALTER_TABLE_UNSUPPORTED",error.code());
        }
        assertDoesNotThrow(()->compiler.compilePreview(task("CREATE TABLE t(id INT, computed AS id+1) WITH('connector'='datagen');CREATE OR REPLACE VIEW v AS SELECT * FROM t;SELECT * FROM v",List.of()),"preview"));
    }
    @Test void explicitStatementSetsCannotBeMergedWithAnotherWriteSegment(){
        String ddl="CREATE TABLE sink(id INT) WITH('connector'='print');";String set="EXECUTE STATEMENT SET BEGIN INSERT INTO sink VALUES(1); INSERT INTO sink VALUES(2); END;";
        assertDoesNotThrow(()->compiler.compile(task(ddl+set,List.of()),"run"));
        assertDoesNotThrow(()->compiler.compile(task(ddl+set.replace("EXECUTE STATEMENT SET","EXECUTE\nSTATEMENT  SET"),List.of()),"run"));
        for(String writes:List.of(set+set,"INSERT INTO sink VALUES(0);"+set,set+"INSERT INTO sink VALUES(3);"))assertEquals("REALTIME_SCRIPT_WRITES",assertThrows(StudioException.class,()->compiler.compile(task(ddl+writes,List.of()),"run")).code());
        assertEquals(List.of("SELECT 1"),compiler.compilePreview(task(ddl+set+set,List.of()),"preview","SELECT 1").queries());
    }
    @Test void generatedPhysicalIdentityIgnoresInsertExpressionsCredentialsAndDynamicLabels(){
        var source=binding("SOURCE","MYSQL_CDC");var sink=binding("SINK","MYSQL_JDBC");var before=task("INSERT INTO sink SELECT id FROM source",List.of(source,sink));var signatures=compiler.physicalIdentities(before);
        assertEquals(Set.of("source","sink"),signatures.keySet());assertEquals(signatures,compiler.physicalIdentities(task("INSERT INTO sink SELECT id+1 FROM source",List.of(source,sink))));
        when(sources.credentials(mysql)).thenReturn(Map.of("username","reader","password","rotated-secret"));assertEquals(signatures,compiler.physicalIdentities(before));
        var changed=binding("SOURCE","MYSQL_CDC");changed.put("physicalTable","other_orders");assertNotEquals(signatures,compiler.physicalIdentities(task("INSERT INTO sink SELECT id FROM source",List.of(changed,sink))));
        changed=binding("SOURCE","MYSQL_CDC");changed.put("fields",List.of(Map.of("name","id","type","BIGINT","nullable",false,"primaryKey",true)));assertNotEquals(signatures,compiler.physicalIdentities(task("INSERT INTO sink SELECT id FROM source",List.of(changed,sink))));
        var doris=new DatasourceService.ConnectionSpec("doris","workspace","Doris","localhost",9030,"warehouse","writer","cipher","DORIS",Map.of("feHttpUrls",List.of("http://localhost:8030")));when(sources.forWorkspace("doris","workspace")).thenReturn(doris);when(sources.credentials(doris)).thenReturn(Map.of("username","writer","password","doris-secret"));
        var target=binding("SINK","DORIS");target.put("datasourceId","doris");target.put("dorisModel","UNIQUE");target.put("labelPrefix","first");var identity=compiler.physicalIdentities(task("INSERT INTO sink VALUES(1)",List.of(target)));var other=new LinkedHashMap<>(target);other.put("labelPrefix","other");assertEquals(identity,compiler.physicalIdentities(task("INSERT INTO sink VALUES(1)",List.of(other))));
    }
    @Test void handwrittenPhysicalIdentityProtectsTableTopicSchemaAndPrimaryKey(){
        String ddl="CREATE TABLE t(id INT NOT NULL) WITH('connector'='jdbc','studio.datasource-id'='mysql','table-name'='orders');";var before=compiler.physicalIdentities(task(ddl+"INSERT INTO t VALUES(1)",List.of()));
        assertEquals(before,compiler.physicalIdentities(task(ddl+"INSERT INTO t VALUES(2)",List.of())));assertNotEquals(before,compiler.physicalIdentities(task(ddl.replace("orders","other_orders")+"INSERT INTO t VALUES(1)",List.of())));
        assertNotEquals(before,compiler.physicalIdentities(task(ddl.replace("id INT NOT NULL","id BIGINT NOT NULL")+"INSERT INTO t VALUES(1)",List.of())));assertNotEquals(before,compiler.physicalIdentities(task(ddl.replace("id INT NOT NULL","id INT NOT NULL, PRIMARY KEY(id) NOT ENFORCED")+"INSERT INTO t VALUES(1)",List.of())));
        var kafka=new DatasourceService.ConnectionSpec("kafka","workspace","Kafka","",0,"","","","KAFKA",Map.of("bootstrapServers","localhost:19092","securityProtocol","PLAINTEXT","saslMechanism","PLAIN"));when(sources.forWorkspace("kafka","workspace")).thenReturn(kafka);when(sources.credentials(kafka)).thenReturn(Map.of("username","","password",""));
        String kafkaSql="CREATE TABLE k(id INT) WITH('connector'='kafka','studio.datasource-id'='kafka','topic'='topic_one','format'='json','properties.group.id'='group_one');CREATE TABLE p(id INT) WITH('connector'='print');INSERT INTO p SELECT id FROM k";
        var kafkaIdentity=compiler.physicalIdentities(task(kafkaSql,List.of()));for(String changed:List.of(kafkaSql.replace("topic_one","topic_two"),kafkaSql.replace("group_one","group_two"),kafkaSql.replace("'json'","'csv'")))assertNotEquals(kafkaIdentity,compiler.physicalIdentities(task(changed,List.of())));
    }
    @Test void everyPreviewIsolatesBoundAndHandwrittenKafkaAndExplicitCdcIdentities(){
        var kafka=new DatasourceService.ConnectionSpec("kafka","workspace","Kafka","",0,"","","","KAFKA",Map.of("bootstrapServers","localhost:19092","securityProtocol","PLAINTEXT","saslMechanism","PLAIN"));when(sources.forWorkspace("kafka","workspace")).thenReturn(kafka);when(sources.credentials(kafka)).thenReturn(Map.of("username","","password",""));
        var boundKafka=binding("SOURCE","KAFKA");boundKafka.put("id","bound-kafka");boundKafka.put("tableName","bound_kafka");boundKafka.put("datasourceId","kafka");boundKafka.put("topic","events");boundKafka.put("format","json");boundKafka.put("consumerGroup","production_group");boundKafka.put("startupMode","group-offsets");
        var boundCdc=binding("SOURCE","MYSQL_CDC");boundCdc.put("id","bound-cdc");boundCdc.put("tableName","bound_cdc");boundCdc.put("serverId","700-900");
        String sql="CREATE TABLE manual_k(id INT) WITH('connector'='kafka','studio.datasource-id'='kafka','topic'='events','format'='json','properties.group.id'='manual_production_group');CREATE TABLE manual_c(id INT NOT NULL, PRIMARY KEY(id) NOT ENFORCED) WITH('connector'='mysql-cdc','studio.datasource-id'='mysql','table-name'='orders','server-id'='123-128');CREATE TABLE output(id INT) WITH('connector'='print');INSERT INTO output SELECT id FROM bound_kafka";
        var original=task(sql,List.of(boundKafka,boundCdc));String saved=json.write(original);var first=compiler.compilePreview(original,"debug-one","SELECT * FROM manual_k");var second=compiler.compilePreview(original,"debug-two","SELECT * FROM manual_k");
        assertEquals(saved,json.write(original));assertEquals(first.fingerprint(),second.fingerprint());assertNotEquals(first.statements(),second.statements());
        var production=compiler.compile(original,"production");assertTrue(String.join("\n",production.statements()).contains("'properties.group.id' = 'production_group'"));assertTrue(String.join("\n",production.statements()).contains("'server-id' = '700-900'"));assertTrue(String.join("\n",production.statements()).contains("'server-id' = '123-128'"));
        for(var preview:List.of(first,second)){int kafkaTables=0,cdcTables=0;for(String ddl:preview.statements()){
            if(ddl.contains("'connector' = 'kafka'")){kafkaTables++;assertTrue(ddl.contains("'properties.group.id' = 'sinket_debug_"));assertFalse(ddl.contains("production_group"));}
            if(ddl.contains("'connector' = 'mysql-cdc'")){cdcTables++;var ids=java.util.regex.Pattern.compile("'server-id' = '(\\d+)-(\\d+)'").matcher(ddl);assertTrue(ids.find());assertTrue(Integer.parseInt(ids.group(1))>=1_200_000_000);assertTrue(Integer.parseInt(ids.group(2))<=Integer.MAX_VALUE);assertEquals(255,Long.parseLong(ids.group(2))-Long.parseLong(ids.group(1)));}
        }assertEquals(2,kafkaTables);assertEquals(2,cdcTables);}
    }
    @Test void cdcAllocationsAndExplicitRangesFitConnectorsSignedIntegerParser(){
        var source=binding("SOURCE","MYSQL_CDC");var original=task("CREATE TABLE output(id INT) WITH('connector'='print');INSERT INTO output SELECT id FROM source",List.of(source));
        for(String attempt:List.of("attempt-a","attempt-b","attempt-c","attempt-d","debug-one","debug-two")){
            var prepared=compiler.compile(original,attempt);var ddl=prepared.statements().stream().filter(value->value.contains("'connector' = 'mysql-cdc'")).findFirst().orElseThrow();var ids=java.util.regex.Pattern.compile("'server-id' = '(\\d+)-(\\d+)'").matcher(ddl);assertTrue(ids.find());assertTrue(Integer.parseInt(ids.group(1))>=100000);assertTrue(Integer.parseInt(ids.group(2))<1_200_000_000);
        }
        source.put("serverId","2147483648-2147483903");assertThrows(StudioException.class,()->compiler.compile(original,"run"));
        String manual="CREATE TABLE c(id INT NOT NULL, PRIMARY KEY(id) NOT ENFORCED) WITH('connector'='mysql-cdc','studio.datasource-id'='mysql','table-name'='orders','server-id'='3300000000-3300000255');CREATE TABLE output(id INT) WITH('connector'='print');INSERT INTO output SELECT id FROM c";
        assertThrows(StudioException.class,()->compiler.compile(task(manual,List.of()),"run"));
    }
    @Test void overlappingCdcRangesOnSameRuntimeServerAreRejectedAcrossDatasourceAndDatabaseNames(){
        var alias=new DatasourceService.ConnectionSpec("alias","workspace","other database","MYSQL-RLT",3306,"other_database","reader","cipher","MYSQL",Map.of());when(sources.forWorkspace("alias","workspace")).thenReturn(alias);when(sources.credentials(alias)).thenReturn(Map.of("username","reader","password","alias-secret"));
        var bound=binding("SOURCE","MYSQL_CDC");bound.put("serverId","15401-15656");String manual="CREATE TABLE other_source(id INT NOT NULL, PRIMARY KEY(id) NOT ENFORCED) WITH('connector'='mysql-cdc','studio.datasource-id'='alias','table-name'='other_orders','server-id'='15600-15855');CREATE TABLE output(id INT) WITH('connector'='print');INSERT INTO output SELECT id FROM source UNION ALL SELECT id FROM other_source";
        var failure=assertThrows(StudioException.class,()->compiler.compile(task(manual,List.of(bound)),"run"));assertEquals("REALTIME_CDC_SERVER_ID_CONFLICT",failure.code());assertTrue(failure.getMessage().contains("mysql-rlt:3306"));assertTrue(failure.getMessage().contains("source"));assertTrue(failure.getMessage().contains("other_source"));
        assertDoesNotThrow(()->compiler.compile(task(manual.replace("15600-15855","15657-15912"),List.of(bound)),"run"));
    }
    @Test void cdcAutoAllocationReservesAllExplicitRangesBeforeAllocatingAndRegistryUsesActualTargets(){
        String auto="CREATE TABLE automatic(id INT NOT NULL, PRIMARY KEY(id) NOT ENFORCED) WITH('connector'='mysql-cdc','studio.datasource-id'='mysql','table-name'='orders');";String output="CREATE TABLE output(id INT) WITH('connector'='print');INSERT INTO output SELECT id FROM automatic";
        var original=compiler.compile(task(auto+output,List.of()),"stable-attempt");var identity=compiler.cdcServerIds(original).getFirst();long first=(Long)identity.get("first"),last=(Long)identity.get("last");String explicit="CREATE TABLE explicit_source(id INT NOT NULL, PRIMARY KEY(id) NOT ENFORCED) WITH('connector'='mysql-cdc','studio.datasource-id'='mysql','table-name'='other_orders','server-id'='"+first+"-"+last+"');";
        var moved=compiler.cdcServerIds(compiler.compile(task(auto+explicit+output,List.of()),"stable-attempt"));assertEquals(2,moved.size());assertEquals("mysql-rlt:3306",moved.getFirst().get("endpoint"));assertEquals("automatic",moved.getFirst().get("tableName"));assertInstanceOf(Long.class,moved.getFirst().get("first"));assertTrue((Long)moved.getFirst().get("last")<first||(Long)moved.getFirst().get("first")>last);assertEquals(first,moved.get(1).get("first"));assertFalse(moved.getFirst().containsKey("datasourceId"));assertFalse(moved.getFirst().containsKey("database"));
    }
    @Test void previewAllocationsAvoidEveryProductionTableRangeAndOneAnother(){
        String first="CREATE TABLE wide_source(id INT NOT NULL, PRIMARY KEY(id) NOT ENFORCED) WITH('connector'='mysql-cdc','studio.datasource-id'='mysql','table-name'='orders','server-id'='1200000000-2147483647');";
        String second="CREATE TABLE other_source(id INT NOT NULL, PRIMARY KEY(id) NOT ENFORCED) WITH('connector'='mysql-cdc','studio.datasource-id'='mysql','table-name'='other_orders','server-id'='15401-15656');";
        var preview=compiler.compilePreview(task(first+second+"SELECT id FROM wide_source",List.of()),"isolated-attempt");var ranges=compiler.cdcServerIds(preview);assertEquals(2,ranges.size());for(var range:ranges){long start=(Long)range.get("first"),end=(Long)range.get("last");assertTrue(start>0&&end<1_200_000_000L);assertTrue(end<15401||start>15656);assertEquals(255,end-start);}
        assertTrue((Long)ranges.get(0).get("last")<(Long)ranges.get(1).get("first")||(Long)ranges.get(1).get("last")<(Long)ranges.get(0).get("first"));
    }
}
