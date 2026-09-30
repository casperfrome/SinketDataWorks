package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.StudioRepository;
import io.github.casperfrome.dunnelean.*;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SyncPartitionTest {
    private DatasourceService sources;private DunneleanClient client;private StudioRepository repo;private JdbcTemplate jdbc;private TransactionTemplate tx;private SyncExecutionService sync;
    private final DatasourceService.ConnectionSpec mysql=new DatasourceService.ConnectionSpec("mysql","workspace","MySQL","127.0.0.1",3307,"mysql_db","reader","cipher","MYSQL",Map.of());
    private final DatasourceService.ConnectionSpec doris=new DatasourceService.ConnectionSpec("doris","workspace","Doris","127.0.0.1",9030,"test_ods","writer","cipher","DORIS",Map.of("feHttpUrls",List.of("http://127.0.0.1:8030"),"beHttpUrls",List.of("http://127.0.0.1:8040"),"flightUri","grpc://127.0.0.1:8070","flightEndpointMap",Map.of(),"httpEndpointMap",Map.of()));
    private final Map<String,Object> sourceMetadata=Map.of("columns",List.of(Map.of("name","id","type","bigint"),Map.of("name","ordered_at","type","datetime")));
    @BeforeEach void setup(){sources=mock(DatasourceService.class);client=mock(DunneleanClient.class);repo=mock(StudioRepository.class);jdbc=mock(JdbcTemplate.class);tx=mock(TransactionTemplate.class);sync=new SyncExecutionService(repo,sources,client,new SqlGuard(),jdbc,tx,new JsonCodec(),false);when(sources.credentials(any())).thenReturn(Map.of("username","business","password","private-password-sentinel"));}
    @AfterEach void close(){sync.close();}
    private Map<String,Object> target(boolean automatic,boolean existing){
        var partition=new SyncPartitionMetadata("RANGE",automatic,automatic?"date_trunc(`ds`, 'day')":"`ds`",List.of(new SyncPartitionMetadata.Column("ds","date")),existing?List.of(new SyncPartitionMetadata.Partition("real_partition_name","bounds","2026-09-29","2026-09-30",List.of())):List.of());
        return Map.of("model","DUPLICATE","columns",List.of(Map.of("name","ds","type","date"),Map.of("name","id","type","bigint"),Map.of("name","ordered_at","type","datetime")),"partition",partition.view());
    }
    private Map<String,Object> config(String mode){var c=new LinkedHashMap<String,Object>();c.put("sourceTable","orders");c.put("targetTable","ods_orders_di");c.put("writeMode",mode);c.put("targetPartitionAssignments",List.of(Map.of("target","ds","mode","value","value","${bizdate}")));return c;}
    private Map<String,Object> run(){var r=new LinkedHashMap<String,Object>();r.put("id","local");r.put("status","QUEUED");r.put("writeMode","overwrite");r.put("createdAt",Instant.now().toString());r.put("scheduleParameters",Map.of("bizdate","20260929","region","Chinese' OR 1=1"));r.put("parameters",Map.of("bizdate","2026-09-28"));return r;}
    private SyncExecutionService.Prepared prepared(Map<String,Object> config,Map<String,Object> target){return new SyncExecutionService.Prepared(null,mysql,doris,config,"target-key","DUPLICATE",sourceMetadata,target);}
    @Test void constantDateProjectionBindsCustomParameterAndAddsGeneratedMapping(){
        var c=config("append");c.put("where","id > 0 AND name = '${region}'");var r=run();var spec=sync.spec(prepared(c,target(true,false)),r);var reader=assertInstanceOf(MysqlReader.class,spec.reader());var writer=assertInstanceOf(DorisWriter.class,spec.writer());
        assertNull(reader.source().table());assertNull(reader.source().predicate());assertEquals(List.of(),reader.source().columns());assertTrue(reader.source().query().contains("CAST(? AS DATE) AS `__sync_partition_0`"));
        assertEquals(List.of("2026-09-29","Chinese' OR 1=1"),reader.source().params().stream().map(p->assertInstanceOf(Parameter.StringValue.class,p).value()).toList());
        assertEquals("ds",spec.mapping().getLast().target());assertEquals("__sync_partition_0",spec.mapping().getLast().source());assertTrue(writer.partitions().isEmpty());assertTrue(writer.options().preSql().isEmpty());
        assertEquals(List.of(Map.of("target","ds","value","2026-09-29")),r.get("resolvedPartitionAssignments"));
    }
    @Test void overwriteExistingDateClearsOnlyItsRealPartitionAndNewDateSkipsClear(){
        var c=config("overwrite");var oldRun=run();var existing=assertInstanceOf(DorisWriter.class,sync.spec(prepared(c,target(true,true)),oldRun).writer());
        assertEquals(List.of("TRUNCATE TABLE `test_ods`.`ods_orders_di` PARTITION (`real_partition_name`)"),existing.options().preSql());assertTrue(existing.partitions().isEmpty());assertEquals(List.of("real_partition_name"),oldRun.get("resolvedTargetPartitions"));
        var newRun=run();var missing=assertInstanceOf(DorisWriter.class,sync.spec(prepared(c,target(true,false)),newRun).writer());assertTrue(missing.options().preSql().isEmpty());assertTrue(missing.partitions().isEmpty());assertEquals("SKIPPED_NO_PARTITION",newRun.get("clearStatus"));
    }
    @Test void sourceFieldRoutingCastsDatesAndNeedsExplicitScopeForOverwrite(){
        var c=config("append");c.put("targetPartitionAssignments",List.of(Map.of("target","ds","mode","column","source","ordered_at")));var spec=sync.spec(prepared(c,target(true,true)),run());
        assertTrue(assertInstanceOf(MysqlReader.class,spec.reader()).source().query().contains("CAST(`ordered_at` AS DATE)"));
        c.put("writeMode","overwrite");assertEquals("SYNC_PARTITION_SELECTION_REQUIRED",assertThrows(StudioException.class,()->sync.spec(prepared(c,target(true,true)),run())).code());
        c.put("targetPartitions",List.of("real_partition_name"));var writer=assertInstanceOf(DorisWriter.class,sync.spec(prepared(c,target(true,true)),run()).writer());assertEquals(List.of("real_partition_name"),writer.partitions());assertTrue(writer.options().preSql().getFirst().endsWith("PARTITION (`real_partition_name`)"));
    }
    @Test void manualMissingPartitionAndUnknownExplicitPartitionFailBeforeSubmission(){
        assertEquals("SYNC_PARTITION_NOT_FOUND",assertThrows(StudioException.class,()->sync.spec(prepared(config("overwrite"),target(false,false)),run())).code());
        var c=config("append");c.put("targetPartitions",List.of("p_guessed"));assertEquals("SYNC_PARTITION_NOT_FOUND",assertThrows(StudioException.class,()->sync.spec(prepared(c,target(true,true)),run())).code());verify(client,never()).submit(anyString(),any(),any());
    }
    @Test void partitionedOverwriteCannotFallBackToClearingTheEntireTable(){
        var c=config("overwrite");c.remove("targetPartitionAssignments");assertEquals("SYNC_PARTITION_SELECTION_REQUIRED",assertThrows(StudioException.class,()->sync.spec(prepared(c,target(true,true)),run())).code());
        c.put("targetPartitions",List.of("real_partition_name"));var writer=assertInstanceOf(DorisWriter.class,sync.spec(prepared(c,target(true,true)),run()).writer());assertEquals(List.of("TRUNCATE TABLE `test_ods`.`ods_orders_di` PARTITION (`real_partition_name`)"),writer.options().preSql());
    }
    @Test void dorisSourceCombinesPartitionSelectionPartitionFilterAndDataFilter(){
        var c=new LinkedHashMap<String,Object>();c.put("sourceTable","ods_orders_di");c.put("targetTable","returned_orders");c.put("where","id > 0");c.put("sourcePartitionFilter",Map.of("partitions",List.of("real_partition_name"),"where","ds = '${day}'"));
        var r=run();r.put("scheduleParameters",Map.of("day","2026-09-29"));var p=new SyncExecutionService.Prepared(null,doris,mysql,c,"key","INNODB",target(true,true),sourceMetadata);var spec=sync.spec(p,r);var reader=assertInstanceOf(DorisReader.class,spec.reader());
        assertTrue(reader.source().query().contains("PARTITION (`real_partition_name`)"));assertTrue(reader.source().query().contains("FROM_BASE64('"));assertTrue(reader.source().query().contains("AND (id > 0)"));assertTrue(reader.source().params().isEmpty());
        c.put("sourcePartitionFilter",Map.of("where","id > 0"));assertEquals("INVALID_SYNC_PARTITION_FILTER",assertThrows(StudioException.class,()->sync.spec(p,r)).code());
        c.put("sourcePartitionFilter",Map.of("where","ds IN (SELECT ds FROM ods_orders_di)"));assertEquals("INVALID_SYNC_FILTER",assertThrows(StudioException.class,()->sync.spec(p,r)).code());
    }
    @Test void preflightScansAssignmentParametersAndPreservesCustomValues(){
        var c=config("append");c.put("sourceDataSourceId","mysql");c.put("targetDataSourceId","doris");var snapshot=new StudioObject("object","workspace",null,"NODE","数据集成","sync","","",Map.of("run",Map.of("provider","SYNC"),"sync",c,"schedule",Map.of("parameters",List.of(Map.of("name","bizdate","value","20260929","source","MANUAL")))),List.of(),false,false,1,"local_admin",Instant.now().toString());
        when(sources.forWorkspace("mysql","workspace")).thenReturn(mysql);when(sources.forWorkspace("doris","workspace")).thenReturn(doris);when(sources.columns(mysql,"orders")).thenReturn((List<Map<String,Object>>)sourceMetadata.get("columns"));when(sources.syncMetadata(doris,"ods_orders_di")).thenReturn(target(true,false));
        sync.prepare(snapshot);var captor=ArgumentCaptor.forClass(RunSpec.class);verify(client).validate(captor.capture());var reader=assertInstanceOf(MysqlReader.class,captor.getValue().reader());assertEquals("2026-09-29",assertInstanceOf(Parameter.StringValue.class,reader.source().params().getFirst()).value());
        assertEquals(List.of("bizdate"),SqlParameters.extract(SyncExecutionService.parameterCode(snapshot)));verify(jdbc,never()).update(anyString(),any(),any());
    }
    @Test @SuppressWarnings("unchecked") void debugPreflightUsesFrozenOverridesAndCutoffWithoutSavedDefinitions(){
        var c=config("append");c.put("sourceDataSourceId","mysql");c.put("targetDataSourceId","doris");c.put("where","ordered_at < :source_cutoff AND id = '${region}'");
        var snapshot=new StudioObject("object","workspace",null,"NODE","数据集成","sync","","",Map.of("run",Map.of("provider","SYNC"),"sync",c),List.of(),false,false,1,"local_admin",Instant.now().toString());
        when(sources.forWorkspace("mysql","workspace")).thenReturn(mysql);when(sources.forWorkspace("doris","workspace")).thenReturn(doris);when(sources.columns(mysql,"orders")).thenReturn((List<Map<String,Object>>)sourceMetadata.get("columns"));when(sources.syncMetadata(doris,"ods_orders_di")).thenReturn(target(true,false));
        String literal="literal $bizdate O'Reilly = 中文";var options=Map.<String,Object>of("businessDate","2026-09-28","scheduledAt","2026-09-29T02:00:00Z","timezone","UTC","sourceCutoffAt","2026-09-29T01:02:03.123456Z","scheduleParameters",Map.of("bizdate","20260929","region",literal));
        var prepared=sync.prepare(snapshot,List.of(),options);var captured=ArgumentCaptor.forClass(RunSpec.class);verify(client).validate(captured.capture());
        var validated=assertInstanceOf(MysqlReader.class,captured.getValue().reader());
        assertEquals(List.of("2026-09-29","2026-09-29 01:02:03.123456",literal),validated.source().params().stream().map(p->assertInstanceOf(Parameter.StringValue.class,p).value()).toList());
        var run=sync.newRun(prepared,"MANUAL");ScheduleParameters.attach(run,snapshot,options,List.of());run.put("parameters",Map.of("bizdate",run.get("businessDate"),"source_cutoff",options.get("sourceCutoffAt"),"build_id",run.get("id")));
        var execution=assertInstanceOf(MysqlReader.class,sync.spec(prepared,run).reader());assertEquals(validated.source().params(),execution.source().params());
        verify(client,never()).submit(anyString(),any(),any());verify(jdbc,never()).update(anyString(),any(),any());
    }
    @Test @SuppressWarnings("unchecked") void targetPartitionScopeIsRefreshedAfterLockAndFrozenForResubmission(){
        var c=config("overwrite");var r=run();var p=prepared(c,target(true,false));var state=new LinkedHashMap<String,Object>(Map.of("submitted",false,"cancel_requested",false,"service_url","http://127.0.0.1:9876"));
        when(repo.run("local")).thenReturn(Optional.of(r));when(jdbc.queryForMap(anyString(),eq("local"))).thenReturn(state);when(tx.execute(any())).thenReturn(true);when(client.health(anyString())).thenReturn(Map.of("state_store_id","store"));when(sources.syncMetadata(doris,"ods_orders_di")).thenReturn(target(true,true));
        var changed=new DatasourceService.ConnectionSpec("doris","workspace","Doris","changed-host",9030,"changed_schema","writer","cipher","DORIS",doris.options());when(sources.get("doris")).thenReturn(changed);when(sources.syncMetadata("doris","ods_orders_di")).thenReturn(target(true,false));
        ((Map<String,SyncExecutionService.Prepared>)ReflectionTestUtils.getField(sync,"prepared")).put("local",p);
        when(client.submit(anyString(),any(),eq("store"))).thenThrow(new StudioException("SYNC_UNAVAILABLE","unavailable",502));ReflectionTestUtils.invokeMethod(sync,"advance","local");
        var captor=ArgumentCaptor.forClass(RunSpec.class);verify(client).submit(anyString(),captor.capture(),eq("store"));assertTrue(assertInstanceOf(DorisWriter.class,captor.getValue().writer()).options().preSql().getFirst().contains("real_partition_name"));
        assertTrue(assertInstanceOf(DorisWriter.class,captor.getValue().writer()).options().preSql().getFirst().contains("`test_ods`"));verify(sources,never()).get(anyString());verify(sources,never()).syncMetadata(anyString(),anyString());
        state.put("submitted",true);state.put("state_store_id","store");when(client.getRequest(anyString(),eq("local"),eq("store"))).thenThrow(new StudioException("SYNC_NOT_FOUND","missing",404));ReflectionTestUtils.invokeMethod(sync,"advance","local");verify(sources,times(1)).syncMetadata(same(doris),eq("ods_orders_di"));verify(client,times(2)).submit(anyString(),same(captor.getValue()),eq("store"));
    }
    @Test void lockRefreshRejectsRebuiltUnsupportedOrChangedTargetModels(){
        var p=prepared(config("overwrite"),target(true,false));var unsupported=new LinkedHashMap<>(target(true,true));unsupported.put("model","UNSUPPORTED");when(sources.syncMetadata(doris,"ods_orders_di")).thenReturn(unsupported);
        assertEquals("SYNC_TARGET_MODEL",assertThrows(StudioException.class,()->ReflectionTestUtils.invokeMethod(sync,"spec",p,run(),true)).code());
        var changed=new LinkedHashMap<>(target(true,true));changed.put("model","UNIQUE_MOW");when(sources.syncMetadata(doris,"ods_orders_di")).thenReturn(changed);
        assertEquals("SYNC_TARGET_MODEL_CHANGED",assertThrows(StudioException.class,()->ReflectionTestUtils.invokeMethod(sync,"spec",p,run(),true)).code());verify(client,never()).submit(anyString(),any(),any());
    }
    @SuppressWarnings("unchecked") private void complete(Map<String,Object> r,SyncExecutionService.Prepared p,int writtenRows){
        when(repo.run("local")).thenReturn(Optional.of(r));when(repo.transitionRun(anyMap(),anyString())).thenReturn(true);
        when(jdbc.queryForMap(anyString(),eq("local"))).thenReturn(Map.of("submitted",true,"cancel_requested",false,"service_url","http://127.0.0.1:9876","state_store_id","store"));
        doAnswer(i->{Consumer<TransactionStatus> callback=i.getArgument(0);callback.accept(mock(TransactionStatus.class));return null;}).when(tx).executeWithoutResult(any());
        if(p!=null)((Map<String,SyncExecutionService.Prepared>)ReflectionTestUtils.getField(sync,"prepared")).put("local",p);
        when(client.getRequest(anyString(),eq("local"),eq("store"))).thenReturn(Map.of("run",Map.of("run_id","remote","state","SUCCEEDED","stage","complete","rows_committed",writtenRows,"commit_unknown",false)));
        ReflectionTestUtils.invokeMethod(sync,"advance","local");
    }
    @Test void completedFixedAutoWriteDisplaysCreatedPartitionWithoutChangingFrozenClearScope(){
        var p=prepared(config("overwrite"),target(true,false));var r=run();sync.spec(p,r);Object scope=r.get("clearScope");assertEquals(List.of(),r.get("resolvedTargetPartitions"));
        when(sources.syncMetadata(same(doris),eq("ods_orders_di"))).thenReturn(target(true,true));
        complete(r,p,1);
        assertEquals("SUCCESS",r.get("status"));assertEquals(List.of("real_partition_name"),r.get("resolvedTargetPartitions"));assertEquals(scope,r.get("clearScope"));assertEquals("SKIPPED_NO_PARTITION",r.get("clearStatus"));
        verify(sources).syncMetadata(same(doris),eq("ods_orders_di"));verify(sources,never()).get(anyString());verify(jdbc).update("DELETE FROM dw_sync_target_lock WHERE run_id=?","local");
    }
    @Test void completedWriteMetadataReadFailureCannotTurnSuccessIntoRecovering(){
        var p=prepared(config("overwrite"),target(true,false));var r=run();sync.spec(p,r);Object scope=r.get("clearScope");when(sources.syncMetadata(same(doris),eq("ods_orders_di"))).thenThrow(new StudioException("DATASOURCE_UNAVAILABLE","metadata unavailable",502));
        complete(r,p,1);
        assertEquals("SUCCESS",r.get("status"));assertEquals(List.of(),r.get("resolvedTargetPartitions"));assertEquals(scope,r.get("clearScope"));assertFalse(r.containsKey("errorCode"));verify(jdbc).update("DELETE FROM dw_sync_target_lock WHERE run_id=?","local");
    }
    @Test void emptyWritesDynamicRoutingAndRestartedRunsDoNotInventTouchedPartitions(){
        var p=prepared(config("overwrite"),target(true,false));var empty=run();sync.spec(p,empty);complete(empty,p,0);assertEquals(List.of(),empty.get("resolvedTargetPartitions"));assertEquals("SUCCESS",empty.get("status"));
        var c=config("append");c.put("targetPartitionAssignments",List.of(Map.of("target","ds","mode","column","source","ordered_at")));var dynamic=run();dynamic.put("writeMode","append");var routed=prepared(c,target(true,true));sync.spec(routed,dynamic);complete(dynamic,routed,5);assertEquals(List.of(),dynamic.get("resolvedTargetPartitions"));assertEquals("SUCCESS",dynamic.get("status"));
        var recovered=run();recovered.put("resolvedTargetPartitions",List.of("previously_frozen_partition"));complete(recovered,null,5);assertEquals(List.of("previously_frozen_partition"),recovered.get("resolvedTargetPartitions"));assertEquals("SUCCESS",recovered.get("status"));verify(sources,never()).syncMetadata(any(DatasourceService.ConnectionSpec.class),anyString());
    }
    private Map<String,Object> compositeTarget(List<Map<String,Object>> partitions){
        var columns=List.<Map<String,Object>>of(Map.of("name","ds","type","date"),Map.of("name","zone","type","varchar(20)"),Map.of("name","id","type","bigint"),Map.of("name","ordered_at","type","datetime"));
        return Map.of("model","DUPLICATE","columns",columns,"partition",SyncPartitionMetadata.parse("AUTO PARTITION BY LIST(zone,ds) ()",columns,partitions).view());
    }
    private Map<String,Object> compositeConfig(String mode){var c=config(mode);c.put("targetPartitionAssignments",List.of(Map.of("target","ds","mode","value","value","${bizdate}"),Map.of("target","zone","mode","value","value","east")));return c;}
    @Test void emptyCompositeAutoAppendProjectsAllFixedKeysWithoutPhysicalHeader(){
        var r=run();r.put("writeMode","append");var spec=sync.spec(prepared(compositeConfig("append"),compositeTarget(List.of())),r);var reader=assertInstanceOf(MysqlReader.class,spec.reader());var writer=assertInstanceOf(DorisWriter.class,spec.writer());
        assertTrue(reader.source().query().contains("CAST(? AS DATE)"));assertTrue(reader.source().query().contains("CAST(? AS CHAR)"));assertEquals(List.of("2026-09-29","east"),reader.source().params().stream().map(p->assertInstanceOf(Parameter.StringValue.class,p).value()).toList());assertTrue(writer.partitions().isEmpty());assertTrue(writer.options().preSql().isEmpty());assertEquals(List.of(),r.get("resolvedTargetPartitions"));
    }
    @Test void compositeOverwriteClearsMatchingTupleAndSupportsExplicitPartitionSelection(){
        var target=compositeTarget(List.of(Map.of("PartitionName","east_real","Range","[types: [VARCHAR, DATEV2]; keys: [east, 2026-09-29]; ]"),Map.of("PartitionName","west_real","Range","[types: [VARCHAR, DATEV2]; keys: [west, 2026-09-29]; ]")));var c=compositeConfig("overwrite");var r=run();var writer=assertInstanceOf(DorisWriter.class,sync.spec(prepared(c,target),r).writer());
        assertEquals(List.of("east_real"),r.get("resolvedTargetPartitions"));assertEquals(List.of("TRUNCATE TABLE `test_ods`.`ods_orders_di` PARTITION (`east_real`)"),writer.options().preSql());assertTrue(writer.partitions().isEmpty());
        c.put("targetPartitions",List.of("east_real"));writer=assertInstanceOf(DorisWriter.class,sync.spec(prepared(c,target),run()).writer());assertEquals(List.of("east_real"),writer.partitions());assertTrue(writer.options().preSql().getFirst().endsWith("PARTITION (`east_real`)"));
        c.put("targetPartitions",List.of("west_real"));assertEquals("INVALID_SYNC_PARTITION",assertThrows(StudioException.class,()->sync.spec(prepared(c,target),run())).code());
    }
    @Test void uncertainCompositeMetadataAllowsAutoAppendButCannotAuthorizeOverwrite(){
        var target=compositeTarget(List.of(Map.of("PartitionName","unknown","Range","[types: [VARCHAR, DATEV2]; keys: [east, north, 2026-09-29]; ]")));var c=compositeConfig("append");var writer=assertInstanceOf(DorisWriter.class,sync.spec(prepared(c,target),run()).writer());assertTrue(writer.partitions().isEmpty());assertTrue(writer.options().preSql().isEmpty());
        c.put("writeMode","overwrite");assertEquals("SYNC_PARTITION_METADATA",assertThrows(StudioException.class,()->sync.spec(prepared(c,target),run())).code());c.put("targetPartitions",List.of("unknown"));assertEquals("SYNC_PARTITION_METADATA",assertThrows(StudioException.class,()->sync.spec(prepared(c,target),run())).code());
    }
}
