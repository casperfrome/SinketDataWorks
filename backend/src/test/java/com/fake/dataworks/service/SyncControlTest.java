package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.StudioRepository;
import io.github.casperfrome.dunnelean.DorisReader;
import io.github.casperfrome.dunnelean.DorisWriteMode;
import io.github.casperfrome.dunnelean.DorisWriter;
import io.github.casperfrome.dunnelean.MysqlReader;
import io.github.casperfrome.dunnelean.MysqlWriteMode;
import io.github.casperfrome.dunnelean.MysqlWriter;
import io.github.casperfrome.dunnelean.Parameter;
import io.github.casperfrome.dunnelean.RunSpec;
import io.github.casperfrome.dunnelean.RunSpecJson;
import java.math.BigInteger;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.support.TransactionCallback;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Control failures must never release a target until the remote outcome is known. */
class SyncControlTest {
    StudioRepository repo; DunneleanClient client; JdbcTemplate jdbc; SyncExecutionService sync; DatasourceService sources;
    Map<String,Object> run,state;
    @BeforeEach void setup() {
        repo=mock(StudioRepository.class);client=mock(DunneleanClient.class);jdbc=mock(JdbcTemplate.class);sources=mock(DatasourceService.class);
        var tx=mock(TransactionTemplate.class);
        doAnswer(i->{Consumer<TransactionStatus> callback=i.getArgument(0);callback.accept(mock(TransactionStatus.class));return null;}).when(tx).executeWithoutResult(any());
        sync=new SyncExecutionService(repo,sources,client,new SqlGuard(),jdbc,tx,new JsonCodec(),false);
        run=new LinkedHashMap<>(Map.of("id","local","status","RUNNING","createdAt",Instant.now().toString(),"writeMode","overwrite"));
        state=new LinkedHashMap<>(Map.of("submitted",true,"cancel_requested",false,"service_url","http://127.0.0.1:9876","state_store_id","original-store"));
        when(repo.run("local")).thenReturn(Optional.of(run));when(repo.transitionRun(anyMap(),anyString())).thenReturn(true);
        when(jdbc.queryForMap(anyString(),eq("local"))).thenReturn(state);
    }
    @AfterEach void close(){sync.close();}
    void tick(){ReflectionTestUtils.invokeMethod(sync,"advance","local");}
    Map<String,Object> remote(String status,boolean unknown){return Map.of("run_id","remote","state",status,"stage","transfer","commit_unknown",unknown,"rows_committed",100,"partial_write",true);}
    void held(){verify(jdbc,never()).update(eq("DELETE FROM dw_sync_target_lock WHERE run_id=?"),eq("local"));}
    @Test void unavailableOrReplacedStoreKeepsTargetOccupied() {
        for(String code:List.of("SYNC_UNAVAILABLE","SYNC_STATE_STORE_CHANGED")) {
            when(client.getRequest(anyString(),eq("local"),eq("original-store"))).thenThrow(new StudioException(code,"unconfirmed",502));
            tick();assertEquals("RECOVERING",run.get("status"));assertEquals(code,run.get("errorCode"));held();
        }
    }
    @Test void cancelledBeforeRemoteAcceptanceUsesDurableTombstone() {
        state.put("cancel_requested",true);
        when(client.cancelRequest(anyString(),eq("local"),eq("original-store"))).thenReturn(Map.of("cancel_requested",true));
        tick();assertEquals("CANCELLED",run.get("status"));
        verify(jdbc).update("DELETE FROM dw_sync_target_lock WHERE run_id=?","local");
        verify(client,never()).submit(anyString(),any(),any());
    }
    @Test void unknownCommitCannotBeClearedByRepeatedPollingOrCancellation() {
        when(client.getRequest(anyString(),eq("local"),eq("original-store"))).thenReturn(Map.of("run",remote("FAILED",true)));
        when(client.cancelRequest(anyString(),eq("local"),eq("original-store"))).thenReturn(Map.of("run",remote("FAILED",true)));
        tick();assertEquals("RECOVERING",run.get("status"));assertEquals(100,run.get("writtenRows"));assertEquals("CLEARED",run.get("clearStatus"));held();
        state.put("cancel_requested",true);tick();assertEquals("RECOVERING",run.get("status"));held();
        sync.resolve("local","已核对数据库与批次，确认远端已停止");
        assertEquals("FAILED",run.get("status"));assertEquals("SYNC_MANUALLY_RESOLVED",run.get("errorCode"));assertNotNull(run.get("resolution"));
        verify(jdbc).update("DELETE FROM dw_sync_target_lock WHERE run_id=?","local");
    }
    @Test void manualResolutionRefusesActiveRemoteWriter() {
        run.put("status","RECOVERING");
        when(client.getRequest(anyString(),eq("local"),eq("original-store"))).thenReturn(Map.of("run",remote("RUNNING",false)));
        assertEquals("SYNC_STILL_ACTIVE",assertThrows(StudioException.class,()->sync.resolve("local","已检查但远端仍运行")).code());held();
    }
    @Test void missingRequestAfterRestartIsNotResubmitted() {
        when(client.getRequest(anyString(),eq("local"),eq("original-store"))).thenThrow(new StudioException("SYNC_NOT_FOUND","missing",404));
        tick();assertEquals("RECOVERING",run.get("status"));assertEquals("SYNC_REQUEST_MISSING",run.get("errorCode"));held();
        verify(client,never()).submit(anyString(),any(),any());
    }
    @Test @SuppressWarnings("unchecked") void uncertainSubmitReusesSameRequestAndFrozenSpec() throws Exception {
        var original=RunSpecJson.fromJson("""
                {"request_id":"local","reader":{"type":"mysql","connection":{"host":"127.0.0.1","database":"source_db","credentials":{"username":"reader","password":"fixture-password"}},"source":{"table":"source"}},"writer":{"type":"doris","sql":{"host":"127.0.0.1","database":"target_db","credentials":{"username":"writer","password":"fixture-password"}},"fe_http_urls":["http://127.0.0.1:8030"],"table":"target"}}
                """);
        ((Map<String,RunSpec>)ReflectionTestUtils.getField(sync,"specs")).put("local",original);
        when(client.getRequest(anyString(),eq("local"),eq("original-store"))).thenThrow(new StudioException("SYNC_NOT_FOUND","missing",404));
        when(client.submit(anyString(),same(original),eq("original-store"))).thenReturn(remote("SUCCEEDED",false));
        tick();assertEquals("SUCCESS",run.get("status"));
        verify(client).submit("http://127.0.0.1:9876",original,"original-store");
        verify(jdbc).update("DELETE FROM dw_sync_target_lock WHERE run_id=?","local");
    }
    @Test void pollingAndBatchReceiptsKeepPersistedEndpointAndStateIdentity() {
        state.put("service_url","http://127.0.0.1:9988");state.put("remote_run_id","saved-remote");
        when(client.getRequest("http://127.0.0.1:9988","local","original-store")).thenReturn(Map.of("run",remote("RUNNING",false)));
        var batches=Map.<String,Object>of("batches",List.of(Map.of("batch_id","batch-1","state","COMMITTED")));
        when(client.listBatches("http://127.0.0.1:9988","remote","original-store")).thenReturn(batches);
        tick();assertEquals("RUNNING",run.get("status"));held();
        // The remote ID persisted by acceptRemote is used for subsequent receipt lookup.
        state.put("remote_run_id","remote");assertSame(batches,sync.batches("local"));
        verify(client).getRequest("http://127.0.0.1:9988","local","original-store");
        verify(client).listBatches("http://127.0.0.1:9988","remote","original-store");
    }
    @Test void unavailableCancellationKeepsTargetOccupied() {
        state.put("cancel_requested",true);
        when(client.cancelRequest("http://127.0.0.1:9876","local","original-store")).thenThrow(new StudioException("SYNC_UNAVAILABLE","unconfirmed",502));
        tick();assertEquals("RECOVERING",run.get("status"));assertEquals("SYNC_UNAVAILABLE",run.get("errorCode"));held();
        verify(client,never()).getRequest(anyString(),anyString(),anyString());
        verify(client,never()).submit(anyString(),any(),any());
    }
    @Test void queuedPauseWithdrawsOnlyBeforeTheDurableRemoteSubmissionBoundary(){
        run.put("status","QUEUED");run.put("workspaceId","workspace");state.put("submitted",false);var tx=(TransactionTemplate)ReflectionTestUtils.getField(sync,"tx");when(tx.execute(any())).thenAnswer(i->{TransactionCallback<Boolean> callback=i.getArgument(0);return callback.doInTransaction(mock(TransactionStatus.class));});
        assertTrue(sync.withdrawQueued("local","SCHEDULE_PAUSED"));assertEquals("CANCELLED",run.get("status"));verify(jdbc).update("DELETE FROM dw_sync_target_lock WHERE run_id=?","local");verify(client,never()).cancelRequest(anyString(),anyString(),any());verify(client,never()).submit(anyString(),any(),any());
    }
    @Test void aQueuedRemoteRequestAlreadySubmittedContinuesWhenThePlanIsPaused(){
        run.put("status","QUEUED");run.put("workspaceId","workspace");state.put("submitted",true);var tx=(TransactionTemplate)ReflectionTestUtils.getField(sync,"tx");when(tx.execute(any())).thenAnswer(i->{TransactionCallback<Boolean> callback=i.getArgument(0);return callback.doInTransaction(mock(TransactionStatus.class));});
        assertFalse(sync.withdrawQueued("local","SCHEDULE_PAUSED"));assertEquals("QUEUED",run.get("status"));held();verify(repo,never()).transitionRun(anyMap(),anyString());verify(client,never()).cancelRequest(anyString(),anyString(),any());
    }
    @Test @SuppressWarnings("unchecked") void incompatibleHealthFailsBeforeSubmissionAndReleasesTarget() {
        state.put("submitted",false);run.put("status","QUEUED");
        var prepared=new SyncExecutionService.Prepared(null,mysql(),doris(),Map.of(),"target-key","DUPLICATE");
        ((Map<String,SyncExecutionService.Prepared>)ReflectionTestUtils.getField(sync,"prepared")).put("local",prepared);
        var tx=(TransactionTemplate)ReflectionTestUtils.getField(sync,"tx");when(tx.execute(any())).thenReturn(true);
        when(client.health("http://127.0.0.1:9876")).thenThrow(StudioException.bad("SYNC_UPGRADE_REQUIRED","请更新 Dunnelean 至支持可靠取消的版本"));
        tick();assertEquals("FAILED",run.get("status"));assertEquals("SYNC_UPGRADE_REQUIRED",run.get("errorCode"));
        verify(jdbc,never()).update(startsWith("UPDATE dw_sync_execution SET submitted=TRUE"),any(),any(),any());
        verify(client,never()).submit(anyString(),any(),any());
        verify(jdbc).update("DELETE FROM dw_sync_target_lock WHERE run_id=?","local");
    }
    private DatasourceService.ConnectionSpec mysql() {
        return new DatasourceService.ConnectionSpec("mysql","workspace","MySQL","127.0.0.1",3307,"mysql_db","reader","cipher","MYSQL",Map.of());
    }
    private DatasourceService.ConnectionSpec doris() {
        return new DatasourceService.ConnectionSpec("doris","workspace","Doris","127.0.0.1",9030,"doris_db","writer","cipher","DORIS",Map.of("feHttpUrls",List.of("http://127.0.0.1:8030"),"beHttpUrls",List.of("http://127.0.0.1:8040"),"flightUri","grpc://127.0.0.1:8070","flightEndpointMap",Map.of("grpc+tcp://internal:8050","grpc://127.0.0.1:8050"),"httpEndpointMap",Map.of("http://internal:8040","http://127.0.0.1:8040")));
    }
    @Test void typedSpecsPreserveDirectionalParametersEndpointsAndWriteModes() throws Exception {
        when(sources.credentials(any())).thenReturn(Map.of("username","business","password","private-password-sentinel"));
        var config=new LinkedHashMap<String,Object>(Map.of("sourceTable","orders","targetTable","orders_copy","where","name = '${day}' AND updated_at <= :source_cutoff","mapping",List.of(Map.of("source","name","target","display_name")),"writeMode","overwrite","batchRows",25,"parallelism",2,"timeoutSeconds",3));
        run.put("scheduleParameters",Map.of("day","中文'💫"));run.put("parameters",Map.of("source_cutoff","2026-09-30T01:02:03.123456Z"));
        var forward=sync.spec(new SyncExecutionService.Prepared(null,mysql(),doris(),config,"key","UNIQUE_MOW"),run);
        var mysqlReader=assertInstanceOf(MysqlReader.class,forward.reader());var dorisWriter=assertInstanceOf(DorisWriter.class,forward.writer());
        assertEquals("local",forward.requestId());assertEquals("name = ? AND updated_at <= ?",mysqlReader.source().predicate());assertEquals("display_name",forward.mapping().getFirst().target());
        assertEquals(List.of("中文'💫","2026-09-30 01:02:03.123456"),mysqlReader.source().params().stream().map(p->assertInstanceOf(Parameter.StringValue.class,p).value()).toList());
        assertEquals("+08:00",mysqlReader.connection().timeZone());assertEquals(BigInteger.valueOf(25),mysqlReader.batch().rows());
        assertEquals(DorisWriteMode.UPSERT,dorisWriter.mode());assertEquals(List.of("TRUNCATE TABLE `doris_db`.`orders_copy`"),dorisWriter.options().preSql());
        assertEquals(Map.of("http://internal:8040","http://127.0.0.1:8040"),dorisWriter.endpointMap());assertEquals(2,dorisWriter.options().parallelism());assertEquals(BigInteger.valueOf(3000),forward.execution().timeoutMs());
        config.put("writeMode","upsert");config.put("keyColumns",List.of("id"));
        var reverse=sync.spec(new SyncExecutionService.Prepared(null,doris(),mysql(),config,"key","INNODB"),run);
        var dorisReader=assertInstanceOf(DorisReader.class,reverse.reader());var mysqlWriter=assertInstanceOf(MysqlWriter.class,reverse.writer());
        assertTrue(dorisReader.source().predicate().contains("FROM_BASE64('"));assertTrue(dorisReader.source().params().isEmpty());
        assertEquals(Map.of("grpc+tcp://internal:8050","grpc://127.0.0.1:8050"),dorisReader.endpointMap());assertEquals("+08:00",dorisReader.sessionVariables().get("time_zone"));
        assertEquals(MysqlWriteMode.UPSERT,mysqlWriter.mode());assertEquals(List.of("id"),mysqlWriter.keyColumns());assertTrue(mysqlWriter.options().preSql().isEmpty());
        assertEquals(forward,RunSpecJson.fromJson(RunSpecJson.toJson(forward)));assertEquals(reverse,RunSpecJson.fromJson(RunSpecJson.toJson(reverse)));
        // Changing the editable map cannot change the already generated submission.
        assertEquals(List.of("TRUNCATE TABLE `doris_db`.`orders_copy`"),dorisWriter.options().preSql());
        config.put("writeMode","append");
        var appendForward=sync.spec(new SyncExecutionService.Prepared(null,mysql(),doris(),config,"key","DUPLICATE"),run);
        var appendReverse=sync.spec(new SyncExecutionService.Prepared(null,doris(),mysql(),config,"key","INNODB"),run);
        assertEquals(DorisWriteMode.APPEND,assertInstanceOf(DorisWriter.class,appendForward.writer()).mode());
        var mysqlAppend=assertInstanceOf(MysqlWriter.class,appendReverse.writer());assertEquals(MysqlWriteMode.INSERT,mysqlAppend.mode());assertTrue(mysqlAppend.keyColumns().isEmpty());
        config.put("writeMode","overwrite");
        var overwriteReverse=assertInstanceOf(MysqlWriter.class,sync.spec(new SyncExecutionService.Prepared(null,doris(),mysql(),config,"key","INNODB"),run).writer());
        assertEquals(MysqlWriteMode.INSERT,overwriteReverse.mode());assertEquals(List.of("TRUNCATE TABLE `mysql_db`.`orders_copy`"),overwriteReverse.options().preSql());
        config.put("writeMode","upsert");
        var upsertForward=assertInstanceOf(DorisWriter.class,sync.spec(new SyncExecutionService.Prepared(null,mysql(),doris(),config,"key","UNIQUE_MOW"),run).writer());
        assertEquals(DorisWriteMode.UPSERT,upsertForward.mode());assertTrue(upsertForward.options().preSql().isEmpty());
    }
    @Test void malformedTypedConfigurationDoesNotExposeSerializedCredentials() {
        when(sources.credentials(any())).thenReturn(Map.of("username","business","password","private-password-sentinel"));
        var config=Map.<String,Object>of("sourceTable","orders","targetTable","orders_copy","mapping",List.of(Map.of("source","name","target","name","unsupported","configuration-sentinel")));
        var prepared=new SyncExecutionService.Prepared(null,mysql(),doris(),config,"key","DUPLICATE");
        var error=assertThrows(StudioException.class,()->sync.spec(prepared,run));
        assertEquals("INVALID_SYNC",error.code());assertFalse(error.getMessage().contains("private-password-sentinel"));assertFalse(error.getMessage().contains("configuration-sentinel"));
    }
}
