package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SqlExecutionProviderTest {
    private StudioObject node(String nodeType,String provider,Map<String,Object> run){var config=new LinkedHashMap<String,Object>(run);config.put("provider",provider);return new StudioObject("node","workspace",null,"NODE",nodeType,"SQL","","SHOW PARTITIONS FROM orders",Map.of("run",config),List.of(),false,false,1,"admin","now");}
    private DatasourceService.ConnectionSpec source(String type){return new DatasourceService.ConnectionSpec("source","workspace","SQL","localhost",9030,"test_ods","root","encrypted",type,Map.of());}
    @Test void matchingDorisNodeCreatesDurableRealDorisRunWithoutMysqlInventoryMode(){
        var p=new MysqlExecutionProvider(mock(StudioRepository.class),mock(DatasourceService.class),new SqlGuard(),new JsonCodec(),mock(TransactionTemplate.class),1,2);
        try{
            var node=node("Doris","DORIS",Map.of());var prepared=p.prepare(node,source("DORIS"));var run=p.newRun(prepared,"MANUAL");
            assertEquals("DORIS",run.get("provider"));assertEquals(false,run.get("simulation"));assertEquals(false,run.get("containsWrites"));assertTrue(run.get("logs").toString().contains("[Doris]"));assertFalse(InventoryExecutionService.materializes(node));
            assertThrows(StudioException.class,()->p.prepare(node,source("MYSQL")));assertThrows(StudioException.class,()->p.prepare(node("MySQL","DORIS",Map.of()),source("DORIS")));
            assertThrows(StudioException.class,()->p.prepare(node("Doris","DORIS",Map.of("executionMode","MATERIALIZE")),source("DORIS")));
        }finally{p.shutdown();}
    }
    @Test void dorisSessionsDisableAsynchronousCommitAndApplyBothTimeouts() throws Exception {
        Connection connection=mock(Connection.class);Statement settings=mock(Statement.class);when(connection.createStatement()).thenReturn(settings);
        MysqlExecutionProvider.configureDoris(connection,"DORIS",42);var order=inOrder(settings);order.verify(settings).setQueryTimeout(5);order.verify(settings).execute("SET time_zone = 'Asia/Shanghai'");order.verify(settings).execute("SET query_timeout = 42");order.verify(settings).execute("SET insert_timeout = 42");order.verify(settings).execute("SET group_commit = 'off_mode'");order.verify(settings).close();
        Connection mysql=mock(Connection.class);MysqlExecutionProvider.configureDoris(mysql,"MYSQL",42);verifyNoInteractions(mysql);
    }
    @Test void realTaskAndDevelopmentWhiteListsIncludeDoris(){
        var node=node("Doris","DORIS",Map.of());var objects=mock(ObjectService.class);when(objects.active("node")).thenReturn(node);
        var tasks=new TaskService(mock(TaskRepository.class),mock(StudioRepository.class),objects,mock(DatasourceService.class),mock(MysqlExecutionProvider.class),mock(InventoryExecutionService.class),new JsonCodec(),mock(TransactionTemplate.class),mock(SyncExecutionService.class));
        assertSame(node,tasks.task("node"));assertTrue(RunService.isSql(node));assertFalse(RunService.isMysql(node));
        var runs=new RunService(mock(StudioRepository.class),objects,mock(LocalSimulationProvider.class),mock(MysqlExecutionProvider.class),mock(WorkflowService.class),tasks,mock(SyncExecutionService.class));
        assertEquals("VERSION_CONFLICT",assertThrows(StudioException.class,()->runs.submit("node","MANUAL",false,null)).code());
        assertEquals("SIMULATION_ONLY",assertThrows(StudioException.class,()->runs.submit("node","MANUAL",true,1)).code());
    }
    @Test void stoppingLegacySimulationWithoutProviderStillUsesSimulation(){
        var repo=mock(StudioRepository.class);var simulation=mock(LocalSimulationProvider.class);var run=Map.<String,Object>of("id","legacy","status","RUNNING","simulation",true);when(repo.run("legacy")).thenReturn(Optional.of(run));when(simulation.stop("legacy")).thenReturn(run);
        var runs=new RunService(repo,mock(ObjectService.class),simulation,mock(MysqlExecutionProvider.class),mock(WorkflowService.class),mock(TaskService.class),mock(SyncExecutionService.class));assertSame(run,runs.stop("legacy"));verify(simulation).stop("legacy");
    }
    @Test void lostConnectionPreservesAcknowledgedDorisWriteAndDoesNotDispatchLaterCommand() throws Exception {
        try(var execution=new Execution("INSERT INTO orders VALUES(1);ALTER TABLE orders ADD COLUMN extra INT;SELECT 1")){
            when(execution.statements.get(1).execute()).thenThrow(new SQLException("lost","08006"));execution.run();
            assertEquals("FAILED",execution.run.get("status"));assertEquals("COMMIT_UNKNOWN",execution.run.get("errorCode"));assertEquals(true,execution.run.get("containsWrites"));
            var items=SqlResults.items(execution.result.get());assertEquals("COMMITTED",items.get(0).get("commitStatus"));assertEquals("UNKNOWN",items.get(1).get("commitStatus"));assertEquals("SKIPPED",items.get(2).get("status"));
            verify(execution.statements.get(0),times(1)).execute();verify(execution.statements.get(1),times(1)).execute();verify(execution.statements.get(2),never()).execute();
            assertTrue(execution.run.get("logs").toString().contains("[Doris] 含写入任务不自动失败重试"));
        }
    }
    @Test void failingDorisMetadataQueryHasNoUnknownCommit() throws Exception {
        try(var execution=new Execution("SHOW CREATE TABLE orders;SHOW PARTITIONS FROM orders")){
            when(execution.statements.get(0).execute()).thenThrow(new SQLException("missing","42S02",1146));execution.run();
            assertEquals("QUERY_FAILED",execution.run.get("errorCode"));assertEquals(false,execution.run.get("containsWrites"));var items=SqlResults.items(execution.result.get());assertEquals("NOT_APPLICABLE",items.get(0).get("commitStatus"));assertEquals("SKIPPED",items.get(1).get("status"));verify(execution.statements.get(1),never()).execute();
        }
    }
    private final class Execution implements AutoCloseable {
        final StudioRepository repo=mock(StudioRepository.class);final DatasourceService sources=mock(DatasourceService.class);final TransactionTemplate tx=mock(TransactionTemplate.class);
        final MysqlExecutionProvider provider=new MysqlExecutionProvider(repo,sources,new SqlGuard(),new JsonCodec(),tx,1,2);
        final List<PreparedStatement> statements=new ArrayList<>();final AtomicReference<Map<String,Object>> result=new AtomicReference<>();final CountDownLatch finished=new CountDownLatch(1);
        final MysqlExecutionProvider.PreparedQuery prepared;final Map<String,Object> run;
        @SuppressWarnings("unchecked") Execution(String sql) throws Exception {
            var base=node("Doris","DORIS",Map.of());var snapshot=new StudioObject(base.id(),base.workspaceId(),null,"NODE","Doris",base.name(),"",sql,base.config(),List.of(),false,false,1,"admin","now");prepared=provider.prepare(snapshot,source("DORIS"));run=provider.newRun(prepared,"MANUAL");
            when(repo.run(anyString())).thenReturn(Optional.of(run));when(repo.transitionRun(anyMap(),anyString())).thenReturn(true);
            doAnswer(invocation->{((Consumer<TransactionStatus>)invocation.getArgument(0)).accept(mock(TransactionStatus.class));return null;}).when(tx).executeWithoutResult(any());
            doAnswer(invocation->{result.set(invocation.getArgument(1));if(Set.of("SUCCESS","FAILED","CANCELLED").contains(run.get("status")))finished.countDown();return null;}).when(repo).saveResult(anyString(),anyMap());
            var connection=mock(Connection.class);when(sources.open(any(),anyInt(),eq(true))).thenReturn(connection);when(connection.createStatement()).thenReturn(mock(Statement.class));when(connection.prepareStatement(anyString())).thenAnswer(invocation->mock(PreparedStatement.class));
            for(var command:prepared.script().commands()){var statement=mock(PreparedStatement.class);when(statement.execute()).thenReturn(false);when(statement.getLargeUpdateCount()).thenReturn(command.writes()?1L:0L);statements.add(statement);when(connection.prepareStatement(command.parameters().sql(),ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY)).thenReturn(statement);}
        }
        void run() throws Exception {provider.enqueueExisting(run.get("id").toString(),prepared);assertTrue(finished.await(5,TimeUnit.SECONDS),"SQL executor did not finish");}
        @Override public void close(){provider.shutdown();}
    }
}
