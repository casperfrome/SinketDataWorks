package com.fake.dataworks.service;

import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.repository.StudioRepository;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SimulationDebugParametersTest {
    StudioObject node() {return new StudioObject("node","workspace",null,"NODE","MaxCompute SQL","demo","","SELECT '${region}'",Map.of(),List.of(),false,false,3,"admin","now");}
    Map<String,Object> options() {return Map.of("businessDate","2026-09-28","scheduledAt","2026-09-29T02:00:00Z","timezone","UTC","sourceCutoffAt","2026-09-29T01:00:00Z","scheduleParameters",Map.of("region","literal $bizdate"));}
    @Test void simulationFreezesParametersAndDispatchesOnlyAfterCommit() throws Exception {
        var repo=mock(StudioRepository.class);var provider=new LocalSimulationProvider(repo,0,0,false);var began=new CountDownLatch(1);
        TransactionSynchronizationManager.initSynchronization();
        try {
            var run=provider.start(node(),"MANUAL",false,options());
            assertEquals(Map.of("region","literal $bizdate"),run.get("scheduleParameters"));assertEquals("2026-09-28",run.get("businessDate"));assertEquals(3,run.get("objectVersion"));
            when(repo.run(run.get("id").toString())).thenReturn(Optional.of(run));
            doAnswer(i->{began.countDown();return null;}).when(repo).updateRun(anyMap());
            assertFalse(began.await(100,TimeUnit.MILLISECONDS));
            var callbacks=TransactionSynchronizationManager.getSynchronizations();assertEquals(1,callbacks.size());
            callbacks.forEach(TransactionSynchronization::afterCommit);
            assertTrue(began.await(2,TimeUnit.SECONDS));
        } finally {TransactionSynchronizationManager.clearSynchronization();provider.shutdown();}
    }
    @Test void rolledBackSimulationNeverStartsItsWorker() throws Exception {
        var repo=mock(StudioRepository.class);var provider=new LocalSimulationProvider(repo,0,0,false);
        TransactionSynchronizationManager.initSynchronization();
        try {
            var run=provider.start(node(),"MANUAL",false,options());when(repo.run(anyString())).thenReturn(Optional.of(run));
            TransactionSynchronizationManager.getSynchronizations().forEach(callback->callback.afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK));
            Thread.sleep(100);verify(repo,never()).run(anyString());verify(repo,never()).updateRun(anyMap());
        } finally {TransactionSynchronizationManager.clearSynchronization();provider.shutdown();}
    }
}
