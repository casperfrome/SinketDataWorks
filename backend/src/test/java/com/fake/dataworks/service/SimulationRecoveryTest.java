package com.fake.dataworks.service;

import com.fake.dataworks.repository.StudioRepository;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SimulationRecoveryTest {
    @Test void restartPreservesCompletedWritesAndMarksOnlyInFlightCommitUnknown() {
        StudioRepository repo=mock(StudioRepository.class);
        var run=new LinkedHashMap<String,Object>(Map.of("id","script","status","RUNNING","containsWrites",true,"logs",List.of()));
        var items=SqlResults.pending(SqlScript.prepare("INSERT INTO t VALUES(1);UPDATE t SET id=2;SELECT 1","studio_demo",new SqlGuard()));
        items.get(0).put("status","SUCCESS");items.get(0).put("commitStatus","COMMITTED");items.get(1).put("status","RUNNING");items.get(1).put("commitStatus","IN_PROGRESS");
        var result=SqlResults.envelope(items);when(repo.unfinishedRuns()).thenReturn(List.of(run));when(repo.result("script")).thenReturn(Optional.of(result));
        var provider=new LocalSimulationProvider(repo,100,100,true);
        try {
            provider.recover();assertEquals("COMMIT_UNKNOWN",run.get("errorCode"));assertEquals("COMMITTED",items.get(0).get("commitStatus"));assertEquals("UNKNOWN",items.get(1).get("commitStatus"));assertEquals("SKIPPED",items.get(2).get("status"));
            verify(repo).saveResult("script",result);verify(repo,never()).insertRun(anyMap(),any());
        } finally {provider.shutdown();}
    }
    @Test void restartTerminatesWaitingQueuedAndRunningWithoutReplaying() {
        StudioRepository repo=mock(StudioRepository.class);
        List<Map<String,Object>> interrupted=new ArrayList<>();
        for(String status:List.of("WAITING","QUEUED","RUNNING")) interrupted.add(new LinkedHashMap<>(Map.of("id",status,"status",status,"logs",new ArrayList<String>())));
        when(repo.unfinishedRuns()).thenReturn(interrupted);
        LocalSimulationProvider provider=new LocalSimulationProvider(repo,100,100,true);
        try {provider.recover();for(var run:interrupted) {assertEquals("FAILED",run.get("status"));assertEquals("SERVICE_RESTARTED",run.get("errorCode"));verify(repo).transitionRun(run,run.get("id").toString());}verify(repo,never()).insertRun(anyMap(),any());}finally{provider.shutdown();}
    }
    @Test void restartFailsInterruptedRunAndExplainsWhy() {
        StudioRepository repo=mock(StudioRepository.class);
        Map<String,Object> run=new LinkedHashMap<>(Map.of("id","interrupted","status","RUNNING","logs",new ArrayList<String>()));
        when(repo.unfinishedRuns()).thenReturn(List.of(run));
        LocalSimulationProvider provider=new LocalSimulationProvider(repo,100,100,true);
        try {provider.recover();assertEquals("FAILED",run.get("status"));assertNotNull(run.get("finishedAt"));assertTrue(run.get("logs").toString().contains("服务已重新启动"));verify(repo).transitionRun(run,"RUNNING");}finally{provider.shutdown();}
    }
}
