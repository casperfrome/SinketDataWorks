package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.repository.*;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SqlRetryTest {
    @Test void transientFailuresRetryQueriesButNeverWriteScriptsOrWriteWorkflows() {
        var repo=mock(StudioRepository.class);var store=mock(TaskRepository.class);var jdbc=mock(JdbcTemplate.class);
        var taskService=mock(TaskService.class);var workflowService=mock(WorkflowService.class);
        var workflow=new ScheduleService(jdbc,new JsonCodec(),workflowService,null,repo,null,null,false);
        var task=new TaskScheduleService(store,repo,taskService,workflow,null,false);
        try {
            for(boolean writes:List.of(false,true)) {
                when(repo.run("run")).thenReturn(Optional.of(Map.of("containsWrites",writes)));
                var config=Map.of("retries",3,"retryIntervalSeconds",60);
                var t=new LinkedHashMap<String,Object>(Map.of("id","trigger","runId","run","attempt",1,"scheduleSnapshot",config));
                ReflectionTestUtils.invokeMethod(task,"failure",t,"DB_TRANSIENT",Instant.now());assertEquals(writes?"FAILED":"RETRY_WAIT",t.get("status"));
                var w=new LinkedHashMap<String,Object>(Map.of("id","workflow-trigger","runId","run","attempt",1,"scheduleSnapshot",config));
                ReflectionTestUtils.invokeMethod(workflow,"completeFailure",w,"DATASOURCE_UNAVAILABLE",Instant.now());assertEquals(writes?"FAILED":"RETRY_WAIT",w.get("status"));
                when(taskService.release("release")).thenReturn(Map.of("containsWrites",writes));when(workflowService.release("release")).thenReturn(Map.of("containsWrites",writes));
                t.remove("runId");t.put("releaseId","release");ReflectionTestUtils.invokeMethod(task,"failure",t,"QUEUE_FULL",Instant.now());assertEquals(writes?"FAILED":"RETRY_WAIT",t.get("status"));
                w.remove("runId");w.put("releaseId","release");ReflectionTestUtils.invokeMethod(workflow,"completeFailure",w,"QUEUE_FULL",Instant.now());assertEquals(writes?"FAILED":"RETRY_WAIT",w.get("status"));
            }
        } finally {task.close();workflow.close();}
    }
}
