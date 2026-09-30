package com.fake.dataworks;

import com.fake.dataworks.dto.WorkspaceInput;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.StudioRepository;
import com.fake.dataworks.service.WorkspaceService;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties={"spring.main.web-application-type=none","studio.simulation.recover-on-start=false","studio.storage-path=./target/test-storage"})
class WorkspaceIntegrationTest {
    @Autowired WorkspaceService service;
    @Autowired StudioRepository repo;
    @Autowired JdbcTemplate jdbc;

    @Test void newWorkspaceIsEmptyAndInternalFixturesAreAbsentFromUserList() {
        String code="project_"+UUID.randomUUID().toString().replace("-","");
        String fixture="internal-"+UUID.randomUUID();
        Map<String,Object> workspace=service.create(new WorkspaceInput("  中文业务项目  ",code));
        String id=workspace.get("id").toString();
        try {
            jdbc.update("INSERT INTO dw_workspace(id,name,code,region,workspace_type) VALUES(?,?,?,'local','TEST')",fixture,"内部验收",fixture);
            assertEquals("中文业务项目",workspace.get("name"));assertEquals("USER",workspace.get("type"));
            assertTrue(service.workspaces().stream().anyMatch(w->w.get("id").equals(id)));
            assertFalse(service.workspaces().stream().anyMatch(w->w.get("id").equals(fixture)));
            assertEquals("local-workspace",service.workspaces().getFirst().get("id"));
            assertTrue(repo.list(id,false).isEmpty());assertTrue(repo.runs(id).isEmpty());
            assertEquals(0,jdbc.queryForObject("SELECT COUNT(*) FROM dw_datasource WHERE workspace_id=?",Integer.class,id));
            assertEquals("WORKSPACE_CODE_CONFLICT",assertThrows(StudioException.class,()->service.create(new WorkspaceInput("另一个显示名",code))).code());
        } finally {
            jdbc.update("DELETE FROM dw_workspace WHERE id IN (?,?)",id,fixture);
        }
    }
    @Test void concurrentCreationOfSameCodeHasExactlyOneWinner() throws Exception {
        String code="race_"+UUID.randomUUID().toString().replace("-","");
        var start=new CountDownLatch(1);
        try(var executor=Executors.newFixedThreadPool(2)) {
            Callable<String> create=()->{start.await();try {service.create(new WorkspaceInput("并发项目",code));return "CREATED";}catch(StudioException e){return e.code();}};
            var first=executor.submit(create);var second=executor.submit(create);start.countDown();
            assertEquals(Set.of("CREATED","WORKSPACE_CODE_CONFLICT"),Set.of(first.get(10,TimeUnit.SECONDS),second.get(10,TimeUnit.SECONDS)));
            assertEquals(1,jdbc.queryForObject("SELECT COUNT(*) FROM dw_workspace WHERE code=?",Integer.class,code));
        } finally {jdbc.update("DELETE FROM dw_workspace WHERE code=?",code);}
    }
}
