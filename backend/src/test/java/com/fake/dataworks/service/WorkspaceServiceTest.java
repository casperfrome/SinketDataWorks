package com.fake.dataworks.service;

import com.fake.dataworks.dto.WorkspaceInput;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.StudioRepository;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class WorkspaceServiceTest {
    @Test void rejectsInvalidIdentifiersAndDisplayNamesBeforeWriting() {
        var repo=mock(StudioRepository.class);
        var service=new WorkspaceService(repo,mock(ObjectService.class));
        for(String code:new String[]{"", "ab", "1project", "Project", "bad-name", "a".repeat(65)})
            assertEquals("INVALID_WORKSPACE_CODE",assertThrows(StudioException.class,()->service.create(new WorkspaceInput("业务",code))).code());
        for(String name:new String[]{"", " ", "a".repeat(101), "line\nbreak", "bad\u007f"})
            assertEquals("INVALID_WORKSPACE_NAME",assertThrows(StudioException.class,()->service.create(new WorkspaceInput(name,"business"))).code());
        assertEquals("INVALID_WORKSPACE_NAME",assertThrows(StudioException.class,()->service.create(new WorkspaceInput(null,null))).code());
        verifyNoInteractions(repo);
    }
    @Test void returnsActionableConflictForExistingCode() {
        var repo=mock(StudioRepository.class);
        doThrow(new DuplicateKeyException("duplicate code")).when(repo).insertWorkspace(anyMap());
        var service=new WorkspaceService(repo,mock(ObjectService.class));
        assertEquals("WORKSPACE_CODE_CONFLICT",assertThrows(StudioException.class,()->service.create(new WorkspaceInput("业务","business"))).code());
    }
}
