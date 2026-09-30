package com.fake.dataworks;

import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.dto.ObjectInput;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.StudioRepository;
import com.fake.dataworks.service.*;
import java.util.*;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.web.MockMultipartFile;
import static org.junit.jupiter.api.Assertions.*;

/** Uses the configured MySQL database; each test owns a fresh workspace and removes only that workspace. */
@SpringBootTest(properties={"spring.main.web-application-type=none","studio.simulation.queue-ms=100","studio.simulation.run-ms=160","studio.simulation.recover-on-start=false","studio.storage-path=./target/test-storage"})
class StudioIntegrationTest {
    @Autowired ObjectService service;
    @Autowired StudioRepository repo;
    @Autowired ExecutionProvider execution;
    @Autowired JdbcTemplate jdbc;
    @Autowired FileService files;
    String workspace;
    @BeforeEach void setup() {workspace="test-"+UUID.randomUUID();jdbc.update("INSERT INTO dw_workspace(id,name,code,region,workspace_type) VALUES(?,?,?,?,'TEST')",workspace,"自动测试隔离空间",workspace,"local");}
    @AfterEach void cleanup() throws Exception {
        for(Map<String,Object> run:repo.runs(workspace)) execution.stop(run.get("id").toString());
        var storageNames=jdbc.queryForList("SELECT DISTINCT storage_name FROM dw_file WHERE object_id IN (SELECT id FROM dw_object WHERE workspace_id=?)",String.class,workspace);
        jdbc.update("DELETE FROM dw_file WHERE object_id IN (SELECT id FROM dw_object WHERE workspace_id=?)",workspace);
        jdbc.update("DELETE FROM dw_version WHERE object_id IN (SELECT id FROM dw_object WHERE workspace_id=?)",workspace);
        jdbc.update("DELETE FROM dw_run WHERE workspace_id=?",workspace);
        jdbc.update("DELETE FROM dw_record WHERE workspace_id=?",workspace);
        jdbc.update("DELETE FROM dw_object WHERE workspace_id=?",workspace);
        jdbc.update("DELETE FROM dw_workspace WHERE id=?",workspace);
        for(String storageName:storageNames) if(!repo.storageReferenced(storageName)) Files.deleteIfExists(files.safePath(storageName));
    }
    ObjectInput input(String parent,String kind,String name,String content,Integer version) {return new ObjectInput(workspace,parent,kind,"MAXCOMPUTE_SQL",name,"",content,Map.of(),List.of(),false,version);}
    StudioObject create(String parent,String kind,String name) {return service.create(input(parent,kind,name,"SELECT 1;",null));}
    @Test void enforcesNamesAndPreventsFolderCyclesAndCrossWorkspaceParents() {
        StudioObject root=create(null,"FOLDER","数仓");StudioObject child=create(root.id(),"FOLDER","ods");
        assertEquals("NAME_CONFLICT",assertThrows(StudioException.class,()->create(null,"FOLDER","数仓")).code());
        assertEquals("DIRECTORY_CYCLE",assertThrows(StudioException.class,()->service.update(root.id(),input(child.id(),"FOLDER","数仓","",root.version()))).code());
        assertEquals("WORKSPACE_MISMATCH",assertThrows(StudioException.class,()->service.create(new ObjectInput("local-workspace",root.id(),"NODE","MySQL","跨工作空间","","",Map.of(),List.of(),false,null))).code());
    }
    @Test void preservesContentOnVersionConflictAndRestoresSnapshots() {
        StudioObject original=create(null,"NODE","版本测试");String versionId=repo.versions(original.id()).getFirst().get("id").toString();
        StudioObject changed=service.update(original.id(),input(null,"NODE",original.name(),"SELECT 2;",original.version()));
        assertEquals("VERSION_CONFLICT",assertThrows(StudioException.class,()->service.update(original.id(),input(null,"NODE",original.name(),"lost",original.version()))).code());
        assertEquals("SELECT 2;",service.get(original.id()).content());
        StudioObject restored=service.restoreVersion(original.id(),versionId,changed.version());assertEquals("SELECT 1;",restored.content());assertEquals(3,restored.version());assertEquals(3,repo.versions(original.id()).size());
    }
    @Test void deleteAndRestoreAreAtomicOnNameConflict() {
        StudioObject folder=create(null,"FOLDER","目录");StudioObject child=create(folder.id(),"NODE","节点");service.delete(folder.id());
        assertTrue(service.get(child.id()).deleted());create(null,"FOLDER","目录");
        assertEquals("NAME_CONFLICT",assertThrows(StudioException.class,()->service.restore(folder.id(),null)).code());assertTrue(service.get(child.id()).deleted());
        StudioObject restored=service.restore(folder.id(),"恢复目录");assertFalse(restored.deleted());assertFalse(service.get(child.id()).deleted());
    }
    @Test void restoringParentDoesNotResurrectPreviouslyDeletedChildren() {
        StudioObject folder=create(null,"FOLDER","目录");StudioObject prior=create(folder.id(),"NODE","先删除");StudioObject current=create(folder.id(),"NODE","随目录删除");service.delete(prior.id());service.delete(folder.id());service.restore(folder.id(),null);
        assertTrue(service.get(prior.id()).deleted());assertFalse(service.get(current.id()).deleted());
    }
    @Test void recursiveCopyCanTargetDescendantWithoutInfiniteRecursion() {
        StudioObject folder=create(null,"FOLDER","目录");StudioObject child=create(folder.id(),"FOLDER","下级");create(child.id(),"NODE","节点");StudioObject copy=service.copy(folder.id(),null,child.id(),true);assertEquals(child.id(),copy.parentId());assertEquals(6,service.list(workspace,false).size());
    }
    @Test void simulationRunsSuccessFailureAndCancellationWithoutExecutingContent() throws Exception {
        StudioObject object=create(null,"NODE","执行测试");object=service.update(object.id(),input(null,"NODE",object.name(),"DROP DATABASE fake_dataworks_260927;",object.version()));
        Map<String,Object> ok=execution.start(object,"MANUAL",false);assertEquals("QUEUED",ok.get("status"));
        Map<String,Object> cancelled=execution.start(object,"MANUAL",false);execution.stop(cancelled.get("id").toString());
        Map<String,Object> failed=execution.start(object,"MANUAL",true);
        waitFor(ok.get("id").toString(),"SUCCESS");waitFor(failed.get("id").toString(),"FAILED");assertEquals("CANCELLED",repo.run(cancelled.get("id").toString()).orElseThrow().get("status"));
        assertEquals("DROP DATABASE fake_dataworks_260927;",service.get(object.id()).content());assertTrue(repo.workspaceExists(workspace));assertFalse(((List<?>)repo.run(ok.get("id").toString()).orElseThrow().get("rows")).isEmpty());
    }
    @Test void ownerChangesPersistAndInvalidOwnerDoesNotAdvanceVersion() {
        StudioObject object=create(null,"NODE","负责人测试");
        var input=new ObjectInput(workspace,null,object.kind(),object.nodeType(),object.name(),object.description(),object.content(),object.config(),object.tags(),object.favorite(),object.version()," 数仓负责人 ");
        StudioObject saved=service.update(object.id(),input);assertEquals("数仓负责人",saved.owner());assertEquals(saved.owner(),service.get(object.id()).owner());
        var invalid=new ObjectInput(workspace,null,saved.kind(),saved.nodeType(),saved.name(),saved.description(),saved.content(),saved.config(),saved.tags(),saved.favorite(),saved.version(),"  ");
        assertEquals("INVALID_OWNER",assertThrows(StudioException.class,()->service.update(saved.id(),invalid)).code());assertEquals(saved.version(),service.get(saved.id()).version());
    }
    @Test void uploadReturnsVersionedObjectAndCopiedFilesSurviveReplacement() throws Exception {
        StudioObject object=create(null,"RESOURCE","资源测试");
        StudioObject uploaded=files.upload(object.id(),new MockMultipartFile("file","../../first.csv","text/csv","first,value".getBytes(StandardCharsets.UTF_8)));
        assertEquals(2,uploaded.version());assertEquals("first.csv",uploaded.config().get("fileName"));assertEquals("first.csv",((Map<?,?>)uploaded.config().get("file")).get("name"));assertEquals(2,repo.versions(object.id()).size());
        StudioObject copy=service.copy(object.id(),null,null,false);
        StudioObject replaced=files.upload(object.id(),new MockMultipartFile("file","second.csv","text/csv","second,value".getBytes(StandardCharsets.UTF_8)));
        assertEquals(3,replaced.version());assertEquals("second,value",new String(files.download(object.id()).getContentAsByteArray(),StandardCharsets.UTF_8));assertEquals("first,value",new String(files.download(copy.id()).getContentAsByteArray(),StandardCharsets.UTF_8));
        assertEquals("first.csv",service.get(copy.id()).config().get("fileName"));
    }
    void waitFor(String id,String status) throws InterruptedException {long deadline=System.currentTimeMillis()+5000;while(System.currentTimeMillis()<deadline){if(repo.run(id).orElseThrow().get("status").equals(status))return;Thread.sleep(25);}fail("Did not reach "+status);}
}
