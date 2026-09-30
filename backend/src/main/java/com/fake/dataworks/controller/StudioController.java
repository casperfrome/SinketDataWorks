package com.fake.dataworks.controller;

import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.dto.ObjectInput;
import com.fake.dataworks.dto.WorkspaceInput;
import com.fake.dataworks.service.*;
import java.util.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import java.nio.charset.StandardCharsets;

@RestController
@RequestMapping("/api/v1")
public class StudioController {
    private final WorkspaceService workspaces;private final ObjectService objects;private final RunService executions;private final RecordService records;private final FileService files;
    public StudioController(WorkspaceService workspaces,ObjectService objects,RunService executions,RecordService records,FileService files) {this.workspaces=workspaces;this.objects=objects;this.executions=executions;this.records=records;this.files=files;}
    @GetMapping("/workspaces") public Object workspaces() {return workspaces.workspaces();}
    @PostMapping("/workspaces") @ResponseStatus(HttpStatus.CREATED) public Object createWorkspace(@RequestBody WorkspaceInput input) {return workspaces.create(input);}
    @GetMapping("/objects") public Object objects(@RequestParam(defaultValue="local-workspace") String workspaceId,@RequestParam(defaultValue="false") boolean deleted) {return objects.list(workspaceId,deleted);}
    @GetMapping("/objects/{id}") public StudioObject object(@PathVariable String id) {return objects.get(id);}
    @PostMapping("/objects") @ResponseStatus(HttpStatus.CREATED) public StudioObject create(@RequestBody ObjectInput input) {return objects.create(input);}
    @PutMapping("/objects/{id}") public StudioObject update(@PathVariable String id,@RequestBody ObjectInput input) {return objects.update(id,input);}
    @DeleteMapping("/objects/{id}") public Object delete(@PathVariable String id) {objects.delete(id);return Map.of("deleted",true);}
    @PostMapping("/objects/{id}/restore") public StudioObject restore(@PathVariable String id,@RequestBody(required=false) Map<String,Object> input) {return objects.restore(id,input==null?null:(String)input.get("name"));}
    @PostMapping("/objects/{id}/copy") public StudioObject copy(@PathVariable String id,@RequestBody(required=false) Map<String,Object> input) {return objects.copy(id,input==null?null:(String)input.get("name"),input==null?null:(String)input.get("parentId"),input!=null&&input.containsKey("parentId"));}
    @GetMapping("/objects/{id}/versions") public Object versions(@PathVariable String id) {return objects.versions(id);}
    @PostMapping("/objects/{id}/versions/{versionId}/restore") public StudioObject versionRestore(@PathVariable String id,@PathVariable String versionId,@RequestBody Map<String,Object> input) {return objects.restoreVersion(id,versionId,input.get("version") instanceof Number n?n.intValue():null);}
    @GetMapping("/runs") public Object runs(@RequestParam(defaultValue="local-workspace") String workspaceId,@RequestParam(defaultValue="false") boolean summary,@RequestParam(defaultValue="1") int page,@RequestParam(defaultValue="100") int pageSize,@RequestParam(defaultValue="") String status,@RequestParam(defaultValue="") String search) {return executions.list(workspaceId,summary,page,pageSize,status,search);}
    @GetMapping("/runs/{id}") public Object runDetail(@PathVariable String id) {return executions.detail(id);}
    @GetMapping("/runs/{id}/results") public Object runResults(@PathVariable String id,@RequestParam(defaultValue="1") int page,@RequestParam(defaultValue="100") int pageSize,@RequestParam(required=false) Integer statementIndex) {return executions.results(id,page,pageSize,statementIndex);}
    @PostMapping("/runs") @ResponseStatus(HttpStatus.CREATED) public Object run(@RequestBody Map<String,Object> input) {var options=runOptions(input);if(input.containsKey("debugParameters"))options.put("debugParameters",input.get("debugParameters"));return executions.submit(Objects.toString(input.get("objectId"),""),Objects.toString(input.get("mode"),"MANUAL"),Boolean.TRUE.equals(input.get("simulateFailure")),expectedVersion(input),nodeVersions(input),options);}
    public static Map<String,Object> runOptions(Map<String,Object> input){Map<String,Object> options=new LinkedHashMap<>();if(input!=null)for(String key:List.of("businessDate","sourceCutoffAt"))if(input.containsKey(key))options.put(key,input.get(key));return options;}
    public static Integer expectedVersion(Map<String,Object> input) {Object value=input.get("expectedVersion");if(value==null)return null;if(!(value instanceof Number n)||n.doubleValue()!=n.intValue()||n.intValue()<1)throw com.fake.dataworks.exception.StudioException.bad("INVALID_VERSION","预期版本须为正整数");return n.intValue();}
    @SuppressWarnings("unchecked") public static Map<String,Object> nodeVersions(Map<String,Object> input) {return input.get("expectedNodeVersions") instanceof Map<?,?> map?(Map<String,Object>)map:Map.of();}
    @PostMapping("/runs/{id}/stop") public Object stop(@PathVariable String id) {return executions.stop(id);}
    @GetMapping("/records") public Object records(@RequestParam(defaultValue="local-workspace") String workspaceId,@RequestParam(defaultValue="") String kind) {return workspaces.records(workspaceId,kind);}
    @PostMapping("/records") @ResponseStatus(HttpStatus.CREATED) public Object record(@RequestBody Map<String,Object> input) {return records.create(input);}
    @PatchMapping("/records/{id}") public Object patchRecord(@PathVariable String id,@RequestBody Map<String,Object> input) {return records.update(id,input);}
    @GetMapping("/preferences") public Object preferences() {return workspaces.preferences();}
    @PutMapping("/preferences") public Object preferences(@RequestBody Map<String,Object> input) {return workspaces.preferences(input);}
    @PostMapping(value="/objects/{id}/file",consumes=MediaType.MULTIPART_FORM_DATA_VALUE) public Object upload(@PathVariable String id,@RequestPart("file") MultipartFile file) throws Exception {return files.upload(id,file);}
    @GetMapping("/objects/{id}/file") public ResponseEntity<?> download(@PathVariable String id) {var metadata=files.metadata(id);return ResponseEntity.ok().header(HttpHeaders.CONTENT_DISPOSITION,ContentDisposition.attachment().filename(metadata.get("name").toString(),StandardCharsets.UTF_8).build().toString()).contentType(MediaType.APPLICATION_OCTET_STREAM).body(files.download(id));}
}
