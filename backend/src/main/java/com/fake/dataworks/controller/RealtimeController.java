package com.fake.dataworks.controller;

import com.fake.dataworks.service.RealtimeService;
import java.util.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/realtime")
public class RealtimeController {
    private final RealtimeService realtime;
    public RealtimeController(RealtimeService realtime){this.realtime=realtime;}
    private static String text(Map<String,Object> input,String key){return Objects.toString(input.get(key),"").trim();}
    private static String workspace(Map<String,Object> input){return Objects.toString(input.get("workspaceId"),"local-workspace");}
    @SuppressWarnings("unchecked") private static Map<String,Object> map(Object value){return value instanceof Map<?,?> m?(Map<String,Object>)m:Map.of();}
    private static Map<String,Object> task(Map<String,Object> input){return input.containsKey("task")?map(input.get("task")):input;}
    private static int revision(Map<String,Object> input){Object value=input.getOrDefault("expectedRevision",input.getOrDefault("revision",task(input).get("revision")));return value instanceof Number n?n.intValue():0;}
    private static String request(Map<String,Object> input){return Objects.toString(input.get("requestId"),UUID.randomUUID().toString());}
    private static ResponseEntity<?> accepted(Object value){return ResponseEntity.accepted().body(value);}
    @GetMapping("/state") public Object state(@RequestParam(defaultValue="local-workspace") String workspaceId){return realtime.state(workspaceId);}
    @PostMapping("/tasks") @ResponseStatus(HttpStatus.CREATED) public Object createTask(@RequestBody Map<String,Object> input){return realtime.createTask(workspace(input),task(input));}
    @PutMapping("/tasks/{id}") public Object saveTask(@PathVariable String id,@RequestBody Map<String,Object> input){return realtime.saveTask(workspace(input),id,task(input),revision(input));}
    @DeleteMapping("/tasks/{id}") public Object deleteTask(@PathVariable String id,@RequestParam(defaultValue="local-workspace") String workspaceId){realtime.deleteTask(workspaceId,id);return Map.of("deleted",true);}
    @PutMapping("/tasks/{id}/draft") public Object draft(@PathVariable String id,@RequestBody Map<String,Object> input){return realtime.saveDraft(workspace(input),id,task(input),revision(input));}
    @DeleteMapping("/tasks/{id}/draft") public Object discardDraft(@PathVariable String id,@RequestParam(defaultValue="local-workspace") String workspaceId){realtime.deleteDraft(workspaceId,id);return Map.of("deleted",true);}
    @PostMapping("/tasks/{id}/copy") @ResponseStatus(HttpStatus.CREATED) public Object copyTask(@PathVariable String id,@RequestBody Map<String,Object> input){return realtime.copyTask(workspace(input),id,text(input,"name"));}
    @PostMapping("/folders") @ResponseStatus(HttpStatus.CREATED) public Object createFolder(@RequestBody Map<String,Object> input){return realtime.folder(workspace(input),null,text(input,"name"));}
    @PutMapping("/folders/{id}") public Object renameFolder(@PathVariable String id,@RequestBody Map<String,Object> input){return realtime.folder(workspace(input),id,text(input,"name"));}
    @DeleteMapping("/folders/{id}") public Object deleteFolder(@PathVariable String id,@RequestParam(defaultValue="local-workspace") String workspaceId){realtime.deleteFolder(workspaceId,id);return Map.of("deleted",true);}
    @PostMapping("/tasks/{id}/releases") public Object publish(@PathVariable String id,@RequestBody Map<String,Object> input){return accepted(realtime.publishAsync(workspace(input),id,map(input.get("task")),revision(input),text(input,"note"),request(input)));}
    @PostMapping("/validate") public Object validate(@RequestBody Map<String,Object> input){return accepted(realtime.validateAsync(workspace(input),task(input),false,request(input)));}
    @PostMapping("/plan") public Object plan(@RequestBody Map<String,Object> input){return accepted(realtime.validateAsync(workspace(input),task(input),true,request(input)));}
    @PostMapping("/preview") public Object preview(@RequestBody Map<String,Object> input){return accepted(realtime.preview(workspace(input),task(input),text(input,"query"),request(input)));}
    @GetMapping("/previews/{id}") public Object preview(@PathVariable String id,@RequestParam(defaultValue="local-workspace") String workspaceId,@RequestParam(defaultValue="0") int token){return realtime.previewResult(workspaceId,id,token);}
    @PostMapping("/previews/{id}/cancel") public Object cancelPreview(@PathVariable String id,@RequestBody Map<String,Object> input){realtime.cancelPreview(workspace(input),id);return Map.of("cancelRequested",true);}
    @PostMapping("/jobs") public Object start(@RequestBody Map<String,Object> input){return accepted(realtime.startJob(workspace(input),text(input,"releaseId"),request(input),text(input,"savepointId")));}
    @GetMapping("/jobs/{id}") public Object job(@PathVariable String id,@RequestParam(defaultValue="local-workspace") String workspaceId){return realtime.detail(workspaceId,id);}
    @PostMapping("/jobs/{id}/{action:cancel|stop|savepoint|restart|upgrade}") public Object control(@PathVariable String id,@PathVariable String action,@RequestBody Map<String,Object> input){return accepted(realtime.control(workspace(input),id,action,request(input),text(input,"targetReleaseId"),Boolean.TRUE.equals(input.get("allowFreshStart"))));}
    @GetMapping("/operations/{id}") public Object operation(@PathVariable String id,@RequestParam(defaultValue="local-workspace") String workspaceId){return realtime.operation(workspaceId,id);}
    @PostMapping("/operations/{id}/rollback") public Object rollback(@PathVariable String id,@RequestBody Map<String,Object> input){return accepted(realtime.rollback(workspace(input),id,request(input)));}
    @PostMapping("/import") public Object importState(@RequestBody Map<String,Object> input){return realtime.importState(workspace(input),text(input,"importId"),map(input.get("state")));}
}
