package com.fake.dataworks.controller;

import com.fake.dataworks.service.WorkflowService;
import java.util.*;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1")
public class WorkflowController {
    private final WorkflowService workflows;
    public WorkflowController(WorkflowService workflows) {this.workflows=workflows;}
    @PostMapping("/workflows/{id}/releases") @ResponseStatus(HttpStatus.CREATED)
    public Object publish(@PathVariable String id,@RequestBody Map<String,Object> input) {
        return workflows.publish(id,StudioController.expectedVersion(input),StudioController.nodeVersions(input),Objects.toString(input.get("note"),""));
    }
    @GetMapping("/workflow-releases") public Object releases(@RequestParam String workspaceId,@RequestParam(defaultValue="") String workflowId) {return workflows.releases(workspaceId,workflowId);}
    @GetMapping("/workflow-releases/{id}") public Object release(@PathVariable String id) {return workflows.release(id);}
    @PostMapping("/workflow-releases/{id}/runs") @ResponseStatus(HttpStatus.CREATED) public Object run(@PathVariable String id,@RequestBody(required=false) Map<String,Object> input) {return workflows.startRelease(id,StudioController.runOptions(input));}
    @GetMapping("/runs/{id}/nodes") public Object nodes(@PathVariable String id) {return workflows.nodes(id);}
}
