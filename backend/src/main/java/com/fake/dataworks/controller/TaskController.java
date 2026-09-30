package com.fake.dataworks.controller;
import com.fake.dataworks.service.*;
import java.util.*;
import org.springframework.web.bind.annotation.*;

@RestController @RequestMapping("/api/v1")
public class TaskController {
    private final TaskService tasks;private final TaskScheduleService schedules;
    public TaskController(TaskService tasks,TaskScheduleService schedules){this.tasks=tasks;this.schedules=schedules;}
    @GetMapping("/tasks/{id}/releases") public Object releases(@PathVariable String id){return tasks.releases(id);}
    @PostMapping("/tasks/{id}/releases") public Object publish(@PathVariable String id,@RequestBody Map<String,Object> input){return tasks.publish(id,input.get("expectedVersion") instanceof Number n&&n.doubleValue()==n.intValue()?n.intValue():null,Objects.toString(input.get("note"),""));}
    @GetMapping("/task-releases/{id}") public Object release(@PathVariable String id){return tasks.release(id);}
    @PostMapping("/task-releases/{id}/runs") public Object run(@PathVariable String id,@RequestBody(required=false) Map<String,Object> input){Map<String,Object> safe=new LinkedHashMap<>();if(input!=null&&input.get("businessDate")!=null)safe.put("businessDate",input.get("businessDate"));return tasks.startRelease(id,safe);}
    @GetMapping("/task-schedules") public Object schedules(@RequestParam String workspaceId,@RequestParam(defaultValue="") String taskId){return schedules.list(workspaceId,taskId);}
    @PostMapping("/tasks/{id}/schedule") public Object save(@PathVariable String id,@RequestBody Map<String,Object> input){return schedules.save(id,null,input);}
    @PutMapping("/task-schedules/{id}") public Object update(@PathVariable String id,@RequestBody Map<String,Object> input){return schedules.save(Objects.toString(input.get("taskId"),""),id,input);}
    @PostMapping("/task-schedules/preview") public Object preview(@RequestBody Map<String,Object> input){return schedules.preview(Objects.toString(input.get("taskId"),""),input);}
    @GetMapping("/task-schedules/{id}/triggers") public Object triggers(@PathVariable String id,@RequestParam(defaultValue="1") int page,@RequestParam(defaultValue="20") int pageSize,@RequestParam(defaultValue="") String status){return schedules.triggers(id,page,pageSize,status);}
    @PostMapping("/task-triggers/{id}/rerun") public Object rerun(@PathVariable String id){return schedules.rerun(id);}
}
