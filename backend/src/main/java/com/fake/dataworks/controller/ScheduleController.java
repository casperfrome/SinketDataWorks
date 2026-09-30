package com.fake.dataworks.controller;

import com.fake.dataworks.service.ScheduleService;
import com.fake.dataworks.service.TaskService;
import com.fake.dataworks.repository.StudioRepository;
import java.util.*;
import org.springframework.web.bind.annotation.*;

@RestController @RequestMapping("/api/v1")
public class ScheduleController {
    private final ScheduleService schedules;private final TaskService tasks;private final StudioRepository repo;
    public ScheduleController(ScheduleService schedules,TaskService tasks,StudioRepository repo){this.schedules=schedules;this.tasks=tasks;this.repo=repo;}
    @GetMapping("/workflow-schedules") public Object list(@RequestParam String workspaceId,@RequestParam(defaultValue="") String workflowId){return schedules.list(workspaceId,workflowId);}
    @PostMapping("/workflows/{id}/schedule") public Object create(@PathVariable String id,@RequestBody Map<String,Object> input){return schedules.save(id,null,input);}
    @PutMapping("/workflow-schedules/{id}") public Object update(@PathVariable String id,@RequestBody Map<String,Object> input){return schedules.save(null,id,input);}
    @PostMapping("/workflow-schedules/preview") public Object preview(@RequestBody Map<String,Object> input){return schedules.preview(input);}
    @GetMapping("/workflow-schedules/{id}/triggers") public Object triggers(@PathVariable String id,@RequestParam(defaultValue="1") int page,@RequestParam(defaultValue="20") int pageSize,@RequestParam(defaultValue="") String status){return schedules.triggers(id,page,pageSize,status);}
    @PostMapping("/schedule-triggers/{id}/rerun") public Object rerunTrigger(@PathVariable String id){return schedules.rerunTrigger(id);}
    @PostMapping("/runs/{id}/rerun") public Object rerun(@PathVariable String id){return repo.run(id).filter(r->"TASK".equals(r.get("releaseKind"))).isPresent()?tasks.rerun(id):schedules.rerun(id);}
}
