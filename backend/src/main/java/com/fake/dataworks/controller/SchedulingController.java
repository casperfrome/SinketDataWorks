package com.fake.dataworks.controller;

import com.fake.dataworks.service.SchedulingOperationsService;
import java.util.*;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/scheduling")
public class SchedulingController {
    private final SchedulingOperationsService operations;
    public SchedulingController(SchedulingOperationsService operations) {this.operations=operations;}
    @GetMapping("/tasks") public Object tasks(@RequestParam Map<String,String> query) {return operations.tasks(query);}
    @GetMapping("/instances") public Object instances(@RequestParam Map<String,String> query) {return operations.instances(query);}
    @GetMapping("/instances/{id}") public Object instance(@PathVariable String id) {return operations.instance("",id);}
    @GetMapping("/instances/{kind}/{id}") public Object instance(@PathVariable String kind,@PathVariable String id) {return operations.instance(kind,id);}
    @PutMapping("/tasks/{id}/state") public Object state(@PathVariable String id,@RequestBody Map<String,Object> input) {return operations.state(Objects.toString(input.get("kind"),""),id,input);}
    @PostMapping("/tasks/{kind}/{id}/enabled") public Object state(@PathVariable String kind,@PathVariable String id,@RequestBody Map<String,Object> input) {return operations.state(kind,id,input);}
    @PostMapping("/instances/{id}/rerun") public Object rerun(@PathVariable String id,@RequestBody(required=false) Map<String,Object> input) {return operations.rerun("",id,input==null?Map.of():input);}
    @PostMapping("/instances/{kind}/{id}/rerun") public Object rerun(@PathVariable String kind,@PathVariable String id,@RequestBody(required=false) Map<String,Object> input) {return operations.rerun(kind,id,input==null?Map.of():input);}
    @PostMapping("/instances/{id}/stop") public Object stop(@PathVariable String id) {return operations.stop("",id);}
    @PostMapping("/instances/{kind}/{id}/stop") public Object stop(@PathVariable String kind,@PathVariable String id) {return operations.stop(kind,id);}
    @PostMapping("/backfills/preview") public Object preview(@RequestBody Map<String,Object> input) {return operations.preview(input);}
    @PostMapping("/backfills") public Object backfill(@RequestBody Map<String,Object> input) {return operations.backfill(input);}
    @GetMapping("/backfills/{batchKey}") public Object batch(@PathVariable String batchKey) {return operations.batch(batchKey);}
}
