package com.fake.dataworks.controller;

import com.fake.dataworks.service.DatasourceService;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/datasources")
public class DatasourceController {
    private final DatasourceService sources;
    public DatasourceController(DatasourceService sources) {this.sources=sources;}
    @GetMapping public Object list(@RequestParam(defaultValue="local-workspace") String workspaceId) {return sources.list(workspaceId);}
    @GetMapping("/{id}") public Object get(@PathVariable String id) {return sources.get(id).publicView();}
    @PostMapping @ResponseStatus(HttpStatus.CREATED) public Object create(@RequestBody Map<String,Object> input) {return sources.save(null,input);}
    @PutMapping("/{id}") public Object update(@PathVariable String id,@RequestBody Map<String,Object> input) {return sources.save(id,input);}
    @PostMapping("/test") public Object test(@RequestBody Map<String,Object> input) {return sources.test(sources.input(null,input));}
    @PostMapping("/{id}/test") public Object test(@PathVariable String id,@RequestBody(required=false) Map<String,Object> input) {return sources.test(input==null||input.isEmpty()?sources.get(id):sources.input(id,input));}
    @GetMapping("/{id}/tables") public Object tables(@PathVariable String id) {return sources.tables(id);}
    @GetMapping("/{id}/tables/{table}/columns") public Object columns(@PathVariable String id,@PathVariable String table) {return sources.columns(id,table);}
    @GetMapping("/{id}/tables/{table}/sync-metadata") public Object syncMetadata(@PathVariable String id,@PathVariable String table) {return sources.syncMetadata(id,table);}
}
