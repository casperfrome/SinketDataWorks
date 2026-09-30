package com.fake.dataworks.controller;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.service.*;
import java.util.Map;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1")
public class SyncController {
    private final SyncExecutionService sync;private final ObjectService objects;private final JsonCodec json;
    public SyncController(SyncExecutionService sync,ObjectService objects,JsonCodec json){this.sync=sync;this.objects=objects;this.json=json;}
    @PostMapping("/sync/validate") public Object validate(@RequestBody Map<String,Object> input){
        var object=json.read(json.write(input),StudioObject.class);objects.workspace(object.workspaceId());var p=sync.prepare(object);return Map.of("valid",true,"targetModel",p.targetModel(),"message","连接、字段映射、类型与写入模式校验通过");
    }
    @GetMapping("/runs/{id}/sync/batches") public Object batches(@PathVariable String id){return sync.batches(id);}
    @PostMapping("/runs/{id}/sync/resolve") public Object resolve(@PathVariable String id,@RequestBody Map<String,String> input){return sync.resolve(id,input.get("note"));}
}
