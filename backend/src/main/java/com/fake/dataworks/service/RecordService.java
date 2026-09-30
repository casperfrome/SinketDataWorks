package com.fake.dataworks.service;

import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.StudioRepository;
import java.util.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class RecordService {
    private static final Set<String> KINDS=Set.of("RELEASE");
    private final StudioRepository repo; private final ObjectService objects;
    public RecordService(StudioRepository repo,ObjectService objects) { this.repo=repo;this.objects=objects; }
    @Transactional
    public Map<String,Object> create(Map<String,Object> input) {
        String workspace=Objects.toString(input.get("workspaceId"),"local-workspace"),kind=Objects.toString(input.get("kind"),""); objects.workspace(workspace);
        if(!KINDS.contains(kind)) throw StudioException.bad("INVALID_RECORD_KIND","未知记录类型");
        if(input.get("objectId")!=null) { var o=objects.active(input.get("objectId").toString());if(!o.workspaceId().equals(workspace)) throw StudioException.bad("WORKSPACE_MISMATCH","对象不属于当前工作空间"); }
        Map<String,Object> record=new LinkedHashMap<>();record.put("id",UUID.randomUUID().toString());record.put("workspaceId",workspace);record.put("objectId",input.get("objectId"));record.put("kind",kind);record.put("title",Objects.toString(input.get("title"),"本地模拟记录"));record.put("status",Objects.toString(input.get("status"),"SUCCESS"));record.put("payload",input.getOrDefault("payload",Map.of()));record.put("simulation",true);record.put("owner","local_admin");record.put("createdAt",ObjectService.now());
        repo.insertRecord(record);return record;
    }
    @Transactional
    public Map<String,Object> update(String id,Map<String,Object> input) {
        Map<String,Object> record=repo.record(id).orElseThrow(()->StudioException.missing("记录不存在"));
        if(input.containsKey("status")) record.put("status",input.get("status"));if(input.containsKey("payload")) record.put("payload",input.get("payload"));record.put("updatedAt",ObjectService.now());repo.updateRecord(record);return record;
    }
}
