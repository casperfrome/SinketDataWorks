package com.fake.dataworks.dto;

import java.util.List;
import java.util.Map;

/** PUT accepts a full object; optional fields inherit previous values except parentId (null means root). */
public record ObjectInput(String workspaceId, String parentId, String kind, String nodeType, String name,
                          String description, String content, Map<String,Object> config, List<String> tags,
                          Boolean favorite, Integer version, String owner) {
    public ObjectInput(String workspaceId,String parentId,String kind,String nodeType,String name,String description,String content,Map<String,Object> config,List<String> tags,Boolean favorite,Integer version) {
        this(workspaceId,parentId,kind,nodeType,name,description,content,config,tags,favorite,version,null);
    }
}
