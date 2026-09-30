package com.fake.dataworks.domain;

import java.util.List;
import java.util.Map;

public record StudioObject(String id, String workspaceId, String parentId, String kind, String nodeType,
                           String name, String description, String content, Map<String,Object> config,
                           List<String> tags, boolean favorite, boolean deleted, int version, String owner,
                           String updatedAt) {}
