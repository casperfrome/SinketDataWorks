package com.fake.dataworks.service;

import com.fake.dataworks.repository.StudioRepository;
import java.util.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Read models and local single-user preferences still pass through the service boundary. */
@Service
public class WorkspaceService {
    private final StudioRepository repo;private final ObjectService objects;
    public WorkspaceService(StudioRepository repo,ObjectService objects) {this.repo=repo;this.objects=objects;}
    public List<Map<String,Object>> workspaces() {return repo.workspaces();}
    public List<Map<String,Object>> runs(String workspace) {objects.workspace(workspace);return repo.runs(workspace);}
    public List<Map<String,Object>> records(String workspace,String kind) {objects.workspace(workspace);return kind.isEmpty()||"RELEASE".equals(kind)?repo.records(workspace,"RELEASE"):List.of();}
    public Map<String,Object> preferences() {return repo.preferences();}
    @Transactional public Map<String,Object> preferences(Map<String,Object> input) {repo.preferences(input);return input;}
}
