package com.fake.dataworks.service;

import com.fake.dataworks.repository.StudioRepository;
import com.fake.dataworks.dto.WorkspaceInput;
import com.fake.dataworks.exception.StudioException;
import java.util.*;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Read models and local single-user preferences still pass through the service boundary. */
@Service
public class WorkspaceService {
    private final StudioRepository repo;private final ObjectService objects;
    public WorkspaceService(StudioRepository repo,ObjectService objects) {this.repo=repo;this.objects=objects;}
    public List<Map<String,Object>> workspaces() {return repo.workspaces();}
    @Transactional public Map<String,Object> create(WorkspaceInput input) {
        String name=input.name()==null?"":input.name().strip();
        String code=input.code()==null?"":input.code().strip();
        if(name.isEmpty()||name.length()>100||name.codePoints().anyMatch(Character::isISOControl))
            throw StudioException.bad("INVALID_WORKSPACE_NAME","显示名须为 1–100 个字符，且不能含控制字符");
        if(!code.matches("[a-z][a-z0-9_]{2,63}"))
            throw StudioException.bad("INVALID_WORKSPACE_CODE","工作空间名称须为 3–64 位小写字母、数字或下划线，并以字母开头");
        Map<String,Object> workspace=Map.of("id",UUID.randomUUID().toString(),"name",name,"code",code,"region","本地","type","USER");
        try {repo.insertWorkspace(workspace);}
        catch(DuplicateKeyException e) {throw StudioException.conflict("WORKSPACE_CODE_CONFLICT","该工作空间名称已被使用，请换一个名称");}
        return workspace;
    }
    public List<Map<String,Object>> runs(String workspace) {objects.workspace(workspace);return repo.runs(workspace);}
    public List<Map<String,Object>> records(String workspace,String kind) {objects.workspace(workspace);return kind.isEmpty()||"RELEASE".equals(kind)?repo.records(workspace,"RELEASE"):List.of();}
    public Map<String,Object> preferences() {return repo.preferences();}
    @Transactional public Map<String,Object> preferences(Map<String,Object> input) {repo.preferences(input);return input;}
}
