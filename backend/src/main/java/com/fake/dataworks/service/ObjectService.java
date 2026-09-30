package com.fake.dataworks.service;

import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.dto.ObjectInput;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.StudioRepository;
import java.time.Instant;
import java.util.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ObjectService {
    private static final Set<String> KINDS=Set.of("FOLDER","NODE","WORKFLOW","NOTEBOOK","TABLE","RESOURCE","FUNCTION","COMPONENT","PERSONAL","ENVIRONMENT");
    private static final Set<String> CONTAINERS=Set.of("FOLDER","PERSONAL","WORKFLOW");
    private final StudioRepository repo;
    private final GraphValidator graphs;
    public ObjectService(StudioRepository repo,GraphValidator graphs) { this.repo=repo; this.graphs=graphs; }
    public StudioObject get(String id) { return repo.find(id).orElseThrow(()->StudioException.missing("开发对象不存在")); }
    public StudioObject active(String id) { StudioObject o=get(id); if(o.deleted()) throw StudioException.bad("OBJECT_DELETED","对象已在回收站，请先恢复"); return o; }
    public List<StudioObject> list(String workspace,boolean deleted) { workspace(workspace); return repo.list(workspace,deleted); }
    public void workspace(String id) { if(id==null || !repo.workspaceExists(id)) throw StudioException.missing("工作空间不存在"); }
    @Transactional
    public StudioObject create(ObjectInput input) {
        String workspace=input.workspaceId()==null?"local-workspace":input.workspaceId(); workspace(workspace); repo.lockWorkspace(workspace);
        String kind=input.kind()==null?"NODE":input.kind(); if(!KINDS.contains(kind)) throw StudioException.bad("INVALID_KIND","未知对象类型");
        String name=name(input.name()); String parent=parent(input.parentId()); validateParent(workspace,parent,null); unique(workspace,parent,name,null);
        Map<String,Object> config=input.config()==null?Map.of():input.config(); graphs.validate(config); ScheduleParameters.validateConfig(config);
        StudioObject o=new StudioObject(UUID.randomUUID().toString(),workspace,parent,kind,value(input.nodeType(),"MaxCompute SQL"),name,value(input.description(),""),value(input.content(),""),config,input.tags()==null?List.of():input.tags(),Boolean.TRUE.equals(input.favorite()),false,1,owner(value(input.owner(),"local_admin")),now());
        repo.insert(o); repo.snapshot(o); return o;
    }
    @Transactional
    public StudioObject update(String id,ObjectInput input) {
        StudioObject initial=active(id); repo.lockWorkspace(initial.workspaceId()); StudioObject old=active(id);
        if(input.version()==null||input.version()!=old.version()) throw StudioException.conflict("VERSION_CONFLICT","对象已被修改，请刷新并核对后重新保存");
        if(input.workspaceId()!=null&&!old.workspaceId().equals(input.workspaceId())) throw StudioException.bad("WORKSPACE_MISMATCH","不能跨工作空间移动对象");
        if(input.kind()!=null&&!old.kind().equals(input.kind())) throw StudioException.bad("KIND_IMMUTABLE","不能修改已有对象的类型");
        String name=input.name()==null?old.name():name(input.name()),parent=parent(input.parentId());
        validateParent(old.workspaceId(),parent,id); unique(old.workspaceId(),parent,name,id);
        Map<String,Object> config=input.config()==null?old.config():input.config(); graphs.validate(config); ScheduleParameters.validateConfig(config);
        StudioObject o=new StudioObject(id,old.workspaceId(),parent,old.kind(),value(input.nodeType(),old.nodeType()),name,value(input.description(),old.description()),value(input.content(),old.content()),config,input.tags()==null?old.tags():input.tags(),input.favorite()==null?old.favorite():input.favorite(),false,old.version()+1,owner(value(input.owner(),old.owner())),now());
        if(!repo.update(o,old.version())) throw StudioException.conflict("VERSION_CONFLICT","保存期间对象已被修改，请重新加载");
        repo.snapshot(o); return o;
    }
    @Transactional
    public void delete(String id) {
        StudioObject o=active(id); repo.lockWorkspace(o.workspaceId()); deleteTree(id,UUID.randomUUID().toString());
    }
    private void deleteTree(String id,String group) { for(StudioObject child:repo.children(id,false)) deleteTree(child.id(),group); repo.softDelete(id,group,now()); }
    @Transactional
    public StudioObject restore(String id,String requestedName) {
        StudioObject target=get(id); repo.lockWorkspace(target.workspaceId());
        if(!target.deleted()) return target;
        String group=repo.deletionGroup(id);
        Set<String> ids=new HashSet<>(); collectDeletedDescendants(id,group,ids);
        List<StudioObject> items=repo.deletedGroup(group).stream().filter(o->ids.contains(o.id())).toList();
        String newName=requestedName==null?target.name():name(requestedName);
        if(target.parentId()!=null && get(target.parentId()).deleted()) throw StudioException.conflict("PARENT_DELETED","请先恢复父目录");
        for(StudioObject item:items) unique(item.workspaceId(),item.parentId(),item.id().equals(id)?newName:item.name(),item.id());
        for(StudioObject item:items) { repo.restore(item.id(),item.id().equals(id)?newName:item.name(),now()); repo.snapshot(get(item.id())); }
        return get(id);
    }
    private void collectDeletedDescendants(String id,String group,Set<String> ids) { ids.add(id); for(StudioObject child:repo.children(id,true)) if(Objects.equals(group,repo.deletionGroup(child.id()))) collectDeletedDescendants(child.id(),group,ids); }
    @Transactional
    public StudioObject copy(String id,String requestedName,String parentId,boolean parentSpecified) {
        StudioObject source=active(id); repo.lockWorkspace(source.workspaceId()); String parent=parentSpecified?parent(parentId):source.parentId();
        validateParent(source.workspaceId(),parent,null);
        // Snapshot source descendants before inserting, including when destination is inside this folder.
        List<StudioObject> descendants=new ArrayList<>(); collectActive(source.id(),descendants);
        String name=requestedName==null?copyName(source.workspaceId(),parent,source.name()):name(requestedName); unique(source.workspaceId(),parent,name,null);
        StudioObject root=cloneObject(source,parent,name); Map<String,String> ids=new HashMap<>(); ids.put(id,root.id());
        for(StudioObject child:descendants) { StudioObject clone=cloneObject(child,ids.get(child.parentId()),child.name()); ids.put(child.id(),clone.id()); }
        return root;
    }
    private void collectActive(String id,List<StudioObject> result) { for(StudioObject child:repo.children(id,false)) { result.add(child); collectActive(child.id(),result); } }
    private StudioObject cloneObject(StudioObject source,String parent,String name) {
        StudioObject o=new StudioObject(UUID.randomUUID().toString(),source.workspaceId(),parent,source.kind(),source.nodeType(),name,source.description(),source.content(),source.config(),source.tags(),false,false,1,"local_admin",now()); repo.insert(o); repo.snapshot(o); repo.copyFile(source.id(),o.id()); return o;
    }
    private String copyName(String workspace,String parent,String original) { String base=original.length()>240?original.substring(0,240):original; String name=base+"_副本"; int i=2; while(repo.nameExists(workspace,parent,name,null)) name=base+"_副本"+(i++); return name; }
    public List<Map<String,Object>> versions(String id) { get(id); return repo.versions(id); }
    @SuppressWarnings("unchecked")
    @Transactional
    public StudioObject restoreVersion(String id,String versionId,Integer expected) {
        StudioObject old=active(id); Map<String,Object> v=repo.versions(id).stream().filter(it->it.get("id").equals(versionId)).findFirst().orElseThrow(()->StudioException.missing("版本不存在"));
        return update(id,new ObjectInput(old.workspaceId(),old.parentId(),old.kind(),old.nodeType(),(String)v.get("name"),old.description(),(String)v.get("content"),(Map<String,Object>)v.get("config"),old.tags(),old.favorite(),expected));
    }
    private void validateParent(String workspace,String parent,String self) {
        if(parent==null) return;
        Set<String> seen=new HashSet<>(); String current=parent;
        StudioObject p=active(parent);
        if(!workspace.equals(p.workspaceId())) throw StudioException.bad("WORKSPACE_MISMATCH","父目录不属于当前工作空间");
        if(!CONTAINERS.contains(p.kind())) throw StudioException.bad("INVALID_PARENT","目标对象不是目录或工作流");
        while(current!=null) { if(current.equals(self)||!seen.add(current)) throw StudioException.bad("DIRECTORY_CYCLE","不能将目录移动到自身或子目录"); current=get(current).parentId(); }
    }
    private void unique(String workspace,String parent,String name,String except) { if(repo.nameExists(workspace,parent,name,except)) throw StudioException.conflict("NAME_CONFLICT","同一目录下已存在名称为「"+name+"」的对象"); }
    private String name(String name) { if(name==null||name.isBlank()||name.trim().length()>255||name.contains("/")||name.contains("\\")) throw StudioException.bad("INVALID_NAME","名称须为 1–255 个字符，且不能包含斜杠"); return name.trim(); }
    private String owner(String value) { if(value.isBlank()||value.trim().length()>100||value.codePoints().anyMatch(Character::isISOControl)) throw StudioException.bad("INVALID_OWNER","负责人须为 1–100 个字符，且不能包含控制字符"); return value.trim(); }
    private String parent(String value) { return value==null||value.isBlank()?null:value; }
    private String value(String value,String fallback) { return value==null?fallback:value; }
    public static String now() { return Instant.now().toString(); }
}
