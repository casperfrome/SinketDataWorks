package com.fake.dataworks.repository;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.domain.StudioObject;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class StudioRepository {
    private final JdbcTemplate jdbc;
    private final JsonCodec json;
    private final RowMapper<StudioObject> mapper;
    public StudioRepository(JdbcTemplate jdbc, JsonCodec json) {
        this.jdbc=jdbc; this.json=json;
        this.mapper=(r,n)->new StudioObject(r.getString("id"),r.getString("workspace_id"),r.getString("parent_id"),r.getString("kind"),r.getString("node_type"),r.getString("name"),r.getString("description"),r.getString("content"),json.map(r.getString("config_json")),json.strings(r.getString("tags_json")),r.getBoolean("favorite"),r.getBoolean("deleted"),r.getInt("version"),r.getString("owner"),r.getString("updated_at"));
    }
    public List<Map<String,Object>> workspaces() { return jdbc.query("SELECT * FROM dw_workspace WHERE workspace_type IN ('DEFAULT','USER') ORDER BY workspace_type='DEFAULT' DESC,name,id",(r,n)->Map.of("id",r.getString("id"),"name",r.getString("name"),"code",r.getString("code"),"region",r.getString("region"),"type",r.getString("workspace_type"))); }
    public void insertWorkspace(Map<String,Object> workspace) { jdbc.update("INSERT INTO dw_workspace(id,name,code,region,workspace_type) VALUES(?,?,?,?,'USER')",workspace.get("id"),workspace.get("name"),workspace.get("code"),workspace.get("region")); }
    public boolean workspaceExists(String id) { return jdbc.queryForObject("SELECT COUNT(*) FROM dw_workspace WHERE id=?",Integer.class,id)>0; }
    public void lockWorkspace(String id) { jdbc.queryForObject("SELECT id FROM dw_workspace WHERE id=? FOR UPDATE",String.class,id); }
    public Optional<StudioObject> find(String id) { return jdbc.query("SELECT * FROM dw_object WHERE id=?",mapper,id).stream().findFirst(); }
    public List<StudioObject> list(String workspaceId,boolean deleted) { return jdbc.query("SELECT * FROM dw_object WHERE workspace_id=? AND deleted=? ORDER BY kind,name",mapper,workspaceId,deleted); }
    public List<StudioObject> children(String id,boolean deleted) { return jdbc.query("SELECT * FROM dw_object WHERE parent_id=? AND deleted=? ORDER BY name",mapper,id,deleted); }
    public List<StudioObject> deletedGroup(String group) { return jdbc.query("SELECT * FROM dw_object WHERE deletion_group=? AND deleted=TRUE",mapper,group); }
    public String deletionGroup(String id) { return jdbc.queryForObject("SELECT deletion_group FROM dw_object WHERE id=?",String.class,id); }
    public boolean nameExists(String workspace,String parent,String name,String except) { return jdbc.queryForObject("SELECT COUNT(*) FROM dw_object WHERE workspace_id=? AND parent_key=? AND name=? AND deleted=FALSE AND id<>?",Integer.class,workspace,parent==null?"":parent,name,except==null?"":except)>0; }
    public void insert(StudioObject o) {
        jdbc.update("INSERT INTO dw_object(id,workspace_id,parent_id,kind,node_type,name,description,content,config_json,tags_json,favorite,deleted,version,owner,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",o.id(),o.workspaceId(),o.parentId(),o.kind(),o.nodeType(),o.name(),o.description(),o.content(),json.write(o.config()),json.write(o.tags()),o.favorite(),o.deleted(),o.version(),o.owner(),o.updatedAt());
    }
    public boolean update(StudioObject o,int expectedVersion) {
        return jdbc.update("UPDATE dw_object SET parent_id=?,node_type=?,name=?,description=?,content=?,config_json=?,tags_json=?,favorite=?,version=?,owner=?,updated_at=? WHERE id=? AND version=? AND deleted=FALSE",o.parentId(),o.nodeType(),o.name(),o.description(),o.content(),json.write(o.config()),json.write(o.tags()),o.favorite(),o.version(),o.owner(),o.updatedAt(),o.id(),expectedVersion)==1;
    }
    public void softDelete(String id,String group,String now) { jdbc.update("UPDATE dw_object SET deleted=TRUE,deletion_group=?,version=version+1,updated_at=? WHERE id=?",group,now,id); }
    public void restore(String id,String name,String now) { jdbc.update("UPDATE dw_object SET deleted=FALSE,deletion_group=NULL,name=?,version=version+1,updated_at=? WHERE id=?",name,now,id); }
    public void snapshot(StudioObject o) { jdbc.update("INSERT INTO dw_version(id,object_id,version,name,content,config_json,created_at) VALUES(?,?,?,?,?,?,?)",UUID.randomUUID().toString(),o.id(),o.version(),o.name(),o.content(),json.write(o.config()),o.updatedAt()); }
    public List<Map<String,Object>> versions(String id) { return jdbc.query("SELECT * FROM dw_version WHERE object_id=? ORDER BY version DESC",(r,n)->Map.of("id",r.getString("id"),"objectId",r.getString("object_id"),"version",r.getInt("version"),"name",r.getString("name"),"content",r.getString("content"),"config",json.map(r.getString("config_json")),"createdAt",r.getString("created_at")),id); }
    public void insertRun(Map<String,Object> run,StudioObject snapshot) { jdbc.update("INSERT INTO dw_run(id,workspace_id,object_id,status,data_json,snapshot_json,created_at,parent_run_id,graph_node_id) VALUES(?,?,?,?,?,?,?,?,?)",run.get("id"),run.get("workspaceId"),run.get("objectId"),run.get("status"),json.write(run),json.write(snapshot),run.get("createdAt"),run.get("parentRunId"),run.get("graphNodeId")); }
    public void updateRun(Map<String,Object> run) { jdbc.update("UPDATE dw_run SET status=?,data_json=? WHERE id=?",run.get("status"),json.write(run),run.get("id")); }
    public boolean transitionRun(Map<String,Object> run,String expected) {
        return jdbc.update("UPDATE dw_run SET status=?,data_json=? WHERE id=? AND status=?",run.get("status"),json.write(run),run.get("id"),expected)==1;
    }
    public Map<String,Object> runSnapshot(String id) {return jdbc.queryForObject("SELECT snapshot_json FROM dw_run WHERE id=?",(r,n)->json.map(r.getString(1)),id);}
    public Map<String,Object> runSummaries(String workspace,int page,int pageSize,String status,String search) {
        String where=" WHERE workspace_id=? AND parent_run_id IS NULL AND (?='' OR status=?) AND (?='' OR LOCATE(LOWER(?),LOWER(JSON_UNQUOTE(JSON_EXTRACT(data_json,'$.objectName'))))>0 OR LOCATE(LOWER(?),LOWER(id))>0)";
        var items=jdbc.query("SELECT JSON_REMOVE(data_json,'$.rows','$.columns','$.logs') FROM dw_run"+where+" ORDER BY created_at DESC,id DESC LIMIT ? OFFSET ?",(r,n)->json.map(r.getString(1)),workspace,status,status,search,search,search,pageSize,(page-1)*pageSize);
        var counts=jdbc.query("SELECT status,COUNT(*) FROM dw_run WHERE workspace_id=? AND parent_run_id IS NULL GROUP BY status",(r,n)->Map.entry(r.getString(1),r.getLong(2)),workspace);
        Map<String,Long> stats=new LinkedHashMap<>();counts.forEach(e->stats.put(e.getKey(),e.getValue()));
        long total=jdbc.queryForObject("SELECT COUNT(*) FROM dw_run"+where,Long.class,workspace,status,status,search,search,search);
        return Map.of("items",items,"total",total,"page",page,"pageSize",pageSize,"stats",stats);
    }
    public void saveResult(String id,Map<String,Object> result) {jdbc.update("INSERT INTO dw_run_result(run_id,data_json) VALUES(?,?) ON DUPLICATE KEY UPDATE data_json=?",id,json.write(result),json.write(result));}
    public Optional<Map<String,Object>> result(String id) {return jdbc.query("SELECT data_json FROM dw_run_result WHERE run_id=?",(r,n)->json.map(r.getString(1)),id).stream().findFirst();}
    public Optional<Map<String,Object>> run(String id) { return jdbc.query("SELECT data_json FROM dw_run WHERE id=?",(r,n)->json.map(r.getString(1)),id).stream().findFirst(); }
    public Map<String,Map<String,Object>> runsByIds(Collection<String> ids) {
        var unique=new ArrayList<>(new LinkedHashSet<>(ids));Map<String,Map<String,Object>> result=new HashMap<>();
        for(int offset=0;offset<unique.size();offset+=5000){var chunk=unique.subList(offset,Math.min(unique.size(),offset+5000));String placeholders=String.join(",",Collections.nCopies(chunk.size(),"?"));for(var run:jdbc.query("SELECT JSON_REMOVE(data_json,'$.rows','$.columns','$.logs') FROM dw_run WHERE id IN ("+placeholders+")",(r,n)->json.map(r.getString(1)),chunk.toArray()))result.put(run.get("id").toString(),run);}return result;
    }
    public List<Map<String,Object>> runs(String workspace) { return jdbc.query("SELECT data_json FROM dw_run WHERE workspace_id=? ORDER BY created_at DESC",(r,n)->json.map(r.getString(1)),workspace); }
    public List<Map<String,Object>> unfinishedRuns() { return jdbc.query("SELECT data_json FROM dw_run WHERE status IN ('WAITING','QUEUED','RUNNING','RECOVERING')",(r,n)->json.map(r.getString(1))); }
    public List<Map<String,Object>> topRuns(String workspace) { return jdbc.query("SELECT data_json FROM dw_run WHERE workspace_id=? AND parent_run_id IS NULL ORDER BY created_at DESC",(r,n)->json.map(r.getString(1)),workspace); }
    public boolean hasActiveTaskRun(String workspace,String task,String excludedRunId) {
        return !jdbc.query("SELECT id FROM dw_run WHERE workspace_id=? AND object_id=? AND status IN ('QUEUED','RUNNING','RECOVERING') AND id<>? LIMIT 1",(r,n)->r.getString(1),workspace,task,Objects.toString(excludedRunId,"")).isEmpty();
    }
    public List<Map<String,Object>> childRuns(String id) { return jdbc.query("SELECT data_json FROM dw_run WHERE parent_run_id=? ORDER BY created_at,id",(r,n)->json.map(r.getString(1)),id); }
    public int nextReleaseNo(String workflow) { return jdbc.queryForObject("SELECT COALESCE(MAX(release_no),0)+1 FROM dw_workflow_release WHERE workflow_id=?",Integer.class,workflow); }
    public void insertRelease(Map<String,Object> release,Object bundle) {
        jdbc.update("INSERT INTO dw_workflow_release(id,workspace_id,workflow_id,release_no,workflow_version,name,note,created_at,bundle_json) VALUES(?,?,?,?,?,?,?,?,?)",release.get("id"),release.get("workspaceId"),release.get("workflowId"),release.get("releaseNo"),release.get("workflowVersion"),release.get("name"),release.get("note"),release.get("createdAt"),json.write(bundle));
    }
    private Map<String,Object> releaseRow(java.sql.ResultSet r,boolean detail) throws java.sql.SQLException {
        Map<String,Object> result=new LinkedHashMap<>();result.put("id",r.getString("id"));result.put("workspaceId",r.getString("workspace_id"));result.put("workflowId",r.getString("workflow_id"));result.put("releaseNo",r.getInt("release_no"));result.put("workflowVersion",r.getInt("workflow_version"));result.put("name",r.getString("name"));result.put("note",r.getString("note"));result.put("createdAt",r.getString("created_at"));
        if(detail){var bundle=json.map(r.getString("bundle_json"));result.put("bundle",bundle);result.put("containsWrites",bundle.getOrDefault("containsWrites",bundle.get("schemaVersion") instanceof Number n&&n.intValue()>=4));}return result;
    }
    public Optional<Map<String,Object>> release(String id) { return jdbc.query("SELECT * FROM dw_workflow_release WHERE id=?",(r,n)->releaseRow(r,true),id).stream().findFirst(); }
    public List<Map<String,Object>> releases(String workspace,String workflow) { return jdbc.query("SELECT id,workspace_id,workflow_id,release_no,workflow_version,name,note,created_at FROM dw_workflow_release WHERE workspace_id=? AND (?='' OR workflow_id=?) ORDER BY created_at DESC,release_no DESC",(r,n)->releaseRow(r,false),workspace,workflow,workflow); }
    public List<Map<String,Object>> records(String workspace,String kind) { return jdbc.query("SELECT data_json FROM dw_record WHERE workspace_id=? AND (?='' OR kind=?) ORDER BY created_at DESC",(r,n)->json.map(r.getString(1)),workspace,kind,kind); }
    public Optional<Map<String,Object>> record(String id) { return jdbc.query("SELECT data_json FROM dw_record WHERE id=?",(r,n)->json.map(r.getString(1)),id).stream().findFirst(); }
    public void insertRecord(Map<String,Object> r) { jdbc.update("INSERT INTO dw_record(id,workspace_id,kind,data_json,created_at) VALUES(?,?,?,?,?)",r.get("id"),r.get("workspaceId"),r.get("kind"),json.write(r),r.get("createdAt")); }
    public void updateRecord(Map<String,Object> r) { jdbc.update("UPDATE dw_record SET data_json=? WHERE id=?",json.write(r),r.get("id")); }
    public Map<String,Object> preferences() { return jdbc.query("SELECT data_json FROM dw_preference WHERE id='local_admin'",(r,n)->json.map(r.getString(1))).stream().findFirst().orElse(Map.of()); }
    public void preferences(Map<String,Object> value) { jdbc.update("INSERT INTO dw_preference(id,data_json) VALUES('local_admin',?) ON DUPLICATE KEY UPDATE data_json=?",json.write(value),json.write(value)); }
    public void file(String id,String stored,String original,String type,long size) { jdbc.update("INSERT INTO dw_file(object_id,storage_name,original_name,content_type,file_size) VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE storage_name=?,original_name=?,content_type=?,file_size=?",id,stored,original,type,size,stored,original,type,size); }
    public Optional<Map<String,Object>> file(String id) { return jdbc.query("SELECT * FROM dw_file WHERE object_id=?",(r,n)->Map.<String,Object>of("storageName",r.getString("storage_name"),"name",r.getString("original_name"),"contentType",r.getString("content_type"),"size",r.getLong("file_size")),id).stream().findFirst(); }
    public void copyFile(String sourceId,String targetId) { jdbc.update("INSERT INTO dw_file(object_id,storage_name,original_name,content_type,file_size) SELECT ?,storage_name,original_name,content_type,file_size FROM dw_file WHERE object_id=?",targetId,sourceId); }
    public boolean storageReferenced(String name) { return jdbc.queryForObject("SELECT COUNT(*) FROM dw_file WHERE storage_name=?",Integer.class,name)>0; }
}
