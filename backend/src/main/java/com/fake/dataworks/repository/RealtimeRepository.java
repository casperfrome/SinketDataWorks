package com.fake.dataworks.repository;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.exception.StudioException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.annotation.Isolation;

/** Realtime metadata stays independent of offline runs and schedules. */
@Repository
public class RealtimeRepository {
    private final JdbcTemplate jdbc;
    private final JsonCodec json;
    public RealtimeRepository(JdbcTemplate jdbc,JsonCodec json){this.jdbc=jdbc;this.json=json;}
    public void lockWorkspace(String workspace){var ids=jdbc.queryForList("SELECT id FROM dw_workspace WHERE id=? FOR UPDATE",String.class,workspace);if(ids.isEmpty())throw StudioException.missing("工作空间不存在");}
    public List<Map<String,Object>> tasks(String workspace){return documents("SELECT data_json FROM dw_realtime_task WHERE workspace_id=? AND deleted=FALSE ORDER BY name",workspace);}
    public Map<String,Object> task(String id,String workspace){return scoped("dw_realtime_task",id,workspace," AND deleted=FALSE");}
    public Optional<Map<String,Object>> existingTask(String id){return first("SELECT data_json FROM dw_realtime_task WHERE id=?",id);}
    public void insertTask(Map<String,Object> task){jdbc.update("INSERT INTO dw_realtime_task(id,workspace_id,folder_id,name,revision,data_json,updated_at) VALUES(?,?,?,?,?,?,?)",task.get("id"),task.get("workspaceId"),task.get("folderId"),task.get("name"),task.get("revision"),json.write(task),task.get("updatedAt"));}
    public void updateTask(Map<String,Object> task,int expected){if(jdbc.update("UPDATE dw_realtime_task SET folder_id=?,name=?,revision=?,data_json=?,updated_at=? WHERE id=? AND revision=? AND deleted=FALSE",task.get("folderId"),task.get("name"),task.get("revision"),json.write(task),task.get("updatedAt"),task.get("id"),expected)!=1)throw StudioException.conflict("VERSION_CONFLICT","实时任务已被修改，请重新加载");}
    public void deleteTask(String id){jdbc.update("UPDATE dw_realtime_task SET deleted=TRUE WHERE id=?",id);deleteDraft(id);}
    public Map<String,Object> drafts(String workspace){var rows=jdbc.queryForList("SELECT task_id,data_json FROM dw_realtime_draft WHERE workspace_id=?",workspace);var result=new LinkedHashMap<String,Object>();for(var row:rows)result.put(row.get("task_id").toString(),json.map(row.get("data_json").toString()));return result;}
    public void saveDraft(Map<String,Object> task){jdbc.update("INSERT INTO dw_realtime_draft(task_id,workspace_id,data_json) VALUES(?,?,?) ON DUPLICATE KEY UPDATE data_json=VALUES(data_json)",task.get("id"),task.get("workspaceId"),json.write(task));}
    public void deleteDraft(String id){jdbc.update("DELETE FROM dw_realtime_draft WHERE task_id=?",id);}
    public List<Map<String,Object>> folders(String workspace){return jdbc.query("SELECT id,name FROM dw_realtime_folder WHERE workspace_id=? ORDER BY name",(r,n)->new LinkedHashMap<>(Map.of("id",r.getString(1),"name",r.getString(2))),workspace);}
    public void insertFolder(String id,String workspace,String name){jdbc.update("INSERT INTO dw_realtime_folder(id,workspace_id,name) VALUES(?,?,?)",id,workspace,name);}
    public boolean folderExists(String id,String workspace){return jdbc.queryForObject("SELECT COUNT(*) FROM dw_realtime_folder WHERE id=? AND workspace_id=?",Integer.class,id,workspace)>0;}
    public void renameFolder(String id,String workspace,String name){if(jdbc.update("UPDATE dw_realtime_folder SET name=? WHERE id=? AND workspace_id=?",name,id,workspace)!=1)throw StudioException.missing("实时目录不存在");}
    public void deleteFolder(String id,String workspace){if(jdbc.queryForObject("SELECT COUNT(*) FROM dw_realtime_task WHERE folder_id=? AND deleted=FALSE",Integer.class,id)>0)throw StudioException.conflict("FOLDER_NOT_EMPTY","请先移动或删除目录中的实时任务");if(jdbc.update("DELETE FROM dw_realtime_folder WHERE id=? AND workspace_id=?",id,workspace)!=1)throw StudioException.missing("实时目录不存在");}
    public List<Map<String,Object>> releases(String workspace){return documents("SELECT data_json FROM dw_realtime_release WHERE workspace_id=? ORDER BY created_at DESC",workspace);}
    public Map<String,Object> release(String id,String workspace){return scoped("dw_realtime_release",id,workspace,"");}
    public int nextRelease(String task){return jdbc.queryForObject("SELECT COALESCE(MAX(release_no),0)+1 FROM dw_realtime_release WHERE task_id=?",Integer.class,task);}
    public void insertRelease(Map<String,Object> value){jdbc.update("INSERT INTO dw_realtime_release(id,task_id,workspace_id,release_no,data_json,created_at) VALUES(?,?,?,?,?,?)",value.get("id"),value.get("taskId"),value.get("workspaceId"),value.get("releaseNo"),json.write(value),value.get("createdAt"));}
    public List<Map<String,Object>> jobs(String workspace){return documents("SELECT data_json FROM dw_realtime_job WHERE workspace_id=? ORDER BY updated_at DESC",workspace);}
    public Map<String,Object> job(String id,String workspace){return scoped("dw_realtime_job",id,workspace,"");}
    public List<Map<String,Object>> activeJobs(){return documents("SELECT data_json FROM dw_realtime_job WHERE status NOT IN ('STOPPED','FAILED','FINISHED','CANCELLED')");}
    public List<Map<String,Object>> terminalGatewayCleanupJobs(){return documents("SELECT data_json FROM dw_realtime_job WHERE status IN ('STOPPED','FAILED','FINISHED','CANCELLED') AND COALESCE(JSON_UNQUOTE(JSON_EXTRACT(data_json,'$.sessionHandle')),'')<>'' AND (COALESCE(JSON_UNQUOTE(JSON_EXTRACT(data_json,'$.flinkJobId')),'')<>'' OR JSON_UNQUOTE(JSON_EXTRACT(data_json,'$.submissionRejected'))='true' OR COALESCE(JSON_UNQUOTE(JSON_EXTRACT(data_json,'$.submissionAttempted')),'false')<>'true')");}
    public List<Map<String,Object>> terminalCdcJobs(){return documents("SELECT j.data_json FROM dw_realtime_job j WHERE j.status IN ('STOPPED','FAILED','FINISHED','CANCELLED') AND EXISTS (SELECT 1 FROM dw_realtime_cdc_reservation r WHERE r.owner_type='JOB' AND r.owner_id=j.id AND r.released=FALSE)");}
    public void insertJob(Map<String,Object> job){jdbc.update("INSERT INTO dw_realtime_job(id,task_id,workspace_id,status,data_json,updated_at) VALUES(?,?,?,?,?,?)",job.get("id"),job.get("taskId"),job.get("workspaceId"),job.get("status"),json.write(job),job.get("updatedAt"));}
    public void saveJob(Map<String,Object> job){jdbc.update("UPDATE dw_realtime_job SET status=?,data_json=?,updated_at=? WHERE id=?",job.get("status"),json.write(job),job.get("updatedAt"),job.get("id"));}
    public Optional<Map<String,Object>> request(String workspace,String request){return first("SELECT data_json FROM dw_realtime_operation WHERE workspace_id=? AND request_id=?",workspace,request);}
    public Map<String,Object> operation(String id,String workspace){return scoped("dw_realtime_operation",id,workspace,"");}
    public List<Map<String,Object>> operations(String workspace){return documents("SELECT data_json FROM dw_realtime_operation WHERE workspace_id=? ORDER BY updated_at DESC LIMIT 200",workspace);}
    public List<Map<String,Object>> pendingOperations(){return documents("SELECT data_json FROM dw_realtime_operation WHERE status IN ('RUNNING','RECOVERING') ORDER BY updated_at");}
    public void insertOperation(Map<String,Object> op){jdbc.update("INSERT INTO dw_realtime_operation(id,workspace_id,task_id,request_id,status,data_json,updated_at) VALUES(?,?,?,?,?,?,?)",op.get("id"),op.get("workspaceId"),op.get("taskId"),op.get("requestId"),op.get("status"),json.write(op),op.get("updatedAt"));}
    public void saveOperation(Map<String,Object> op){jdbc.update("UPDATE dw_realtime_operation SET status=?,data_json=?,updated_at=? WHERE id=?",op.get("status"),json.write(op),op.get("updatedAt"),op.get("id"));}
    public Optional<Map<String,Object>> active(String task){var rows=jdbc.queryForList("SELECT job_id,operation_id FROM dw_realtime_active WHERE task_id=?",task);return rows.stream().findFirst();}
    public void acquire(String task,String job,String op){if(active(task).isPresent())throw StudioException.conflict("REALTIME_TASK_ACTIVE","此实时任务已有活动作业或控制操作");jdbc.update("INSERT INTO dw_realtime_active(task_id,job_id,operation_id) VALUES(?,?,?)",task,job,op);}
    public void transfer(String task,String job,String op){jdbc.update("UPDATE dw_realtime_active SET job_id=?,operation_id=? WHERE task_id=?",job,op,task);}
    public void releaseActive(String task,String job){jdbc.update("DELETE FROM dw_realtime_active WHERE task_id=? AND job_id=?",task,job);}
    public boolean operationOwns(String task,String op){return active(task).map(a->Objects.equals(a.get("operation_id"),op)).orElse(false);}
    public void insertPreview(Map<String,Object> p){jdbc.update("INSERT INTO dw_realtime_preview(id,workspace_id,task_id,status,data_json,updated_at) VALUES(?,?,?,?,?,?)",p.get("id"),p.get("workspaceId"),p.get("taskId"),p.get("status"),json.write(p),p.get("updatedAt"));}
    public void savePreview(Map<String,Object> p){jdbc.update("UPDATE dw_realtime_preview SET status=?,data_json=?,updated_at=? WHERE id=?",p.get("status"),json.write(p),p.get("updatedAt"),p.get("id"));}
    public Map<String,Object> preview(String id,String workspace){return scoped("dw_realtime_preview",id,workspace,"");}
    public List<Map<String,Object>> pendingPreviews(){return documents("SELECT data_json FROM dw_realtime_preview WHERE status IN ('STARTING','RUNNING','CANCELLING')");}
    public Optional<String> imported(String workspace,String importId){return jdbc.queryForList("SELECT digest FROM dw_realtime_import WHERE workspace_id=? AND import_id=?",String.class,workspace,importId).stream().findFirst();}
    public void recordImport(String workspace,String importId,String digest,String at){jdbc.update("INSERT INTO dw_realtime_import(workspace_id,import_id,digest,created_at) VALUES(?,?,?,?)",workspace,importId,digest,at);}
    /** The endpoint lock serializes overlaps across tasks, previews, workspaces and backend workers. */
    @Transactional(isolation=Isolation.READ_COMMITTED)
    public void reserveCdcServerIds(String ownerType,String ownerId,String attempt,List<Map<String,Object>> ranges){
        if(ranges.isEmpty())return;
        if(!Set.of("JOB","PREVIEW").contains(ownerType)||ownerId.isBlank()||attempt.isBlank())throw StudioException.bad("INVALID_CDC_RESERVATION","CDC范围占用标识无效");
        var requested=new LinkedHashMap<String,Map<String,Object>>();
        for(var range:ranges){
            String endpoint=Objects.toString(range.get("endpoint"),"").trim().toLowerCase(Locale.ROOT),table=Objects.toString(range.get("tableName"),"");
            if(endpoint.isBlank()||endpoint.length()>320||table.isBlank()||table.length()>512||!(range.get("first") instanceof Number first)||!(range.get("last") instanceof Number last)||first.longValue()<1||last.longValue()<first.longValue())throw StudioException.bad("INVALID_CDC_RESERVATION","CDC范围无效");
            String key=cdcKey(ownerType,ownerId,attempt,endpoint,table);
            var normalized=Map.<String,Object>of("endpoint",endpoint,"tableName",table,"first",first.longValue(),"last",last.longValue());
            if(requested.putIfAbsent(key,normalized)!=null)throw StudioException.bad("INVALID_CDC_RESERVATION","CDC逻辑表重复");
        }
        var prior=jdbc.queryForList("SELECT reservation_key,endpoint,first_id,last_id FROM dw_realtime_cdc_reservation WHERE owner_type=? AND owner_id=? AND attempt_id=? AND released=FALSE",ownerType,ownerId,attempt);
        var endpoints=new TreeSet<String>();requested.values().forEach(range->endpoints.add(range.get("endpoint").toString()));prior.forEach(row->endpoints.add(row.get("endpoint").toString()));
        lockCdcEndpoints(endpoints);
        var active=jdbc.queryForList("SELECT reservation_key,endpoint,first_id,last_id FROM dw_realtime_cdc_reservation WHERE owner_type=? AND owner_id=? AND attempt_id=? AND released=FALSE FOR UPDATE",ownerType,ownerId,attempt);
        for(var existing:active){var range=requested.get(existing.get("reservation_key").toString());if(range==null||((Number)existing.get("first_id")).longValue()!=((Number)range.get("first")).longValue()||((Number)existing.get("last_id")).longValue()!=((Number)range.get("last")).longValue())throw StudioException.conflict("CDC_RESERVATION_CHANGED","同一作业尝试的CDC范围已变化，不能重用未知提交标识");}
        for(var entry:requested.entrySet()){
            var range=entry.getValue();String endpoint=range.get("endpoint").toString();
            var collisions=jdbc.queryForList("SELECT id FROM dw_realtime_cdc_reservation WHERE endpoint=? AND released=FALSE AND reservation_key<>? AND first_id<=? AND last_id>=? LIMIT 1 FOR UPDATE",String.class,endpoint,entry.getKey(),range.get("last"),range.get("first"));
            if(!collisions.isEmpty())throw StudioException.conflict("CDC_SERVER_ID_IN_USE","同一MySQL地址的CDC server-id范围已被活动作业或预览占用，请使用不同范围或等待原作业清理完成");
            if(active.stream().noneMatch(row->entry.getKey().equals(row.get("reservation_key"))))jdbc.update("INSERT INTO dw_realtime_cdc_reservation(id,reservation_key,owner_type,owner_id,attempt_id,endpoint,table_name,first_id,last_id,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",UUID.randomUUID().toString(),entry.getKey(),ownerType,ownerId,attempt,endpoint,range.get("tableName"),range.get("first"),range.get("last"),Instant.now().toString());
        }
    }
    @Transactional(isolation=Isolation.READ_COMMITTED)
    public void releaseCdcServerIds(String ownerType,String ownerId,String attempt){
        var endpoints=new TreeSet<>(jdbc.queryForList("SELECT DISTINCT endpoint FROM dw_realtime_cdc_reservation WHERE owner_type=? AND owner_id=? AND attempt_id=? AND released=FALSE",String.class,ownerType,ownerId,attempt));
        lockCdcEndpoints(endpoints);
        jdbc.update("UPDATE dw_realtime_cdc_reservation SET released=TRUE,released_at=? WHERE owner_type=? AND owner_id=? AND attempt_id=? AND released=FALSE",Instant.now().toString(),ownerType,ownerId,attempt);
    }
    private void lockCdcEndpoints(Collection<String> endpoints){for(String endpoint:endpoints){jdbc.update("INSERT INTO dw_realtime_cdc_endpoint(endpoint) VALUES(?) ON DUPLICATE KEY UPDATE endpoint=VALUES(endpoint)",endpoint);jdbc.queryForObject("SELECT endpoint FROM dw_realtime_cdc_endpoint WHERE endpoint=? FOR UPDATE",String.class,endpoint);}}
    private String cdcKey(String ownerType,String ownerId,String attempt,String endpoint,String table){try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(json.write(List.of(ownerType,ownerId,attempt,endpoint,table)).getBytes(StandardCharsets.UTF_8)));}catch(java.security.NoSuchAlgorithmException e){throw new IllegalStateException(e);}}
    private Map<String,Object> scoped(String table,String id,String workspace,String suffix){return first("SELECT data_json FROM "+table+" WHERE id=? AND workspace_id=?"+suffix,id,workspace).orElseThrow(()->StudioException.missing("实时记录不存在或不属于当前工作空间"));}
    private Optional<Map<String,Object>> first(String sql,Object...args){return documents(sql,args).stream().findFirst();}
    private List<Map<String,Object>> documents(String sql,Object...args){return jdbc.query(sql,(r,n)->json.map(r.getString(1)),args);}
}
