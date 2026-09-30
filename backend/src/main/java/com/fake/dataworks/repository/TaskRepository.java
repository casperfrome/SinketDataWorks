package com.fake.dataworks.repository;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.exception.StudioException;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class TaskRepository {
    private final JdbcTemplate jdbc;private final JsonCodec json;
    public TaskRepository(JdbcTemplate jdbc,JsonCodec json){this.jdbc=jdbc;this.json=json;}
    private List<Map<String,Object>> query(String sql,Object...args){return jdbc.query(sql,(r,n)->json.map(r.getString(1)),args);}
    public List<Map<String,Object>> releases(String task){return query("SELECT data_json FROM dw_task_release WHERE task_id=? ORDER BY release_no DESC",task);}
    public Map<String,Object> release(String id){return query("SELECT data_json FROM dw_task_release WHERE id=?",id).stream().findFirst().orElseThrow(()->StudioException.missing("任务发布版本不存在"));}
    public void insertRelease(Map<String,Object> r){jdbc.update("INSERT INTO dw_task_release(id,workspace_id,task_id,release_no,data_json) VALUES(?,?,?,?,?)",r.get("id"),r.get("workspaceId"),r.get("taskId"),r.get("releaseNo"),json.write(r));}
    public List<Map<String,Object>> schedules(String workspace){return query("SELECT data_json FROM dw_task_schedule WHERE workspace_id=? ORDER BY task_id",workspace);}
    public List<Map<String,Object>> allSchedules(){return query("SELECT data_json FROM dw_task_schedule ORDER BY task_id");}
    public Optional<Map<String,Object>> forTask(String task){return query("SELECT data_json FROM dw_task_schedule WHERE task_id=?",task).stream().findFirst();}
    public Map<String,Object> schedule(String id){return query("SELECT data_json FROM dw_task_schedule WHERE id=?",id).stream().findFirst().orElseThrow(()->StudioException.missing("任务计划不存在"));}
    public void lockSchedule(String id){jdbc.queryForObject("SELECT id FROM dw_task_schedule WHERE id=? FOR UPDATE",String.class,id);}
    public void saveSchedule(Map<String,Object> s,boolean insert){
        if(insert)jdbc.update("INSERT INTO dw_task_schedule(id,workspace_id,task_id,enabled,version,next_fire_at,data_json) VALUES(?,?,?,?,?,?,?)",s.get("id"),s.get("workspaceId"),s.get("taskId"),s.get("enabled"),s.get("version"),s.get("nextFireAt"),json.write(s));
        else jdbc.update("UPDATE dw_task_schedule SET enabled=?,version=?,next_fire_at=?,data_json=? WHERE id=?",s.get("enabled"),s.get("version"),s.get("nextFireAt"),json.write(s),s.get("id"));
    }
    public void insertTrigger(Map<String,Object> t){jdbc.update("INSERT IGNORE INTO dw_task_trigger(id,schedule_id,scheduled_at,status,data_json) VALUES(?,?,?,?,?)",t.get("id"),t.get("scheduleId"),t.get("scheduledAt"),t.get("status"),json.write(t));}
    public void updateTrigger(Map<String,Object> t){jdbc.update("UPDATE dw_task_trigger SET status=?,data_json=? WHERE id=?",t.get("status"),json.write(t),t.get("id"));}
    public Map<String,Object> trigger(String id){return query("SELECT data_json FROM dw_task_trigger WHERE id=?",id).stream().findFirst().orElseThrow(()->StudioException.missing("任务实例不存在"));}
    public Optional<Map<String,Object>> at(String schedule,String at){return query("SELECT data_json FROM dw_task_trigger WHERE schedule_id=? AND scheduled_at=?",schedule,at).stream().findFirst();}
    public List<Map<String,Object>> pending(String workspace){return query("SELECT data_json FROM dw_task_trigger WHERE status IN ('PENDING','WAITING_DEPENDENCY','WAITING_RESOURCE','RUNNING','RETRY_WAIT') AND (?='' OR JSON_UNQUOTE(JSON_EXTRACT(data_json,'$.workspaceId'))=?) ORDER BY scheduled_at,id",workspace,workspace);}
    public Map<String,Object> triggers(String schedule,int page,int size,String status){
        if(page<1||page>100000||size<1||size>100)throw StudioException.bad("INVALID_PAGE","分页参数无效");
        String where=" WHERE schedule_id=? AND (?='' OR status=?)";
        return Map.of("items",query("SELECT data_json FROM dw_task_trigger"+where+" ORDER BY scheduled_at DESC LIMIT ? OFFSET ?",schedule,status,status,size,(page-1)*size),"total",jdbc.queryForObject("SELECT COUNT(*) FROM dw_task_trigger"+where,Long.class,schedule,status,status),"page",page,"pageSize",size);
    }
}
