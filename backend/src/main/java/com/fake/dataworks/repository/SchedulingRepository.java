package com.fake.dataworks.repository;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.exception.StudioException;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/** Read model over the two existing instance tables. Pagination happens in MySQL. */
@Repository
public class SchedulingRepository {
    private final JdbcTemplate jdbc;
    private final JsonCodec json;
    public SchedulingRepository(JdbcTemplate jdbc, JsonCodec json) { this.jdbc=jdbc; this.json=json; }
    private String table(String kind) {
        return switch(kind) { case "TASK" -> "dw_task_trigger"; case "WORKFLOW" -> "dw_schedule_trigger";
            default -> throw StudioException.bad("INVALID_SCHEDULING_KIND","类型须为 TASK 或 WORKFLOW"); };
    }
    private static final String UNION = "SELECT 'TASK' kind,t.id,t.schedule_id,t.scheduled_at,t.business_date,t.batch_key,t.status,t.data_json,s.workspace_id,s.task_id object_id FROM dw_task_trigger t JOIN dw_task_schedule s ON s.id=t.schedule_id UNION ALL SELECT 'WORKFLOW',t.id,t.schedule_id,t.scheduled_at,t.business_date,t.batch_key,t.status,t.data_json,s.workspace_id,s.workflow_id FROM dw_schedule_trigger t JOIN dw_workflow_schedule s ON s.id=t.schedule_id";
    private Map<String,Object> row(java.sql.ResultSet r,int n) throws java.sql.SQLException {
        var item=json.map(r.getString("data_json")); item.put("kind",r.getString("kind"));
        item.put("objectId",r.getString("object_id")); item.put("batchKey",r.getString("batch_key"));
        item.put("source",!r.getString("batch_key").isEmpty()?"BACKFILL":Boolean.TRUE.equals(item.get("manualRerun"))||"RERUN".equals(item.get("triggerType"))?"RERUN":"SCHEDULED");
        return item;
    }
    public Map<String,Object> instance(String kind,String id) {
        String filter=kind.isBlank()?"":" AND kind=?";var args=new ArrayList<Object>();args.add(id);if(!kind.isBlank()){table(kind);args.add(kind);}
        return jdbc.query("SELECT * FROM ("+UNION+") i WHERE id=?"+filter,this::row,args.toArray()).stream().findFirst().orElseThrow(()->StudioException.missing("调度实例不存在"));
    }
    public Map<String,Object> instances(Map<String,String> filters,int page,int size) {
        if(page<1||page>100000||size<1||size>100)throw StudioException.bad("INVALID_PAGE","分页参数无效");
        var args=new ArrayList<Object>();var where=new StringBuilder(" WHERE workspace_id=?");args.add(filters.getOrDefault("workspaceId",""));
        for(var entry:Map.of("kind","kind","scheduleId","schedule_id","objectId","object_id","status","status","batchKey","batch_key","businessDate","business_date").entrySet()) {
            String value=filters.getOrDefault(entry.getKey(),"");if(!value.isBlank()){where.append(" AND ").append(entry.getValue()).append("=?");args.add(value);}
        }
        for(var entry:Map.of("businessDateFrom"," >= ","businessDateTo"," <= ").entrySet()) {
            String value=filters.getOrDefault(entry.getKey(),"");if(!value.isBlank()){where.append(" AND business_date").append(entry.getValue()).append("?");args.add(value);}
        }
        String source=filters.getOrDefault("source","");
        if(source.equals("BACKFILL"))where.append(" AND batch_key<>''");
        else if(source.equals("RERUN"))where.append(" AND batch_key='' AND (JSON_EXTRACT(data_json,'$.manualRerun')=TRUE OR JSON_UNQUOTE(JSON_EXTRACT(data_json,'$.triggerType'))='RERUN')");
        else if(source.equals("SCHEDULED"))where.append(" AND batch_key='' AND COALESCE(JSON_EXTRACT(data_json,'$.manualRerun'),FALSE)<>TRUE AND COALESCE(JSON_UNQUOTE(JSON_EXTRACT(data_json,'$.triggerType')),'SCHEDULED')<>'RERUN'");
        else if(!source.isBlank())throw StudioException.bad("INVALID_SOURCE","实例来源无效");
        String search=filters.getOrDefault("search","");if(!search.isBlank()) {where.append(" AND (LOCATE(LOWER(?),LOWER(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(data_json,'$.name')),'')))>0 OR LOCATE(LOWER(?),LOWER(id))>0)");args.add(search);args.add(search);}
        String from=" FROM ("+UNION+") i"+where;
        long total=jdbc.queryForObject("SELECT COUNT(*)"+from,Long.class,args.toArray());
        var pageArgs=new ArrayList<>(args);pageArgs.add(size);pageArgs.add((page-1)*size);
        return Map.of("items",jdbc.query("SELECT *"+from+" ORDER BY scheduled_at DESC,id DESC LIMIT ? OFFSET ?",this::row,pageArgs.toArray()),"total",total,"page",page,"pageSize",size);
    }
    public List<Map<String,Object>> batch(String batchKey) { return jdbc.query("SELECT * FROM ("+UNION+") i WHERE batch_key=? ORDER BY business_date,scheduled_at,kind,schedule_id,id",this::row,batchKey); }
    public Map<String,Map<String,Object>> latestByObject(String workspace) {
        var result=new HashMap<String,Map<String,Object>>();
        jdbc.query("SELECT * FROM (SELECT i.*,ROW_NUMBER() OVER(PARTITION BY kind,object_id ORDER BY scheduled_at DESC,id DESC) position FROM ("+UNION+") i WHERE workspace_id=?) ranked WHERE position=1",(r,n)->{var item=row(r,n);result.put(r.getString("object_id"),item);return null;},workspace);return result;
    }
    public List<Map<String,Object>> window(String kind,String schedule,String date) {
        return jdbc.query("SELECT data_json FROM "+table(kind)+" WHERE schedule_id=? AND business_date=? AND batch_key=''",(r,n)->json.map(r.getString(1)),schedule,date);
    }
    public void insert(String kind,Map<String,Object> t) {
        jdbc.update("INSERT INTO "+table(kind)+"(id,schedule_id,scheduled_at,business_date,batch_key,status,run_id,data_json) VALUES(?,?,?,?,?,?,?,?)",t.get("id"),t.get("scheduleId"),t.get("scheduledAt"),t.get("businessDate"),t.getOrDefault("batchKey",""),t.get("status"),t.get("runId"),json.write(t));
    }
    public Map<String,Map<String,Object>> runs(Collection<String> ids) {
        if(ids.isEmpty())return Map.of();var result=new HashMap<String,Map<String,Object>>();
        jdbc.query("SELECT id,JSON_REMOVE(data_json,'$.rows','$.columns','$.logs') FROM dw_run WHERE id IN ("+String.join(",",Collections.nCopies(ids.size(),"?"))+")",(r,n)->{result.put(r.getString(1),json.map(r.getString(2)));return null;},ids.toArray());return result;
    }
}
