package com.fake.dataworks.service;

import com.fake.dataworks.exception.StudioException;
import java.util.*;

/** Versioned result envelope; old flat result records remain readable. */
public final class SqlResults {
    private SqlResults() {}
    public static List<Map<String,Object>> pending(SqlScript script) {
        var result=new ArrayList<Map<String,Object>>();
        for(var command:script.commands()) {
            var item=new LinkedHashMap<String,Object>();item.put("statementIndex",result.size()+1);item.put("kind",command.kind());
            item.put("status","SKIPPED");item.put("commitStatus","NOT_STARTED");item.put("elapsedMs",0);
            item.put("columns",List.of());item.put("rows",List.of());item.put("truncated",false);result.add(item);
        }
        return result;
    }
    public static Map<String,Object> envelope(List<Map<String,Object>> items) {return Map.of("schemaVersion",2,"statements",items);}
    @SuppressWarnings("unchecked") public static List<Map<String,Object>> items(Map<String,Object> result) {
        return result.get("statements") instanceof List<?> list?(List<Map<String,Object>>)list:List.of();
    }
    public static List<Map<String,Object>> summaries(List<Map<String,Object>> items) {
        return items.stream().map(item->{var copy=new LinkedHashMap<>(item);copy.remove("columns");copy.remove("rows");return (Map<String,Object>)copy;}).toList();
    }
    public static Map<String,Object> page(Map<String,Object> result,int page,int size,Integer index) {
        var items=items(result);Map<String,Object> selected=result;
        if(!items.isEmpty()) {
            int selectedIndex=index==null?((Number)items.stream().filter(i->"QUERY".equals(i.get("kind"))).findFirst().orElse(items.getFirst()).get("statementIndex")).intValue():index;
            if(selectedIndex<1||selectedIndex>items.size())throw StudioException.bad("INVALID_STATEMENT_INDEX","语句序号超出范围");
            selected=items.get(selectedIndex-1);
        } else if(index!=null&&index!=1)throw StudioException.bad("INVALID_STATEMENT_INDEX","语句序号超出范围");
        var rows=(List<?>)selected.getOrDefault("rows",List.of());int from=Math.min(rows.size(),(page-1)*size),to=Math.min(rows.size(),from+size);
        var response=new LinkedHashMap<String,Object>();response.put("columns",selected.getOrDefault("columns",List.of()));response.put("rows",rows.subList(from,to));response.put("total",rows.size());
        response.put("page",page);response.put("pageSize",size);response.put("truncated",selected.getOrDefault("truncated",false));
        if(!items.isEmpty()) {
            response.putAll(summaries(List.of(selected)).getFirst());response.put("total",rows.size());
            response.put("statementIndex",((Number)selected.get("statementIndex")).intValue());response.put("statements",summaries(items));
        }
        return response;
    }
    /** A durable IN_PROGRESS marker is written before JDBC dispatch. Never infer rollback after a crash. */
    public static boolean recover(Map<String,Object> result) {
        boolean unknown=false;
        for(var item:items(result))if("RUNNING".equals(item.get("status"))) {
            boolean write=!"QUERY".equals(item.get("kind"));item.put("status",write?"UNKNOWN":"FAILED");
            item.put("commitStatus",write?"UNKNOWN":"NOT_APPLICABLE");item.put("errorCode",write?"COMMIT_UNKNOWN":"SERVICE_RESTARTED");
            item.put("message",write?"服务中断，提交结果未知，请核实业务数据。":"服务中断，查询未完成。");unknown|=write;
        }
        return unknown;
    }
}
