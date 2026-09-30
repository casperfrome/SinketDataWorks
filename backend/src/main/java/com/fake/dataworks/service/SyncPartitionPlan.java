package com.fake.dataworks.service;

import com.fake.dataworks.exception.StudioException;
import java.util.*;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.Select;

/** Builds only quoted identifiers, bound values and a validated predicate from the editor controls. */
final class SyncPartitionPlan {
    record Result(Map<String,Object> source,Object mapping,List<String> writerPartitions,List<String> targetPartitions,List<Map<String,Object>> assignments,List<String> preSql,String clearScope,boolean skipClear){}
    private record Assignment(String target,String mode,String value,String source){}
    private SyncPartitionPlan(){}
    @SuppressWarnings("unchecked") static Result build(Map<String,Object> config,DatasourceService.ConnectionSpec source,DatasourceService.ConnectionSpec target,Map<String,Object> sourceMetadata,Map<String,Object> targetMetadata,Map<String,Object> run,SqlGuard guard){
        boolean fromDoris="DORIS".equals(source.type());
        var sourcePartition=SyncPartitionMetadata.from(sourceMetadata);var targetPartition=SyncPartitionMetadata.from(targetMetadata);
        var filter=map(config.get("sourcePartitionFilter"),"来源分区筛选");
        if(!Set.of("partitions","where").containsAll(filter.keySet()))fail("来源分区筛选配置无效");
        var sourcePartitions=strings(filter,"partitions");String partitionWhere=text(filter,"where");
        if((!sourcePartitions.isEmpty()||!partitionWhere.isBlank())&&(!fromDoris||!sourcePartition.partitioned()))fail("来源分区筛选需要 Doris 分区表");
        sourcePartition.checkedNames(sourcePartitions);
        var assignments=assignments(config);var explicit=strings(config,"targetPartitions");
        if((!assignments.isEmpty()||!explicit.isEmpty())&&(!"DORIS".equals(target.type())||!targetPartition.partitioned()))fail("目标分区配置需要 Doris 分区表");
        targetPartition.checkedNames(explicit);
        var sourceColumns=(List<Map<String,Object>>)sourceMetadata.getOrDefault("columns",List.of());
        var selected=strings(config,"columns");var projectionColumns=selected.isEmpty()?sourceColumns.stream().map(c->c.get("name").toString()).toList():selected;
        var fixed=new LinkedHashMap<String,String>();var resolved=new ArrayList<Map<String,Object>>();var projection=new ArrayList<String>();var projectionParameters=new ArrayList<Map<String,Object>>();var generatedMapping=new ArrayList<Map<String,Object>>();
        for(String name:projectionColumns)projection.add(DatasourceService.quote(name));
        boolean dynamic=false;int aliasIndex=0;
        for(var assignment:assignments){
            var column=targetPartition.column(assignment.target());String expression;var detail=new LinkedHashMap<String,Object>();detail.put("target",assignment.target());
            if("value".equals(assignment.mode())){
                String value=SyncParameters.partitionValue(SyncParameters.value(assignment.value(),run),column.type());
                fixed.put(assignment.target(),value);detail.put("value",value);
                expression="CAST(? AS "+castType(column.type())+")";projectionParameters.add(Map.of("type","string","value",value));
            }else{
                if(sourceColumns.stream().noneMatch(c->assignment.source().equals(c.get("name"))))fail("来源分区字段不存在："+assignment.source());
                dynamic=true;detail.put("source",assignment.source());expression="CAST("+DatasourceService.quote(assignment.source())+" AS "+castType(column.type())+")";
            }
            String alias;do{alias="__sync_partition_"+aliasIndex++;}while(projectionColumns.contains(alias));
            projection.add(expression+" AS "+DatasourceService.quote(alias));generatedMapping.add(Map.of("source",alias,"target",assignment.target()));resolved.add(detail);
        }
        Object mapping=config.getOrDefault("mapping",List.of());if(!(mapping instanceof List<?>))fail("字段映射格式无效");
        if(!assignments.isEmpty()){
            if(projectionColumns.isEmpty())fail("无法获取来源字段，不能生成分区映射");
            var rows=new ArrayList<Map<String,Object>>();var assigned=assignments.stream().map(Assignment::target).collect(java.util.stream.Collectors.toSet());
            if(((List<?>)mapping).isEmpty())for(String name:projectionColumns){if(!assigned.contains(name))rows.add(Map.of("source",name,"target",name));}
            else for(Object row:(List<?>)mapping){if(!(row instanceof Map<?,?> m))fail("字段映射格式无效");else if(!assigned.contains(m.get("target")))rows.add(new LinkedHashMap<>((Map<String,Object>)m));}
            rows.addAll(generatedMapping);mapping=rows;
        }
        String where=text(config,"where");String combined=where;
        if(!partitionWhere.isBlank()){
            var compiled=SyncParameters.compile(partitionWhere,run,fromDoris);validatePredicate(compiled.sql(),sourcePartition.columns().stream().map(SyncPartitionMetadata.Column::name).collect(java.util.stream.Collectors.toSet()));
            combined=where.isBlank()?partitionWhere:"("+partitionWhere+") AND ("+where+")";
        }
        var compiled=SyncParameters.compile(combined,run,fromDoris);if(!combined.isBlank())validatePredicate(compiled.sql(),null);
        var sourceConfig=new LinkedHashMap<String,Object>();String from=DatasourceService.quote(source.database())+"."+DatasourceService.quote(text(config,"sourceTable"));
        boolean query=!assignments.isEmpty()||!sourcePartitions.isEmpty();
        if(query){
            if(projection.isEmpty())projection.add("*");
            String sql="SELECT "+String.join(",",projection)+" FROM "+from+(!sourcePartitions.isEmpty()?" PARTITION ("+String.join(",",sourcePartitions.stream().map(DatasourceService::quote).toList())+")":"")+(!combined.isBlank()?" WHERE ("+compiled.sql()+")":"");
            guard.validate(sql,source.database());sourceConfig.put("query",sql);projectionParameters.addAll(compiled.params());if(!projectionParameters.isEmpty())sourceConfig.put("params",projectionParameters);
        }else{
            sourceConfig.put("table",text(config,"sourceTable"));sourceConfig.put("columns",selected);
            if(!combined.isBlank()){guard.validate("SELECT * FROM "+from+" WHERE ("+compiled.sql()+")",source.database());sourceConfig.put("where",compiled.sql());if(!compiled.params().isEmpty())sourceConfig.put("params",compiled.params());}
        }
        String mode=Objects.toString(config.get("writeMode"),"append");boolean overwrite="overwrite".equals(mode);var selectedTargets=explicit;boolean skipClear=false;
        if(targetPartition.partitioned()){
            if(fixed.size()==targetPartition.columns().size()&&!dynamic){
                try{
                    var matching=targetPartition.matching(fixed);
                    if(explicit.isEmpty())selectedTargets=matching;
                    else if(matching.isEmpty()||!explicit.containsAll(matching))fail("固定分区值不属于所选目标分区");
                }catch(StudioException error){
                    if(!"SYNC_PARTITION_METADATA".equals(error.code())||!"append".equals(mode)||(!targetPartition.automatic()&&explicit.isEmpty()))throw error;
                    // Appending can rely on AUTO routing or an explicit writer scope; uncertain bounds cannot authorize clearing.
                }
            }
            if(selectedTargets.isEmpty()&&!targetPartition.automatic()&&!fixed.isEmpty()&&!dynamic)throw StudioException.bad("SYNC_PARTITION_NOT_FOUND","固定分区值没有对应的目标分区");
            if(overwrite&&explicit.isEmpty()&&(dynamic||fixed.size()!=targetPartition.columns().size()))throw StudioException.bad("SYNC_PARTITION_SELECTION_REQUIRED","分区覆盖需要固定分区值或显式选择目标分区");
            skipClear=overwrite&&selectedTargets.isEmpty()&&targetPartition.automatic()&&!fixed.isEmpty()&&!dynamic;
        }
        var preSql=new ArrayList<String>();String clearScope="TABLE";
        if(targetPartition.partitioned())clearScope=selectedTargets.isEmpty()?fixed.isEmpty()?"PARTITION (来源字段路由)":"PARTITION ("+String.join(",",fixed.entrySet().stream().map(e->e.getKey()+"="+e.getValue()).toList())+")":"PARTITION ("+String.join(",",selectedTargets)+")";
        if(overwrite&&!skipClear)preSql.add("TRUNCATE TABLE "+DatasourceService.quote(target.database())+"."+DatasourceService.quote(text(config,"targetTable"))+(targetPartition.partitioned()?" PARTITION ("+String.join(",",selectedTargets.stream().map(DatasourceService::quote).toList())+")":""));
        // AUTO routing needs no partitions header: a new value must be allowed to create its own partition.
        var writerPartitions=targetPartition.automatic()&&explicit.isEmpty()?List.<String>of():selectedTargets;
        return new Result(sourceConfig,mapping,writerPartitions,selectedTargets,List.copyOf(resolved),List.copyOf(preSql),clearScope,skipClear);
    }
    static void validatePredicate(String sql,Set<String> allowedColumns){
        try{
            var expression=CCJSqlParserUtil.parseCondExpression(sql,false);if(expression==null)throw new IllegalArgumentException();
            expression.accept(new ExpressionVisitorAdapter<Void>(){
                @Override public <S> Void visit(Column column,S context){if(allowedColumns!=null&&allowedColumns.stream().noneMatch(n->n.equalsIgnoreCase(column.getUnquotedColumnName())))throw StudioException.bad("INVALID_SYNC_PARTITION_FILTER","分区筛选只能引用分区字段："+column.getUnquotedColumnName());return null;}
                @Override public <S> Void visit(ParenthesedSelect select,S context){throw StudioException.bad("INVALID_SYNC_FILTER","筛选条件不支持子查询");}
                @Override public <S> Void visit(Select select,S context){throw StudioException.bad("INVALID_SYNC_FILTER","筛选条件不支持子查询");}
            },null);
        }catch(StudioException e){throw e;}catch(Exception e){throw StudioException.bad("INVALID_SYNC_FILTER","筛选条件必须是完整的 WHERE 条件表达式");}
    }
    private static String castType(String type){
        String upper=type.toUpperCase(Locale.ROOT);
        if(upper.matches("DATE(?:V2)?(?:\\([^)]*\\))?"))return "DATE";
        if(upper.matches("DATETIME(?:V2)?(?:\\([0-6]\\))?"))return "DATETIME";
        if(upper.matches("(?:CHAR|VARCHAR|STRING|TEXT)(?:\\([0-9]+\\))?"))return "CHAR";
        if(upper.matches("(?:TINYINT|SMALLINT|INT|BIGINT|LARGEINT)(?:\\([0-9]+\\))?(?: UNSIGNED)?"))return upper.endsWith("UNSIGNED")?"UNSIGNED":"SIGNED";
        if(upper.matches("DECIMAL(?:V[23])?\\([0-9]+,[0-9]+\\)"))return upper.replaceAll("V[23]","");
        throw StudioException.bad("INVALID_SYNC_PARTITION","暂不支持该分区字段类型："+type);
    }
    private static List<Assignment> assignments(Map<String,Object> config){
        Object raw=config.getOrDefault("targetPartitionAssignments",List.of());if(!(raw instanceof List<?> list)||list.size()>16)throw StudioException.bad("INVALID_SYNC_PARTITION","目标分区赋值格式无效");
        var result=new ArrayList<Assignment>();var targets=new HashSet<String>();
        for(Object row:list){var item=map(row,"目标分区赋值");if(!Set.of("target","mode","value","source").containsAll(item.keySet()))fail("目标分区赋值配置无效");String target=text(item,"target"),mode=text(item,"mode");if(target.isBlank()||!targets.add(target)||!Set.of("value","column").contains(mode))fail("目标分区赋值字段、模式或重复配置无效");String value=text(item,"value"),source=text(item,"source");if("value".equals(mode)&&value.isBlank()||"column".equals(mode)&&source.isBlank())fail("请输入分区值或选择来源字段");result.add(new Assignment(target,mode,value,source));}
        return result;
    }
    @SuppressWarnings("unchecked") private static Map<String,Object> map(Object raw,String label){if(raw==null)return Map.of();if(!(raw instanceof Map<?,?>))throw StudioException.bad("INVALID_SYNC_PARTITION",label+"格式无效");return (Map<String,Object>)raw;}
    @SuppressWarnings("unchecked") private static List<String> strings(Map<String,Object> map,String key){Object raw=map.getOrDefault(key,List.of());if(!(raw instanceof List<?> list)||list.size()>1024||list.stream().anyMatch(v->!(v instanceof String)))throw StudioException.bad("INVALID_SYNC_PARTITION",key+"格式无效");return (List<String>)raw;}
    private static String text(Map<String,Object> map,String key){Object raw=map.get(key);if(raw==null)return "";if(!(raw instanceof String))throw StudioException.bad("INVALID_SYNC_PARTITION",key+"格式无效");return raw.toString().trim();}
    private static void fail(String message){throw StudioException.bad("INVALID_SYNC_PARTITION",message);}
}
