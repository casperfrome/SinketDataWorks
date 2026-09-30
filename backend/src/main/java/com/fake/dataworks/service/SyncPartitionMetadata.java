package com.fake.dataworks.service;

import com.fake.dataworks.exception.StudioException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.*;
import java.util.regex.*;

/** Doris partitions are identified by their real bounds, never by a guessed name prefix. */
public record SyncPartitionMetadata(String type,boolean automatic,String expression,List<Column> columns,List<Partition> partitions) {
    public record Column(String name,String type) {
        Map<String,Object> view(){return Map.of("name",name,"type",type);}
    }
    public record Partition(String name,String range,String lower,String upper,List<String> values) {
        Map<String,Object> view(){
            var result=new LinkedHashMap<String,Object>();result.put("name",name);result.put("range",range);
            if(lower!=null)result.put("lower",lower);if(upper!=null)result.put("upper",upper);
            if(!values.isEmpty())result.put("values",values);return result;
        }
    }
    public static SyncPartitionMetadata none(){return new SyncPartitionMetadata("NONE",false,null,List.of(),List.of());}
    public boolean partitioned(){return !"NONE".equals(type);}
    public Map<String,Object> view(){
        var result=new LinkedHashMap<String,Object>();result.put("type",type);result.put("automatic",automatic);
        if(expression!=null)result.put("expression",expression);
        result.put("columns",columns.stream().map(Column::view).toList());result.put("partitions",partitions.stream().map(Partition::view).toList());return result;
    }
    /** SHOW CREATE TABLE also describes empty AUTO tables, for which SHOW PARTITIONS has no rows. */
    public static SyncPartitionMetadata parse(String ddl,List<Map<String,Object>> tableColumns,List<Map<String,Object>> rows){
        var matcher=Pattern.compile("(?i)\\b(AUTO\\s+)?PARTITION\\s+BY\\s+(RANGE|LIST)\\s*\\(").matcher(maskQuoted(ddl));
        if(!matcher.find())return none();
        int start=matcher.end()-1,end=closingParenthesis(ddl,start);
        if(end<0)throw StudioException.bad("SYNC_PARTITION_METADATA","无法识别 Doris 分区表达式");
        String expression=ddl.substring(start+1,end).trim();
        var identifiers=new LinkedHashSet<String>();String columnExpression=withoutLiterals(expression);
        var tokens=Pattern.compile("`((?:[^`]|``)+)`|([A-Za-z_][A-Za-z0-9_]*)").matcher(columnExpression);
        while(tokens.find()){
            int next=tokens.end();while(next<columnExpression.length()&&Character.isWhitespace(columnExpression.charAt(next)))next++;
            if(tokens.group(1)==null&&next<columnExpression.length()&&columnExpression.charAt(next)=='(')continue;
            identifiers.add((tokens.group(1)!=null?tokens.group(1).replace("``","`"):tokens.group(2)).toLowerCase(Locale.ROOT));
        }
        var columns=new ArrayList<Column>();
        for(String identifier:identifiers)for(var c:tableColumns)if(identifier.equals(Objects.toString(c.get("name"),"").toLowerCase(Locale.ROOT)))columns.add(new Column(c.get("name").toString(),Objects.toString(c.get("type"),"")));
        if(columns.isEmpty())throw StudioException.bad("SYNC_PARTITION_METADATA","无法识别 Doris 分区字段");
        String type=matcher.group(2).toUpperCase(Locale.ROOT);var partitions=new ArrayList<Partition>();
        for(var row:rows){
            String name=field(row,"PartitionName"),range=field(row,"Range");if(name.isBlank())continue;
            List<List<String>> tuples;
            try{tuples=keyTuples(range,columns.size());}catch(StudioException ignored){tuples=List.of();}
            String lower=null,upper=null;List<String> values=List.of();
            if(columns.size()==1&&"RANGE".equals(type)&&tuples.size()==2){lower=tuples.get(0).getFirst();upper=tuples.get(1).getFirst();}
            if(columns.size()==1&&"LIST".equals(type))values=tuples.stream().map(List::getFirst).toList();
            partitions.add(new Partition(name,range,lower,upper,values));
        }
        return new SyncPartitionMetadata(type,matcher.group(1)!=null,expression,List.copyOf(columns),List.copyOf(partitions));
    }
    @SuppressWarnings("unchecked") public static SyncPartitionMetadata from(Map<String,Object> metadata){
        if(metadata==null||!(metadata.get("partition") instanceof Map<?,?> raw))return none();
        var map=(Map<String,Object>)raw;var columns=new ArrayList<Column>();var partitions=new ArrayList<Partition>();
        for(var item:(List<Map<String,Object>>)map.getOrDefault("columns",List.of()))columns.add(new Column(item.get("name").toString(),item.get("type").toString()));
        for(var item:(List<Map<String,Object>>)map.getOrDefault("partitions",List.of()))partitions.add(new Partition(item.get("name").toString(),Objects.toString(item.get("range"),""),(String)item.get("lower"),(String)item.get("upper"),(List<String>)item.getOrDefault("values",List.of())));
        return new SyncPartitionMetadata(Objects.toString(map.get("type"),"NONE"),Boolean.TRUE.equals(map.get("automatic")),(String)map.get("expression"),List.copyOf(columns),List.copyOf(partitions));
    }
    public Column column(String name){return columns.stream().filter(c->c.name().equals(name)).findFirst().orElseThrow(()->StudioException.bad("INVALID_SYNC_PARTITION","目标字段不是分区字段："+name));}
    public List<String> checkedNames(List<String> names){
        if(names.size()!=new HashSet<>(names).size())throw StudioException.bad("INVALID_SYNC_PARTITION","分区不能重复");
        for(String name:names)if(partitions.stream().noneMatch(p->p.name().equals(name)))throw StudioException.bad("SYNC_PARTITION_NOT_FOUND","分区不存在："+name);
        return List.copyOf(names);
    }
    /** Native keys follow the partition expression's order, which can differ from table declaration order. */
    public List<String> matching(Map<String,String> values){
        if(columns.isEmpty()||columns.stream().anyMatch(column->!values.containsKey(column.name())))throw StudioException.bad("SYNC_PARTITION_SELECTION_REQUIRED","请显式选择目标分区，或为每个分区字段指定固定值");
        var value=columns.stream().map(column->values.get(column.name())).toList();var result=new ArrayList<String>();
        for(var partition:partitions){
            boolean matches;
            if("RANGE".equals(type)){
                var bounds=columns.size()==1&&partition.lower()!=null&&partition.upper()!=null?List.of(List.of(partition.lower()),List.of(partition.upper())):keyTuples(partition.range(),columns.size());
                if(bounds.size()!=2)throw metadataError();
                matches=compareTuple(value,bounds.get(0),true)>=0&&compareTuple(value,bounds.get(1),true)<0;
            }else{
                var tuples=columns.size()==1&&!partition.values().isEmpty()?partition.values().stream().map(List::of).toList():keyTuples(partition.range(),columns.size());
                if(tuples.isEmpty())throw metadataError();
                matches=tuples.stream().anyMatch(tuple->compareTuple(value,tuple,false)==0);
            }
            if(matches)result.add(partition.name());
        }
        return List.copyOf(result);
    }
    private int compareTuple(List<String> left,List<String> right,boolean range){
        if(left.size()!=columns.size()||right.size()!=columns.size())throw metadataError();
        for(int i=0;i<columns.size();i++){int order=compare(left.get(i),right.get(i),columns.get(i).type(),range);if(order!=0)return order;}
        return 0;
    }
    private static int compare(String left,String right,String type,boolean range){
        if(range&&(right.equalsIgnoreCase("MINVALUE")||right.equalsIgnoreCase("MIN_VALUE")))return 1;
        if(range&&(right.equalsIgnoreCase("MAXVALUE")||right.equalsIgnoreCase("MAX_VALUE")))return -1;
        try{
            if(type.toUpperCase(Locale.ROOT).startsWith("DATE")&&!type.toUpperCase(Locale.ROOT).startsWith("DATETIME"))return LocalDate.parse(left).compareTo(LocalDate.parse(right));
            if(type.toUpperCase(Locale.ROOT).matches("(?:TINYINT|SMALLINT|INT|BIGINT|LARGEINT|DECIMAL).*"))return new BigDecimal(left).compareTo(new BigDecimal(right));
            return left.compareTo(right);
        }catch(RuntimeException e){throw StudioException.bad("SYNC_PARTITION_METADATA","分区值与目标范围类型不匹配");}
    }
    private static StudioException metadataError(){return StudioException.bad("SYNC_PARTITION_METADATA","无法可靠识别目标分区范围，请显式选择分区");}
    /** SHOW PARTITIONS emits one types/keys pair per RANGE bound or LIST tuple. */
    private static List<List<String>> keyTuples(String range,int width){
        var result=new ArrayList<List<String>>();var prefix=Pattern.compile("(?i)types\\s*:\\s*\\[([^]]*)]\\s*;\\s*keys\\s*:\\s*\\[").matcher(range);
        int consumed=0;
        while(prefix.find(consumed)){
            if(!range.substring(consumed,prefix.start()).matches("[\\s\\[\\]();.]*"))throw metadataError();
            if(prefix.group(1).split(",",-1).length!=width)throw metadataError();
            int start=prefix.end(),end=range.indexOf(']',start);
            if(end<0||range.substring(start,end).contains("[")||!range.substring(end+1).stripLeading().startsWith(";"))throw metadataError();
            String raw=range.substring(start,end);
            // Native SHOW output is unescaped text, not SQL literals: quotes and backslashes belong to the value.
            // Doris joins keys with comma + space; trimming would change genuine STRING keys.
            var tuple=width==1?List.of(raw):List.of(raw.split(", ",-1));
            if(tuple.size()!=width)throw metadataError();result.add(tuple);consumed=end+1;
        }
        if(!range.substring(consumed).matches("[\\s\\[\\]();.]*"))throw metadataError();
        if(result.isEmpty())throw metadataError();return List.copyOf(result);
    }
    private static String field(Map<String,Object> row,String key){return row.entrySet().stream().filter(e->e.getKey().equalsIgnoreCase(key)).map(e->Objects.toString(e.getValue(),"")).findFirst().orElse("");}
    private static String withoutLiterals(String value){return value.replaceAll("'(?:(?:'')|(?:\\\\.)|[^'\\\\])*'|\"(?:(?:\"\")|(?:\\\\.)|[^\"\\\\])*\""," ");}
    private static String maskQuoted(String sql){
        var out=new StringBuilder(sql);char quote=0;
        for(int i=0;i<sql.length();i++){char c=sql.charAt(i);if(quote!=0){out.setCharAt(i,' ');if(c=='\\'&&i+1<sql.length())out.setCharAt(++i,' ');else if(c==quote){if(i+1<sql.length()&&sql.charAt(i+1)==quote)out.setCharAt(++i,' ');else quote=0;}}else if(c=='\''||c=='"'||c=='`'){quote=c;out.setCharAt(i,' ');}}
        return out.toString();
    }
    private static int closingParenthesis(String sql,int start){
        int depth=0;char quote=0;
        for(int i=start;i<sql.length();i++){char c=sql.charAt(i);if(quote!=0){if(c=='\\'){i++;continue;}if(c==quote){if(i+1<sql.length()&&sql.charAt(i+1)==quote)i++;else quote=0;}}else if(c=='\''||c=='"'||c=='`')quote=c;else if(c=='(')depth++;else if(c==')'&&--depth==0)return i;}
        return -1;
    }
}
