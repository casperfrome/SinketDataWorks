package com.fake.dataworks.service;

import com.fake.dataworks.exception.StudioException;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.*;

/** Reuses SQL placeholder compilation, and renders Flight values without SQL escaping. */
public final class SyncParameters {
    private SyncParameters(){}
    private static final Pattern INTERNAL=Pattern.compile(":(?:bizdate|source_cutoff|build_id|upstream_[A-Za-z][A-Za-z0-9_]{0,31}_build_id)");
    public record Compiled(String sql,List<Map<String,Object>> params){}
    /** Represent assignment text as a SQL literal only for the shared parameter scanner. */
    public static String parameterCode(String input) {
        return INTERNAL.matcher(input).matches()?input:"'"+input.replace("\\","\\\\").replace("'","''")+"'";
    }
    /** A partition value is text, not a SQL fragment; SQL placeholders are generated separately. */
    public static String value(String input,Map<String,Object> run) {
        Map<?,?> custom=run.get("scheduleParameters") instanceof Map<?,?> m?m:Map.of();
        Map<?,?> internal=run.get("parameters") instanceof Map<?,?> m?m:Map.of();
        if(INTERNAL.matcher(input).matches())return required(internal,input.substring(1));
        var matcher=Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]{0,63})}").matcher(input);
        if(matcher.replaceAll("").contains("${"))throw StudioException.bad("INVALID_SYNC_PARTITION","分区参数格式无效");
        matcher.reset();var result=new StringBuilder();
        while(matcher.find())matcher.appendReplacement(result,Matcher.quoteReplacement(required(custom,matcher.group(1))));matcher.appendTail(result);
        return result.toString();
    }
    public static String partitionValue(String value,String type) {
        if(type.toUpperCase(Locale.ROOT).matches("DATE(?:V2)?(?:\\([^)]*\\))?")) {
            try{if(!value.matches("[0-9]{8}|[0-9]{4}-[0-9]{2}-[0-9]{2}"))throw new IllegalArgumentException();return LocalDate.parse(value,value.matches("[0-9]{8}")?DateTimeFormatter.BASIC_ISO_DATE:DateTimeFormatter.ISO_LOCAL_DATE).toString();}
            catch(Exception e){throw StudioException.bad("INVALID_SYNC_PARTITION_DATE","DATE 分区值须为有效的 yyyy-MM-dd 或 yyyyMMdd 日期");}
        }
        if(value.isBlank()||value.length()>4096||value.codePoints().anyMatch(Character::isISOControl))throw StudioException.bad("INVALID_SYNC_PARTITION","分区值不能为空或包含控制字符");
        return value;
    }
    public static Compiled compile(String input,Map<String,Object> run,boolean doris) {
        var compiled=SqlParameters.compile(input);var values=new ArrayList<String>();
        Map<?,?> custom=run.get("scheduleParameters") instanceof Map<?,?> m?m:Map.of();
        Map<?,?> internal=run.get("parameters") instanceof Map<?,?> m?m:Map.of();
        for(String name:compiled.names()) {
            String value;
            if(name.startsWith("\u0001")) {
                var matcher=Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]{0,63})}").matcher(name.substring(1));var result=new StringBuilder();
                while(matcher.find())matcher.appendReplacement(result,Matcher.quoteReplacement(required(custom,matcher.group(1))));matcher.appendTail(result);value=result.toString();
            }else {
                value=required(name.startsWith("$")?custom:internal,name.startsWith("$")?name.substring(1):name);
                // Match the existing JDBC built-in: a UTC DATETIME value, not an ISO string with Z.
                if(name.equals("source_cutoff"))value=LocalDateTime.ofInstant(Instant.parse(value),ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"));
            }
            values.add(value);
        }
        if(!doris)return new Compiled(compiled.sql(),values.stream().map(v->Map.<String,Object>of("type","string","value",v)).toList());
        String sql=compiled.sql();var out=new StringBuilder();int param=0;
        for(int i=0;i<sql.length();) {
            char ch=sql.charAt(i);
            if(ch=='\''||ch=='"'||ch=='`') {
                char quote=ch;out.append(ch);i++;
                while(i<sql.length()){char c=sql.charAt(i++);out.append(c);if(c=='\\'&&i<sql.length()){out.append(sql.charAt(i++));continue;}if(c==quote){if(i<sql.length()&&sql.charAt(i)==quote){out.append(sql.charAt(i++));continue;}break;}}
            } else if(ch=='#'||(ch=='-'&&i+2<sql.length()&&sql.charAt(i+1)=='-'&&Character.isWhitespace(sql.charAt(i+2)))) {
                while(i<sql.length()&&sql.charAt(i)!='\n')out.append(sql.charAt(i++));
            } else if(ch=='/'&&i+1<sql.length()&&sql.charAt(i+1)=='*') {int end=sql.indexOf("*/",i+2);out.append(sql,i,end+2);i=end+2;}
            else if(ch=='?'){out.append("FROM_BASE64('").append(Base64.getEncoder().encodeToString(values.get(param++).getBytes(StandardCharsets.UTF_8))).append("')");i++;}
            else {out.append(ch);i++;}
        }
        return new Compiled(out.toString(),List.of());
    }
    private static String required(Map<?,?> values,String name){Object v=values.get(name);if(v==null)throw StudioException.bad("MISSING_PARAMETER","缺少同步参数："+name);return v.toString();}
}
