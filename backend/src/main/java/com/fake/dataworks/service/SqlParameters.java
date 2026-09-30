package com.fake.dataworks.service;

import com.fake.dataworks.exception.StudioException;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.regex.*;

/** SQL value placeholders become JDBC parameters; values can never become SQL syntax. */
public record SqlParameters(String sql,List<String> names) {
    private static final Pattern MACRO=Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]{0,63})}");
    private static final String LITERAL="\u0001";
    public static List<String> extract(String input) {
        if(input==null||input.length()>200_000)throw StudioException.bad("INVALID_SQL","代码不能超过 200000 字符");
        var code=new StringBuilder();
        for(int i=0;i<input.length();) {
            char ch=input.charAt(i);
            if(ch=='\''||ch=='"'||ch=='`') {
                char quote=ch;code.append(ch);i++;
                while(i<input.length()){char c=input.charAt(i++);code.append(c);if(c=='\\'&&i<input.length()){code.append(input.charAt(i++));continue;}if(c==quote){if(i<input.length()&&input.charAt(i)==quote){code.append(input.charAt(i++));continue;}break;}}
            } else if(ch=='#'||(ch=='-'&&i+2<input.length()&&input.charAt(i+1)=='-'&&Character.isWhitespace(input.charAt(i+2)))) {
                while(i<input.length()&&input.charAt(i)!='\n')i++;
            } else if(ch=='/'&&i+1<input.length()&&input.charAt(i+1)=='*') {int end=input.indexOf("*/",i+2);i=end<0?input.length():end+2;}
            else code.append(input.charAt(i++));
        }
        var found=new LinkedHashSet<String>();var matcher=MACRO.matcher(code);while(matcher.find())found.add(matcher.group(1));return new ArrayList<>(found);
    }
    public static SqlParameters compile(String input) {
        if(input==null)throw StudioException.bad("INVALID_SQL","SQL 不能为空");
        StringBuilder out=new StringBuilder();List<String> names=new ArrayList<>();
        for(int i=0;i<input.length();) {
            char ch=input.charAt(i);
            if(ch=='\''||ch=='"'||ch=='`') {
                int start=i;char quote=ch;i++;boolean closed=false;var literal=new StringBuilder();
                while(i<input.length()) {
                    char c=input.charAt(i++);
                    if(c=='\\'&&i<input.length()){char escaped=input.charAt(i++);literal.append(switch(escaped){case 'n'->'\n';case 'r'->'\r';case 't'->'\t';case '0'->'\0';case 'b'->'\b';default->escaped;});continue;}
                    if(c==quote){if(i<input.length()&&input.charAt(i)==quote){literal.append(input.charAt(i++));continue;}closed=true;break;}literal.append(c);
                }
                if(!closed)throw StudioException.bad("INVALID_SQL","SQL 引号未闭合");
                if(literal.indexOf("${")>=0) {
                    if(quote=='`')throw StudioException.bad("IDENTIFIER_PARAMETER","调度参数仅支持 SQL 值，不支持动态表名或列名");
                    if(MACRO.matcher(literal).replaceAll("").contains("${"))throw StudioException.bad("INVALID_PARAMETER","参数占位符须为 ${参数名}");
                    names.add(LITERAL+literal);out.append('?');
                } else out.append(input,start,i);
            } else if(ch=='#'||(ch=='-'&&i+2<input.length()&&input.charAt(i+1)=='-'&&Character.isWhitespace(input.charAt(i+2)))) {
                while(i<input.length()&&input.charAt(i)!='\n')out.append(input.charAt(i++));
            } else if(ch=='/'&&i+1<input.length()&&input.charAt(i+1)=='*') {
                int end=input.indexOf("*/",i+2);if(end<0)throw StudioException.bad("INVALID_SQL","SQL 注释未闭合");out.append(input,i,end+2);i=end+2;
            } else if(input.startsWith("${",i)) {
                var m=MACRO.matcher(input);m.region(i,input.length());if(!m.lookingAt())throw StudioException.bad("INVALID_PARAMETER","参数占位符须为 ${参数名}");
                names.add("$"+m.group(1));out.append('?');i=m.end();
            } else if(ch==':'&&i+1<input.length()&&Character.isJavaIdentifierStart(input.charAt(i+1))) {
                int start=++i;while(i<input.length()&&Character.isJavaIdentifierPart(input.charAt(i)))i++;
                String name=input.substring(start,i);if(!Set.of("bizdate","source_cutoff","build_id").contains(name)&&!name.matches("upstream_[A-Za-z][A-Za-z0-9_]{0,31}_build_id"))throw StudioException.bad("UNKNOWN_PARAMETER","未知参数："+name);
                names.add(name);out.append('?');
            } else {if(ch=='?')throw StudioException.bad("UNNAMED_PARAMETER","请使用命名参数");out.append(ch);i++;}
        }
        return new SqlParameters(out.toString().strip().replaceFirst(";\\s*$",""),List.copyOf(names));
    }
    public void bind(PreparedStatement statement,Map<String,Object> context,int offset) throws SQLException {bind(statement,context,Map.of(),offset);}
    public void bind(PreparedStatement statement,Map<String,Object> context,Map<String,?> custom,int offset) throws SQLException {
        for(int i=0;i<names.size();i++) {
            String name=names.get(i);int index=i+1+offset;
            if(name.startsWith(LITERAL)) {
                var matcher=MACRO.matcher(name.substring(1));var value=new StringBuilder();while(matcher.find())matcher.appendReplacement(value,Matcher.quoteReplacement(required(custom,matcher.group(1)).toString()));matcher.appendTail(value);statement.setString(index,value.toString());
            } else if(name.startsWith("$"))statement.setString(index,required(custom,name.substring(1)).toString());
            else {
                Object value=required(context,name);
                switch(name) {case "bizdate"->statement.setObject(index,LocalDate.parse(value.toString()));case "source_cutoff"->statement.setObject(index,LocalDateTime.ofInstant(Instant.parse(value.toString()),ZoneOffset.UTC));default->statement.setString(index,value.toString());}
            }
        }
    }
    private static Object required(Map<String,?> values,String name){Object value=values.get(name);if(value==null)throw StudioException.bad("MISSING_PARAMETER","缺少参数："+name);return value;}
    @SuppressWarnings("unchecked") public static Map<String,?> custom(Map<String,Object> run){return run.get("scheduleParameters") instanceof Map<?,?> m?(Map<String,?>)m:Map.of();}
}
