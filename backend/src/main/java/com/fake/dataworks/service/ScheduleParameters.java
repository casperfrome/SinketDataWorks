package com.fake.dataworks.service;

import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.exception.StudioException;
import java.time.*;
import java.util.*;
import java.util.regex.*;
import org.springframework.scheduling.support.CronExpression;

/** One evaluator for editor previews and immutable execution contexts. No expression is executed as code. */
public final class ScheduleParameters {
    private ScheduleParameters() {}
    public record Context(LocalDate businessDate, ZonedDateTime scheduledAt) {
        public Map<String,Object> toMap() { return Map.of("businessDate",businessDate.toString(),"scheduledAt",scheduledAt.toInstant().toString(),"timezone",scheduledAt.getZone().getId()); }
    }
    private static final Pattern NAME=Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,63}");
    private static final Pattern FORMAT=Pattern.compile("yyyy|hh24|hh12|yy|mm|dd|hh|mi|ss");
    private static final Pattern OFFSET=Pattern.compile("([+-])(\\d+)(?:\\*(\\d+))?((?:/24)?(?:/60)?)");
    private static StudioException bad(String message) { return StudioException.bad("INVALID_SCHEDULE_PARAMETER",message); }
    @SuppressWarnings("unchecked") public static Map<String,Object> schedule(StudioObject object) {
        return object.config().get("schedule") instanceof Map<?,?> m ? (Map<String,Object>)m : Map.of();
    }
    public static List<Map<String,String>> definitions(StudioObject object) { return rows(schedule(object).get("parameters")); }
    public static List<Map<String,String>> rows(Object raw) {
        if(raw==null)return List.of();
        if(!(raw instanceof List<?> items)||items.size()>100)throw bad("最多配置 100 个调度参数");
        var result=new ArrayList<Map<String,String>>();var names=new HashSet<String>();
        for(Object item:items) {
            if(!(item instanceof Map<?,?> row))throw bad("调度参数格式无效");
            String name=Objects.toString(row.get("name"),""),value=Objects.toString(row.get("value"),"");
            if(!NAME.matcher(name).matches())throw bad("参数名须以字母或下划线开头，且不超过 64 个字符："+name);
            if(!names.add(name))throw bad("参数名重复："+name);
            if(value.isEmpty()||value.length()>4096||value.chars().anyMatch(Character::isWhitespace)||value.contains("="))throw bad("参数「"+name+"」的值不能为空或包含空白、等号");
            result.add(Map.of("name",name,"value",value,"source","CODE".equals(row.get("source"))?"CODE":"MANUAL"));
        }
        return result;
    }
    public static void validateConfig(Map<String,Object> config) {
        if(config.get("schedule") instanceof Map<?,?> schedule) {
            if(schedule.get("parameterExpressionDraft")!=null)throw bad("调度参数表达式尚有错误，请修正后保存");
            var rows=rows(schedule.get("parameters"));
            resolve(rows,new Context(LocalDate.of(2024,3,31),ZonedDateTime.of(2024,4,1,2,0,0,0,ZoneId.of("Asia/Shanghai"))));
        }
    }
    public static Context context(Map<String,Object> schedule,Map<String,Object> options) {
        try {
            ZoneId zone=ZoneId.of(Objects.toString(options.get("timezone"),Objects.toString(schedule.get("timezone"),"Asia/Shanghai")));
            int offset=schedule.get("businessDateOffset") instanceof Number n?n.intValue():-1;
            if(options.get("scheduledAt")!=null) {
                var at=Instant.parse(options.get("scheduledAt").toString()).atZone(zone);
                return new Context(options.get("businessDate")!=null?LocalDate.parse(options.get("businessDate").toString()):at.toLocalDate().plusDays(offset),at);
            }
            if(options.get("businessDate")!=null) {
                LocalDate day=LocalDate.parse(options.get("businessDate").toString());
                var start=day.minusDays(offset).atStartOfDay(zone);
                var at=CronExpression.parse(Objects.toString(schedule.get("cron"),"0 0 2 * * *")).next(start.minusNanos(1));
                if(at==null||!at.toLocalDate().equals(start.toLocalDate()))throw bad("该业务日期没有匹配的计划执行时间");
                return new Context(day,at);
            }
            var at=Instant.now().atZone(zone);return new Context(at.toLocalDate().plusDays(offset),at);
        } catch(StudioException e){throw e;}catch(Exception e){throw bad("请检查业务日期、时区和 Cron 表达式");}
    }
    public static Map<String,String> resolve(List<Map<String,String>> rows,Context context) {
        var values=new LinkedHashMap<String,String>();
        for(var row:rows) {
            try { values.put(row.get("name"),evaluate(row.get("value"),context)); }
            catch(StudioException e){throw bad("参数「"+row.get("name")+"」："+e.getMessage());}
        }
        return values;
    }
    public static String evaluate(String value,Context context) {
        var out=new StringBuilder();
        for(int i=0;i<value.length();) {
            if(value.startsWith("${",i)||value.startsWith("$[",i)) {
                boolean business=value.charAt(i+1)=='{';char close=business?'}':']';int end=value.indexOf(close,i+2);
                if(end<0)throw bad("时间表达式未闭合");
                out.append(time(value.substring(i+2,end),business,context));i=end+1;
            } else if(value.charAt(i)=='$') {
                if(value.startsWith("$bizdate",i)){out.append(format("yyyymmdd",context.businessDate().atStartOfDay(context.scheduledAt().getZone()),true));i+=8;}
                else if(value.startsWith("$cyctime",i)){out.append(format("yyyymmddhh24miss",context.scheduledAt(),false));i+=8;}
                else throw bad("不支持的系统参数或表达式");
            } else {out.append(value.charAt(i++));}
        }
        return out.toString();
    }
    private static String time(String expression,boolean business,Context context) {
        ZonedDateTime at=business?context.businessDate().atStartOfDay(context.scheduledAt().getZone()):context.scheduledAt();
        String format=expression;String operations="";
        var months=Pattern.compile("add_months\\(([^,()]+),(-?\\d+)(?:\\*(\\d+))?\\)").matcher(expression);
        try {
            if(months.matches()) {
                if(business)throw bad("add_months 请使用 $[...] 格式");
                long count=Long.parseLong(months.group(2));if(months.group(3)!=null)count=Math.multiplyExact(count,Long.parseLong(months.group(3)));
                return format(months.group(1),at.plusMonths(count),false);
            }
            var split=Pattern.compile("^(.+?)([+-]\\d.*)$").matcher(expression);
            if(split.matches()){format=split.group(1);operations=split.group(2);}
            format(format,at,business); // Validate even when no arithmetic is requested.
            Matcher m=OFFSET.matcher(operations);int consumed=0;
            while(m.find()) {
                if(m.start()!=consumed)throw bad("不支持的时间偏移表达式");consumed=m.end();
                long count=Long.parseLong(m.group(2));if(m.group(3)!=null)count=Math.multiplyExact(count,Long.parseLong(m.group(3)));
                if(m.group(1).equals("-"))count=-count;
                if(Math.abs(count)>1_000_000)throw bad("时间偏移过大");
                String fraction=m.group(4);
                if(business){if(!fraction.isEmpty())throw bad("业务日期不支持小时或分钟偏移");at=format.contains("dd")?at.plusDays(count):format.contains("mm")?at.plusMonths(count):at.plusYears(count);}
                else if(fraction.equals("/24/60"))at=at.plusMinutes(count);
                else if(fraction.equals("/24"))at=at.plusHours(count);
                else if(!fraction.isEmpty())throw bad("小时使用 /24，分钟使用 /24/60");
                else {if(!format.contains("dd")&&!format.contains("hh")&&!format.contains("mi")&&!format.contains("ss"))throw bad("计划时间的年月偏移请使用 add_months");at=at.plusDays(count);}
            }
            if(consumed!=operations.length())throw bad("不支持的时间偏移表达式");
            return format(format,at,business);
        }catch(StudioException e){throw e;}catch(Exception e){throw bad("时间表达式或偏移超出有效范围");}
    }
    private static String format(String pattern,ZonedDateTime at,boolean business) {
        var matcher=FORMAT.matcher(pattern);var out=new StringBuilder();int end=0;boolean found=false;
        while(matcher.find()) {
            String literal=pattern.substring(end,matcher.start());if(!literal.matches("[-_/:T0-9]*"))throw bad("不支持的时间格式："+pattern);out.append(literal);
            String token=matcher.group();if(business&&Set.of("hh","hh12","hh24","mi","ss").contains(token))throw bad("业务日期只能格式化年月日");
            int value=switch(token){case "yyyy"->at.getYear();case "yy"->at.getYear()%100;case "mm"->at.getMonthValue();case "dd"->at.getDayOfMonth();case "hh24"->at.getHour();case "hh","hh12"->at.getHour()%12==0?12:at.getHour()%12;case "mi"->at.getMinute();default->at.getSecond();};
            out.append(String.format(Locale.ROOT,token.equals("yyyy")?"%04d":"%02d",value));end=matcher.end();found=true;
        }
        String suffix=pattern.substring(end);if(!found||!suffix.matches("[-_/:T0-9]*"))throw bad("不支持的时间格式："+pattern);return out.append(suffix).toString();
    }
    /** Parameter definitions are inherited, but values are resolved once per node in its parent's fixed time context. */
    public static List<Map<String,String>> merge(List<Map<String,String>> parent,List<Map<String,String>> child) {
        var merged=new LinkedHashMap<String,Map<String,String>>();parent.forEach(p->merged.put(p.get("name"),p));child.forEach(p->merged.put(p.get("name"),p));return new ArrayList<>(merged.values());
    }
    @SuppressWarnings("unchecked") public static void attach(Map<String,Object> run,StudioObject object,Map<String,Object> options,List<Map<String,String>> inherited) {
        Context context=context(schedule(object),options);
        run.putAll(context.toMap());
        var values=options.get("scheduleParameters") instanceof Map<?,?> fixed?(Map<String,String>)fixed:resolve(merge(inherited,definitions(object)),context);
        run.put("scheduleParameters",new LinkedHashMap<>(values));
        for(String name:SqlParameters.extract(SyncExecutionService.parameterCode(object)))if(!values.containsKey(name))throw bad("代码中的参数尚未赋值："+name);
    }
}
