package com.fake.dataworks.service;

import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.DebugParameterRepository;
import java.util.*;
import org.springframework.stereotype.Service;

/** Resolves scheduling defaults and literal debug overrides without changing the node. */
@Service
public class DebugParameterService {
    public record Parameter(String name,String value,String defaultValue,String source) {}
    public record Preparation(List<Parameter> parameters,Map<String,String> debugParameters,List<String> missingParameters) {}
    public record Frozen(Map<String,Object> options,Map<String,String> debugParameters,boolean remember) {}
    private final DebugParameterRepository profiles;
    private final ObjectService objects;
    public DebugParameterService(DebugParameterRepository profiles,ObjectService objects) {this.profiles=profiles;this.objects=objects;}

    public Preparation prepare(StudioObject captured) {
        if(captured==null||captured.id()==null||captured.config()==null||captured.content()==null)throw invalid("请提供完整的节点草稿");
        var current=objects.active(captured.id());
        if(!"NODE".equals(current.kind())||!"NODE".equals(captured.kind())||!current.workspaceId().equals(captured.workspaceId()))throw invalid("调试参数只适用于当前工作空间的单个节点");
        if(captured.version()!=current.version())throw StudioException.conflict("VERSION_CONFLICT","节点已被修改，请刷新并重新运行");
        var names=names(captured);
        var context=ScheduleParameters.context(ScheduleParameters.schedule(captured),Map.of());
        var defaults=defaults(captured,context);
        var overrides=overrides(profiles.load(current.id()),names,false);
        var rows=new ArrayList<Parameter>();var missing=new ArrayList<String>();
        for(String name:names) {
            String fallback=defaults.get(name),value=overrides.getOrDefault(name,fallback);
            String source=overrides.containsKey(name)?"DEBUG":fallback!=null?"SCHEDULE":"MISSING";
            rows.add(new Parameter(name,value==null?"":value,fallback,source));
            if(value==null)missing.add(name);
        }
        return new Preparation(rows,overrides,missing);
    }

    /** Called after the workspace lock; every provider receives this same immutable time/value context. */
    public Frozen freeze(StudioObject object,Map<String,Object> requested) {
        var names=names(object);boolean remember=requested.containsKey("debugParameters");
        var overrides=overrides(remember?requested.get("debugParameters"):profiles.load(object.id()),names,remember);
        var context=ScheduleParameters.context(ScheduleParameters.schedule(object),requested);
        var values=new LinkedHashMap<>(defaults(object,context));values.putAll(overrides);
        var missing=names.stream().filter(name->!values.containsKey(name)).toList();
        if(!missing.isEmpty())throw StudioException.bad("MISSING_DEBUG_PARAMETERS","请填写运行参数："+String.join("、",missing));
        var options=new LinkedHashMap<>(requested);options.remove("debugParameters");options.putAll(context.toMap());
        var internal=InventoryExecutionService.runtimeParameters(options,"DEBUG_PREPARE");
        options.put("sourceCutoffAt",internal.get("source_cutoff"));options.put("scheduleParameters",Collections.unmodifiableMap(values));
        return new Frozen(Collections.unmodifiableMap(options),Collections.unmodifiableMap(overrides),remember);
    }

    public void remember(String objectId,Frozen frozen) {
        if(frozen.remember())profiles.save(objectId,frozen.debugParameters(),ObjectService.now());
    }

    private static List<String> names(StudioObject object) {
        var names=SqlParameters.extract(SyncExecutionService.parameterCode(object));
        if(names.size()>100)throw invalid("最多支持 100 个运行参数");return names;
    }
    private static Map<String,String> defaults(StudioObject object,ScheduleParameters.Context context) {
        ScheduleParameters.validateConfig(object.config());
        return ScheduleParameters.resolve(ScheduleParameters.definitions(object),context);
    }
    private static Map<String,String> overrides(Object raw,List<String> names,boolean rejectUnknown) {
        if(!(raw instanceof Map<?,?> map)||map.size()>100)throw invalid("运行参数须为最多 100 项的参数名与常量值映射");
        var result=new LinkedHashMap<String,String>();
        for(var entry:map.entrySet()) {
            if(!(entry.getKey() instanceof String name)||!name.matches("[A-Za-z_][A-Za-z0-9_]{0,63}"))throw invalid("运行参数名格式无效");
            if(!names.contains(name)) {if(rejectUnknown)throw invalid("代码未引用运行参数："+name);continue;}
            if(!(entry.getValue() instanceof String value)||value.isEmpty()||value.length()>4096)throw invalid("参数「"+name+"」须为非空常量字符串，且不超过 4096 个字符");
            result.put(name,value);
        }
        return result;
    }
    private static StudioException invalid(String message) {return StudioException.bad("INVALID_DEBUG_PARAMETERS",message);}
}
