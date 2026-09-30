package com.fake.dataworks.controller;

import com.fake.dataworks.service.*;
import com.fake.dataworks.exception.StudioException;
import java.time.*;
import java.util.*;
import org.springframework.web.bind.annotation.*;

@RestController @RequestMapping("/api/v1/schedule-parameters")
public class ScheduleParameterController {
    private final ScheduleService schedules;
    public ScheduleParameterController(ScheduleService schedules){this.schedules=schedules;}
    @PostMapping("/extract") public Object extract(@RequestBody Map<String,Object> input) {
        return SqlParameters.extract(Objects.toString(input.get("code"),""));
    }
    @PostMapping("/preview") public Object preview(@RequestBody Map<String,Object> input) {
        var rows=ScheduleParameters.rows(input.get("parameters"));var config=schedules.timeConfig(input);
        for(String name:SqlParameters.extract(Objects.toString(input.get("code"),"")))if(rows.stream().noneMatch(r->name.equals(r.get("name"))))throw StudioException.bad("MISSING_PARAMETER","代码中的参数尚未赋值："+name);
        Object raw=input.getOrDefault("count",5);if(!(raw instanceof Number n)||n.doubleValue()!=n.intValue()||n.intValue()<1||n.intValue()>20)throw StudioException.bad("INVALID_PREVIEW_COUNT","预览数量须为 1–20");
        var context=ScheduleParameters.context(config,Map.of("businessDate",Objects.toString(input.get("businessDate"),LocalDate.now(ZoneId.of(config.get("timezone").toString())).minusDays(1).toString())));
        ScheduleParameters.resolve(rows,context);
        Instant at=context.scheduledAt().toInstant().minusNanos(1);var result=new ArrayList<Map<String,Object>>();
        for(int i=0;i<n.intValue();i++) {
            at=schedules.next(config,at);if(at==null)break;
            var fixed=ScheduleParameters.context(config,Map.of("scheduledAt",at.toString()));
            var item=new LinkedHashMap<>(fixed.toMap());item.put("values",ScheduleParameters.resolve(rows,fixed));result.add(item);
        }
        return result;
    }
}
