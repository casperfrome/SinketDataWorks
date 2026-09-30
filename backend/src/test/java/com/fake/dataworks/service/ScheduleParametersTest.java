package com.fake.dataworks.service;

import com.fake.dataworks.controller.ScheduleParameterController;
import com.fake.dataworks.exception.StudioException;
import java.sql.PreparedStatement;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ScheduleParametersTest {
    ScheduleParameters.Context at(String day,String time) {return new ScheduleParameters.Context(LocalDate.parse(day),ZonedDateTime.parse(time));}
    final ScheduleParameters.Context example=at("2026-09-28","2026-09-29T02:00:00+08:00[Asia/Shanghai]");
    @Test void distinguishesBusinessAndScheduledTimeAndFormatsConstants() {
        assertEquals("20260928",ScheduleParameters.evaluate("$[yyyymmdd-1]",example));
        assertEquals("20260928",ScheduleParameters.evaluate("${yyyymmdd}",example));
        assertEquals("20260927",ScheduleParameters.evaluate("${yyyymmdd-1}",example));
        assertEquals("20260929020000",ScheduleParameters.evaluate("$cyctime",example));
        assertEquals("20260928",ScheduleParameters.evaluate("$bizdate",example));
        assertEquals("partition_2026-09-28_01",ScheduleParameters.evaluate("partition_${yyyy-mm-dd}_01",example));
        assertEquals("杭州",ScheduleParameters.evaluate("杭州",example));
        assertEquals("202508",ScheduleParameters.evaluate("${yyyymm-13}",example));
    }
    @Test void handlesLeapDatesMonthEndsYearsAndFractionalOffsets() {
        var leap=at("2024-03-31","2024-03-31T00:05:00Z[UTC]");
        assertEquals("20240229",ScheduleParameters.evaluate("$[add_months(yyyymmdd,-1)]",leap));
        assertEquals("20230331",ScheduleParameters.evaluate("$[add_months(yyyymmdd,-12)]",leap));
        assertEquals("20240330235000",ScheduleParameters.evaluate("$[yyyymmddhh24miss-15/24/60]",leap));
        assertEquals("20240329230500",ScheduleParameters.evaluate("$[yyyymmddhh24miss-1-1/24]",leap));
        assertEquals("20240317",ScheduleParameters.evaluate("${yyyymmdd-7*2}",leap));
        assertEquals("20231231",ScheduleParameters.evaluate("$[yyyymmdd-1]",at("2023-12-31","2024-01-01T00:00:00Z[UTC]")));
    }
    @Test void rejectsMalformedUnsupportedAndDuplicateDefinitions() {
        for(String value:List.of("${hh24}","${yyyymmdd-1/24}","$[yyyy-1]","$[yyyymmdd+1/60]","$[unknown]","$unknown","$[yyyymmdd")) assertThrows(StudioException.class,()->ScheduleParameters.evaluate(value,example),value);
        var row=Map.of("name","bizdate","value","$bizdate","source","CODE");
        assertThrows(StudioException.class,()->ScheduleParameters.rows(List.of(row,row)));
        for(String value:List.of("","a b","a=b"))assertThrows(StudioException.class,()->ScheduleParameters.rows(List.of(Map.of("name","x","value",value))));
        assertThrows(StudioException.class,()->ScheduleParameters.validateConfig(Map.of("schedule",Map.of("parameterExpressionDraft","bad input"))));
    }
    @Test void extractsCodeParametersAndBindsQuotedAndUnquotedValuesWithoutInjection() throws Exception {
        String code="SELECT '${bizdate}', ${region}, 'prefix_${region}_suffix', :bizdate /* ${ignored} */ -- ${also_ignored}\n";
        assertEquals(List.of("bizdate","region"),SqlParameters.extract(code));
        var compiled=SqlParameters.compile(code);new SqlGuard().validate(compiled.sql(),"studio_demo");
        var statement=mock(PreparedStatement.class);String hostile="x';DROP TABLE users;--";
        compiled.bind(statement,Map.of("bizdate","2026-09-28"),Map.of("bizdate","20260928","region",hostile),0);
        verify(statement).setString(1,"20260928");verify(statement).setString(2,hostile);verify(statement).setString(3,"prefix_"+hostile+"_suffix");verify(statement).setObject(4,LocalDate.of(2026,9,28));
        assertThrows(StudioException.class,()->SqlParameters.compile("SELECT `${column}` FROM users"));
        assertThrows(StudioException.class,()->new SqlGuard().validate(SqlParameters.compile("SELECT * FROM ${table}").sql(),"studio_demo"));
        assertThrows(StudioException.class,()->compiled.bind(statement,Map.of("bizdate","2026-09-28"),Map.of(),0));
    }
    @Test void previewsCronSlotsAndFixedContextWithoutUsingActualExecutionTime() {
        var times=new ScheduleService(null,null,null,null,null,null,null,false);
        var endpoint=new ScheduleParameterController(times);
        var input=new LinkedHashMap<String,Object>(Map.of("businessDate","2026-09-28","cron","0 0 2 * * *","timezone","Asia/Shanghai","count",2,"parameters",List.of(Map.of("name","bizdate","value","$[yyyymmdd-1]"))));
        var preview=(List<?>)endpoint.preview(input);var first=(Map<?,?>)preview.getFirst();
        assertEquals("2026-09-28T18:00:00Z",first.get("scheduledAt"));assertEquals(Map.of("bizdate","20260928"),first.get("values"));
        var context=ScheduleParameters.context(times.timeConfig(input),Map.of("scheduledAt",first.get("scheduledAt"),"businessDate",first.get("businessDate")));
        assertEquals("20260928",ScheduleParameters.evaluate("$[yyyymmdd-1]",context));
        input.put("timezone","UTC");assertEquals("2026-09-29T02:00:00Z",((Map<?,?>)((List<?>)endpoint.preview(input)).getFirst()).get("scheduledAt"));
    }
    @Test void nodeDefinitionsOverrideInheritedDefaults() {
        var parent=List.of(Map.of("name","a","value","parent","source","MANUAL"),Map.of("name","b","value","$bizdate","source","MANUAL"));
        var child=List.of(Map.of("name","a","value","child","source","CODE"));
        assertEquals(Map.of("a","child","b","20260928"),ScheduleParameters.resolve(ScheduleParameters.merge(parent,child),example));
    }
}
