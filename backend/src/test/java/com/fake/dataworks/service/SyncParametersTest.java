package com.fake.dataworks.service;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.Test;

class SyncParametersTest {
    private Map<String,Object> run(String value){return Map.of("scheduleParameters",Map.of("value",value),"parameters",Map.of("bizdate","2026-09-29"));}
    @Test void mysqlBindsWithoutInterpolation(){
        String hostile="中文' OR 1=1 -- \\";var result=SyncParameters.compile("name = '${value}' AND d=:bizdate",run(hostile),false);
        assertEquals("name = ? AND d=?",result.sql());assertEquals(hostile,result.params().getFirst().get("value"));assertEquals("2026-09-29",result.params().get(1).get("value"));
    }
    @Test void flightEncodingPreservesLiteralsAndComments(){
        String hostile="中文' OR 1=1 -- \\";var result=SyncParameters.compile("name='prefix${value}' AND other='?' /* ? */ AND d=:bizdate",run(hostile),true);
        assertTrue(result.sql().contains("other='?' /* ? */"));assertFalse(result.sql().contains("OR 1=1"));
        assertTrue(result.sql().contains(Base64.getEncoder().encodeToString(("prefix"+hostile).getBytes(StandardCharsets.UTF_8))));assertTrue(result.params().isEmpty());
    }
    @Test void missingAndIdentifierParametersFail(){assertThrows(RuntimeException.class,()->SyncParameters.compile("x=${missing}",run("x"),true));assertThrows(RuntimeException.class,()->SyncParameters.compile("`${value}`=1",run("x"),false));}
    @Test void cutoffUsesMicrosecondUtcDatetimeAndKeepsCustomValuesLiteral(){
        var context=Map.<String,Object>of("parameters",Map.of("source_cutoff","2026-09-29T15:20:31.123456789Z"),"scheduleParameters",Map.of("source_cutoff","unchanged"));
        var result=SyncParameters.compile("event_at<=:source_cutoff AND note='${source_cutoff}'",context,false);
        assertEquals("2026-09-29 15:20:31.123456",result.params().getFirst().get("value"));assertEquals("unchanged",result.params().get(1).get("value"));
    }
    @Test void dateAssignmentsNormalizeOnlyPartitionValues(){
        var context=Map.<String,Object>of("parameters",Map.of("bizdate","2026-09-28"),"scheduleParameters",Map.of("bizdate","20260929"));
        assertEquals("20260929",SyncParameters.value("${bizdate}",context));assertEquals("2026-09-28",SyncParameters.value(":bizdate",context));
        assertEquals("2026-09-29",SyncParameters.partitionValue(SyncParameters.value("${bizdate}",context),"date"));
        assertEquals("20260929",SyncParameters.compile("ds='${bizdate}'",context,false).params().getFirst().get("value"));
        for(String invalid:List.of("20260230","2026-02-30","2026-9-29","20260929' OR 1=1","2026-09-29T00:00:00"))assertThrows(RuntimeException.class,()->SyncParameters.partitionValue(invalid,"date"));
    }
    @Test void assignmentScannerTreatsTextAsLiteralAndOnlyWholeBuiltinValuesAsParameters(){
        for(String text:List.of("O'Reilly","foo:bar","http://example","C:\\orders\\","embedded:bizdate? -- comment")) {
            var code=SyncParameters.parameterCode(text);assertTrue(SqlParameters.compile(code).names().isEmpty());assertTrue(SqlParameters.extract(code).isEmpty());assertEquals(text,SyncParameters.value(text,run("custom")));
        }
        String template="O'Reilly${value}\\foo:bar? --";String code=SyncParameters.parameterCode(template);
        assertEquals(List.of("value"),SqlParameters.extract(code));assertEquals("O'Reillycustom\\foo:bar? --",SyncParameters.compile(code,run("custom"),false).params().getFirst().get("value"));
        for(String builtin:List.of(":bizdate",":source_cutoff",":build_id",":upstream_orders_build_id"))assertEquals(List.of(builtin.substring(1)),SqlParameters.compile(SyncParameters.parameterCode(builtin)).names());
        assertThrows(RuntimeException.class,()->SqlParameters.compile(SyncParameters.parameterCode("${unclosed")));
    }
    @Test void assignmentOverridesAreLiteralAndNeverParsedAsNestedTemplates(){
        for(String literal:List.of("${literal}","${unclosed","$[yyyymmdd-1] $bizdate", "value ${literal} = 中文 O'Reilly \\")) {
            String resolved=SyncParameters.value("prefix_${value}_suffix",run(literal));
            assertEquals("prefix_"+literal+"_suffix",resolved);
            assertEquals(resolved,SyncParameters.partitionValue(resolved,"varchar(4096)"));
            assertEquals(literal,SyncParameters.compile("name='${value}'",run(literal),false).params().getFirst().get("value"));
        }
        for(String malformed:List.of("${unclosed","${value}_${unclosed","${bad-name}","${value} ${}"))
            assertThrows(RuntimeException.class,()->SyncParameters.value(malformed,run("${literal}")));
        assertThrows(RuntimeException.class,()->SyncParameters.partitionValue(SyncParameters.value("${value}",run("${literal}")),"DATEV2"));
        assertEquals("2026-09-29",SyncParameters.partitionValue(SyncParameters.value("${value}",run("20260929")),"DATEV2"));
    }
}
