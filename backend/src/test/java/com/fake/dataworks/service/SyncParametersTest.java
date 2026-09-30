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
}
