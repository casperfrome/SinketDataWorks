package com.fake.dataworks.service;

import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.DebugParameterRepository;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DebugParametersTest {
    final DebugParameterRepository profiles=mock(DebugParameterRepository.class);
    final ObjectService objects=mock(ObjectService.class);
    final DebugParameterService service=new DebugParameterService(profiles,objects);
    StudioObject node(String code,List<Map<String,String>> defaults) {
        return new StudioObject("node","workspace",null,"NODE","MySQL","SQL","",code,Map.of("run",Map.of("provider","MYSQL"),"schedule",Map.of("parameters",defaults,"timezone","Asia/Shanghai","businessDateOffset",-1)),List.of(),false,false,1,"admin","now");
    }
    final List<Map<String,String>> defaults=List.of(Map.of("name","day","value","$bizdate"),Map.of("name","region","value","杭州"));
    Map<String,Object> options() {return Map.of("businessDate","2026-09-28","sourceCutoffAt","2026-09-29T01:00:00Z");}
    @Test void preparationUsesCapturedSqlAndReportsAllMissingValuesWithoutWriting() {
        var saved=node("SELECT '${region}'",defaults);when(objects.active("node")).thenReturn(saved);
        when(profiles.load("node")).thenReturn(Map.of("region","old value", "removed","stale"));
        var captured=node("SELECT '${day}', ${region}, '${one}', '${two}' /* ${ignored} */",defaults);
        var result=service.prepare(captured);
        assertEquals(List.of("day","region","one","two"),result.parameters().stream().map(DebugParameterService.Parameter::name).toList());
        assertEquals(List.of("one","two"),result.missingParameters());assertEquals(Map.of("region","old value"),result.debugParameters());
        assertEquals("SCHEDULE",result.parameters().get(0).source());assertEquals("DEBUG",result.parameters().get(1).source());
        assertEquals("杭州",result.parameters().get(1).defaultValue());assertEquals("",result.parameters().get(2).value());
        verify(profiles,never()).save(anyString(),anyMap(),anyString());
    }
    @Test void frozenDefaultsRecalculateAndOmissionUsesOnlyReferencedRememberedConstants() {
        var node=node("SELECT '${day}', '${region}'",defaults);when(profiles.load("node")).thenReturn(Map.of("region","a b=c","removed","stale"));
        var first=service.freeze(node,options());var later=service.freeze(node,Map.of("businessDate","2026-09-29"));
        assertEquals(Map.of("day","20260928","region","a b=c"),first.options().get("scheduleParameters"));
        assertEquals(Map.of("day","20260929","region","a b=c"),later.options().get("scheduleParameters"));
        assertEquals("2026-09-29T01:00:00Z",first.options().get("sourceCutoffAt"));assertFalse(first.remember());
        service.remember("node",first);verify(profiles,never()).save(anyString(),anyMap(),anyString());
    }
    @Test void explicitMapReplacesProfileAndEmptyMapRestoresDefaultWithoutFreezingIt() {
        var node=node("SELECT '${day}', '${region}'",defaults);when(profiles.load("node")).thenReturn(Map.of("day","old date","region","old region"));
        var input=new LinkedHashMap<>(options());input.put("debugParameters",Map.of("region","$bizdate ${literal} O'Reilly = 中文"));
        var frozen=service.freeze(node,input);
        assertEquals(Map.of("day","20260928","region","$bizdate ${literal} O'Reilly = 中文"),frozen.options().get("scheduleParameters"));
        assertEquals(Map.of("region","$bizdate ${literal} O'Reilly = 中文"),frozen.debugParameters());assertTrue(frozen.remember());
        service.remember("node",frozen);verify(profiles).save(eq("node"),eq(frozen.debugParameters()),anyString());
        input.put("debugParameters",Map.of());var cleared=service.freeze(node,input);
        assertEquals(Map.of("day","20260928","region","杭州"),cleared.options().get("scheduleParameters"));
        assertTrue(cleared.debugParameters().isEmpty());verify(profiles,never()).load(anyString());
    }
    @Test void requiresACompleteSetAndRejectsMalformedOverrides() {
        var node=node("SELECT '${one}', '${two}'",List.of());when(profiles.load("node")).thenReturn(Map.of());
        var missing=assertThrows(StudioException.class,()->service.freeze(node,Map.of()));
        assertEquals("MISSING_DEBUG_PARAMETERS",missing.code());assertTrue(missing.getMessage().contains("one、two"));
        var invalid=new LinkedHashMap<String,Object>();invalid.put("debugParameters",null);
        for(Object raw:Arrays.asList(null,List.of(),Map.of("one",23),Map.of("one",""),Map.of("one","x".repeat(4097)),Map.of("unknown","x"),Map.of("bad-name","x"))) {
            invalid.put("debugParameters",raw);assertEquals("INVALID_DEBUG_PARAMETERS",assertThrows(StudioException.class,()->service.freeze(node,invalid)).code());
        }
        verify(profiles,never()).save(anyString(),anyMap(),anyString());
    }
    @Test void capturesSyncFiltersAndPartitionValuesUsingTheSameScanner() {
        var config=Map.<String,Object>of("run",Map.of("provider","SYNC"),"sync",Map.of("where","day='${day}'", "sourcePartitionFilter",Map.of("where","region=${region}"),"targetPartitionAssignments",List.of(Map.of("mode","value","value","prefix_${partition}"))),"schedule",Map.of("parameters",defaults));
        var node=new StudioObject("node","workspace",null,"NODE","离线同步","sync","","",config,List.of(),false,false,1,"admin","now");
        when(objects.active("node")).thenReturn(node);when(profiles.load("node")).thenReturn(Map.of("partition","actual"));
        var prepared=service.prepare(node);assertTrue(prepared.missingParameters().isEmpty());
        assertEquals(List.of("day","region","partition"),prepared.parameters().stream().map(DebugParameterService.Parameter::name).toList());
        assertEquals("actual",prepared.parameters().get(2).value());
    }
    @Test void preparationRejectsStaleVersionsAndNonNodeCaptures() {
        var node=node("SELECT 1",List.of());when(objects.active("node")).thenReturn(node);
        var stale=new StudioObject(node.id(),node.workspaceId(),null,node.kind(),node.nodeType(),node.name(),"",node.content(),node.config(),List.of(),false,false,2,"admin","now");
        assertEquals("VERSION_CONFLICT",assertThrows(StudioException.class,()->service.prepare(stale)).code());
        assertEquals("INVALID_DEBUG_PARAMETERS",assertThrows(StudioException.class,()->service.prepare(null)).code());
        verifyNoInteractions(profiles);
    }
    @Test void allProvidersShareTheExistingBusinessDateAndCutoffValidation() {
        var node=node("SELECT 1",List.of());when(profiles.load("node")).thenReturn(Map.of());
        for(var invalid:List.of(Map.<String,Object>of("businessDate","2099-01-01"),Map.<String,Object>of("sourceCutoffAt","invalid"),Map.<String,Object>of("sourceCutoffAt","2099-01-01T00:00:00Z")))
            assertEquals("INVALID_BUSINESS_DATE",assertThrows(StudioException.class,()->service.freeze(node,invalid)).code());
        verify(profiles,never()).save(anyString(),anyMap(),anyString());
    }
}
