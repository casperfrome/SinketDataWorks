package com.fake.dataworks.service;

import com.fake.dataworks.exception.StudioException;
import java.sql.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ScheduleRulesTest {
    final ScheduleService service=new ScheduleService(null,null,null,null,null,null,null,false);
    @Test void previewsDefaultMinuteHourWeekAndMonthWithinEffectiveDates(){
        var daily=service.timeConfig(Map.of());assertFalse((boolean)daily.get("enabled"));assertEquals(0,daily.get("retries"));assertEquals(-1,daily.get("businessDateOffset"));
        assertEquals(Instant.parse("2026-09-28T18:00:00Z"),service.next(daily,Instant.parse("2026-09-28T00:00:00Z")));
        for(var entry:Map.of("0 */5 * * * *","2026-09-28T00:05:00Z","0 15 */2 * * *","2026-09-28T00:15:00Z","0 0 2 * * TUE","2026-09-28T18:00:00Z","0 0 2 31 * *","2026-10-30T18:00:00Z").entrySet())assertEquals(Instant.parse(entry.getValue()),service.next(service.timeConfig(Map.of("cron",entry.getKey())),Instant.parse("2026-09-28T00:00:00Z")));
        var range=service.timeConfig(Map.of("startDate","2026-10-01","endDate","2026-10-02"));assertEquals(Instant.parse("2026-09-30T18:00:00Z"),service.next(range,Instant.parse("2026-09-28T00:00:00Z")));assertNull(service.next(range,Instant.parse("2026-10-01T18:00:00Z")));
        var preview=service.preview(Map.of("cron","0 * * * * *","timezone","Asia/Shanghai"));assertEquals(5,preview.size());for(var row:preview){Instant i=Instant.parse(row.get("scheduledAt").toString());assertEquals(0,i.atZone(ZoneOffset.UTC).getSecond());assertEquals(i.atZone(ZoneId.of("Asia/Shanghai")).toLocalDate().minusDays(1).toString(),row.get("businessDate"));}
    }
    @Test void handlesTimezoneDstTransitionsWithUniqueInstants(){
        var config=service.timeConfig(Map.of("cron","0 30 2 * * *","timezone","America/New_York"));
        assertEquals(Instant.parse("2026-03-09T06:30:00Z"),service.next(config,Instant.parse("2026-03-08T06:00:00Z")));
        var repeat=service.timeConfig(Map.of("cron","0 30 1 * * *","timezone","America/New_York"));var first=service.next(repeat,Instant.parse("2026-11-01T04:00:00Z"));var second=service.next(repeat,first);assertTrue(second.isAfter(first));assertEquals(Instant.parse("2026-11-01T05:30:00Z"),first);assertEquals(Instant.parse("2026-11-01T06:30:00Z"),second);
    }
    @Test void rejectsSubMinuteInvalidDatesAndUnboundedRetry(){
        for(String cron:List.of("* * * * * *","*/10 * * * * *","0 * * * *","garbage"))assertThrows(StudioException.class,()->service.timeConfig(Map.of("cron",cron)));
        for(var bad:List.of(Map.of("retries",11),Map.of("retryIntervalSeconds",30),Map.of("businessDateOffset",1),Map.of("timezone","nonsense"),Map.of("startDate","2026-10-03","endDate","2026-10-01")))assertThrows(StudioException.class,()->service.timeConfig(new HashMap<>(bad)));
    }
    @Test void bindsOnlyWhitelistedValuesOutsideSqlLiteralsAndComments() throws Exception {
        var sql=SqlParameters.compile("SELECT :bizdate, ':bizdate', :source_cutoff, :build_id /* :unknown */ -- :other\n;");assertEquals(List.of("bizdate","source_cutoff","build_id"),sql.names());assertTrue(sql.sql().contains("':bizdate'"));
        PreparedStatement stmt=mock(PreparedStatement.class);sql.bind(stmt,Map.of("bizdate","2026-09-27","source_cutoff","2026-09-28T00:00:00Z","build_id","abc"),1);verify(stmt).setObject(2,LocalDate.of(2026,9,27));verify(stmt).setObject(3,LocalDateTime.of(2026,9,28,0,0));verify(stmt).setString(4,"abc");
        assertThrows(StudioException.class,()->SqlParameters.compile("SELECT :unknown"));assertThrows(StudioException.class,()->SqlParameters.compile("SELECT ?"));assertThrows(StudioException.class,()->SqlParameters.compile("SELECT 'unclosed"));
        var guard=new SqlGuard();assertThrows(StudioException.class,()->guard.validate(SqlParameters.compile("SELECT :bizdate; DELETE FROM dim_sku").sql(),"studio_inventory"));assertThrows(StudioException.class,()->guard.validate("SELECT * FROM fake_dataworks_260927.dw_run","studio_inventory"));
    }
    @Test void retriesOnlyRecognizedTransientInventoryErrors(){
        assertEquals("DB_TRANSIENT",InventoryExecutionService.errorCode(new SQLException("deadlock","40001",1213)));assertEquals("DB_TRANSIENT",InventoryExecutionService.errorCode(new SQLException("connections","HY000",1040)));assertEquals("DATASOURCE_UNAVAILABLE",InventoryExecutionService.errorCode(new SQLException("link","08S01",0)));assertEquals("QUERY_TIMEOUT",InventoryExecutionService.errorCode(new SQLTimeoutException()));assertEquals("MATERIALIZATION_FAILED",InventoryExecutionService.errorCode(new SQLException("syntax","42000",1064)));
    }
}
