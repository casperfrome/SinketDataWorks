package com.fake.dataworks.service;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class TaskScheduleRulesTest {
    final ScheduleService times=new ScheduleService(null,null,null,null,null,null,null,false);
    final TaskScheduleService tasks=new TaskScheduleService(null,null,null,times,null,false);
    @Test void hourlyUpstreamMatchesLatestSlotBeforeDailyDownstream(){
        var upstream=times.timeConfig(Map.of("cron","0 0 * * * *"));
        assertEquals(Instant.parse("2026-09-27T18:00:00Z"),tasks.matchingSlot(upstream,"2026-09-27",Instant.parse("2026-09-27T18:05:00Z")));
        assertNull(tasks.matchingSlot(upstream,"2026-09-28",Instant.parse("2026-09-27T18:05:00Z")));
    }
    @Test void minuteAndDstSlotsRemainDistinctAndDateBoundsApply(){
        var minute=times.timeConfig(Map.of("cron","0 * * * * *","timezone","UTC"));assertEquals(Instant.parse("2026-09-28T02:05:00Z"),tasks.matchingSlot(minute,"2026-09-27",Instant.parse("2026-09-28T02:05:59Z")));
        var dst=times.timeConfig(Map.of("cron","0 30 1 * * *","timezone","America/New_York"));assertEquals(Instant.parse("2026-11-01T06:30:00Z"),tasks.matchingSlot(dst,"2026-10-31",Instant.parse("2026-11-01T07:00:00Z")));
        var ended=times.timeConfig(Map.of("endDate","2026-09-26"));assertNull(tasks.matchingSlot(ended,"2026-09-27",Instant.parse("2026-09-28T06:00:00Z")));
    }
}
