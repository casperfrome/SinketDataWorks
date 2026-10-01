package com.fake.dataworks.service;

import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.concurrent.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

/** One local polling clock, using existing durable tables and execution providers. */
@Service
public class SchedulingClock {
    private final ScheduleService workflows;private final TaskScheduleService tasks;private final boolean enabled;
    private final ScheduledExecutorService timer=Executors.newSingleThreadScheduledExecutor(r->new Thread(r,"scheduling-clock"));
    private boolean tasksFirst=true;
    private static final org.slf4j.Logger log=org.slf4j.LoggerFactory.getLogger(SchedulingClock.class);
    public SchedulingClock(ScheduleService workflows,TaskScheduleService tasks,@Value("${studio.scheduler.enabled:true}") boolean enabled){this.workflows=workflows;this.tasks=tasks;this.enabled=enabled;}
    @EventListener(ApplicationReadyEvent.class) public void ready(){Instant now=Instant.now();int used=tasks.ready(now,200);workflows.ready(now,200-used);if(enabled)timer.scheduleWithFixedDelay(this::tick,1,1,TimeUnit.SECONDS);}
    private void tick(){Instant now=Instant.now();int remaining=200;if(tasksFirst){remaining-=scanTasks(now,remaining);scanWorkflows(now,remaining);}else{remaining-=scanWorkflows(now,remaining);scanTasks(now,remaining);}tasksFirst=!tasksFirst;}
    private int scanTasks(Instant now,int budget){try{return tasks.scan(now,"",budget);}catch(Exception e){log.warn("Task scheduling deferred",e);return budget;}}
    private int scanWorkflows(Instant now,int budget){try{return workflows.scan(now,"",budget);}catch(Exception e){log.warn("Workflow scheduling deferred",e);return budget;}}
    @PreDestroy public void close(){timer.shutdownNow();}
}
