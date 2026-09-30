package com.fake.dataworks.service;

import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.StudioRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Service
public class LocalSimulationProvider implements ExecutionProvider {
    private final StudioRepository repo;
    private final long queueMs,runMs;
    private final boolean recoverOnStart;
    private final ScheduledExecutorService worker=Executors.newSingleThreadScheduledExecutor(r->{ Thread t=new Thread(r,"local-simulation"); t.setDaemon(true); return t; });
    public LocalSimulationProvider(StudioRepository repo,@Value("${studio.simulation.queue-ms:1200}") long queueMs,@Value("${studio.simulation.run-ms:2500}") long runMs,@Value("${studio.simulation.recover-on-start:true}") boolean recoverOnStart) { this.repo=repo;this.queueMs=queueMs;this.runMs=runMs;this.recoverOnStart=recoverOnStart; }
    // Invoked by the coordinator once for all providers. Kept public for the recovery unit test.
    public synchronized void recover() {
        if(!recoverOnStart)return;
        for(Map<String,Object> run:repo.unfinishedRuns()) {
            if("SYNC".equals(run.get("provider"))||Boolean.TRUE.equals(run.get("containsSync")))continue;
            if(Boolean.TRUE.equals(run.get("materialization"))||"MATERIALIZE".equals(run.get("executionMode")))continue;
            String expected=run.get("status").toString();
            var result=repo.result(run.get("id").toString()).orElse(Map.of());boolean unknown=SqlResults.recover(result);
            if(!SqlResults.items(result).isEmpty()) {repo.saveResult(run.get("id").toString(),result);run.put("statements",SqlResults.summaries(SqlResults.items(result)));}
            run.put("status","FAILED");run.put("errorCode",unknown?"COMMIT_UNKNOWN":"SERVICE_RESTARTED");run.put("finishedAt",ObjectService.now());
            log(run,unknown?"服务中断，提交结果未知，请核实业务数据；不会自动重放写入。":Boolean.TRUE.equals(run.get("containsWrites"))?"服务重启，执行已中断；已提交操作保留，请查看逐语句结果。":"服务已重新启动，上次运行已中断，请重新运行。");
            repo.transitionRun(run,expected);
        }
    }
    @PreDestroy
    void shutdown() { worker.shutdownNow(); }
    @Override
    public synchronized Map<String,Object> start(StudioObject snapshot,String mode,boolean fail) {
        return start(snapshot,mode,fail,Map.of());
    }
    public synchronized Map<String,Object> start(StudioObject snapshot,String mode,boolean fail,Map<String,Object> options) {
        Map<String,Object> r=new LinkedHashMap<>(); String id=UUID.randomUUID().toString();
        r.put("id",id);r.put("workspaceId",snapshot.workspaceId());r.put("objectId",snapshot.id());r.put("objectName",snapshot.name());r.put("objectVersion",snapshot.version());r.put("provider","SIMULATION");r.put("status","QUEUED");r.put("mode",mode==null?"MANUAL":mode);r.put("simulation",true);r.put("logs",new ArrayList<>(List.of("[本地模拟] 已创建运行，代码和配置快照已保存。","[本地模拟] 等待本地演示资源。")));r.put("columns",List.of());r.put("rows",List.of());r.put("createdAt",ObjectService.now());
        if(options.containsKey("scheduleParameters")) {
            ScheduleParameters.attach(r,snapshot,options,List.of());
            r.put("sourceCutoffAt",options.get("sourceCutoffAt"));r.put("parameters",Map.of("bizdate",r.get("businessDate"),"source_cutoff",options.get("sourceCutoffAt"),"build_id",id));
        }
        repo.insertRun(r,snapshot);
        if(TransactionSynchronizationManager.isSynchronizationActive())TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization(){@Override public void afterCommit(){worker.schedule(()->begin(id,fail),queueMs,TimeUnit.MILLISECONDS);}});
        else worker.schedule(()->begin(id,fail),queueMs,TimeUnit.MILLISECONDS);
        return r;
    }
    private synchronized void begin(String id,boolean fail) {
        Map<String,Object> r=repo.run(id).orElse(null); if(r==null||!r.get("status").equals("QUEUED")) return;
        r.put("status","RUNNING");r.put("startedAt",ObjectService.now());log(r,"[本地模拟] 正在校验开发对象，未连接计算引擎，未执行用户代码。");repo.updateRun(r);worker.schedule(()->finish(id,fail),runMs,TimeUnit.MILLISECONDS);
    }
    private synchronized void finish(String id,boolean fail) {
        Map<String,Object> r=repo.run(id).orElse(null); if(r==null||!r.get("status").equals("RUNNING")) return;
        r.put("status",fail?"FAILED":"SUCCESS");r.put("finishedAt",ObjectService.now());
        log(r,fail?"[本地模拟] 演示失败：模拟资源校验未通过，可修正配置后重试。":"[本地模拟] 运行成功，返回固定演示数据（非真实查询结果）。");
        if(!fail) { r.put("columns",List.of("order_id","user_id","amount","ds"));r.put("rows",List.of(List.of("O202609270001","U10001",128.50,"20260927"),List.of("O202609270002","U10002",269.00,"20260927"),List.of("O202609270003","U10003",59.90,"20260927"),List.of("O202609270004","U10001",399.00,"20260927"))); }
        repo.updateRun(r);
    }
    @Override
    public synchronized Map<String,Object> stop(String id) { Map<String,Object> r=repo.run(id).orElseThrow(()->StudioException.missing("运行记录不存在")); if(Set.of("QUEUED","RUNNING").contains(r.get("status"))) {r.put("status","CANCELLED");r.put("finishedAt",ObjectService.now());log(r,"[本地模拟] 用户已停止运行。");repo.updateRun(r);}return r; }
    @SuppressWarnings("unchecked")
    private void log(Map<String,Object> run,String line) { var lines=new ArrayList<Object>((List<?>)run.getOrDefault("logs",List.of()));lines.add(line);run.put("logs",lines); }
}
