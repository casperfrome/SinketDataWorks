package com.fake.dataworks.service;

import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.StudioRepository;
import jakarta.annotation.PostConstruct;
import java.util.*;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

@Service
@Primary
public class RunService implements ExecutionProvider {
    private final StudioRepository repo;
    private final ObjectService objects;
    private final LocalSimulationProvider simulation;
    private final MysqlExecutionProvider mysql;
    private final WorkflowService workflows;
    private final TaskService tasks;
    private final SyncExecutionService sync;
    private final DebugParameterService debugParameters;
    private final TransactionTemplate transactions;
    public RunService(StudioRepository repo,ObjectService objects,LocalSimulationProvider simulation,MysqlExecutionProvider mysql,WorkflowService workflows,TaskService tasks,SyncExecutionService sync,DebugParameterService debugParameters,TransactionTemplate transactions) {this.sync=sync;this.repo=repo;this.objects=objects;this.simulation=simulation;this.mysql=mysql;this.workflows=workflows;this.tasks=tasks;this.debugParameters=debugParameters;this.transactions=transactions;}
    @PostConstruct public void recover() {sync.recover();simulation.recover();}
    public static Map<?,?> config(StudioObject object) {return object.config().get("run") instanceof Map<?,?> map?map:Map.of();}
    public static boolean isMysql(StudioObject object) {return "MYSQL".equals(config(object).get("provider"));}
    public static boolean isDoris(StudioObject object) {return "DORIS".equals(config(object).get("provider"));}
    public static boolean isSql(StudioObject object) {return isMysql(object)||isDoris(object);}
    public Map<String,Object> submit(String objectId,String mode,boolean fail,Integer expectedVersion) {
        return submit(objectId,mode,fail,expectedVersion,Map.of());
    }
    public Map<String,Object> submit(String objectId,String mode,boolean fail,Integer expectedVersion,Map<String,Object> expectedNodeVersions) {
        return submit(objectId,mode,fail,expectedVersion,expectedNodeVersions,Map.of());
    }
    public Map<String,Object> submit(String objectId,String mode,boolean fail,Integer expectedVersion,Map<String,Object> expectedNodeVersions,Map<String,Object> options) {
        StudioObject snapshot=objects.active(objectId);
        if(WorkflowService.isWorkflow(snapshot)) {
            if(options.containsKey("debugParameters"))throw StudioException.bad("INVALID_DEBUG_PARAMETERS","工作流不支持单节点调试参数");
            if(fail) throw StudioException.bad("SIMULATION_ONLY","真实工作流不支持模拟失败");
            return workflows.startDevelopment(objectId,expectedVersion,expectedNodeVersions,options);
        }
        if((isSql(snapshot)||SyncExecutionService.isSync(snapshot))&&(expectedVersion==null||expectedVersion!=snapshot.version())) throw StudioException.conflict("VERSION_CONFLICT","执行需要最新保存版本，请保存并重新运行");
        if((isSql(snapshot)||SyncExecutionService.isSync(snapshot))&&fail)throw StudioException.bad("SIMULATION_ONLY","真实任务不支持模拟失败");
        if(!"NODE".equals(snapshot.kind())||!"MANUAL".equals(mode)) {
            if(options.containsKey("debugParameters"))throw StudioException.bad("INVALID_DEBUG_PARAMETERS","调试参数只适用于单节点手动开发运行");
            return (isSql(snapshot)||SyncExecutionService.isSync(snapshot))?tasks.startDevelopment(snapshot,options):start(snapshot,mode,fail);
        }
        return transactions.execute(t->{
            repo.lockWorkspace(snapshot.workspaceId());var current=objects.active(objectId);
            int version=expectedVersion==null?snapshot.version():expectedVersion;
            if(current.version()!=version)throw StudioException.conflict("VERSION_CONFLICT","执行需要最新保存版本，请保存并重新运行");
            var frozen=debugParameters.freeze(current,options);
            var run=(isSql(current)||SyncExecutionService.isSync(current))?tasks.startDevelopment(current,frozen.options()):simulation.start(current,mode,fail,frozen.options());
            debugParameters.remember(current.id(),frozen);return run;
        });
    }
    @Override public Map<String,Object> start(StudioObject snapshot,String mode,boolean fail) {
        if(WorkflowService.isWorkflow(snapshot)) throw StudioException.bad("WORKFLOW_VERSION_REQUIRED","工作流执行需要父子节点预期版本");
        if(SyncExecutionService.isSync(snapshot))return tasks.startDevelopment(snapshot,Map.of());
        if(isSql(snapshot)) {
            if(InventoryExecutionService.materializes(snapshot))throw StudioException.bad("RUN_INVENTORY_WORKFLOW","请运行所属库存工作流；三层结果将一起发布");
            if(fail) throw StudioException.bad("SIMULATION_ONLY","真实查询不支持模拟失败选项");
            return mysql.start(snapshot,mode,false);
        }
        return simulation.start(snapshot,mode,fail);
    }
    @Override public Map<String,Object> stop(String id) {
        var run=required(id);
        if(run.get("parentRunId")!=null) throw StudioException.bad("STOP_PARENT_WORKFLOW","请停止所属工作流");
        if("WORKFLOW".equals(run.get("provider")))return workflows.stop(id);
        if(Boolean.TRUE.equals(run.get("taskExecution"))&&Boolean.TRUE.equals(run.get("materialization")))return tasks.stop(id);
        if("SYNC".equals(run.get("provider")))return sync.stop(id);
        return ("MYSQL".equals(run.get("provider"))||"DORIS".equals(run.get("provider")))?mysql.stop(id):simulation.stop(id);
    }
    public Map<String,Object> required(String id) {return repo.run(id).orElseThrow(()->StudioException.missing("运行记录不存在"));}
    public Map<String,Object> detail(String id) {var run=required(id);run.put("snapshot",repo.runSnapshot(id));return run;}
    public Map<String,Object> results(String id,int page,int pageSize) {return results(id,page,pageSize,null);}
    public Map<String,Object> results(String id,int page,int pageSize,Integer statementIndex) {
        page(page,pageSize);var run=required(id);
        return SqlResults.page(repo.result(id).orElse(run),page,pageSize,statementIndex);
    }
    public Object list(String workspace,boolean summary,int page,int pageSize,String status,String search) {objects.workspace(workspace);page(page,pageSize);return summary?repo.runSummaries(workspace,page,pageSize,status,search):repo.topRuns(workspace);}
    private void page(int page,int size) {if(page<1||page>100000||size<1||size>100) throw StudioException.bad("INVALID_PAGE","页码须大于 0，每页 1–100 行");}
}
