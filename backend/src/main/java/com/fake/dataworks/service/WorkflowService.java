package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.StudioRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** Single-host orchestration. Only immutable, fully prepared plans enter the scheduler. */
@Service
public class WorkflowService {
    public record NodeSnapshot(String graphNodeId,StudioObject object) {}
    public record Bundle(int schemaVersion,StudioObject workflow,List<NodeSnapshot> nodes,List<Map<String,Object>> datasourceBindings) {}
    private record Plan(Bundle bundle,Map<String,MysqlExecutionProvider.PreparedQuery> queries,Map<String,InventoryExecutionService.PreparedStage> stages,Map<String,SyncExecutionService.Prepared> syncs) {}
    private static final Set<String> PENDING=Set.of("WAITING","QUEUED","RUNNING","RECOVERING");
    private static final org.slf4j.Logger log=org.slf4j.LoggerFactory.getLogger(WorkflowService.class);
    private final StudioRepository repo;
    private final ObjectService objects;
    private final DatasourceService sources;
    private final GraphValidator graphs;
    private final MysqlExecutionProvider mysql;
    private final InventoryExecutionService inventory;
    private final TaskService tasks;
    private final SyncExecutionService sync;
    private final JsonCodec json;
    private final TransactionTemplate transactions;
    private final int maxNodes,maxActive;
    private final Map<String,Context> active=new ConcurrentHashMap<>();
    private final ScheduledExecutorService coordinator=Executors.newSingleThreadScheduledExecutor(r->new Thread(r,"workflow-coordinator"));
    private boolean closing;
    private static final class Context {
        final Map<String,Object> parent;
        final Plan plan;
        final Map<String,String> children=new LinkedHashMap<>();
        final Map<String,List<String>> upstream=new LinkedHashMap<>();
        InventoryExecutionService.Batch batch;
        Context(Map<String,Object> parent,Plan plan) {this.parent=parent;this.plan=plan;}
    }
    public WorkflowService(StudioRepository repo,ObjectService objects,DatasourceService sources,GraphValidator graphs,
            MysqlExecutionProvider mysql,InventoryExecutionService inventory,TaskService tasks,JsonCodec json,TransactionTemplate transactionTemplate,
            @Value("${studio.workflow.max-nodes:100}") int maxNodes,@Value("${studio.workflow.max-active:8}") int maxActive,SyncExecutionService sync) {this.sync=sync;
        this.repo=repo;this.objects=objects;this.sources=sources;this.graphs=graphs;this.mysql=mysql;this.inventory=inventory;this.tasks=tasks;this.json=json;
        this.maxNodes=maxNodes;this.maxActive=maxActive;
        transactions=new TransactionTemplate(Objects.requireNonNull(transactionTemplate.getTransactionManager()));
        transactions.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
    }
    @PostConstruct public void initialize() {coordinator.scheduleWithFixedDelay(this::tick,200,200,TimeUnit.MILLISECONDS);}
    public static boolean isWorkflow(StudioObject object) {return "WORKFLOW".equals(RunService.config(object).get("provider"));}
    private void version(Object expected,int actual,String name) {
        if(!(expected instanceof Number n)||n.doubleValue()!=actual) throw StudioException.conflict("VERSION_CONFLICT","「"+name+"」版本已变化，请保存并核对后重试");
    }
    @SuppressWarnings("unchecked")
    private Map<String,Object> graph(StudioObject workflow) {return (Map<String,Object>)workflow.config().get("graph");}
    @SuppressWarnings("unchecked")
    private List<Map<String,Object>> graphItems(StudioObject workflow,String key) {return (List<Map<String,Object>>)graph(workflow).get(key);}
    private void validateWorkflow(StudioObject workflow) {
        if(!"WORKFLOW".equals(workflow.kind())||!isWorkflow(workflow)) throw StudioException.bad("WORKFLOW_REQUIRED","请选择真实工作流执行方式");
        graphs.validate(workflow.config());
        if(graph(workflow)==null||graphItems(workflow,"nodes").isEmpty()) throw StudioException.bad("EMPTY_WORKFLOW","工作流至少需要一个已绑定的 MySQL、Doris 或离线同步节点");
        if(graphItems(workflow,"nodes").size()>maxNodes) throw StudioException.bad("WORKFLOW_TOO_LARGE","工作流最多包含 "+maxNodes+" 个节点");
        for(var node:graphItems(workflow,"nodes")) if(Objects.toString(node.get("id"),"").length()>64) throw StudioException.bad("INVALID_GRAPH","图节点 ID 不能超过 64 个字符");
    }
    private Plan developmentPlan(String id,Integer expected,Map<String,Object> expectedNodes) {
        StudioObject initial=objects.active(id);repo.lockWorkspace(initial.workspaceId());
        StudioObject workflow=objects.active(id);version(expected,workflow.version(),workflow.name());validateWorkflow(workflow);
        List<NodeSnapshot> nodes=new ArrayList<>();Map<String,StudioObject> unique=new HashMap<>();
        for(var node:graphItems(workflow,"nodes")) {
            String objectId=Objects.toString(node.get("objectId"),"");
            if(objectId.isBlank()) throw StudioException.bad("UNBOUND_WORKFLOW_NODE","「"+node.get("label")+"」尚未绑定开发节点，请从已有节点导入");
            var object=unique.computeIfAbsent(objectId,objects::active);
            if(!workflow.workspaceId().equals(object.workspaceId())) throw StudioException.bad("WORKSPACE_MISMATCH","工作流不能引用其他工作空间的节点");
            version(expectedNodes.get(objectId),object.version(),object.name());
            nodes.add(new NodeSnapshot(node.get("id").toString(),object));
        }
        return prepare(new Bundle(1,workflow,nodes,List.of()),false);
    }
    private Plan prepare(Bundle bundle,boolean released) {
        validateWorkflow(bundle.workflow());
        Map<String,DatasourceService.ConnectionSpec> connections=new LinkedHashMap<>();
        Map<String,MysqlExecutionProvider.PreparedQuery> queries=new LinkedHashMap<>();
        Map<String,InventoryExecutionService.PreparedStage> stages=new LinkedHashMap<>();
        Map<String,SyncExecutionService.Prepared> syncs=new LinkedHashMap<>();
        for(NodeSnapshot node:bundle.nodes()) {
            ScheduleParameters.attach(new LinkedHashMap<>(),node.object(),Map.of(),ScheduleParameters.definitions(bundle.workflow()));
            if(SyncExecutionService.isSync(node.object())) {
                var p=sync.prepare(node.object(),ScheduleParameters.definitions(bundle.workflow()));if(released)TaskService.verifyBindings(bundle.datasourceBindings(),p.bindings());syncs.put(node.graphNodeId(),p);connections.put(p.source().id(),p.source());connections.put(p.target().id(),p.target());continue;
            }
            String sourceId=Objects.toString(RunService.config(node.object()).get("dataSourceId"),"");
            var source=connections.computeIfAbsent(sourceId,id->sources.forWorkspace(id,bundle.workflow().workspaceId()));
            if(released) {
                var binding=bundle.datasourceBindings().stream().filter(b->sourceId.equals(b.get("id"))).findFirst()
                    .orElseThrow(()->StudioException.bad("INVALID_RELEASE","发布包缺少数据源绑定"));
                var current=source.publicView();
                for(String key:List.of("workspaceId","type","host","port","database","username","options")) {
                    Object before=binding.get(key),now=current.get(key);
                    boolean same=before instanceof Number a&&now instanceof Number b?a.doubleValue()==b.doubleValue():Objects.equals(before,now);
                    if(!same) throw StudioException.conflict("DATASOURCE_BINDING_CHANGED","发布时的数据源目标已变化，请恢复原连接或发布新版本");
                }
            }
            if(InventoryExecutionService.materializes(node.object()))stages.put(node.graphNodeId(),inventory.prepare(node.object(),source));
            else queries.put(node.graphNodeId(),mysql.prepare(node.object(),source));
        }
        boolean independent=!syncs.isEmpty()||(released?bundle.schemaVersion()>=3:bundle.nodes().stream().anyMatch(n->RunService.config(n.object()).containsKey("taskExecutionVersion")));
        if(!stages.isEmpty()&&!independent) {
            if(!queries.isEmpty()||connections.size()!=1||stages.size()!=3||!new HashSet<>(stages.values().stream().map(InventoryExecutionService.PreparedStage::target).toList()).equals(new HashSet<>(InventorySql.TARGETS)))
                throw StudioException.bad("INVALID_INVENTORY_WORKFLOW","库存工作流须包含同一数据源的 DWD、DWS、ADS 三个落表节点");
            var ids=new HashMap<String,String>();stages.forEach((id,stage)->ids.put(stage.target(),id));
            for(int i=0;i<2;i++){String from=ids.get(InventorySql.TARGETS.get(i)),to=ids.get(InventorySql.TARGETS.get(i+1));if(graphItems(bundle.workflow(),"edges").stream().noneMatch(e->from.equals(e.get("source"))&&to.equals(e.get("target"))))throw StudioException.bad("INVALID_INVENTORY_DEPENDENCY","请按 DWD → DWS → ADS 连接库存节点");}
        }
        return new Plan(new Bundle(!syncs.isEmpty()?4:independent?3:stages.isEmpty()?1:2,bundle.workflow(),bundle.nodes(),connections.values().stream().map(DatasourceService.ConnectionSpec::publicView).toList()),queries,stages,syncs);
    }
    public Map<String,Object> publish(String id,Integer expected,Map<String,Object> expectedNodes,String note) {
        if(note.length()>4000) throw StudioException.bad("INVALID_RELEASE_NOTE","发布说明最多 4000 个字符");
        return transactions.execute(tx->{
            var plan=developmentPlan(id,expected,expectedNodes);var workflow=plan.bundle().workflow();
            Map<String,Object> release=new LinkedHashMap<>();release.put("id",UUID.randomUUID().toString());release.put("workspaceId",workflow.workspaceId());release.put("workflowId",id);
            release.put("containsWrites",(!plan.syncs().isEmpty()||plan.queries().values().stream().anyMatch(q->q.script().writes())));
            release.put("releaseNo",repo.nextReleaseNo(id));release.put("workflowVersion",workflow.version());release.put("name",workflow.name());release.put("note",note);release.put("createdAt",ObjectService.now());
            var durableBundle=json.map(json.write(plan.bundle()));
            durableBundle.put("containsWrites",release.get("containsWrites"));
            repo.insertRelease(release,durableBundle);return release;
        });
    }
    public List<Map<String,Object>> releases(String workspace,String workflow) {objects.workspace(workspace);return repo.releases(workspace,workflow);}
    public Map<String,Object> release(String id) {return repo.release(id).orElseThrow(()->StudioException.missing("发布版本不存在"));}
    private void admission() {
        if(closing) throw new StudioException("SERVICE_STOPPING","服务正在停止",503);
        if(active.size()>=maxActive) throw new StudioException("WORKFLOW_LIMIT","活动工作流已达上限，请稍后重试",429);
    }
    public synchronized Map<String,Object> startDevelopment(String id,Integer expected,Map<String,Object> expectedNodes) {
        return startDevelopment(id,expected,expectedNodes,Map.of());
    }
    public synchronized Map<String,Object> startDevelopment(String id,Integer expected,Map<String,Object> expectedNodes,Map<String,Object> options) {
        admission();
        var context=transactions.execute(tx->persist(developmentPlan(id,expected,expectedNodes),null,options));
        return context.parent;
    }
    public synchronized Map<String,Object> startRelease(String id) {
        return startRelease(id,Map.of());
    }
    public synchronized Map<String,Object> startRelease(String id,Map<String,Object> options) {
        admission();
        var release=release(id);var bundle=json.read(json.write(release.get("bundle")),Bundle.class);
        // Deliberately do not resolve development objects here: the release is authoritative.
        var context=transactions.execute(tx->persist(prepare(bundle,true),release,options));
        return context.parent;
    }
    private Context persist(Plan plan,Map<String,Object> release,Map<String,Object> options) {
        var workflow=plan.bundle().workflow();
        if(!plan.syncs().isEmpty()&&repo.topRuns(workflow.workspaceId()).stream().anyMatch(r->workflow.id().equals(r.get("objectId"))&&PENDING.contains(r.get("status"))))throw StudioException.conflict("TASK_OVERLAP","该工作流仍在执行或核实中");
        options=new LinkedHashMap<>(options);options.putAll(ScheduleParameters.context(ScheduleParameters.schedule(workflow),options).toMap());
        Map<String,Object> parent=new LinkedHashMap<>();
        parent.put("id",UUID.randomUUID().toString());parent.put("workspaceId",workflow.workspaceId());parent.put("objectId",workflow.id());parent.put("objectName",workflow.name());parent.put("objectVersion",workflow.version());
        parent.put("containsWrites",(!plan.syncs().isEmpty()||plan.queries().values().stream().anyMatch(q->q.script().writes())));
        parent.put("containsSync",!plan.syncs().isEmpty());
        parent.put("provider","WORKFLOW");parent.put("simulation",false);parent.put("status","QUEUED");parent.put("mode","MANUAL");parent.put("createdAt",ObjectService.now());
        parent.put("executionSource",release==null?"DEVELOPMENT":"RELEASE");parent.put("nodeCount",plan.bundle().nodes().size());
        parent.put("logs",List.of("[工作流] 完整图及所有节点快照已保存。失败下游跳过，独立分支继续。"));parent.put("columns",List.of());parent.put("rows",List.of());
        if(release!=null) {parent.put("releaseId",release.get("id"));parent.put("releaseNo",release.get("releaseNo"));}
        for(String key:List.of("triggerType","scheduleId","triggerId","scheduledAt","attempt","retryOfRunId"))if(options.containsKey(key))parent.put(key,options.get(key));
        ScheduleParameters.attach(parent,workflow,options,List.of());
        parent.putIfAbsent("triggerType","MANUAL");parent.putIfAbsent("attempt",1);
        if("SCHEDULED".equals(parent.get("triggerType")))parent.put("mode","SCHEDULED");
        Context context=new Context(parent,plan);
        if(!plan.stages().isEmpty()&&plan.bundle().schemaVersion()<3){
            var stage=plan.stages().values().iterator().next();var parameters=inventory.parameters(options,parent.get("id").toString());
            context.batch=inventory.begin(parent.get("id").toString(),stage.source(),parameters);
            parent.put("materialization",true);parent.put("parameters",parameters);parent.put("businessDate",parameters.get("bizdate"));parent.put("sourceCutoffAt",parameters.get("source_cutoff"));parent.put("buildId",parent.get("id"));parent.put("dataSource",stage.source().publicView());parent.put("publicationStatus","STAGING");
        }else {if(options.containsKey("businessDate"))parent.put("businessDate",options.get("businessDate"));if(options.containsKey("sourceCutoffAt"))parent.put("sourceCutoffAt",options.get("sourceCutoffAt"));}
        if(plan.bundle().schemaVersion()>=3){var params=inventory.parameters(options,parent.get("id").toString());parent.put("businessDate",params.get("bizdate"));parent.put("sourceCutoffAt",params.get("source_cutoff"));}
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization(){
            @Override public void afterCommit(){active.put(parent.get("id").toString(),context);}
            @Override public void afterCompletion(int status){if(status!=STATUS_COMMITTED)inventory.release(context.batch);}
        });
        repo.insertRun(parent,workflow);
        for(var node:plan.bundle().nodes()) {
            var child=plan.syncs().containsKey(node.graphNodeId())?sync.newRun(plan.syncs().get(node.graphNodeId()),"MANUAL"):plan.stages().containsKey(node.graphNodeId())?inventory.newRun(plan.stages().get(node.graphNodeId())):mysql.newRun(plan.queries().get(node.graphNodeId()),"MANUAL");child.put("parentRunId",parent.get("id"));child.put("graphNodeId",node.graphNodeId());child.put("status","WAITING");child.put("executionSource",parent.get("executionSource"));child.put("mode",parent.get("mode"));
            for(String key:List.of("parameters","businessDate","sourceCutoffAt","buildId","triggerType","scheduleId","triggerId","scheduledAt","attempt"))if(parent.containsKey(key))child.put(key,parent.get(key));
            var timeContext=new LinkedHashMap<String,Object>();for(String key:List.of("businessDate","scheduledAt","timezone"))timeContext.put(key,parent.get(key));
            ScheduleParameters.attach(child,node.object(),timeContext,ScheduleParameters.definitions(workflow));
            var internal=new LinkedHashMap<String,Object>((Map<String,Object>)child.getOrDefault("parameters",Map.of()));internal.put("bizdate",parent.get("businessDate"));child.put("parameters",internal);
            if(release!=null) {child.put("releaseId",release.get("id"));child.put("releaseNo",release.get("releaseNo"));}
            child.put("logs",List.of("[工作流] 执行快照已固定，等待依赖。"));repo.insertRun(child,node.object());
            context.children.put(node.graphNodeId(),child.get("id").toString());context.upstream.put(node.graphNodeId(),new ArrayList<>());
        }
        for(var edge:graphItems(workflow,"edges")) context.upstream.get(edge.get("target").toString()).add(edge.get("source").toString());
        return context;
    }
    public List<Map<String,Object>> nodes(String id) {
        var parent=repo.run(id).orElseThrow(()->StudioException.missing("运行不存在"));
        if(!"WORKFLOW".equals(parent.get("provider"))) throw StudioException.bad("WORKFLOW_RUN_REQUIRED","请选择工作流运行");
        return repo.childRuns(id).stream().map(child->{var summary=new LinkedHashMap<>(child);summary.remove("logs");summary.remove("rows");summary.remove("columns");return (Map<String,Object>)summary;}).toList();
    }
    private void tick() {
        for(var context:active.values()) {
            try {synchronized(context) {advance(context);}}
            catch(Exception e) {log.warn("Workflow coordinator will retry: run={}, type={}",context.parent.get("id"),e.getClass().getSimpleName());}
        }
    }
    private void advance(Context context) {
        String id=context.parent.get("id").toString();var parent=repo.run(id).orElseThrow();
        if(!PENDING.contains(parent.get("status"))) {if(context.batch==null||!inventory.busy(context.batch)){inventory.release(context.batch);active.remove(id,context);}return;}
        if(Boolean.TRUE.equals(parent.get("cancelRequested"))){
            if(repo.childRuns(id).stream().noneMatch(r->PENDING.contains(r.get("status")))){finish(parent,"CANCELLED","WORKFLOW_CANCELLED","工作流已停止；已提交数据保留。");active.remove(id,context);}return;
        }
        if("QUEUED".equals(parent.get("status"))) {parent.put("status","RUNNING");parent.put("startedAt",ObjectService.now());if(!repo.transitionRun(parent,"QUEUED"))return;}
        Map<String,Map<String,Object>> children=new LinkedHashMap<>();repo.childRuns(id).forEach(r->children.put(r.get("graphNodeId").toString(),r));
        boolean changed;
        do {
            changed=false;
            for(String node:context.children.keySet()) {
                var child=children.get(node);
                if("WAITING".equals(child.get("status"))&&context.upstream.get(node).stream().anyMatch(u->Set.of("FAILED","SKIPPED","CANCELLED").contains(children.get(u).get("status")))) {
                    finish(child,"SKIPPED","UPSTREAM_FAILED","上游未成功，本节点已跳过。");changed=true;
                }
            }
        } while(changed);
        if(children.values().stream().anyMatch(r->Set.of("QUEUED","RUNNING","RECOVERING").contains(r.get("status"))))return;
        for(String node:context.children.keySet()) {
            var child=children.get(node);
            if("WAITING".equals(child.get("status"))&&context.upstream.get(node).stream().allMatch(u->"SUCCESS".equals(children.get(u).get("status")))) {
                child.put("status","QUEUED");if(!repo.transitionRun(child,"WAITING"))return;
                try {if(context.plan.bundle().schemaVersion()>=3){var snapshot=context.plan.bundle().nodes().stream().filter(n->n.graphNodeId().equals(node)).findFirst().orElseThrow().object();tasks.startWorkflowNode(snapshot,child,context.upstream.get(node).stream().map(children::get).toList(),context.plan.syncs().get(node));}else if(context.batch==null)mysql.enqueueExisting(child.get("id").toString(),context.plan.queries().get(node));else inventory.enqueue(child.get("id").toString(),context.plan.stages().get(node),context.batch);}
                catch(RuntimeException e) {var latest=repo.run(child.get("id").toString()).orElseThrow();finish(latest,"FAILED",e instanceof StudioException se?se.code():"SUBMISSION_FAILED","节点提交失败。");}
                return;
            }
        }
        if(children.values().stream().noneMatch(r->PENDING.contains(r.get("status")))) {
            boolean success=children.values().stream().allMatch(r->"SUCCESS".equals(r.get("status")));
            if(context.batch!=null){
                if(inventory.busy(context.batch))return;
                parent.put("writtenRows",children.values().stream().mapToLong(r->((Number)r.getOrDefault("writtenRows",0)).longValue()).sum());
                if(success){try{inventory.publish(context.batch);parent.put("publicationStatus","PUBLISHED");}catch(java.sql.SQLException e){parent.put("publicationStatus","RECOVERING");parent.put("logs",List.of("[库存] 正在核实最终事务提交结果，暂不允许冲突写入。"));repo.transitionRun(parent,"RUNNING");return;}catch(RuntimeException e){success=false;parent.put("errorCode",e instanceof StudioException se?se.code():"PUBLISH_FAILED");}}
                if(!success)parent.put("publicationStatus","NOT_PUBLISHED");
            }
            String error=children.values().stream().filter(r->"FAILED".equals(r.get("status"))).map(r->Objects.toString(r.get("errorCode"),"WORKFLOW_FAILED")).findFirst().orElse(Objects.toString(parent.get("errorCode"),"WORKFLOW_FAILED"));
            finish(parent,success?"SUCCESS":"FAILED",success?null:error,success?(context.batch==null?"全部节点执行成功。":"库存三层数据已在一个事务内发布。"):"工作流结束，部分节点失败或跳过。");inventory.release(context.batch);active.remove(id,context);
        }
    }
    private void finish(Map<String,Object> run,String status,String code,String message) {
        String expected=run.get("status").toString();if(!PENDING.contains(expected))return;
        run.put("status",status);run.put("finishedAt",ObjectService.now());
        run.put("elapsedMs",Duration.between(Instant.parse(run.getOrDefault("startedAt",run.get("createdAt")).toString()),Instant.now()).toMillis());
        if(code!=null)run.put("errorCode",code);List<Object> logs=new ArrayList<>((List<?>)run.getOrDefault("logs",List.of()));logs.add(message);run.put("logs",logs);repo.transitionRun(run,expected);
    }
    public synchronized Map<String,Object> stop(String id) {
        var context=active.get(id);
        if(context!=null) {synchronized(context) {if(context.batch!=null&&context.batch.committed){advance(context);}else{if(context.batch!=null&&context.batch.recovering)throw StudioException.conflict("RECOVERY_REQUIRED","正在核实提交状态，请等待结果");cancel(id);if(context.batch==null&&!Boolean.TRUE.equals(context.parent.get("containsSync")))active.remove(id,context);}}}
        else cancel(id);
        return repo.run(id).orElseThrow();
    }
    private void cancel(String id) {
        var parent=repo.run(id).orElseThrow(()->StudioException.missing("运行不存在"));if(!PENDING.contains(parent.get("status")))return;
        var context=active.get(id);if(context!=null&&context.batch!=null){inventory.cancel(context.batch);parent.put("publicationStatus","NOT_PUBLISHED");}
        for(var child:repo.childRuns(id)) {
            if("WAITING".equals(child.get("status")))finish(child,"CANCELLED","WORKFLOW_CANCELLED","用户已停止整个工作流。");
            else if(PENDING.contains(child.get("status"))){if(context!=null&&context.batch!=null)inventory.stop(child.get("id").toString());else if(Boolean.TRUE.equals(child.get("taskExecution"))&&Boolean.TRUE.equals(child.get("materialization")))tasks.stop(child.get("id").toString());else if("SYNC".equals(child.get("provider")))sync.stop(child.get("id").toString());else mysql.stop(child.get("id").toString());}
        }
        if(Boolean.TRUE.equals(parent.get("containsSync"))&&repo.childRuns(id).stream().anyMatch(r->PENDING.contains(r.get("status")))){parent.put("cancelRequested",true);repo.updateRun(parent);return;}
        finish(parent,"CANCELLED","WORKFLOW_CANCELLED","用户已停止整个工作流。");
    }
    @PreDestroy public synchronized void shutdown() {closing=true;coordinator.shutdownNow();active.clear();}
}
