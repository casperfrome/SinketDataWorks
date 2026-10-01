package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.repository.StudioRepository;
import jakarta.annotation.PreDestroy;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Service
public class MysqlExecutionProvider implements ExecutionProvider {
    private static final org.slf4j.Logger log=org.slf4j.LoggerFactory.getLogger(MysqlExecutionProvider.class);
    private final StudioRepository repo;private final DatasourceService sources;private final SqlGuard guard;private final JsonCodec json;private final TransactionTemplate transactions;
    private final ThreadPoolExecutor workers;
    private final Semaphore capacity;
    private final Set<String> reservations=ConcurrentHashMap.newKeySet();
    private final Map<String,PreparedQuery> deferred=new ConcurrentHashMap<>();
    private final ScheduledExecutorService deadlines=Executors.newScheduledThreadPool(2,r->new Thread(r,"sql-deadline"));
    private volatile boolean shuttingDown;
    private final ConcurrentMap<String,Job> jobs=new ConcurrentHashMap<>();
    private static final class Job {
        volatile FutureTask<Void> future;volatile Connection connection;volatile Statement statement;
        volatile boolean cancelled;volatile boolean timedOut;volatile ScheduledFuture<?> deadline;
        List<Map<String,Object>> results;Map<String,Object> current;boolean dispatched;
    }
    public MysqlExecutionProvider(StudioRepository repo,DatasourceService sources,SqlGuard guard,JsonCodec json,TransactionTemplate transactions,
            @Value("${studio.mysql.workers:4}") int count,@Value("${studio.mysql.queue-size:32}") int queueSize) {
        this.repo=repo;this.sources=sources;this.guard=guard;this.json=json;this.transactions=transactions;
        workers=new ThreadPoolExecutor(count,count,0,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(queueSize),r->new Thread(r,"sql-query"),new ThreadPoolExecutor.AbortPolicy());
        capacity=new Semaphore(count+queueSize);
    }
    public record PreparedQuery(StudioObject snapshot,DatasourceService.ConnectionSpec source,int timeout,SqlScript script) {
        @Override public String toString() {return "PreparedQuery[objectId="+snapshot.id()+"]";}
    }
    public PreparedQuery prepare(StudioObject snapshot,DatasourceService.ConnectionSpec source) {
        boolean mysql=RunService.isMysql(snapshot)&&"MySQL".equals(snapshot.nodeType()),doris=RunService.isDoris(snapshot)&&"Doris".equals(snapshot.nodeType());
        if(!"NODE".equals(snapshot.kind())||!mysql&&!doris) throw StudioException.bad("SQL_NODE_REQUIRED","真实 SQL 执行需要绑定数据源的 MySQL 或 Doris 节点");
        if(!source.workspaceId().equals(snapshot.workspaceId())) throw StudioException.bad("WORKSPACE_MISMATCH","数据源不属于当前工作空间");
        String dialect=mysql?"MYSQL":"DORIS";
        if(!dialect.equals(source.type()))throw StudioException.bad("SQL_DATASOURCE_REQUIRED",(mysql?"MySQL":"Doris")+" 节点需要相同类型的数据源");
        if(doris&&"MATERIALIZE".equals(RunService.config(snapshot).get("executionMode")))throw StudioException.bad("MATERIALIZATION_NODE_REQUIRED","库存落表模式仅支持 MySQL 节点");
        var config=RunService.config(snapshot);
        Object seconds=config.containsKey("timeoutSeconds")?config.get("timeoutSeconds"):30;
        if(!(seconds instanceof Number number)||number.doubleValue()!=number.intValue()||number.intValue()<1||number.intValue()>300) throw StudioException.bad("INVALID_TIMEOUT","运行超时须为 1–300 秒");
        var script=SqlScript.prepare(snapshot.content(),source.database(),guard,dialect);
        return new PreparedQuery(snapshot,source,((Number)seconds).intValue(),script);
    }
    public Map<String,Object> newRun(PreparedQuery prepared,String mode) {
        var snapshot=prepared.snapshot();Map<String,Object> run=new LinkedHashMap<>();
        run.put("id",UUID.randomUUID().toString());run.put("workspaceId",snapshot.workspaceId());run.put("objectId",snapshot.id());run.put("objectName",snapshot.name());run.put("objectVersion",snapshot.version());
        run.put("provider",prepared.source().type());run.put("simulation",false);run.put("dataSource",prepared.source().publicView());run.put("status","QUEUED");run.put("mode",mode==null?"MANUAL":mode);run.put("timeoutSeconds",prepared.timeout());
        run.put("containsWrites",prepared.script().writes());run.put("statements",SqlResults.pending(prepared.script()));
        run.put("createdAt",ObjectService.now());run.put("parameters",Map.of("bizdate",java.time.LocalDate.now(java.time.ZoneId.of("Asia/Shanghai")).minusDays(1).toString(),"source_cutoff",java.time.Instant.now().toString(),"build_id",run.get("id")));run.put("logs",List.of(label(prepared)+" 已保存执行代码与版本快照，等待执行资源。"));run.put("columns",List.of());run.put("rows",List.of());
        return run;
    }
    @Override public Map<String,Object> start(StudioObject snapshot,String mode,boolean fail) {
        return transactions.execute(t->{repo.lockWorkspace(snapshot.workspaceId());if(repo.hasActiveTaskRun(snapshot.workspaceId(),snapshot.id(),""))throw StudioException.conflict("TASK_OVERLAP","当前任务仍在运行或核实提交");
            String sourceId=Objects.toString(RunService.config(snapshot).get("dataSourceId"),"");
            var prepared=prepare(snapshot,sources.forWorkspace(sourceId,snapshot.workspaceId()));
            var run=newRun(prepared,mode);ScheduleParameters.attach(run,snapshot,Map.of(),List.of());var internal=new LinkedHashMap<String,Object>((Map<String,Object>)run.get("parameters"));internal.put("bizdate",run.get("businessDate"));run.put("parameters",internal);String id=run.get("id").toString();if(!reserve(id))throw new StudioException("QUEUE_FULL","执行资源已满，等待空闲资源",429);
            boolean synchronizedTransaction=TransactionSynchronizationManager.isSynchronizationActive();if(synchronizedTransaction)TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization(){@Override public void afterCommit(){enqueueDeferred(id,prepared);}@Override public void afterCompletion(int status){if(status!=STATUS_COMMITTED)releaseReservation(id);}});
            repo.insertRun(run,snapshot);if(!synchronizedTransaction)enqueueDeferred(id,prepared);return run;
        });
    }
    public void enqueueExisting(String id,PreparedQuery prepared) {
        if(!tryEnqueueExisting(id,prepared)){finish(id,"FAILED","QUEUE_FULL","执行队列已满，请稍后重试",null,0);throw new StudioException("QUEUE_FULL","执行队列已满，请稍后重试",429);}
    }
    public boolean tryEnqueueExisting(String id,PreparedQuery prepared) {
        if(repo.run(id).filter(r->"QUEUED".equals(r.get("status"))).isEmpty()){releaseReservation(id);return true;}if(!reserve(id))return false;
        Job job=new Job();job.results=SqlResults.pending(prepared.script());job.future=new FutureTask<>(()->{execute(id,prepared,job);return null;});jobs.put(id,job);
        try {workers.execute(job.future);return true;} catch(RejectedExecutionException e) {jobs.remove(id,job);releaseReservation(id);return false;}
    }
    public boolean reserve(String id){if(shuttingDown)return false;if(reservations.contains(id))return true;if(!capacity.tryAcquire())return false;if(!reservations.add(id))capacity.release();return true;}
    public void releaseReservation(String id){if(reservations.remove(id))capacity.release();}
    public void enqueueDeferred(String id,PreparedQuery prepared){if(tryEnqueueExisting(id,prepared)){deferred.remove(id);return;}deferred.put(id,prepared);if(!shuttingDown)deadlines.schedule(()->{if(deferred.remove(id,prepared))enqueueDeferred(id,prepared);},1,TimeUnit.SECONDS);}
    private void execute(String id,PreparedQuery prepared,Job job) {
        long started=System.nanoTime();long statementStarted=started;
        try {
            var run=repo.run(id).orElseThrow();run.put("status","RUNNING");run.put("startedAt",ObjectService.now());
            run.put("logs",List.of(label(prepared)+" 已保存执行快照。",label(prepared)+" 按顺序逐条执行并提交，失败后停止；已提交操作保留。"));
            if(prepared.script().writes()){var logs=new ArrayList<Object>((List<?>)run.get("logs"));logs.add(label(prepared)+" 含写入任务不自动失败重试；手动重跑会从第一条语句重新执行。");run.put("logs",logs);}
            if(!repo.transitionRun(run,"QUEUED"))return;
            job.deadline=deadlines.schedule(()->{job.timedOut=true;interruptQuery(job);},prepared.timeout(),TimeUnit.SECONDS);
            checkStopped(job);
            try(Connection connection=sources.open(prepared.source(),prepared.timeout(),true)) {
                job.connection=connection;checkStopped(job);connection.setAutoCommit(true);configureDoris(connection,prepared.source().type(),prepared.timeout());
                @SuppressWarnings("unchecked") var parameters=(Map<String,Object>)run.getOrDefault("parameters",Map.of());
                // Resolve every parameter before dispatching the first command.
                for(var command:prepared.script().commands())try(var statement=connection.prepareStatement(command.parameters().sql())) {
                    command.parameters().bind(statement,parameters,SqlParameters.custom(run),0);
                }
                for(int i=0;i<prepared.script().commands().size();i++) {
                    checkStopped(job);var command=prepared.script().commands().get(i);job.current=job.results.get(i);job.dispatched=false;statementStarted=System.nanoTime();
                    try(PreparedStatement statement=connection.prepareStatement(command.parameters().sql(),ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY)) {
                        command.parameters().bind(statement,parameters,SqlParameters.custom(run),0);
                        job.statement=statement;
                        int remaining=Math.max(1,(int)Math.ceil(prepared.timeout()-elapsed(started)/1000.0));statement.setQueryTimeout(remaining);
                        if(!command.writes()){statement.setMaxRows(1001);statement.setFetchSize(Integer.MIN_VALUE);}
                        job.current.put("status","RUNNING");job.current.put("commitStatus",command.writes()?"IN_PROGRESS":"NOT_APPLICABLE");
                        persist(id,job);checkStopped(job);job.dispatched=true;
                        boolean hasResult=statement.execute();
                        // With autocommit, a normal return acknowledges this statement's commit.
                        if(command.writes())job.current.put("commitStatus","COMMITTED");
                        if(hasResult)try(ResultSet rs=statement.getResultSet()) {
                            int budget=5*1024*1024-json.write(SqlResults.envelope(job.results)).getBytes(StandardCharsets.UTF_8).length-1024*job.results.size()-4096;
                            job.current.putAll(readResult(rs,job,Math.max(0,budget)));
                        } else job.current.put("affectedRows",Math.max(0,statement.getLargeUpdateCount()));
                        job.current.put("status","SUCCESS");job.current.put("elapsedMs",elapsed(statementStarted));job.current.put("total",((List<?>)job.current.get("rows")).size());
                        persist(id,job);job.current=null;job.dispatched=false;job.statement=null;
                    }
                }
                finish(id,"SUCCESS",null,label(prepared)+" 全部语句执行成功。",SqlResults.envelope(job.results),elapsed(started));
            }
        } catch(Exception e) {
            boolean timeout=job.timedOut||e instanceof SQLTimeoutException||(e instanceof SQLException se&&(se.getErrorCode()==3024||"S1T00".equals(se.getSQLState())));
            boolean connectionLost=e instanceof SQLException se&&se.getSQLState()!=null&&se.getSQLState().startsWith("08");
            boolean unknown=job.current!=null&&job.dispatched&&"IN_PROGRESS".equals(job.current.get("commitStatus"))&&(timeout||job.cancelled||connectionLost||shuttingDown);
            String code=unknown?"COMMIT_UNKNOWN":timeout?"QUERY_TIMEOUT":job.cancelled?"QUERY_CANCELLED":connectionLost?"DATASOURCE_UNAVAILABLE":"DB_TRANSIENT".equals(InventoryExecutionService.errorCode(e))?"DB_TRANSIENT":"QUERY_FAILED";
            String message=unknown?"提交结果未知，请核实业务数据；后续语句未执行。":timeout?"运行超过时间限制，后续语句未执行。":job.cancelled?"用户已停止执行；已提交操作保留。":e instanceof SQLException se?sqlError(se,prepared.source().type()):"执行失败，请检查数据库与 SQL 配置。";
            if(job.current!=null) {
                // A metadata-persistence failure after JDBC success must not disguise a committed write.
                boolean committed="COMMITTED".equals(job.current.get("commitStatus"));
                if(!committed){job.current.put("status",unknown?"UNKNOWN":job.cancelled&&!timeout?"CANCELLED":"FAILED");job.current.put("commitStatus",job.current.get("kind").equals("QUERY")?"NOT_APPLICABLE":unknown?"UNKNOWN":job.dispatched?"NOT_COMMITTED":"NOT_STARTED");}
                job.current.put("errorCode",code);job.current.put("message",message);job.current.put("elapsedMs",elapsed(statementStarted));
            }
            if(!job.cancelled&&!timeout)log.warn("SQL run failed: provider={}, run={}, type={}, code={}",prepared.source().type(),id,e.getClass().getSimpleName(),code);
            if(!shuttingDown)finish(id,unknown||!job.cancelled||timeout?"FAILED":"CANCELLED",code,message,SqlResults.envelope(job.results),elapsed(started));
        } finally {
            if(job.deadline!=null)job.deadline.cancel(false);job.statement=null;job.connection=null;jobs.remove(id,job);releaseReservation(id);
        }
    }
    private void checkStopped(Job job) {if(job.cancelled||job.timedOut||shuttingDown)throw new CancellationException();}
    private void persist(String id,Job job) {repo.saveResult(id,SqlResults.envelope(job.results));}
    private static String label(PreparedQuery prepared){return "DORIS".equals(prepared.source().type())?"[Doris]":"[MySQL]";}
    static void configureDoris(Connection connection,String type,int timeout) throws SQLException {
        if(!"DORIS".equals(type))return;
        try(Statement settings=connection.createStatement()) {
            settings.setQueryTimeout(Math.min(timeout,5));
            settings.execute("SET time_zone = 'Asia/Shanghai'");
            settings.execute("SET query_timeout = "+timeout);
            settings.execute("SET insert_timeout = "+timeout);
            settings.execute("SET group_commit = 'off_mode'");
        }
    }
    private String sqlError(SQLException e,String type) {
        return switch(e.getErrorCode()) {
            case 1045 -> "业务数据库认证失败，请检查数据源凭据。";
            case 1064 -> "SQL 语法错误，请检查 "+("DORIS".equals(type)?"Doris":"MySQL")+" 查询语法。";
            case 1146 -> "查询引用的表不存在。";
            case 1054 -> "查询引用的字段不存在。";
            case 1044,1142,1143,1227,1792 -> "业务数据库拒绝访问，请检查账号对当前业务库的读写及表结构权限。";
            default -> "数据库查询失败（错误码 "+e.getErrorCode()+"），请检查 SQL、连接和权限。";
        };
    }
    private long elapsed(long start) {return (System.nanoTime()-start)/1_000_000;}
    private void finish(String id,String status,String code,String message,Map<String,Object> result,long elapsed) {
        transactions.executeWithoutResult(transaction -> {
            var run=repo.run(id).orElse(null);if(run==null||!Set.of("QUEUED","RUNNING").contains(run.get("status")))return;
            String expected=run.get("status").toString();run.put("status",status);run.put("finishedAt",ObjectService.now());run.put("elapsedMs",elapsed);
            if(code!=null)run.put("errorCode",code);
            List<Object> logs=new ArrayList<>((List<?>)run.getOrDefault("logs",List.of()));logs.add(message);run.put("logs",logs);
            if(result!=null) {
                var items=SqlResults.items(result);run.put("statements",SqlResults.summaries(items));
                run.put("rowCount",items.stream().mapToInt(i->((List<?>)i.getOrDefault("rows",List.of())).size()).sum());
                run.put("affectedRows",items.stream().mapToLong(i->((Number)i.getOrDefault("affectedRows",0)).longValue()).sum());
                run.put("truncated",items.stream().anyMatch(i->Boolean.TRUE.equals(i.get("truncated"))));
            }
            if(repo.transitionRun(run,expected)&&result!=null)repo.saveResult(id,result);
        });
    }
    private Map<String,Object> readResult(ResultSet rs,Job job,int budget) throws Exception {
        List<String> columns=new ArrayList<>();var meta=rs.getMetaData();for(int i=1;i<=meta.getColumnCount();i++)columns.add(meta.getColumnLabel(i));
        List<List<Object>> rows=new ArrayList<>();boolean[] truncated={false};int bytes=json.write(columns).getBytes(StandardCharsets.UTF_8).length;
        if(bytes>budget)return Map.of("columns",List.of(),"rows",List.of(),"truncated",true);
        while(rs.next()) {
            if(job.cancelled||job.timedOut) throw new CancellationException();
            if(rows.size()==1000) {truncated[0]=true;break;}
            List<Object> row=new ArrayList<>();
            for(int i=1;i<=columns.size();i++) {
                int type=meta.getColumnType(i);
                if(Set.of(Types.BINARY,Types.VARBINARY,Types.LONGVARBINARY,Types.BLOB).contains(type)) {
                    int binaryLimit=(65536-7)/4*3;
                    try(InputStream stream=rs.getBinaryStream(i)) {if(stream==null)row.add(null);else {byte[] value=stream.readNBytes(binaryLimit+1);if(value.length>binaryLimit) {truncated[0]=true;value=Arrays.copyOf(value,binaryLimit);}row.add("base64:"+Base64.getEncoder().encodeToString(value));}}
                } else if(Set.of(Types.CHAR,Types.VARCHAR,Types.LONGVARCHAR,Types.NCHAR,Types.NVARCHAR,Types.LONGNVARCHAR,Types.CLOB,Types.NCLOB,Types.SQLXML).contains(type)) {
                    try(Reader reader=rs.getCharacterStream(i)) {row.add(reader==null?null:cell(reader,truncated));}
                } else row.add(rs.getString(i));
            }
            int size=json.write(row).getBytes(StandardCharsets.UTF_8).length+1;
            if(bytes+size>budget) {truncated[0]=true;break;}bytes+=size;rows.add(row);
        }
        return Map.of("columns",columns,"rows",rows,"truncated",truncated[0]);
    }
    private String cell(Reader reader,boolean[] truncated) throws IOException {
        char[] buffer=new char[65537];int count=0,n;
        while(count<buffer.length&&(n=reader.read(buffer,count,buffer.length-count))!=-1)count+=n;
        String value=new String(buffer,0,count);byte[] encoded=value.getBytes(StandardCharsets.UTF_8);
        if(count==buffer.length||encoded.length>65536) {
            truncated[0]=true;int low=0,high=Math.min(value.length(),65536);
            while(low<high) {int middle=(low+high+1)/2;if(value.substring(0,middle).getBytes(StandardCharsets.UTF_8).length<=65536)low=middle;else high=middle-1;}
            int end=low;
            if(end>0&&Character.isHighSurrogate(value.charAt(end-1)))end--;value=value.substring(0,end);
        }
        return value;
    }
    private void interruptQuery(Job job) {
        Statement stmt=job.statement;Connection connection=job.connection;
        try {if(stmt!=null)stmt.cancel();} catch(SQLException ignored) {}
        // Closing/aborting the physical connection also covers the gap before executeQuery.
        try {if(connection!=null)connection.abort(command->Thread.ofVirtual().start(command));} catch(SQLException ignored) {}
    }
    @Override public Map<String,Object> stop(String id) {
        var run=repo.run(id).orElseThrow(()->StudioException.missing("运行记录不存在"));
        if(!Set.of("QUEUED","RUNNING").contains(run.get("status")))return run;
        Job job=jobs.get(id);
        if(job!=null) {
            job.cancelled=true;
            if(workers.remove(job.future)) {
                job.future.cancel(false);jobs.remove(id,job);
                releaseReservation(id);
                finish(id,"CANCELLED","QUERY_CANCELLED","用户已停止排队任务。",SqlResults.envelope(job.results),0);
            } else interruptQuery(job); // The worker alone settles an in-flight commit.
        } else {deferred.remove(id);releaseReservation(id);finish(id,"CANCELLED","QUERY_CANCELLED","用户已停止执行。",null,0);}
        return repo.run(id).orElseThrow();
    }
    public boolean withdrawQueued(String id,String reason){return Boolean.TRUE.equals(transactions.execute(t->{
        var run=repo.run(id).orElseThrow();repo.lockWorkspace(run.get("workspaceId").toString());run=repo.run(id).orElseThrow();if(!"QUEUED".equals(run.get("status")))return false;
        run.put("status","CANCELLED");run.put("errorCode",reason);run.put("finishedAt",ObjectService.now());run.put("elapsedMs",0);run.put("logs",List.of("[调度] 周期实例已暂停，尚未开始的执行已撤回。"));if(!repo.transitionRun(run,"QUEUED"))return false;
        Runnable cleanup=()->{deferred.remove(id);Job job=jobs.get(id);if(job!=null){job.cancelled=true;workers.remove(job.future);job.future.cancel(false);jobs.remove(id,job);}releaseReservation(id);};
        if(TransactionSynchronizationManager.isSynchronizationActive())TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization(){@Override public void afterCommit(){cleanup.run();}});else cleanup.run();return true;
    }));}
    @PreDestroy public void shutdown() {
        shuttingDown=true;
        jobs.values().forEach(job->{job.cancelled=true;interruptQuery(job);});workers.shutdownNow();deadlines.shutdownNow();deferred.clear();reservations.forEach(this::releaseReservation);
    }
}
