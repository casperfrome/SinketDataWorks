package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.exception.StudioException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.ZoneId;
import java.util.*;
import java.util.regex.*;
import org.springframework.stereotype.Component;

/** Compiles credential-free authoring SQL into an ephemeral, workspace-bound Flink program. */
@Component
public class RealtimeSqlCompiler {
    private final DatasourceService sources;
    private final JsonCodec json;
    private static final String IDENT="(?:`(?:[^`]|``)+`|[\\p{L}_][\\p{L}\\p{N}_$]*)";
    private static final Pattern CREATE=Pattern.compile("(?is)^CREATE\\s+(?:TEMPORARY\\s+)?TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?("+IDENT+")\\s*\\(");
    private static final Pattern MARKER=Pattern.compile("^\\s*-- @realtime-binding:(.+):(begin|end)\\s*$");
    private static final Set<String> CONNECTORS=Set.of("kafka","upsert-kafka","mysql-cdc","jdbc","doris","datagen","print","blackhole");
    private static final Set<String> RESERVED=Set.of("pipeline.name","pipeline.fixed-job-id","execution.target","execution.attached","execution.shutdown-on-attached-exit","execution.runtime-mode","parallelism.default","execution.checkpointing.interval","execution.checkpointing.dir","execution.checkpointing.savepoint-dir","execution.state-recovery.path","execution.state-recovery.claim-mode","execution.state-recovery.ignore-unclaimed-state","restart-strategy.type","restart-strategy.fixed-delay.attempts","restart-strategy.fixed-delay.delay","rest.address","rest.port","table.exec.uid.generation");

    public RealtimeSqlCompiler(DatasourceService sources,JsonCodec json){this.sources=sources;this.json=json;}

    public record Prepared(List<String> statements,String execution,List<String> queries,Map<String,String> properties,
            List<Map<String,Object>> datasourceBindings,String fingerprint,Set<String> secrets) {
        public Prepared {statements=List.copyOf(statements);queries=List.copyOf(queries);properties=Map.copyOf(properties);datasourceBindings=List.copyOf(datasourceBindings);secrets=Set.copyOf(secrets);}
        public String explain(){String sql=execution.isBlank()?queries.getFirst():execution;return "EXPLAIN "+(sql.startsWith("EXECUTE ")?sql.substring(8):sql);}
        public String redact(String text){return RealtimeSqlCompiler.redact(text,secrets);}
        @Override public String toString(){return "Prepared[fingerprint="+fingerprint+",statements="+statements.size()+"]";}
    }

    public Prepared compile(Map<String,Object> task,String attemptId){return prepare(task,attemptId,false);}
    public Prepared compilePreview(Map<String,Object> task,String attemptId){return prepare(task,attemptId,true);}
    public Prepared compilePreview(Map<String,Object> task,String attemptId,String query){return prepare(task,attemptId,true,query);}

    private Prepared prepare(Map<String,Object> task,String attemptId,boolean preview){return prepare(task,attemptId,preview,null);}
    private Prepared prepare(Map<String,Object> task,String attemptId,boolean preview,String queryOverride){
        String workspace=text(task,"workspaceId");if(workspace.isBlank())fail("REALTIME_WORKSPACE_REQUIRED","实时任务需要工作空间");
        String sql=text(task,"sql");if(sql.isBlank()||sql.length()>200_000)fail("INVALID_REALTIME_SQL","SQL 必须为 1–200000 字符");
        Map<String,Object> runtime=map(task.get("runtime"));int parallel=integer(runtime,"parallelism",1,1,256);
        var properties=new LinkedHashMap<String,String>();properties.put("execution.target","remote");properties.put("rest.address","jobmanager");properties.put("rest.port","8081");properties.put("execution.runtime-mode","STREAMING");
        properties.put("execution.attached",String.valueOf(preview));properties.put("execution.shutdown-on-attached-exit","false");properties.put("parallelism.default",String.valueOf(parallel));
        properties.put("execution.checkpointing.interval",integer(runtime,"checkpointSeconds",60,1,86400)+" s");properties.put("execution.checkpointing.mode","EXACTLY_ONCE");
        properties.put("execution.checkpointing.dir","file:///opt/flink/state/checkpoints");properties.put("execution.checkpointing.savepoint-dir","file:///opt/flink/state/savepoints");
        properties.put("restart-strategy.type","fixed-delay");properties.put("restart-strategy.fixed-delay.attempts",String.valueOf(integer(runtime,"restartAttempts",3,0,100)));
        properties.put("restart-strategy.fixed-delay.delay",integer(runtime,"restartDelaySeconds",10,1,3600)+" s");
        var bindings=new LinkedHashMap<String,Map<String,Object>>();var specs=new LinkedHashMap<String,DatasourceService.ConnectionSpec>();var tableNames=new HashSet<String>();
        for(Object value:list(task.get("bindings"))){var binding=map(value);String id=text(binding,"id");if(id.isBlank()||bindings.putIfAbsent(id,binding)!=null)fail("INVALID_REALTIME_BINDING","连接 ID 为空或重复");
            var spec=sources.forWorkspace(text(binding,"datasourceId"),workspace);specs.put(spec.id(),spec);validateBinding(binding,spec,parallel);
            if(!tableNames.add(text(binding,"tableName").toLowerCase(Locale.ROOT)))fail("REALTIME_DDL_CONFLICT","逻辑表名重复");}
        var secrets=new LinkedHashSet<String>();var generated=new LinkedHashMap<String,String>();var publicDdl=new LinkedHashMap<String,String>();
        for(var entry:bindings.entrySet()){var binding=entry.getValue();var spec=specs.get(text(binding,"datasourceId"));
            publicDdl.put(entry.getKey(),ddl(binding,spec,text(task,"id"),null,false,secrets));generated.put(entry.getKey(),ddl(binding,spec,text(task,"id"),attemptId,true,secrets));}
        var managed=managed(sql,publicDdl,generated);sql=managed.sql();
        var setup=new ArrayList<String>();var inserts=new ArrayList<String>();var queries=new ArrayList<String>();var canonicalProgram=new ArrayList<String>();var declared=new HashSet<String>();boolean terminalSeen=false;int explicitSets=0;
        for(var entry:bindings.entrySet())if(!managed.ids().contains(entry.getKey())){setup.add(generated.get(entry.getKey()));canonicalProgram.add(publicDdl.get(entry.getKey()));}
        for(String statement:split(sql)){
            String upper=statement.stripLeading().toUpperCase(Locale.ROOT);
            if(upper.matches("(?s)^EXECUTE\\s+STATEMENT\\s+SET\\b.*")){if(queryOverride!=null)continue;if(preview)fail("REALTIME_PREVIEW_WRITES","预览只允许查询");if(++explicitSets>1||!inserts.isEmpty())fail("REALTIME_SCRIPT_WRITES","一次发布只能包含一个 Statement Set，不能混入外部 INSERT");for(String inner:statementSetInserts(statement))inserts.add(inner);terminalSeen=true;canonicalProgram.add(statement);continue;}
            if(upper.matches("(?s)^INSERT\\b.*")){if(queryOverride!=null)continue;if(preview)fail("REALTIME_PREVIEW_WRITES","预览只允许查询");if(explicitSets>0)fail("REALTIME_SCRIPT_WRITES","Statement Set 不能与外部 INSERT 混用，请合并为同一个写入段");inserts.add(statement);terminalSeen=true;canonicalProgram.add(statement);continue;}
            if(upper.matches("(?s)^(SELECT|WITH|VALUES)\\b.*")){if(queryOverride!=null)continue;if(!preview)fail("REALTIME_QUERY_IN_PRODUCTION","SELECT 请使用结果预览；发布 SQL 需要 INSERT");queries.add(statement);terminalSeen=true;canonicalProgram.add(statement);continue;}
            if(terminalSeen)fail("REALTIME_SCRIPT_ORDER","DDL 和 SET 必须写在查询/INSERT 之前");
            if(upper.matches("(?s)^SET\\b.*")){validateSet(statement);setup.add(statement);canonicalProgram.add(statement);continue;}
            var create=CREATE.matcher(statement);
            if(create.find()){
                String tail=maskQuoted(statement.substring(matchingParen(statement,create.end()-1)+1));
                if(Pattern.compile("(?is)\\bAS\\b").matcher(tail).find())fail("REALTIME_CTAS_UNSUPPORTED","CREATE TABLE AS 查询会提交独立作业，请先定义表，再使用 INSERT 或结果预览");
                String table=unquote(create.group(1)).toLowerCase(Locale.ROOT);
                if(!declared.add(table))fail("REALTIME_DDL_CONFLICT","逻辑表存在重复 CREATE TABLE");
                if(tableNames.contains(table)&&!managed.tables().contains(table))fail("REALTIME_DDL_CONFLICT","手写表与绑定逻辑表重名，请使用受管 DDL 块或更改表名");
                String compiled=statement;
                // Managed blocks have already been validated against their public preview and hydrated.
                if(!managed.tables().contains(table))compiled=hydrate(statement,workspace,specs,secrets,attemptId);
                setup.add(compiled);canonicalProgram.add(redact(statement,secrets));continue;
            }
            if(upper.matches("(?s)^ALTER\\s+TABLE\\b.*"))fail("REALTIME_ALTER_TABLE_UNSUPPORTED","请在表定义中修改字段或连接配置，不能通过 ALTER TABLE 覆盖发布绑定");
            if(upper.matches("(?s)^CREATE\\s+(?:OR\\s+REPLACE\\s+)?(?:TEMPORARY\\s+)?VIEW\\b.*")||upper.matches("(?s)^(DROP\\s+(?:TEMPORARY\\s+)?(?:TABLE|VIEW)|ALTER\\s+VIEW|USE(?:\\s+CATALOG)?|SHOW|DESCRIBE|DESC|EXPLAIN)\\b.*")){
                if(upper.contains("ADD JAR")||upper.contains("CREATE CATALOG"))fail("UNSUPPORTED_REALTIME_SQL","不支持动态外部 JAR 或 Catalog");setup.add(statement);canonicalProgram.add(statement);continue;}
            fail("UNSUPPORTED_REALTIME_SQL","实时 SQL 仅支持表/视图 DDL、SET、查询、INSERT 和 Statement Set");
        }
        if(queryOverride!=null){for(String query:split(queryOverride)){if(!query.matches("(?is)^(SELECT|WITH|VALUES)\\b.*"))fail("REALTIME_PREVIEW_WRITES","预览只允许一条 SELECT 查询");queries.add(query);}}
        if(preview&&queries.size()!=1)fail("REALTIME_PREVIEW_QUERY_REQUIRED","预览需要且只能包含一条 SELECT 查询");
        if(!preview&&inserts.isEmpty())fail("REALTIME_INSERT_REQUIRED","发布任务需要至少一条 INSERT");
        if(preview)setup.replaceAll(statement->isolatePreviewTable(statement,attemptId));
        configureCdcServerIds(setup,preview,attemptId,parallel);
        String execution=inserts.isEmpty()?"":"EXECUTE STATEMENT SET BEGIN\n"+String.join(";\n",inserts)+";\nEND";
        List<Map<String,Object>> snapshots=specs.values().stream().map(RealtimeSqlCompiler::snapshot).toList();
        // Hash authoring inputs and fixed datasource targets, never passwords or per-attempt labels.
        String fingerprint=hash(json.write(stable(Map.of("sql",text(task,"sql"),"bindings",list(task.get("bindings")),"runtime",runtime,"datasources",snapshots))));
        return new Prepared(setup,execution,queries,properties,snapshots,fingerprint,secrets);
    }

    public void verifyDatasourceBindings(String workspace,List<Map<String,Object>> snapshots){
        for(var expected:snapshots){var current=snapshot(sources.forWorkspace(text(expected,"id"),workspace));if(!json.write(stable(expected)).equals(json.write(stable(current))))throw StudioException.conflict("DATASOURCE_BINDING_CHANGED","发布版本的数据源连接目标已改变，请重新发布");}
    }

    /** Stateful upgrades compare every physical table, including handwritten DDL, before stopping the old job. */
    public Map<String,String> physicalIdentities(Map<String,Object> task){
        var prepared=compile(task,"physical_identity");var identities=new TreeMap<String,String>();
        for(String statement:prepared.statements()){
            var create=CREATE.matcher(statement);if(!create.find())continue;String name=unquote(create.group(1)).toLowerCase(Locale.ROOT);var options=parseOptions(statement);var targets=new TreeMap<String,String>();String schema=statement;
            if(options!=null){schema=statement.substring(0,options.prefix().length()-1);for(var option:options.values().entrySet()){
                    String key=option.getKey();if(key.equals("sink.label-prefix")||key.equals("server-id")||key.equals("username")||key.equals("properties.sasl.jaas.config")||key.contains("password")||key.contains("secret")||key.contains("token"))continue;
                    targets.put(key,option.getValue());
                }}
            identities.put(name,hash(json.write(Map.of("schema",canonical(schema),"options",targets))));
        }
        return Collections.unmodifiableMap(identities);
    }

    /** The durable registry reserves these hydrated runtime identities before any job submission. */
    public List<Map<String,Object>> cdcServerIds(Prepared prepared){
        var identities=new ArrayList<Map<String,Object>>();for(String statement:prepared.statements()){
            var create=CREATE.matcher(statement);if(!create.find())continue;var options=parseOptions(statement);if(options==null||!"mysql-cdc".equalsIgnoreCase(options.values().get("connector")))continue;
            String ids=options.values().get("server-id");if(ids==null)fail("REALTIME_CDC_SERVER_ID_UNAVAILABLE","CDC 表缺少已分配的 Server ID");validateCdcServerIds(ids,1);String[] bounds=ids.split("-");
            identities.add(Map.of("endpoint",cdcEndpoint(options.values()),"first",Long.parseLong(bounds[0]),"last",Long.parseLong(bounds[bounds.length-1]),"tableName",unquote(create.group(1))));
        }return List.copyOf(identities);
    }
    private static String cdcEndpoint(Map<String,String> options){return options.getOrDefault("hostname","").toLowerCase(Locale.ROOT)+":"+options.getOrDefault("port","3306");}

    public String previewDDL(Map<String,Object> binding,String workspace,String taskId){var spec=sources.forWorkspace(text(binding,"datasourceId"),workspace);validateBinding(binding,spec,1);return ddl(binding,spec,taskId,null,false,new HashSet<>());}

    /** Preview readers always use their own Kafka group and CDC client identities, including handwritten tables. */
    private String isolatePreviewTable(String statement,String attempt){
        var create=CREATE.matcher(statement);if(!create.find())return statement;var options=parseOptions(statement);if(options==null)return statement;var values=options.values();String connector=values.getOrDefault("connector","").toLowerCase(Locale.ROOT);String identity=hash(Objects.toString(attempt,"")+":"+unquote(create.group(1)));
        if(connector.equals("kafka")||connector.equals("upsert-kafka"))values.put("properties.group.id","sinket_debug_"+identity.substring(0,32));
        else return statement;
        return options.prefix()+renderOptions(values)+options.suffix();
    }

    private record CdcRange(String table,long first,long last){}
    private record CdcTable(int position,String table,String endpoint,Options options){}
    /** One MySQL server has a shared replication-client ID namespace across databases and tables. */
    private void configureCdcServerIds(List<String> setup,boolean preview,String attempt,int parallel){
        var tables=new ArrayList<CdcTable>();var occupied=new LinkedHashMap<String,List<CdcRange>>();
        for(int i=0;i<setup.size();i++){
            String statement=setup.get(i);var create=CREATE.matcher(statement);if(!create.find())continue;var options=parseOptions(statement);if(options==null||!"mysql-cdc".equalsIgnoreCase(options.values().get("connector")))continue;
            String table=unquote(create.group(1)),endpoint=cdcEndpoint(options.values());var item=new CdcTable(i,table,endpoint,options);tables.add(item);var ranges=occupied.computeIfAbsent(endpoint,key->new ArrayList<>());
            String ids=options.values().get("server-id");if(ids!=null){validateCdcServerIds(ids,parallel);String[] bounds=ids.split("-");var range=new CdcRange(table,Long.parseLong(bounds[0]),Long.parseLong(bounds[bounds.length-1]));
                if(!preview)for(var prior:ranges)if(overlaps(prior,range))fail("REALTIME_CDC_SERVER_ID_CONFLICT","MySQL "+endpoint+" 的 CDC 表 "+prior.table()+" 与 "+table+" Server ID 范围重叠："+prior.first()+"-"+prior.last()+" / "+ids);
                ranges.add(range);
            }
        }
        // Explicit ranges are reserved first, regardless of their position in the script.
        for(var table:tables){var values=table.options().values();if(!preview&&values.containsKey("server-id"))continue;var ranges=occupied.get(table.endpoint());
            long lower=preview?1_200_000_000L:100_000L,upper=preview?Integer.MAX_VALUE:1_199_999_999L;
            String identity=hash(Objects.toString(attempt,"")+":"+table.table());long candidate=lower+Long.parseUnsignedLong(identity.substring(0,8),16)%((upper-lower+1)/256)*256;
            long first=findCdcGap(candidate,upper,ranges);if(first<0)first=findCdcGap(lower,candidate-1,ranges);
            if(first<0&&preview)first=findCdcGap(1,Integer.MAX_VALUE,ranges);
            if(first<0)fail(preview?"REALTIME_PREVIEW_SERVER_ID_UNAVAILABLE":"REALTIME_CDC_SERVER_ID_UNAVAILABLE","MySQL "+table.endpoint()+" 没有可用的 CDC Server ID 段，请缩小显式范围");
            var allocated=new CdcRange(table.table(),first,first+255);ranges.add(allocated);values.put("server-id",first+"-"+allocated.last());setup.set(table.position(),table.options().prefix()+renderOptions(values)+table.options().suffix());
        }
    }
    private static boolean overlaps(CdcRange first,CdcRange second){return first.first()<=second.last()&&second.first()<=first.last();}
    private static long findCdcGap(long first,long upper,List<CdcRange> occupied){
        if(first+255>upper)return -1;var sorted=occupied.stream().sorted(Comparator.comparingLong(CdcRange::first)).toList();
        for(var range:sorted){if(range.last()<first)continue;if(first+255<range.first())return first;first=range.last()+1;if(first+255>upper)return -1;}
        return first+255<=upper?first:-1;
    }

    private record Managed(String sql,Set<String> ids,Set<String> tables){}
    private Managed managed(String sql,Map<String,String> expected,Map<String,String> generated){
        var output=new StringBuilder();var content=new StringBuilder();var seen=new HashSet<String>();var tables=new HashSet<String>();String active=null;
        for(String line:sql.split("(?<=\\n)",-1)){
            var marker=MARKER.matcher(line.strip());
            if(marker.matches()){
                String id;try{id=URLDecoder.decode(marker.group(1).replace("+","%2B"),StandardCharsets.UTF_8);}catch(Exception e){fail("REALTIME_DDL_MARKERS","DDL 标记 ID 无效");return null;}
                if(marker.group(2).equals("begin")){
                    if(active!=null||!seen.add(id)||!expected.containsKey(id))fail("REALTIME_DDL_MARKERS","DDL 标记重复、嵌套或引用不存在的绑定");active=id;content.setLength(0);
                }else{
                    if(!Objects.equals(active,id))fail("REALTIME_DDL_MARKERS","DDL 标记不完整或顺序错误");
                    if(!canonical(content.toString()).equals(canonical(expected.get(id))))fail("REALTIME_DDL_STALE","受管 DDL 已修改或与当前绑定配置不一致，请重新插入 DDL");
                    var m=CREATE.matcher(stripComments(expected.get(id)));if(m.find())tables.add(unquote(m.group(1)).toLowerCase(Locale.ROOT));output.append(generated.get(id)).append("\n");active=null;
                }
            }else if(active!=null)content.append(line);else output.append(line);
        }
        if(active!=null)fail("REALTIME_DDL_MARKERS","DDL 标记未闭合");return new Managed(output.toString(),seen,tables);
    }

    private String hydrate(String statement,String workspace,Map<String,DatasourceService.ConnectionSpec> specs,Set<String> secrets,String attempt){
        Options options=parseOptions(statement);if(options==null)return statement;
        var values=options.values();String connector=values.getOrDefault("connector","").toLowerCase(Locale.ROOT);
        if(!CONNECTORS.contains(connector))fail("UNSUPPORTED_REALTIME_CONNECTOR","当前集群不支持此连接器");
        for(String key:values.keySet())if(key.equals("password")||key.equals("properties.sasl.jaas.config"))fail("REALTIME_INLINE_SECRET","SQL 不能保存密码，请使用 studio.datasource-id 注入数据源凭据");
        String id=values.remove("studio.datasource-id");if(id==null){if(Set.of("datagen","print","blackhole").contains(connector))return statement;fail("REALTIME_DATASOURCE_REFERENCE_REQUIRED","手写连接器 DDL 需要 studio.datasource-id");}
        var spec=sources.forWorkspace(id,workspace);specs.put(id,spec);String expected=connector.equals("doris")?"DORIS":connector.contains("kafka")?"KAFKA":"MYSQL";
        if(!expected.equals(spec.type()))fail("REALTIME_CONNECTOR_MISMATCH","DDL 连接器与数据源类型不匹配");
        if(connector.equals("mysql-cdc")&&values.containsKey("server-id"))validateCdcServerIds(values.get("server-id"),1);
        inject(values,spec,connector,true,secrets);
        if(connector.equals("doris")){values.put("sink.enable-2pc","true");values.put("sink.label-prefix",label(values.getOrDefault("sink.label-prefix","studio"),attempt));}
        for(String key:values.keySet())if(key.startsWith("studio."))fail("INVALID_REALTIME_SQL","不支持的 studio DDL 属性");
        return options.prefix()+renderOptions(values)+options.suffix();
    }

    private String ddl(Map<String,Object> b,DatasourceService.ConnectionSpec s,String taskId,String attempt,boolean executable,Set<String> secrets){
        String connector=text(b,"connector"),role=text(b,"role");boolean upsert=connector.equals("KAFKA")&&role.equals("SINK")&&text(b,"writeMode").equals("upsert");
        var fields=list(b.get("fields")).stream().map(RealtimeSqlCompiler::map).toList();var columns=new ArrayList<String>();var keys=new ArrayList<String>();
        for(var f:fields){String name=text(f,"name");boolean pk=bool(f,"primaryKey",false);columns.add("  "+identifier(name)+" "+text(f,"type").trim().toUpperCase(Locale.ROOT)+(pk||!bool(f,"nullable",true)?" NOT NULL":""));if(pk)keys.add(identifier(name));}
        boolean pkRequired=connector.equals("MYSQL_CDC")||upsert||connector.equals("MYSQL_JDBC")&&text(b,"writeMode").equals("upsert");
        if(!keys.isEmpty()&&(pkRequired||executable&&connector.equals("DORIS")))columns.add("  PRIMARY KEY ("+String.join(", ",keys)+") NOT ENFORCED");
        if(role.equals("SOURCE")&&!text(b,"eventTimeField").isBlank())columns.add("  WATERMARK FOR "+identifier(text(b,"eventTimeField"))+" AS "+identifier(text(b,"eventTimeField"))+" - INTERVAL "+literal(String.valueOf(integer(b,"watermarkSeconds",5,0,86400)))+" SECOND");
        var o=new LinkedHashMap<String,String>();
        if(connector.equals("KAFKA")){
            o.put("connector",upsert?"upsert-kafka":"kafka");o.put("topic",text(b,"topic"));o.put("properties.bootstrap.servers",option(s,"bootstrapServers",s.host()+":"+s.port()));
            if(upsert){o.put("key.format",text(b,"format"));o.put("value.format",text(b,"format"));}else o.put("format",text(b,"format"));
            if(role.equals("SOURCE")){o.put("properties.group.id",text(b,"consumerGroup"));o.put("scan.startup.mode",text(b,"startupMode"));}
            String protocol=option(s,"securityProtocol","PLAINTEXT");if(!protocol.equals("PLAINTEXT")){o.put("properties.security.protocol",protocol);o.put("properties.sasl.mechanism",option(s,"saslMechanism","PLAIN"));}
        }else if(connector.equals("MYSQL_CDC")){
            o.put("connector","mysql-cdc");o.put("hostname",s.host());o.put("port",String.valueOf(s.port()));o.put("database-name",regexLiteral(s.database()));o.put("table-name",regexLiteral(text(b,"physicalTable")));o.put("scan.startup.mode",text(b,"cdcStartupMode"));o.put("server-time-zone",text(b,"timezone"));if(!text(b,"serverId").isBlank())o.put("server-id",text(b,"serverId"));o.put("username",s.username());
        }else if(connector.equals("MYSQL_JDBC")){
            o.put("connector","jdbc");o.put("url",jdbc(s.host(),s.port(),s.database()));o.put("table-name",text(b,"physicalTable"));o.put("username",s.username());
        }else{
            String configured=text(b,"feHttpUrls");o.put("connector","doris");o.put("fenodes",httpNodes(configured.isBlank()?s.options().get("feHttpUrls"):configured));o.put("table.identifier",s.database()+"."+text(b,"physicalTable"));
            o.put("sink.label-prefix",text(b,"labelPrefix").isBlank()?label(taskId+"_"+text(b,"id"),null):text(b,"labelPrefix"));o.put("sink.properties.format","json");o.put("sink.properties.read_json_by_line","true");o.put("sink.enable-delete",String.valueOf(bool(b,"syncDeletes",false)));o.put("username",s.username());
        }
        if(executable){inject(o,s,o.get("connector"),true,secrets);if(connector.equals("DORIS")){o.put("sink.enable-2pc","true");o.put("sink.label-prefix",label(o.get("sink.label-prefix"),attempt));}}
        return "CREATE TABLE "+identifier(text(b,"tableName"))+" (\n"+String.join(",\n",columns)+"\n) WITH (\n"+renderOptions(o)+"\n);";
    }

    private void inject(Map<String,String> o,DatasourceService.ConnectionSpec s,String connector,boolean credential,Set<String> secrets){
        String host=option(s,"flinkHost",s.host());int port=numberOption(s,"flinkPort",s.port());
        if(!s.options().containsKey("flinkHost")&&loopback(s.host())&&s.type().equals("MYSQL")&&s.port()==3307){host="mysql-rlt";if(!s.options().containsKey("flinkPort"))port=3306;}
        if(!s.options().containsKey("flinkHost")&&loopback(s.host())&&s.type().equals("DORIS")&&s.port()==9030)host="fe";
        var creds=sources.credentials(s);String password=Objects.toString(creds.get("password"),"");if(!password.isEmpty())secrets.add(password);
        if(connector.contains("kafka")){
            String broker=option(s,"flinkBootstrapServers",option(s,"bootstrapServers",s.host()+":"+s.port()));if(broker.equals("localhost:19092")||broker.equals("127.0.0.1:19092"))broker="kafka_rlt_4_3_1:9092";
            o.put("properties.bootstrap.servers",broker);String protocol=option(s,"securityProtocol","PLAINTEXT");o.put("properties.security.protocol",protocol);
            if(!protocol.equals("PLAINTEXT")){String mechanism=option(s,"saslMechanism","PLAIN");o.put("properties.sasl.mechanism",mechanism);String module=mechanism.equals("PLAIN")?"org.apache.kafka.common.security.plain.PlainLoginModule":"org.apache.kafka.common.security.scram.ScramLoginModule";
                String jaas=module+" required username=\""+jaas(Objects.toString(creds.get("username"),s.username()))+"\" password=\""+jaas(password)+"\";";secrets.add(jaas);o.put("properties.sasl.jaas.config",jaas);}
        }else{
            o.put("username",Objects.toString(creds.get("username"),s.username()));o.put("password",password);
            if(connector.equals("mysql-cdc")){o.put("hostname",host);o.put("port",String.valueOf(port));o.put("database-name",regexLiteral(s.database()));}
            if(connector.equals("jdbc"))o.put("url",jdbc(host,port,s.database()));
            if(connector.equals("doris")){
                String nodes=httpNodes(s.options().get("flinkFeHttpUrls"));if(nodes.isBlank())nodes=o.getOrDefault("fenodes",httpNodes(s.options().get("feHttpUrls")));if(host.equals("fe"))nodes=nodes.replace("127.0.0.1:8030","fe:8030").replace("localhost:8030","fe:8030");
                o.put("fenodes",nodes);o.put("jdbc-url",jdbc(host,port,s.database()));String be=httpNodes(s.options().get("flinkBeHttpUrls"));if(be.isBlank()&&host.equals("fe"))be="172.30.41.3:8040";if(!be.isBlank())o.put("benodes",be);
                String target=o.getOrDefault("table.identifier","");int dot=target.indexOf('.');o.put("table.identifier",s.database()+"."+(dot>=0?target.substring(dot+1):target));
            }
        }
    }

    private void validateBinding(Map<String,Object> b,DatasourceService.ConnectionSpec s,int parallel){
        String connector=text(b,"connector"),role=text(b,"role");if(!Set.of("SOURCE","SINK").contains(role))fail("INVALID_REALTIME_BINDING","连接角色无效");
        if(role.equals("SOURCE")&&!Set.of("KAFKA","MYSQL_CDC").contains(connector)||role.equals("SINK")&&!Set.of("KAFKA","MYSQL_JDBC","DORIS").contains(connector))fail("INVALID_REALTIME_BINDING","连接器不能用于此角色");
        String expected=connector.equals("KAFKA")?"KAFKA":connector.equals("DORIS")?"DORIS":"MYSQL";if(!expected.equals(s.type()))fail("REALTIME_CONNECTOR_MISMATCH","连接器与数据源类型不匹配");
        validName(text(b,"tableName"));var names=new HashSet<String>();int keys=0;var fields=list(b.get("fields"));if(fields.isEmpty()||fields.size()>512)fail("INVALID_REALTIME_FIELD","需要 1–512 个字段");
        for(Object value:fields){var f=map(value);validName(text(f,"name"));if(!names.add(text(f,"name").toLowerCase(Locale.ROOT)))fail("INVALID_REALTIME_FIELD","字段名重复");if(!validType(text(f,"type")))fail("INVALID_REALTIME_FIELD","Flink 字段类型无效");if(bool(f,"primaryKey",false)){keys++;if(bool(f,"nullable",true))fail("INVALID_REALTIME_FIELD","主键字段不可为空");}}
        String event=text(b,"eventTimeField");if(!event.isBlank()){
            var field=fields.stream().map(RealtimeSqlCompiler::map).filter(f->text(f,"name").equals(event)).findFirst().orElse(Map.of());if(!role.equals("SOURCE")||!text(field,"type").replaceAll("\\s","").matches("(?i)TIMESTAMP(?:_LTZ)?\\(3\\)"))fail("INVALID_REALTIME_WATERMARK","事件时间需要来源表 TIMESTAMP(3) 字段");integer(b,"watermarkSeconds",5,0,86400);}
        if(connector.equals("KAFKA")){
            String topic=text(b,"topic");if(!topic.matches("[a-zA-Z0-9._-]{1,249}")||Set.of(".","..").contains(topic))fail("INVALID_REALTIME_BINDING","Kafka Topic 无效");
            if(!Set.of("json","csv").contains(text(b,"format")))fail("INVALID_REALTIME_BINDING","当前配置器只支持 JSON/CSV");
            if(role.equals("SOURCE")&&(text(b,"consumerGroup").isBlank()||!Set.of("group-offsets","earliest-offset","latest-offset").contains(text(b,"startupMode"))))fail("INVALID_REALTIME_BINDING","Kafka 消费组或起始位点无效");
            if(role.equals("SINK")&&!Set.of("append","upsert").contains(text(b,"writeMode")))fail("INVALID_REALTIME_BINDING","Kafka 写入模式无效");
            if(role.equals("SINK")&&text(b,"writeMode").equals("upsert")&&(keys==0||!text(b,"format").equals("json")))fail("INVALID_REALTIME_BINDING","Upsert Kafka 需要主键和 JSON");
        }else if(text(b,"physicalTable").isBlank()||text(b,"physicalTable").length()>256)fail("INVALID_REALTIME_BINDING","需要物理表名");
        if(connector.equals("MYSQL_CDC")){
            if(keys==0)fail("INVALID_REALTIME_BINDING","当前 CDC 配置器需要主键");String server=text(b,"serverId");if(!server.isBlank())validateCdcServerIds(server,parallel);
            try{ZoneId.of(text(b,"timezone"));}catch(Exception e){fail("INVALID_REALTIME_BINDING","CDC 时区无效");}
            if(!Set.of("initial","latest-offset").contains(text(b,"cdcStartupMode")))fail("INVALID_REALTIME_BINDING","CDC 起始模式无效");
        }
        if(connector.equals("MYSQL_JDBC")&&(!Set.of("append","upsert").contains(text(b,"writeMode"))||text(b,"writeMode").equals("upsert")&&keys==0))fail("INVALID_REALTIME_BINDING","JDBC Upsert 需要主键");
        if(connector.equals("DORIS")){
            String nodes=httpNodes(text(b,"feHttpUrls").isBlank()?s.options().get("feHttpUrls"):text(b,"feHttpUrls"));if(nodes.isBlank()||nodes.contains(":9030"))fail("INVALID_REALTIME_BINDING","Doris 需要 FE HTTP 地址");if(!text(b,"labelPrefix").matches("[a-zA-Z0-9_-]{1,100}"))fail("INVALID_REALTIME_BINDING","Doris Label Prefix 无效");
            if(bool(b,"syncDeletes",false)&&(!text(b,"dorisModel").equals("UNIQUE")||keys==0))fail("INVALID_REALTIME_BINDING","Doris 同步删除需要 Unique 模型和主键");}
    }

    private static void validateCdcServerIds(String server,int parallel){
        try{if(!server.matches("\\d+(?:-\\d+)?"))throw new IllegalArgumentException();String[] ids=server.split("-");long first=Long.parseLong(ids[0]),last=Long.parseLong(ids[ids.length-1]);if(first<1||last>Integer.MAX_VALUE||last-first+1<parallel)throw new IllegalArgumentException();}
        catch(Exception e){fail("INVALID_REALTIME_BINDING","CDC Server ID 须为 1–2147483647 的递增范围，且数量不少于并行度");}
    }

    /** SQL lexer: quoted text/comments never supply statement separators or Statement Set boundaries. */
    public static List<String> split(String input){
        if(input==null||input.length()>200_000)fail("INVALID_REALTIME_SQL","SQL 超过长度限制");String code=stripComments(input);var pieces=new ArrayList<String>();var current=new StringBuilder();
        for(int i=0;i<code.length();){char c=code.charAt(i);if(c=='\''||c=='"'||c=='`'){int end=quotedEnd(code,i);current.append(code,i,end);i=end;}else if(c==';'){if(!current.toString().isBlank())pieces.add(current.toString().trim());current.setLength(0);i++;}else{current.append(c);i++;}}
        if(!current.toString().isBlank())pieces.add(current.toString().trim());var result=new ArrayList<String>();StringBuilder group=null;
        for(String piece:pieces){String upper=piece.toUpperCase(Locale.ROOT);
            if(group!=null){if(upper.equals("END")){group.append("END");result.add(group.toString());group=null;}else{if(upper.matches("(?s)^(?:EXECUTE|BEGIN)\\s+STATEMENT\\s+SET\\b.*"))fail("INVALID_REALTIME_SQL","Statement Set 不能嵌套");group.append(piece).append(";\n");}continue;}
            if(upper.matches("(?s)^EXECUTE\\s+STATEMENT\\s+SET\\b.*")){if(!upper.matches("(?s)^EXECUTE\\s+STATEMENT\\s+SET\\s+BEGIN\\b.*"))fail("INVALID_REALTIME_SQL","Statement Set 缺少 BEGIN");group=new StringBuilder(piece).append(";\n");}
            else if(upper.matches("(?s)^BEGIN\\s+STATEMENT\\s+SET$")){group=new StringBuilder("EXECUTE STATEMENT SET BEGIN\n");}
            else if(upper.equals("END"))fail("INVALID_REALTIME_SQL","孤立的 END");else result.add(piece);
        }
        if(group!=null)fail("INVALID_REALTIME_SQL","Statement Set 缺少 END");if(result.size()>1000)fail("INVALID_REALTIME_SQL","一次最多 1000 条语句");return List.copyOf(result);
    }

    private static List<String> statementSetInserts(String set){String inner=set.replaceFirst("(?is)^EXECUTE\\s+STATEMENT\\s+SET\\s+BEGIN\\s*","").replaceFirst("(?is)\\bEND\\s*$","");var parts=split(inner);if(parts.isEmpty())fail("INVALID_REALTIME_SQL","Statement Set 不能为空");for(String part:parts)if(!part.matches("(?is)^INSERT\\b.*"))fail("INVALID_REALTIME_SQL","Statement Set 内只支持 INSERT");return parts;}
    private static void validateSet(String statement){var matcher=Pattern.compile("(?is)^SET\\s+'((?:''|[^'])+)'\\s*=\\s*'((?:''|[^'])*)'$ ".trim()).matcher(statement);if(!matcher.matches())fail("INVALID_REALTIME_SQL","SET 需要带引号的键和值");String key=matcher.group(1).toLowerCase(Locale.ROOT);if(RESERVED.contains(key)||key.startsWith("execution.state-recovery")||key.startsWith("sql-gateway.")||key.startsWith("pipeline.jars")||key.startsWith("pipeline.classpaths"))fail("REALTIME_RESERVED_CONFIG","该运行参数由平台管理，请在运行配置中设置");}
    private record Options(String prefix,LinkedHashMap<String,String> values,String suffix){}
    private static Options parseOptions(String sql){
        String masked=maskQuoted(sql);var with=Pattern.compile("(?is)\\bWITH\\s*\\(").matcher(masked);if(!with.find())return null;int open=masked.indexOf('(',with.start()),close=matchingParen(sql,open);String body=sql.substring(open+1,close);var values=new LinkedHashMap<String,String>();
        int i=0;while(i<body.length()){while(i<body.length()&&(Character.isWhitespace(body.charAt(i))||body.charAt(i)==','))i++;if(i==body.length())break;if(body.charAt(i)!='\'')fail("INVALID_REALTIME_SQL","WITH 属性名需要单引号");int keyEnd=quotedEnd(body,i);String key=decodeLiteral(body.substring(i,keyEnd));i=keyEnd;while(i<body.length()&&Character.isWhitespace(body.charAt(i)))i++;if(i>=body.length()||body.charAt(i++)!='=')fail("INVALID_REALTIME_SQL","WITH 属性需要 =");while(i<body.length()&&Character.isWhitespace(body.charAt(i)))i++;if(i>=body.length()||body.charAt(i)!='\'')fail("INVALID_REALTIME_SQL","WITH 属性值需要单引号");int end=quotedEnd(body,i);String value=decodeLiteral(body.substring(i,end));i=end;if(values.putIfAbsent(key.toLowerCase(Locale.ROOT),value)!=null)fail("INVALID_REALTIME_SQL","WITH 属性重复");while(i<body.length()&&Character.isWhitespace(body.charAt(i)))i++;if(i<body.length()&&body.charAt(i)!=',')fail("INVALID_REALTIME_SQL","WITH 属性缺少逗号");}
        return new Options(sql.substring(0,open+1),values,sql.substring(close));
    }
    private static int matchingParen(String sql,int open){int depth=1;for(int i=open+1;i<sql.length();i++){char c=sql.charAt(i);if(c=='\''||c=='"'||c=='`'){i=quotedEnd(sql,i)-1;continue;}if(c=='(')depth++;if(c==')'&&--depth==0)return i;}fail("INVALID_REALTIME_SQL","SQL 括号未闭合");return -1;}
    public static String stripComments(String sql){var out=new StringBuilder();for(int i=0;i<sql.length();){char c=sql.charAt(i);if(c=='\''||c=='"'||c=='`'){int end=quotedEnd(sql,i);out.append(sql,i,end);i=end;}else if(i+1<sql.length()&&(c=='-'&&sql.charAt(i+1)=='-'||c=='/'&&sql.charAt(i+1)=='/')){while(i<sql.length()&&sql.charAt(i)!='\n')i++;out.append('\n');}else if(c=='/'&&i+1<sql.length()&&sql.charAt(i+1)=='*'){int end=sql.indexOf("*/",i+2);if(end<0)fail("INVALID_REALTIME_SQL","SQL 注释未闭合");if(i+2<sql.length()&&sql.charAt(i+2)=='!')fail("INVALID_REALTIME_SQL","不支持可执行注释");if(i+2<sql.length()&&sql.charAt(i+2)=='+'){String hint=sql.substring(i,end+2);if(hint.matches("(?is).*\\bOPTIONS\\s*\\(.*"))fail("REALTIME_DYNAMIC_OPTIONS_UNSUPPORTED","连接参数请定义在 DDL 中，不能通过 OPTIONS Hint 覆盖数据源");out.append(hint);}else out.append(' ');i=end+2;}else{out.append(c);i++;}}return out.toString();}
    private static int quotedEnd(String text,int start){char quote=text.charAt(start);for(int i=start+1;i<text.length();i++){if(text.charAt(i)==quote){if(i+1<text.length()&&text.charAt(i+1)==quote){i++;continue;}return i+1;}}fail("INVALID_REALTIME_SQL","SQL 引号未闭合");return -1;}
    private static String maskQuoted(String text){var result=new StringBuilder(text);for(int i=0;i<text.length();i++)if(text.charAt(i)=='\''||text.charAt(i)=='"'||text.charAt(i)=='`'){int end=quotedEnd(text,i);for(int n=i;n<end;n++)result.setCharAt(n,' ');i=end-1;}return result.toString();}
    private static String canonical(String sql){String code=stripComments(sql);var out=new StringBuilder();for(int i=0;i<code.length();){char c=code.charAt(i);if(c=='\''||c=='"'||c=='`'){int end=quotedEnd(code,i);out.append(code,i,end);i=end;}else{if(!Character.isWhitespace(c)&&c!=';')out.append(Character.toLowerCase(c));i++;}}return out.toString();}
    private static boolean validType(String type){String s=type.toUpperCase(Locale.ROOT).replaceAll("\\s","");if(s.matches("STRING|BOOLEAN|TINYINT|SMALLINT|INT|INTEGER|BIGINT|FLOAT|DOUBLE|DATE|BYTES|(?:TIME|TIMESTAMP|TIMESTAMP_LTZ)(?:\\([0-9]\\))?"))return true;var m=Pattern.compile("(?:DECIMAL|NUMERIC)\\((\\d+),(\\d+)\\)").matcher(s);if(m.matches()){try{int p=Integer.parseInt(m.group(1)),scale=Integer.parseInt(m.group(2));return p>=1&&p<=38&&scale<=p;}catch(Exception e){return false;}}m=Pattern.compile("(?:CHAR|VARCHAR|BINARY|VARBINARY)\\((\\d+)\\)").matcher(s);if(m.matches()){try{return Long.parseLong(m.group(1))>0&&Long.parseLong(m.group(1))<=Integer.MAX_VALUE;}catch(Exception e){return false;}}return false;}
    private static void validName(String name){if(name.isBlank()||name.length()>256||name.codePoints().anyMatch(Character::isISOControl))fail("INVALID_REALTIME_FIELD","表名或字段名无效");}
    private static String renderOptions(Map<String,String> options){return String.join(",\n",options.entrySet().stream().map(e->"  "+literal(e.getKey())+" = "+literal(e.getValue())).toList());}
    private static String identifier(String s){return "`"+s.replace("`","``")+"`";}
    private static String literal(String s){return "'"+s.replace("'","''")+"'";}
    private static String decodeLiteral(String s){return s.substring(1,s.length()-1).replace("''","'");}
    private static String unquote(String s){return s.startsWith("`")?s.substring(1,s.length()-1).replace("``","`"):s;}
    private static String regexLiteral(String s){return s.replaceAll("([.*+?^${}()|\\[\\]\\\\])","\\\\$1");}
    private static String jaas(String s){return s.replace("\\","\\\\").replace("\"","\\\"");}
    private static String label(String value,String attempt){String base=value.replaceAll("[^a-zA-Z0-9_-]","_");if(base.length()>80)base=base.substring(0,80);return base+(attempt==null||attempt.isBlank()?"":"_"+attempt.replaceAll("[^a-zA-Z0-9_-]","_").substring(0,Math.min(32,attempt.length())));}
    private static String jdbc(String host,int port,String database){return "jdbc:mysql://"+(host.contains(":")&&!host.startsWith("[")?"["+host+"]":host)+":"+port+"/"+database;}
    private static boolean loopback(String host){return Set.of("127.0.0.1","localhost","::1","[::1]").contains(host);}
    private static String httpNodes(Object value){List<String> nodes=value instanceof List<?> list?list.stream().map(Object::toString).toList():value==null?List.of():Arrays.asList(value.toString().split(","));var result=new ArrayList<String>();for(String node:nodes){if(node.isBlank())continue;try{URI uri=URI.create(node.contains("://")?node:"http://"+node);if(!"http".equals(uri.getScheme())||uri.getHost()==null||uri.getUserInfo()!=null||uri.getQuery()!=null||uri.getFragment()!=null||!Set.of("","/").contains(uri.getPath()))throw new IllegalArgumentException();result.add(uri.getAuthority());}catch(Exception e){fail("INVALID_REALTIME_BINDING","FE/BE HTTP 地址无效");}}return String.join(",",result);}
    private static Map<String,Object> snapshot(DatasourceService.ConnectionSpec s){var result=new LinkedHashMap<String,Object>();for(String key:List.of("id","workspaceId","type","host","port","database","username","options","bootstrapServers","securityProtocol","saslMechanism","flinkBootstrapServers"))if(s.publicView().containsKey(key))result.put(key,s.publicView().get(key));return result;}
    public static String redact(String input,Collection<String> secrets){if(input==null)return "";String value=input;for(String secret:secrets.stream().filter(s->s!=null&&!s.isEmpty()).sorted(Comparator.comparingInt(String::length).reversed()).toList()){value=value.replace(secret,"***").replace(secret.replace("'","''"),"***").replace(secret.replace("\\","\\\\").replace("\"","\\\""),"***");}return value.replaceAll("(?is)('(?:password|properties\\.sasl\\.jaas\\.config)'\\s*=\\s*)'(?:''|[^'])*'","$1'***'").replaceAll("(?i)(password\\s*=\\s*\\\")[^\\\"]*\\\"","$1***\"");}
    private static String hash(String value){try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));}catch(Exception e){throw new IllegalStateException(e);}}
    private static Object stable(Object value){if(value instanceof Map<?,?> m){var out=new TreeMap<String,Object>();m.forEach((k,v)->out.put(k.toString(),stable(v)));return out;}if(value instanceof List<?> l)return l.stream().map(RealtimeSqlCompiler::stable).toList();if(value instanceof Number n)return new java.math.BigDecimal(n.toString()).stripTrailingZeros();return value;}
    private static String option(DatasourceService.ConnectionSpec s,String key,String fallback){return Objects.toString(s.options().get(key),fallback);}
    private static int numberOption(DatasourceService.ConnectionSpec s,String key,int fallback){Object v=s.options().get(key);if(v==null)return fallback;try{return Integer.parseInt(v.toString().replaceAll("\\.0$",""));}catch(Exception e){fail("INVALID_REALTIME_BINDING","Flink 连接端口无效");return 0;}}
    static String text(Map<String,Object> value,String key){return Objects.toString(value.get(key),"");}
    @SuppressWarnings("unchecked") static Map<String,Object> map(Object value){return value instanceof Map<?,?> m?(Map<String,Object>)m:Map.of();}
    @SuppressWarnings("unchecked") static List<Object> list(Object value){return value instanceof List<?> l?(List<Object>)l:List.of();}
    private static boolean bool(Map<String,Object> value,String key,boolean fallback){return value.get(key) instanceof Boolean b?b:fallback;}
    private static int integer(Map<String,Object> value,String key,int fallback,int min,int max){Object raw=value.getOrDefault(key,fallback);if(!(raw instanceof Number n)||n.doubleValue()!=Math.rint(n.doubleValue())||n.doubleValue()<min||n.doubleValue()>max)fail("INVALID_REALTIME_RUNTIME","运行参数无效："+key);return ((Number)raw).intValue();}
    private static void fail(String code,String message){throw StudioException.bad(code,message);}
}
