package com.fake.dataworks.service;

import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.config.JsonCodec;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.time.Duration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DatasourceService {
    public record ConnectionSpec(String id,String workspaceId,String name,String host,int port,String database,String username,String encryptedPassword,String type,Map<String,Object> options) {
        @Override public String toString() { return "ConnectionSpec[id="+id+"]"; }
        public Map<String,Object> publicView() {
            if("KAFKA".equals(type)) {
                var view=new LinkedHashMap<String,Object>();
                view.put("id",id);view.put("workspaceId",workspaceId);view.put("name",name);view.put("type",type);
                view.put("username",username);view.put("passwordSet",!encryptedPassword.isEmpty());
                view.putAll(options);return view;
            }
            Map<String,Object> view=new LinkedHashMap<>(Map.of("id",id,"workspaceId",workspaceId,"name",name,"host",host,"port",port,"database",database,"username",username,"passwordSet",!encryptedPassword.isEmpty()));
            view.put("type",type);view.put("options",options);return view;
        }
    }
    private final JdbcTemplate jdbc;
    private final ObjectService objects;
    private final SecretCipher cipher;
    private final JsonCodec json;
    private static final Executor NETWORK_EXECUTOR=command -> Thread.ofVirtual().start(command);
    public DatasourceService(JdbcTemplate jdbc,ObjectService objects,SecretCipher cipher,JsonCodec json) {this.jdbc=jdbc;this.objects=objects;this.cipher=cipher;this.json=json;}
    public List<Map<String,Object>> list(String workspace) {
        objects.workspace(workspace);
        return jdbc.query("SELECT * FROM dw_datasource WHERE workspace_id=? ORDER BY name",(r,n)->read(r).publicView(),workspace);
    }
    private ConnectionSpec read(ResultSet r) throws SQLException {
        return new ConnectionSpec(r.getString("id"),r.getString("workspace_id"),r.getString("name"),r.getString("host"),r.getInt("port"),r.getString("database_name"),r.getString("username"),r.getString("password_cipher"),r.getString("type"),r.getString("options_json")==null?Map.of():json.map(r.getString("options_json")));
    }
    public ConnectionSpec get(String id) {return jdbc.query("SELECT * FROM dw_datasource WHERE id=?",(r,n)->read(r),id).stream().findFirst().orElseThrow(()->StudioException.missing("数据源不存在"));}
    public ConnectionSpec forWorkspace(String id,String workspace) {
        var spec=get(id);if(!spec.workspaceId().equals(workspace)) throw StudioException.bad("WORKSPACE_MISMATCH","数据源不属于当前工作空间");return spec;
    }
    private String field(Map<String,Object> input,String key,String fallback,int max) {
        Object value=input.getOrDefault(key,fallback);
        if(!(value instanceof String s)||s.isBlank()||s.length()>max||s.codePoints().anyMatch(Character::isISOControl)) throw StudioException.bad("INVALID_DATASOURCE","请检查数据源字段："+key);
        return s.trim();
    }
    public ConnectionSpec input(String id,Map<String,Object> input) {
        return input(id,input,false);
    }
    private ConnectionSpec input(String id,Map<String,Object> input,boolean pendingCredentials) {
        ConnectionSpec old=id==null?null:get(id);
        String workspace=field(input,"workspaceId",old==null?"local-workspace":old.workspaceId(),64);
        objects.workspace(workspace);
        if(old!=null&&!workspace.equals(old.workspaceId())) throw StudioException.bad("WORKSPACE_MISMATCH","不能跨空间移动数据源");
        String type=field(input,"type",old==null?"MYSQL":old.type(),16);
        if(!Set.of("MYSQL","DORIS","KAFKA").contains(type))throw StudioException.bad("INVALID_DATASOURCE","不支持的数据源类型");
        if(old!=null&&!type.equals(old.type()))throw StudioException.bad("INVALID_DATASOURCE","已有数据源不能更换数据库类型，请新增连接");
        if("KAFKA".equals(type))return kafkaInput(old,input,workspace,pendingCredentials);
        Map<String,Object> options=normalizeOptions(type,input.getOrDefault("options",old==null?Map.of():old.options()));
        String name=field(input,"name",old==null?"MySQL 数据源":old.name(),100);
        String host=field(input,"host",old==null?"127.0.0.1":old.host(),253);
        String database=field(input,"database",old==null?"studio_demo":old.database(),64);
        String username=field(input,"username",old==null?"studio_reader":old.username(),100);
        Object portValue=input.getOrDefault("port",old==null?3307:old.port());
        if(!(portValue instanceof Number n)||n.doubleValue()!=n.intValue()||n.intValue()<1||n.intValue()>65535) throw StudioException.bad("INVALID_DATASOURCE","端口须为 1–65535 的整数");
        if(!host.matches("[a-zA-Z0-9._:-]+")||!database.matches("[a-zA-Z0-9_]+")) throw StudioException.bad("INVALID_DATASOURCE","主机或数据库名称格式不正确");
        String metadata=jdbc.queryForObject("SELECT DATABASE()",String.class);
        if(Set.of("mysql","information_schema","performance_schema","sys","fake_dataworks_260927").contains(database.toLowerCase(Locale.ROOT))||database.equalsIgnoreCase(metadata)) throw StudioException.bad("METADATA_DATABASE_FORBIDDEN","请选择独立业务数据库，不能查询元数据库或系统库");
        if(username.equalsIgnoreCase("root")||username.contains("@")) throw StudioException.bad("PRIVILEGED_ACCOUNT","请使用专门的业务库账号，不能使用 root");
        String encrypted=old==null?"":old.encryptedPassword();
        if(input.containsKey("password")) {
            if(!(input.get("password") instanceof String password)||password.isEmpty()||password.length()>4096) throw StudioException.bad("INVALID_DATASOURCE","请输入有效密码；编辑时不填写则保留原密码");
            encrypted=cipher.encrypt(password);
        }
        if(encrypted.isEmpty()) throw StudioException.bad("PASSWORD_REQUIRED","新数据源需要密码");
        // Legacy capability fields are deliberately ignored. Database grants apply to every source.
        return new ConnectionSpec(old==null?UUID.randomUUID().toString():old.id(),workspace,name,host,n.intValue(),database,username,encrypted,type,options);
    }
    @Transactional
    public Map<String,Object> save(String id,Map<String,Object> input) {
        var s=input(id,input);
        return persist(id,s);
    }
    /** Only legacy import may preserve SASL configurations whose browser-session password was lost. */
    @Transactional public Map<String,Object> importKafka(Map<String,Object> input) {
        var values=new LinkedHashMap<>(input);values.put("type","KAFKA");
        return persist(null,input(null,values,true));
    }
    private Map<String,Object> persist(String id,ConnectionSpec s) {
        if(id==null) jdbc.update("INSERT INTO dw_datasource(id,workspace_id,name,host,port,database_name,username,password_cipher,updated_at,type,options_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)",s.id(),s.workspaceId(),s.name(),s.host(),s.port(),s.database(),s.username(),s.encryptedPassword(),ObjectService.now(),s.type(),json.write(s.options()));
        else jdbc.update("UPDATE dw_datasource SET name=?,host=?,port=?,database_name=?,username=?,password_cipher=?,updated_at=?,type=?,options_json=? WHERE id=?",s.name(),s.host(),s.port(),s.database(),s.username(),s.encryptedPassword(),ObjectService.now(),s.type(),json.write(s.options()),id);
        return s.publicView();
    }
    public Connection open(ConnectionSpec s,int timeoutSeconds) throws SQLException {
        return open(s,timeoutSeconds,false);
    }
    public Connection open(ConnectionSpec s,int timeoutSeconds,boolean writable) throws SQLException {
        if(!Set.of("MYSQL","DORIS").contains(s.type()))throw StudioException.bad("DATASOURCE_TYPE_MISMATCH","此操作需要数据库数据源");
        String host=s.host().contains(":")?"["+s.host()+"]":s.host();
        Properties props=new Properties();props.setProperty("user",s.username());props.setProperty("password",cipher.decrypt(s.encryptedPassword()));
        props.setProperty("connectTimeout","5000");props.setProperty("socketTimeout",Integer.toString((timeoutSeconds+5)*1000));
        props.setProperty("allowMultiQueries","false");props.setProperty("allowLoadLocalInfile","false");props.setProperty("allowUrlInLocalInfile","false");props.setProperty("allowPublicKeyRetrieval","true");
        props.setProperty("characterEncoding","UTF-8");props.setProperty("connectionTimeZone","Asia/Shanghai");
        Connection c=DriverManager.getConnection("jdbc:mysql://"+host+":"+s.port()+"/"+s.database(),props);
        try {
            c.setNetworkTimeout(NETWORK_EXECUTOR,(timeoutSeconds+5)*1000);
            if("MYSQL".equals(s.type())) {
                try(Statement settings=c.createStatement()) {settings.setQueryTimeout(5);settings.execute("SET SESSION max_execution_time="+(timeoutSeconds*1000));}
                c.setReadOnly(!writable);c.setAutoCommit(false);
            }
            return c;
        }
        catch(SQLException e) { c.close();throw e; }
    }
    public Map<String,Object> test(ConnectionSpec s) {
        if("KAFKA".equals(s.type()))return testKafka(s);
        long started=System.nanoTime();
        try(Connection c=open(s,5);Statement stmt=c.createStatement()) {
            stmt.setQueryTimeout(5);
            try(ResultSet rs=stmt.executeQuery("SELECT VERSION()")) {rs.next();return Map.of("success",true,"version",rs.getString(1),"elapsedMs",(System.nanoTime()-started)/1_000_000,"message","连接成功");}
        } catch(SQLException e) {throw connectionError(e);}
    }
    public StudioException connectionError(SQLException e) {
        if(e.getErrorCode()==1045) return StudioException.bad("DATASOURCE_AUTH_FAILED","业务数据库认证失败，请检查账号与密码");
        if(e.getErrorCode()==1044||e.getErrorCode()==1142||e.getErrorCode()==1143) return StudioException.bad("DATASOURCE_PERMISSION_DENIED","业务数据库拒绝访问，请检查账号对当前业务库的权限");
        return new StudioException("DATASOURCE_UNAVAILABLE","无法访问业务数据库，请检查地址、数据库名称、权限和服务状态",502);
    }
    public List<Map<String,Object>> tables(String id) {
        var s=get(id);
        return metadata(s,"SELECT TABLE_NAME AS name,TABLE_TYPE AS type,TABLE_COMMENT AS comment FROM information_schema.tables WHERE table_schema=? ORDER BY table_name",s.database());
    }
    public List<Map<String,Object>> columns(String id,String table) {
        return columns(get(id),table);
    }
    public List<Map<String,Object>> columns(ConnectionSpec s,String table) {
        return metadata(s,"SELECT COLUMN_NAME AS name,COLUMN_TYPE AS type,IS_NULLABLE AS nullable,COLUMN_KEY AS columnKey,COLUMN_COMMENT AS comment FROM information_schema.columns WHERE table_schema=? AND table_name=? ORDER BY ordinal_position",s.database(),table);
    }
    private List<Map<String,Object>> metadata(ConnectionSpec s,String sql,String... parameters) {
        try(Connection c=open(s,5);PreparedStatement stmt=c.prepareStatement(sql)) {
            stmt.setQueryTimeout(5);for(int i=0;i<parameters.length;i++) stmt.setString(i+1,parameters[i]);
            try(ResultSet rs=stmt.executeQuery()) {List<Map<String,Object>> rows=new ArrayList<>();var meta=rs.getMetaData();while(rs.next()) {Map<String,Object> row=new LinkedHashMap<>();for(int i=1;i<=meta.getColumnCount();i++)row.put(meta.getColumnLabel(i),rs.getString(i));rows.add(row);}return rows;}
        } catch(SQLException e) {throw connectionError(e);}
    }

    public Map<String,Object> credentials(ConnectionSpec s) {return Map.of("username",s.username(),"password",s.encryptedPassword().isEmpty()?"":cipher.decrypt(s.encryptedPassword()));}
    @SuppressWarnings("unchecked") private Map<String,Object> normalizeOptions(String type,Object raw) {
        if(!(raw instanceof Map<?,?>))throw StudioException.bad("INVALID_DATASOURCE","连接配置格式无效");
        var m=new LinkedHashMap<String,Object>((Map<String,Object>)raw);
        var allowed=new HashSet<>(Set.of("flinkHost","flinkPort","flinkFeHttpUrls","flinkBeHttpUrls"));
        if("DORIS".equals(type))allowed.addAll(Set.of("feHttpUrls","beHttpUrls","flightUri","flightEndpointMap","httpEndpointMap"));
        if(!allowed.containsAll(m.keySet()))throw StudioException.bad("INVALID_DATASOURCE","未知的连接配置");
        if(m.containsKey("flinkHost")&&(!(m.get("flinkHost") instanceof String host)||host.isBlank()||host.length()>253||!host.matches("[a-zA-Z0-9._:-]+")))throw StudioException.bad("INVALID_DATASOURCE","Flink 执行主机无效");
        if(m.containsKey("flinkPort")&&(!(m.get("flinkPort") instanceof Number n)||n.doubleValue()!=n.intValue()||n.intValue()<1||n.intValue()>65535))throw StudioException.bad("INVALID_DATASOURCE","Flink 执行端口无效");
        for(String key:List.of("flinkFeHttpUrls","flinkBeHttpUrls"))if(m.containsKey(key)) {
            if(!(m.get(key) instanceof List<?> urls)||urls.isEmpty()||urls.size()>32)throw StudioException.bad("INVALID_DATASOURCE","Flink HTTP 地址列表无效");
            for(Object url:urls)endpoint(url,false);
        }
        if(!"DORIS".equals(type))return Collections.unmodifiableMap(m);
        m.putIfAbsent("feHttpUrls",List.of("http://127.0.0.1:8030"));m.putIfAbsent("beHttpUrls",List.of("http://127.0.0.1:8040"));m.putIfAbsent("flightUri","grpc://127.0.0.1:8070");
        m.putIfAbsent("flightEndpointMap",Map.of("grpc+tcp://172.30.41.2:8070","grpc://127.0.0.1:8070","grpc+tcp://172.30.41.3:8050","grpc://127.0.0.1:8050"));m.putIfAbsent("httpEndpointMap",Map.of());
        for(String key:List.of("feHttpUrls","beHttpUrls")) {
            if(!(m.get(key) instanceof List<?> urls)||urls.size()>32||key.equals("feHttpUrls")&&urls.isEmpty())throw StudioException.bad("INVALID_DATASOURCE","HTTP 地址列表无效");
            for(Object url:urls)endpoint(url,false);
        }
        endpoint(m.get("flightUri"),true);
        for(String key:List.of("flightEndpointMap","httpEndpointMap")) {
            if(!(m.get(key) instanceof Map<?,?> map)||map.size()>64)throw StudioException.bad("INVALID_DATASOURCE","地址映射无效");
            for(var entry:map.entrySet()){endpoint(entry.getKey(),key.startsWith("flight"));endpoint(entry.getValue(),key.startsWith("flight"));}
        }
        return Collections.unmodifiableMap(m);
    }
    private void endpoint(Object value,boolean flight) {
        try {var uri=java.net.URI.create((String)value);if(uri.getHost()==null||uri.getUserInfo()!=null||uri.getQuery()!=null||uri.getFragment()!=null||!(flight?Set.of("grpc","grpc+tcp"):Set.of("http")).contains(uri.getScheme())||!(uri.getPath()==null||uri.getPath().isEmpty()||uri.getPath().equals("/")))throw new IllegalArgumentException();}
        catch(Exception e){throw StudioException.bad("INVALID_DATASOURCE","本地 Doris 端点须为无账号和路径的 http/grpc 地址");}
    }
    private ConnectionSpec kafkaInput(ConnectionSpec old,Map<String,Object> input,String workspace,boolean pending) {
        String name=field(input,"name",old==null?"Kafka 数据源":old.name(),100);
        var previous=old==null?Map.<String,Object>of():old.options();
        String brokers=field(input,"bootstrapServers",Objects.toString(previous.get("bootstrapServers"),"localhost:19092"),4096);
        validateBrokers(brokers);
        String protocol=field(input,"securityProtocol",Objects.toString(previous.get("securityProtocol"),"PLAINTEXT"),32);
        String mechanism=field(input,"saslMechanism",Objects.toString(previous.get("saslMechanism"),"PLAIN"),32);
        if(!Set.of("PLAINTEXT","SASL_PLAINTEXT","SASL_SSL").contains(protocol)||!Set.of("PLAIN","SCRAM-SHA-256","SCRAM-SHA-512").contains(mechanism))throw StudioException.bad("INVALID_DATASOURCE","Kafka 认证协议无效");
        String username="PLAINTEXT".equals(protocol)?"":field(input,"username",old==null?"":old.username(),256);
        String encrypted=old==null?"":old.encryptedPassword();
        if("PLAINTEXT".equals(protocol))encrypted="";
        else if(input.get("password") instanceof String password&&!password.isEmpty()) {
            if(password.length()>4096)throw StudioException.bad("INVALID_DATASOURCE","Kafka 密码长度无效");
            encrypted=cipher.encrypt(password);
        }
        if(!"PLAINTEXT".equals(protocol)&&encrypted.isEmpty()&&!pending)throw StudioException.bad("PASSWORD_REQUIRED","Kafka SASL 认证需要密码");
        var options=new LinkedHashMap<String,Object>();options.put("bootstrapServers",brokers);options.put("securityProtocol",protocol);options.put("saslMechanism",mechanism);
        String runtime=Objects.toString(input.getOrDefault("flinkBootstrapServers",previous.getOrDefault("flinkBootstrapServers","")),"").trim();
        if(!runtime.isEmpty()){validateBrokers(runtime);options.put("flinkBootstrapServers",runtime);}
        return new ConnectionSpec(old==null?UUID.randomUUID().toString():old.id(),workspace,name,"",0,"",username,encrypted,"KAFKA",Collections.unmodifiableMap(options));
    }
    public static void validateBrokers(String brokers) {
        if(brokers==null||brokers.isBlank()||brokers.length()>4096)throw StudioException.bad("INVALID_DATASOURCE","Kafka Broker 地址不能为空");
        for(String broker:brokers.split(",",-1)) {
            String value=broker.trim();var match=java.util.regex.Pattern.compile("^(?:[a-zA-Z0-9._-]+|\\[[0-9a-fA-F:]+\\]):([0-9]{1,5})$").matcher(value);
            if(!match.matches()||Integer.parseInt(match.group(1))<1||Integer.parseInt(match.group(1))>65535)throw StudioException.bad("INVALID_DATASOURCE","Kafka Broker 使用 host:port 格式，多个地址以逗号分隔");
        }
    }
    private Properties kafkaProperties(ConnectionSpec s) {
        var properties=new Properties();properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,s.options().get("bootstrapServers").toString());
        properties.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,"5000");properties.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,"8000");
        properties.setProperty("security.protocol",s.options().get("securityProtocol").toString());
        if(!"PLAINTEXT".equals(s.options().get("securityProtocol"))) {
            if(s.encryptedPassword().isEmpty())throw StudioException.bad("PASSWORD_REQUIRED","请补齐 Kafka SASL 凭据");
            String mechanism=s.options().get("saslMechanism").toString();properties.setProperty("sasl.mechanism",mechanism);
            String module="PLAIN".equals(mechanism)?"org.apache.kafka.common.security.plain.PlainLoginModule":"org.apache.kafka.common.security.scram.ScramLoginModule";
            properties.setProperty("sasl.jaas.config",module+" required username=\""+jaas(s.username())+"\" password=\""+jaas(cipher.decrypt(s.encryptedPassword()))+"\";");
        }
        return properties;
    }
    private static String jaas(String text){return text.replace("\\","\\\\").replace("\"","\\\"");}
    AdminClient kafkaClient(ConnectionSpec spec){return AdminClient.create(kafkaProperties(spec));}
    public Map<String,Object> testKafka(ConnectionSpec s) {
        long started=System.nanoTime();
        AdminClient client=kafkaClient(s);
        try {
            var cluster=client.describeCluster();
            String clusterId=cluster.clusterId().get(8,TimeUnit.SECONDS);
            int brokers=cluster.nodes().get(8,TimeUnit.SECONDS).size();
            return Map.of("success",true,"message","Kafka 连接成功","version","Kafka","clusterId",Objects.toString(clusterId,""),"brokers",brokers,"elapsedMs",(System.nanoTime()-started)/1_000_000);
        } catch(InterruptedException e){Thread.currentThread().interrupt();throw StudioException.bad("KAFKA_UNAVAILABLE","Kafka 连接检查已中断");}
        catch(StudioException e){throw e;}catch(Exception e){throw StudioException.bad("KAFKA_UNAVAILABLE","无法访问 Kafka，请检查 Broker 通告地址、认证及网络");}
        finally {client.close(Duration.ofSeconds(1));}
    }
    public List<Map<String,Object>> topics(String id) {
        var s=get(id);if(!"KAFKA".equals(s.type()))throw StudioException.bad("DATASOURCE_TYPE_MISMATCH","Topic 浏览需要 Kafka 数据源");
        AdminClient client=kafkaClient(s);
        try {
            var names=client.listTopics().names().get(8,TimeUnit.SECONDS);
            var topics=client.describeTopics(names).allTopicNames().get(8,TimeUnit.SECONDS);
            return topics.values().stream().sorted(Comparator.comparing(org.apache.kafka.clients.admin.TopicDescription::name)).map(t->Map.<String,Object>of("name",t.name(),"partitions",t.partitions().size(),"internal",t.isInternal())).toList();
        } catch(InterruptedException e){Thread.currentThread().interrupt();throw StudioException.bad("KAFKA_UNAVAILABLE","Kafka Topic 浏览已中断");}
        catch(StudioException e){throw e;}catch(Exception e){throw StudioException.bad("KAFKA_UNAVAILABLE","Kafka Topic 元数据读取失败，请检查连接和权限");}
        finally {client.close(Duration.ofSeconds(1));}
    }
    public Map<String,Object> cdcMetadata(String id,String table) {
        var s=get(id);if(!"MYSQL".equals(s.type()))throw StudioException.bad("DATASOURCE_TYPE_MISMATCH","CDC 检查需要 MySQL 数据源");
        var tableMetadata=syncMetadata(s,table);var issues=new ArrayList<String>();var config=new LinkedHashMap<String,String>();
        try(Connection c=open(s,5)) {
            try(Statement statement=c.createStatement()) {
                statement.setQueryTimeout(5);
                try(ResultSet rows=statement.executeQuery("SHOW GLOBAL VARIABLES WHERE Variable_name IN ('log_bin','binlog_format','binlog_row_image','server_id','time_zone','system_time_zone')")) {
                    while(rows.next())config.put(rows.getString(1).toLowerCase(Locale.ROOT),rows.getString(2));
                }
            }
            issues.addAll(cdcVariableIssues(config));
            // SHOW CREATE TABLE can succeed with other table privileges; prove snapshot SELECT access.
            try(Statement statement=c.createStatement()) {
                statement.setQueryTimeout(5);
                try(ResultSet ignored=statement.executeQuery("SELECT * FROM "+quote(s.database())+"."+quote(table)+" LIMIT 0")) { /* No business rows are read. */ }
            }catch(SQLException e) {
                if(Set.of(1044,1142,1143).contains(e.getErrorCode()))issues.add("CDC 账号需要当前来源表全部字段的 SELECT 权限");else throw e;
            }
            String roles="";boolean rolesVerified=true;
            try(Statement statement=c.createStatement()) {
                statement.setQueryTimeout(5);
                try(ResultSet rows=statement.executeQuery("SELECT CURRENT_ROLE()")) {if(rows.next())roles=validatedCdcRoles(rows.getString(1));}
            }catch(IllegalArgumentException e) {rolesVerified=false;issues.add("无法安全展开当前激活角色，请检查角色名称格式");}
            catch(SQLException e) {if(e.getErrorCode()!=1305&&e.getErrorCode()!=1064)throw e;/* MySQL 5.7 has no roles. */}
            var grants=cdcGrants(c,"SHOW GRANTS FOR CURRENT_USER");
            if(!roles.isEmpty()) {
                try {grants=cdcGrants(c,"SHOW GRANTS FOR CURRENT_USER USING "+roles);}
                catch(SQLException e) {if(Set.of(1044,1142,1143,1396,3530).contains(e.getErrorCode())){rolesVerified=false;issues.add("无法确认当前激活角色的有效 CDC 权限，请检查角色授权");}else throw e;}
            }
            if(rolesVerified&&!cdcGlobalPrivileges(grants).containsAll(Set.of("REPLICATION SLAVE","REPLICATION CLIENT")))issues.add("CDC 账号需要有效的 REPLICATION SLAVE 与 REPLICATION CLIENT 全局权限");
        }catch(SQLException e){throw connectionError(e);}
        return Map.of("valid",issues.isEmpty(),"issues",issues,"variables",config,"table",tableMetadata);
    }
    private List<String> cdcGrants(Connection c,String query) throws SQLException {
        try(Statement statement=c.createStatement()) {statement.setQueryTimeout(5);try(ResultSet rows=statement.executeQuery(query)){var result=new ArrayList<String>();while(rows.next())result.add(rows.getString(1));return result;}}
    }
    static List<String> cdcVariableIssues(Map<String,String> config) {
        var issues=new ArrayList<String>();if(!"ON".equalsIgnoreCase(config.get("log_bin")))issues.add("MySQL 必须开启 log_bin");
        if(!"ROW".equalsIgnoreCase(config.get("binlog_format")))issues.add("binlog_format 必须为 ROW");
        if(!"FULL".equalsIgnoreCase(config.get("binlog_row_image")))issues.add("binlog_row_image 必须为 FULL");
        try{long id=Long.parseLong(config.getOrDefault("server_id","0"));if(id<1||id>4294967295L)issues.add("MySQL server_id 必须为大于 0 的有效整数");}catch(NumberFormatException e){issues.add("MySQL server_id 必须为大于 0 的有效整数");}return issues;
    }
    static Set<String> cdcGlobalPrivileges(List<String> grants) {
        var privileges=new HashSet<String>();var pattern=java.util.regex.Pattern.compile("(?is)^\\s*GRANT\\s+([A-Z_ ,\\s]+)\\s+ON\\s+\\*\\s*\\.\\s*\\*\\s+TO\\b.*$");
        for(String grant:grants){var match=pattern.matcher(grant);if(match.matches())for(String token:match.group(1).split(",")){String value=token.trim().replaceAll("\\s+"," ").toUpperCase(Locale.ROOT);if(value.equals("ALL PRIVILEGES"))privileges.addAll(Set.of("REPLICATION SLAVE","REPLICATION CLIENT"));else privileges.add(value);}}return privileges;
    }
    /** Only server-returned quoted role accounts can enter SHOW GRANTS USING; no user SQL is accepted. */
    static String validatedCdcRoles(String raw) {
        if(raw==null||raw.isBlank()||raw.equalsIgnoreCase("NONE"))return "";
        if(raw.length()>8192||raw.codePoints().anyMatch(Character::isISOControl))throw new IllegalArgumentException("Invalid active roles");
        String quoted="(?:`(?:[^`\\\\]|``)+`|'(?:[^'\\\\]|'')+')";var pattern=java.util.regex.Pattern.compile(quoted+"\\s*@\\s*"+quoted);var roles=new ArrayList<String>();int position=0;
        while(position<raw.length()){while(position<raw.length()&&Character.isWhitespace(raw.charAt(position)))position++;var match=pattern.matcher(raw);match.region(position,raw.length());if(!match.lookingAt())throw new IllegalArgumentException("Invalid active roles");roles.add(match.group());if(roles.size()>128)throw new IllegalArgumentException("Too many active roles");position=match.end();while(position<raw.length()&&Character.isWhitespace(raw.charAt(position)))position++;if(position==raw.length())break;if(raw.charAt(position++)!=','||position==raw.length())throw new IllegalArgumentException("Invalid active roles");}
        return String.join(",",roles);
    }
    @Transactional public void delete(String id) {
        var source=get(id);
        // Do not delete credentials still referenced by saved offline objects or releases.
        int references=jdbc.queryForObject("SELECT COUNT(*) FROM dw_object WHERE workspace_id=? AND (JSON_SEARCH(config_json,'one',?) IS NOT NULL OR LOCATE(?,content)>0)",Integer.class,source.workspaceId(),id,id);
        if(references>0)throw StudioException.conflict("DATASOURCE_IN_USE","数据源仍被离线对象引用");
        for(String table:List.of("dw_realtime_task","dw_realtime_draft","dw_realtime_release"))
            if(jdbc.queryForObject("SELECT COUNT(*) FROM "+table+" WHERE workspace_id=? AND LOCATE(?,CAST(data_json AS CHAR))>0",Integer.class,source.workspaceId(),id)>0)
                throw StudioException.conflict("DATASOURCE_IN_USE","数据源仍被实时任务、草稿或发布版本引用");
        jdbc.update("DELETE FROM dw_datasource WHERE id=?",id);
    }
    public static String quote(String name) {
        if(name==null||name.isBlank()||name.length()>128||name.codePoints().anyMatch(Character::isISOControl))throw StudioException.bad("INVALID_TABLE","表或字段名无效");
        return "`"+name.replace("`","``")+"`";
    }
    /** Bound schemas are checked against live metadata immediately before submitting a job. */
    public void validateRealtimeTable(Map<String,Object> binding,String workspace) {
        String connector=Objects.toString(binding.get("connector"),"");if(!Set.of("MYSQL_CDC","MYSQL_JDBC","DORIS").contains(connector))return;
        String code=connector.equals("MYSQL_CDC")?"CDC_SCHEMA_MISMATCH":"TARGET_SCHEMA_MISMATCH";
        String table=Objects.toString(binding.get("physicalTable"),"").trim();quote(table);
        var spec=forWorkspace(Objects.toString(binding.get("datasourceId"),""),workspace);
        if(!(connector.equals("DORIS")?"DORIS":"MYSQL").equals(spec.type()))throw StudioException.bad("REALTIME_CONNECTOR_MISMATCH","连接器与数据源类型不匹配");
        Map<String,Object> metadata;
        try{metadata=syncMetadata(spec,table);}catch(StudioException e){if(e.code().equals("TABLE_NOT_FOUND"))throw realtimeSchemaError(code,table,"物理表不存在或字段不可访问");throw e;}
        var columns=new LinkedHashMap<String,Map<?,?>>();Object rawColumns=metadata.get("columns");
        if(rawColumns instanceof List<?> list)for(Object value:list)if(value instanceof Map<?,?> column)columns.put(Objects.toString(column.get("name"),"").toLowerCase(Locale.ROOT),column);
        if(columns.isEmpty())throw realtimeSchemaError(code,table,"物理表没有可访问字段");
        if(!(binding.get("fields") instanceof List<?> fields)||fields.isEmpty())throw realtimeSchemaError(code,table,"至少配置一个字段");
        var keys=new LinkedHashSet<String>();var names=new HashSet<String>();
        for(Object value:fields){
            if(!(value instanceof Map<?,?> field))throw realtimeSchemaError(code,table,"字段配置无效");
            String name=Objects.toString(field.get("name"),"").trim(),normalized=name.toLowerCase(Locale.ROOT);var physical=columns.get(normalized);
            if(!names.add(normalized))throw realtimeSchemaError(code,table,"字段重复："+name);
            if(physical==null)throw realtimeSchemaError(code,table,"字段不存在："+name);
            String configured=Objects.toString(field.get("type"),""),actual=Objects.toString(physical.get("type"),"");
            String configuredFamily=realtimeTypeFamily(configured,false),actualFamily=realtimeTypeFamily(actual,true);
            if(configuredFamily.isEmpty()||!configuredFamily.equals(actualFamily))throw realtimeSchemaError(code,table,"字段 "+name+" 类型不兼容：配置 "+configured+"，实际 "+actual);
            boolean source=connector.equals("MYSQL_CDC");String from=source?actual:configured,to=source?configured:actual;
            if(configuredFamily.equals("INTEGER")&&!realtimeIntegerFits(from,to))throw realtimeSchemaError(code,table,"字段 "+name+" 整数范围会丢失：配置 "+configured+"，实际 "+actual);
            if(configuredFamily.equals("FLOATING")&&realtimeFloatingBits(from)>realtimeFloatingBits(to))throw realtimeSchemaError(code,table,"字段 "+name+" 浮点精度会丢失：配置 "+configured+"，实际 "+actual);
            if(configuredFamily.equals("DECIMAL")&&!realtimeDecimalShape(configured).equals(realtimeDecimalShape(actual)))throw realtimeSchemaError(code,table,"字段 "+name+" DECIMAL 精度与小数位须一致：配置 "+configured+"，实际 "+actual);
            if(Set.of("TIME","TIMESTAMP").contains(configuredFamily)){
                int configuredPrecision=realtimeTemporalPrecision(configured),actualPrecision=realtimeTemporalPrecision(actual);
                if(connector.equals("MYSQL_CDC")?configuredPrecision<actualPrecision:configuredPrecision>actualPrecision)throw realtimeSchemaError(code,table,"字段 "+name+" 时间精度会丢失：配置 "+configured+"，实际 "+actual);
            }
            if(!connector.equals("MYSQL_CDC")&&"NO".equalsIgnoreCase(Objects.toString(physical.get("nullable"),""))&&!Boolean.FALSE.equals(field.get("nullable")))throw realtimeSchemaError(code,table,"目标字段 "+name+" 为 NOT NULL，绑定字段不能允许空值");
            if(Boolean.TRUE.equals(field.get("primaryKey")))keys.add(normalized);
        }
        if(connector.equals("MYSQL_CDC")){
            var actualKeys=new LinkedHashSet<String>();columns.forEach((name,column)->{if("PRI".equalsIgnoreCase(Objects.toString(column.get("columnKey"),"")))actualKeys.add(name);});
            if(keys.isEmpty()||!keys.equals(actualKeys))throw realtimeSchemaError(code,table,"CDC 主键须与物理表主键一致；配置 "+keys+"，实际 "+actualKeys);
        }else if(connector.equals("MYSQL_JDBC")&&"upsert".equals(binding.get("writeMode"))){
            var uniqueKeys=new ArrayList<Set<String>>();if(metadata.get("uniqueKeys") instanceof List<?> indexes)for(Object index:indexes)if(index instanceof List<?> fieldsInIndex){var key=new LinkedHashSet<String>();for(Object field:fieldsInIndex)if(field instanceof String name)key.add(name.toLowerCase(Locale.ROOT));if(key.size()==fieldsInIndex.size()&&!key.isEmpty())uniqueKeys.add(key);}
            if(keys.isEmpty()||!uniqueKeys.contains(keys))throw realtimeSchemaError(code,table,"JDBC Upsert 主键须完整匹配物理表主键或唯一键；配置 "+keys+"，实际 "+uniqueKeys);
        }else if(connector.equals("DORIS")){
            var schema=realtimeDorisSchema(spec,table);String configuredModel=Objects.toString(binding.get("dorisModel"),"");String actualModel=schema.model().startsWith("UNIQUE_")?"UNIQUE":schema.model();
            if(!Set.of("UNIQUE","DUPLICATE","AGGREGATE").contains(configuredModel)||!configuredModel.equals(actualModel))throw realtimeSchemaError(code,table,"Doris 表模型不一致：配置 "+configuredModel+"，实际 "+schema.model());
            if(actualModel.equals("UNIQUE")&&!keys.equals(schema.keys()))throw realtimeSchemaError(code,table,"Doris Unique 主键须与物理表 Unique Key 一致；配置 "+keys+"，实际 "+schema.keys());
            if(Boolean.TRUE.equals(binding.get("syncDeletes"))&&!schema.model().equals("UNIQUE_MOW"))throw realtimeSchemaError(code,table,"Doris 同步删除要求实际目标为 Unique Key Merge-on-Write 表");
        }
    }
    private static StudioException realtimeSchemaError(String code,String table,String detail){return StudioException.bad(code,"实时绑定表 "+table+"："+detail);}
    private static java.math.BigInteger[] realtimeIntegerRange(String type){
        String value=type.trim().toUpperCase(Locale.ROOT);int bits=value.startsWith("TINYINT")?8:value.startsWith("SMALLINT")?16:value.startsWith("MEDIUMINT")?24:value.startsWith("BIGINT")?64:32;boolean unsigned=value.matches(".*\\bUNSIGNED\\b.*");
        var extent=java.math.BigInteger.ONE.shiftLeft(unsigned?bits:bits-1);return new java.math.BigInteger[]{unsigned?java.math.BigInteger.ZERO:extent.negate(),extent.subtract(java.math.BigInteger.ONE)};
    }
    private static boolean realtimeIntegerFits(String from,String to){var input=realtimeIntegerRange(from);var output=realtimeIntegerRange(to);return input[0].compareTo(output[0])>=0&&input[1].compareTo(output[1])<=0;}
    private static int realtimeFloatingBits(String type){return type.trim().toUpperCase(Locale.ROOT).startsWith("FLOAT")?32:64;}
    private static String realtimeDecimalShape(String type){
        if(type.trim().toUpperCase(Locale.ROOT).matches("BIGINT(?:\\(\\d+\\))?\\s+UNSIGNED(?:\\s+ZEROFILL)?"))return "20,0";
        var match=java.util.regex.Pattern.compile("(?i)(?:DECIMAL(?:V3|32|64|128|256)?|NUMERIC)\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)").matcher(type.trim());if(match.find())return match.group(1).replaceFirst("^0+(?!$)","")+","+match.group(2).replaceFirst("^0+(?!$)","");return "unsupported:"+type;
    }
    private static int realtimeTemporalPrecision(String type){var match=java.util.regex.Pattern.compile("\\(\\s*(\\d+)\\s*\\)").matcher(type);try{return match.find()?Integer.parseInt(match.group(1)):0;}catch(NumberFormatException e){return Integer.MAX_VALUE;}}
    /** Matches the UI MySQL mapping while accepting equivalent Flink character/binary and temporal families. */
    static String realtimeTypeFamily(String type,boolean physical){
        String value=type.trim().toUpperCase(Locale.ROOT).replaceAll("\\s+"," ");boolean unsigned=value.matches(".*\\bUNSIGNED\\b.*");value=value.replaceAll("\\s+(?:UNSIGNED|ZEROFILL)\\b","").replaceAll("\\s","");
        if(value.matches("BOOLEAN|BOOL")||physical&&value.equals("TINYINT(1)"))return "BOOLEAN";
        if(value.matches("(?:TINYINT|SMALLINT|MEDIUMINT|INT|INTEGER|BIGINT)(?:\\(\\d+\\))?"))return unsigned&&value.startsWith("BIGINT")?"DECIMAL":"INTEGER";
        if(value.matches("(?:DECIMAL(?:V3|32|64|128|256)?|NUMERIC)(?:\\(\\d+,\\d+\\))?"))return "DECIMAL";
        if(value.matches("(?:FLOAT|DOUBLE|REAL)(?:\\([^)]*\\))?"))return "FLOATING";
        if(value.matches("STRING|(?:CHAR|VARCHAR)\\(\\d+\\)|(?:TINY|MEDIUM|LONG)?TEXT|JSONB?|ENUM\\(.*\\)|SET\\(.*\\)"))return "STRING";
        if(value.matches("BYTES|(?:BINARY|VARBINARY)\\(\\d+\\)|(?:TINY|MEDIUM|LONG)?BLOB"))return "BYTES";
        if(value.matches("DATE(?:V2)?"))return "DATE";
        if(value.matches("TIME(?:\\(\\d+\\))?"))return "TIME";
        if(value.matches("(?:DATETIME(?:V2)?|TIMESTAMP(?:_LTZ)?)(?:\\(\\d+\\))?"))return "TIMESTAMP";
        return "";
    }
    private record RealtimeDorisSchema(String model,Set<String> keys){}
    private RealtimeDorisSchema realtimeDorisSchema(ConnectionSpec spec,String table){
        String ddl;try(Connection connection=open(spec,10);Statement statement=connection.createStatement()){
            statement.setQueryTimeout(10);try(ResultSet rows=statement.executeQuery("SHOW CREATE TABLE "+quote(spec.database())+"."+quote(table))){if(!rows.next())throw realtimeSchemaError("TARGET_SCHEMA_MISMATCH",table,"物理表不存在");ddl=rows.getString(2);}
        }catch(SQLException e){throw connectionError(e);}
        // Skip column definitions so a quoted COMMENT cannot impersonate a table model.
        String tail=realtimeCreateTableTail(ddl);var model=java.util.regex.Pattern.compile("(?is)\\b(UNIQUE|DUPLICATE|AGGREGATE)\\s+KEY\\s*\\(((?:[^`)]|`(?:[^`]|``)*`)*)\\)").matcher(tail);
        if(!model.find())throw realtimeSchemaError("TARGET_SCHEMA_MISMATCH",table,"无法识别 Doris 物理表模型");
        var keys=new LinkedHashSet<String>();var keyPattern=java.util.regex.Pattern.compile("\\G\\s*(?:`((?:[^`]|``)+)`|([a-zA-Z_][a-zA-Z0-9_]*))\\s*(?:,|$)");String keyText=model.group(2);int position=0;
        while(position<keyText.length()){var key=keyPattern.matcher(keyText);key.region(position,keyText.length());if(!key.lookingAt())throw realtimeSchemaError("TARGET_SCHEMA_MISMATCH",table,"无法识别 Doris Key 列");keys.add((key.group(1)==null?key.group(2):key.group(1).replace("``","`")).toLowerCase(Locale.ROOT));position=key.end();}
        String kind=model.group(1).toUpperCase(Locale.ROOT);if(kind.equals("UNIQUE"))kind=java.util.regex.Pattern.compile("(?is)[\"']ENABLE_UNIQUE_KEY_MERGE_ON_WRITE[\"']\\s*=\\s*[\"']TRUE[\"']").matcher(tail).find()?"UNIQUE_MOW":"UNIQUE_MOR";
        return new RealtimeDorisSchema(kind,Collections.unmodifiableSet(keys));
    }
    private static String realtimeCreateTableTail(String ddl){
        int depth=0;boolean started=false;char quoted=0;
        for(int i=0;i<ddl.length();i++){char ch=ddl.charAt(i);if(quoted!=0){if(ch=='\\'&&quoted!='`'){i++;continue;}if(ch==quoted){if(i+1<ddl.length()&&ddl.charAt(i+1)==quoted)i++;else quoted=0;}continue;}if(ch=='`'||ch=='\''||ch=='\"'){quoted=ch;continue;}if(ch=='('){depth++;started=true;}else if(ch==')'&&--depth==0&&started)return ddl.substring(i+1);}
        return "";
    }
    public Map<String,Object> syncMetadata(String id,String table) {
        return syncMetadata(get(id),table);
    }
    public Map<String,Object> syncMetadata(ConnectionSpec s,String table) {
        var columns=columns(s,table);if(columns.isEmpty())throw StudioException.bad("TABLE_NOT_FOUND","表不存在或没有可访问字段");
        String ddl;
        try(Connection c=open(s,10);Statement stmt=c.createStatement();ResultSet rs=stmt.executeQuery("SHOW CREATE TABLE "+quote(s.database())+"."+quote(table))) {if(!rs.next())throw StudioException.bad("TABLE_NOT_FOUND","表不存在");ddl=rs.getString(2);}
        catch(SQLException e){throw connectionError(e);}
        String upper=ddl.toUpperCase(Locale.ROOT);String model="MYSQL".equals(s.type())?(upper.contains("ENGINE=INNODB")?"INNODB":"UNSUPPORTED"):upper.contains("DUPLICATE KEY")?"DUPLICATE":upper.contains("UNIQUE KEY")&&upper.matches("(?s).*ENABLE_UNIQUE_KEY_MERGE_ON_WRITE[^=]*=\\s*\"TRUE\".*")?"UNIQUE_MOW":"UNSUPPORTED";
        List<List<String>> keys=new ArrayList<>();
        if("MYSQL".equals(s.type())) {
            var rows=metadata(s,"SELECT INDEX_NAME AS indexName,COLUMN_NAME AS columnName FROM information_schema.statistics WHERE table_schema=? AND table_name=? AND NON_UNIQUE=0 ORDER BY INDEX_NAME,SEQ_IN_INDEX",s.database(),table);
            var grouped=new LinkedHashMap<String,List<String>>();for(var row:rows)grouped.computeIfAbsent(row.get("indexName").toString(),k->new ArrayList<>()).add(row.get("columnName").toString());keys.addAll(grouped.values());
        }
        var partition=SyncPartitionMetadata.none();
        if("DORIS".equals(s.type())) {
            partition=SyncPartitionMetadata.parse(ddl,columns,List.of());
            if(partition.partitioned())partition=SyncPartitionMetadata.parse(ddl,columns,metadata(s,"SHOW PARTITIONS FROM "+quote(s.database())+"."+quote(table)));
        }
        return Map.of("columns",columns,"model",model,"uniqueKeys",keys,"partition",partition.view());
    }
}
