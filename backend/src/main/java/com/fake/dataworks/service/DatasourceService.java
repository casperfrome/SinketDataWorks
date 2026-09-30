package com.fake.dataworks.service;

import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.config.JsonCodec;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DatasourceService {
    public record ConnectionSpec(String id,String workspaceId,String name,String host,int port,String database,String username,String encryptedPassword,String type,Map<String,Object> options) {
        @Override public String toString() { return "ConnectionSpec[id="+id+"]"; }
        public Map<String,Object> publicView() {
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
        ConnectionSpec old=id==null?null:get(id);
        String workspace=field(input,"workspaceId",old==null?"local-workspace":old.workspaceId(),64);
        objects.workspace(workspace);
        if(old!=null&&!workspace.equals(old.workspaceId())) throw StudioException.bad("WORKSPACE_MISMATCH","不能跨空间移动数据源");
        String type=field(input,"type",old==null?"MYSQL":old.type(),16);
        if(!Set.of("MYSQL","DORIS").contains(type))throw StudioException.bad("INVALID_DATASOURCE","不支持的数据源类型");
        if(old!=null&&!type.equals(old.type()))throw StudioException.bad("INVALID_DATASOURCE","已有数据源不能更换数据库类型，请新增连接");
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
        if(id==null) jdbc.update("INSERT INTO dw_datasource(id,workspace_id,name,host,port,database_name,username,password_cipher,updated_at,type,options_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)",s.id(),s.workspaceId(),s.name(),s.host(),s.port(),s.database(),s.username(),s.encryptedPassword(),ObjectService.now(),s.type(),json.write(s.options()));
        else jdbc.update("UPDATE dw_datasource SET name=?,host=?,port=?,database_name=?,username=?,password_cipher=?,updated_at=?,type=?,options_json=? WHERE id=?",s.name(),s.host(),s.port(),s.database(),s.username(),s.encryptedPassword(),ObjectService.now(),s.type(),json.write(s.options()),id);
        return s.publicView();
    }
    public Connection open(ConnectionSpec s,int timeoutSeconds) throws SQLException {
        return open(s,timeoutSeconds,false);
    }
    public Connection open(ConnectionSpec s,int timeoutSeconds,boolean writable) throws SQLException {
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

    public Map<String,Object> credentials(ConnectionSpec s) {return Map.of("username",s.username(),"password",cipher.decrypt(s.encryptedPassword()));}
    @SuppressWarnings("unchecked") private Map<String,Object> normalizeOptions(String type,Object raw) {
        if(!"DORIS".equals(type))return Map.of();
        if(!(raw instanceof Map<?,?>))throw StudioException.bad("INVALID_DATASOURCE","Doris 配置格式无效");
        var m=new LinkedHashMap<String,Object>((Map<String,Object>)raw);
        if(!Set.of("feHttpUrls","beHttpUrls","flightUri","flightEndpointMap","httpEndpointMap").containsAll(m.keySet()))throw StudioException.bad("INVALID_DATASOURCE","未知的 Doris 连接配置");
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
    public static String quote(String name) {
        if(name==null||name.isBlank()||name.length()>128||name.codePoints().anyMatch(Character::isISOControl))throw StudioException.bad("INVALID_TABLE","表或字段名无效");
        return "`"+name.replace("`","``")+"`";
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
