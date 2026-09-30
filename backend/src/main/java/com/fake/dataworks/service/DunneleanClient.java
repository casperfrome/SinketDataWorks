package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.exception.StudioException;
import io.github.casperfrome.dunnelean.*;
import jakarta.annotation.PreDestroy;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** SDK control adapter. Request credentials and raw SDK exception bodies are never logged. */
@Component
public class DunneleanClient {
    private static final Duration CONNECT_TIMEOUT=Duration.ofSeconds(5);
    private static final Duration CONTROL_TIMEOUT=Duration.ofSeconds(15);
    private static final Duration VALIDATE_TIMEOUT=Duration.ofSeconds(65);
    private final HttpClient http=HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build();
    private final JsonCodec json;
    // Also redact later polling/batch diagnostics, whose requests contain no credentials.
    private final Map<String,Set<String>> passwords=new ConcurrentHashMap<>();
    public final String baseUrl;
    public DunneleanClient(JsonCodec json,@Value("${studio.sync.service-url:http://127.0.0.1:9876}") String url) {
        this.json=json;this.baseUrl=normalizeBase(url);
    }

    private static String normalizeBase(String url) {
        String base=url.replaceAll("/+$","");
        var uri=URI.create(base);
        if(!"http".equals(uri.getScheme())||!Set.of("127.0.0.1","localhost","[::1]").contains(uri.getHost())
                ||uri.getUserInfo()!=null||uri.getQuery()!=null||uri.getFragment()!=null)
            throw new IllegalArgumentException("Dunnelean must listen on loopback");
        return base;
    }

    private io.github.casperfrome.dunnelean.DunneleanClient sdk(String base,String stateId,Duration timeout) {
        return io.github.casperfrome.dunnelean.DunneleanClient.builder().baseUrl(normalizeBase(base))
                .httpClient(http).connectTimeout(CONNECT_TIMEOUT).requestTimeout(timeout).stateStoreId(stateId).build();
    }

    public Map<String,Object> health(String base) {
        return invoke(base,()->{
            Health health;
            try {health=sdk(base,null,CONTROL_TIMEOUT).health();}
            catch(DunneleanProtocolException e) {
                if(legacyHealth(e.responseBody()))throw upgradeRequired();
                throw e;
            }
            if(health.stateStoreId().isBlank())throw upgradeRequired();
            return Map.of("status",health.status(),"service",health.service(),"version",health.version(),"state_store_id",health.stateStoreId());
        });
    }

    private boolean legacyHealth(String body) {
        try {
            var health=json.map(body);
            return health!=null&&health.get("status") instanceof String&&health.get("service") instanceof String
                    &&health.get("version") instanceof String&&health.get("state_store_id")==null;
        }catch(RuntimeException e){return false;}
    }

    private StudioException upgradeRequired() {
        return StudioException.bad("SYNC_UPGRADE_REQUIRED","请更新 Dunnelean 至支持可靠取消的版本");
    }

    public void validate(RunSpec spec) {
        remember(baseUrl,spec);
        invoke(baseUrl,()->sdk(baseUrl,null,VALIDATE_TIMEOUT).validate(spec));
    }

    public Map<String,Object> submit(String base,RunSpec spec,String stateId) {
        remember(base,spec);
        return invoke(base,()->run(base,sdk(base,stateId,CONTROL_TIMEOUT).submit(spec)));
    }

    public Map<String,Object> getRequest(String base,String requestId,String stateId) {
        return invoke(base,()->request(base,sdk(base,stateId,CONTROL_TIMEOUT).getRequest(requestId)));
    }

    public Map<String,Object> cancelRequest(String base,String requestId,String stateId) {
        return invoke(base,()->request(base,sdk(base,stateId,CONTROL_TIMEOUT).cancelRequest(requestId)));
    }

    public Map<String,Object> listBatches(String base,String runId,String stateId) {
        return invoke(base,()->Map.of("batches",sdk(base,stateId,CONTROL_TIMEOUT).listBatches(runId).batches().stream().map(batch->{
            var value=new LinkedHashMap<String,Object>();
            value.put("batch_id",batch.batchId());value.put("state",batch.state());value.put("rows",batch.rows());value.put("bytes",batch.bytes());
            value.put("label",batch.label());value.put("detail",redact(base,batch.detail()));return value;
        }).toList()));
    }

    private Map<String,Object> request(String base,RequestStatus status) {
        var value=new LinkedHashMap<String,Object>();
        value.put("request_id",status.requestId());value.put("cancel_requested",status.cancelRequested());
        value.put("run",status.run()==null?null:run(base,status.run()));return value;
    }

    private Map<String,Object> run(String base,Run result) {
        var value=new LinkedHashMap<String,Object>();
        value.put("run_id",result.runId());value.put("request_id",result.requestId());value.put("state",result.state());value.put("stage",result.stage());
        value.put("created_at",result.createdAt().toString());value.put("updated_at",result.updatedAt().toString());
        value.put("rows_read",result.rowsRead());value.put("bytes_read",result.bytesRead());value.put("rows_submitted",result.rowsSubmitted());
        value.put("rows_committed",result.rowsCommitted());value.put("rows_filtered",result.rowsFiltered());value.put("server_affected_rows",result.serverAffectedRows());
        value.put("batches_committed",result.batchesCommitted());value.put("partial_write",result.partialWrite());value.put("commit_unknown",result.commitUnknown());
        value.put("error",result.error()==null?null:Map.of("code",result.error().code(),"message",redact(base,result.error().message()),
                "commit_unknown",result.error().commitUnknown(),"retryable",result.error().retryable()));
        // The SDK's config is a redacted diagnostic echo, never a submission or public snapshot.
        return value;
    }

    @FunctionalInterface private interface SdkCall<T> { T call() throws InterruptedException; }

    private <T> T invoke(String base,SdkCall<T> call) {
        normalizeBase(base);
        try {return call.call();}
        catch(DunneleanApiException e) {
            String code=e.error()==null?"REMOTE_ERROR":e.error().code();
            String message=e.error()==null?"同步引擎请求失败":redact(base,e.error().message());
            throw new StudioException("SYNC_"+code,message,e.statusCode());
        }catch(InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StudioException("SYNC_UNAVAILABLE","同步引擎请求中断，正在核实远端状态",502);
        }catch(DunneleanException e) {
            throw new StudioException("SYNC_UNAVAILABLE","无法确认同步引擎响应，正在核实远端状态",502);
        }
    }

    private void remember(String base,RunSpec spec) {
        String normalized=normalizeBase(base);
        Credentials source=switch(spec.reader()) {
            case MysqlReader reader -> reader.connection().credentials();
            case DorisReader reader -> reader.credentials();
        };
        Credentials target=switch(spec.writer()) {
            case MysqlWriter writer -> writer.connection().credentials();
            case DorisWriter writer -> writer.sql().credentials();
        };
        var values=passwords.computeIfAbsent(normalized,k->ConcurrentHashMap.newKeySet());
        for(var credentials:List.of(source,target))if(credentials.password()!=null&&!credentials.password().isEmpty())values.add(credentials.password());
    }

    private String redact(String base,String message) {
        if(message==null)return null;
        var secrets=new ArrayList<>(passwords.getOrDefault(normalizeBase(base),Set.of()));
        secrets.sort(Comparator.comparingInt(String::length).reversed());
        for(String password:secrets)message=message.replace(password,"[REDACTED]");
        return message;
    }

    @PreDestroy public void close() {passwords.clear();http.shutdownNow();}
}
