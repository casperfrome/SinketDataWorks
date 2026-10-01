package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.exception.StudioException;
import java.io.ByteArrayOutputStream;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Bounded SQL Gateway and JobManager transport; secrets never appear in public diagnostics. */
@Component
public class FlinkGatewayClient {
    private final JsonCodec json;
    private final HttpClient http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).followRedirects(HttpClient.Redirect.NEVER).build();
    private final URI gateway;
    private final URI flink;
    private final int operationTimeoutSeconds;
    private final Duration requestTimeout;
    private final Set<String> secrets=ConcurrentHashMap.newKeySet();
    private static final Set<String> TERMINAL=Set.of("FINISHED","FAILED","CANCELED");
    private static final Pattern JOB=Pattern.compile("[a-f0-9]{32}");
    private static final Pattern HANDLE=Pattern.compile("[a-zA-Z0-9-]{1,80}");
    public static final String STATE_ROOT="file:///opt/flink/state/";

    @Autowired
    public FlinkGatewayClient(JsonCodec json,@Value("${studio.flink.gateway-url:http://127.0.0.1:8083}") String gateway,
            @Value("${studio.flink.jobmanager-url:http://127.0.0.1:8081}") String flink,
            @Value("${studio.flink.operation-timeout-seconds:90}") int timeout){
        this(json,gateway,flink,timeout,Duration.ofSeconds(20));
    }
    public FlinkGatewayClient(JsonCodec json,String gateway,String flink){this(json,gateway,flink,90);}
    FlinkGatewayClient(JsonCodec json,String gateway,String flink,int timeout,Duration requestTimeout){
        if(requestTimeout.isNegative()||requestTimeout.isZero()||requestTimeout.toMillis()<1||requestTimeout.compareTo(Duration.ofSeconds(20))>0)throw new IllegalArgumentException("Invalid request deadline");
        this.json=json;this.gateway=base(gateway);this.flink=base(flink);this.operationTimeoutSeconds=Math.max(1,Math.min(timeout,300));this.requestTimeout=requestTimeout;
    }
    private static URI base(String input){var uri=URI.create(input.replaceAll("/+$","")+"/");if(!Set.of("http","https").contains(uri.getScheme())||uri.getHost()==null||uri.getUserInfo()!=null||uri.getQuery()!=null||uri.getFragment()!=null)throw new IllegalArgumentException("Invalid Flink endpoint");return uri;}
    public void rememberSecrets(Collection<String> values){values.stream().filter(v->v!=null&&!v.isEmpty()).forEach(v->{secrets.add(v);String encoded=json.write(v);if(encoded.length()>2)secrets.add(encoded.substring(1,encoded.length()-1));});}
    public String redact(String text){return RealtimeSqlCompiler.redact(text,secrets);}

    public Map<String,Object> health(){var overview=request(flink,"GET","overview",null);var info=request(gateway,"GET","v1/info",null);return Map.of("available",true,"cluster",overview,"gateway",info);}
    public String openSession(Map<String,String> properties){var response=request(gateway,"POST","v1/sessions",Map.of("sessionName","sinket-realtime-"+UUID.randomUUID(),"properties",properties));if(!(response.get("sessionHandle") instanceof String session)||!HANDLE.matcher(session).matches())throw remoteProtocol("SQL Gateway 未返回有效会话 ID");return session;}
    public String statement(String session,String sql){var response=request(gateway,"POST",sessionPath(session)+"/statements",Map.of("statement",sql,"executionTimeout",0));if(!(response.get("operationHandle") instanceof String operation)||!HANDLE.matcher(operation).matches())throw remoteProtocol("SQL Gateway 未返回有效操作 ID，提交结果需先对账");return operation;}
    public String status(String session,String operation){return Objects.toString(request(gateway,"GET",operationPath(session,operation)+"/status",null).get("status"),"");}
    public Map<String,Object> result(String session,String operation,long token){if(token<0)throw protocol("结果页 token 无效");return request(gateway,"GET",operationPath(session,operation)+"/result/"+token+"?rowFormat=JSON",null);}
    public Map<String,Object> result(String session,String operation,String nextResultUri){
        String expected="/"+operationPath(session,operation)+"/result/";String path=nextResultUri.startsWith("/v1/")?nextResultUri:nextResultUri.startsWith("/sessions/")?"/v1"+nextResultUri:nextResultUri;
        if(!path.startsWith(expected)||path.contains("..")||path.contains("%")||!path.matches(Pattern.quote(expected)+"\\d+(?:\\?rowFormat=(?:JSON|PLAIN_TEXT))?"))throw protocol("SQL Gateway 返回非法分页路径");
        return request(gateway,"GET",path.substring(1),null);
    }
    public void heartbeat(String session){request(gateway,"POST",sessionPath(session)+"/heartbeat",Map.of());}
    public void cancelOperation(String session,String operation){request(gateway,"POST",operationPath(session,operation)+"/cancel",Map.of());}
    public void closeOperation(String session,String operation){ignoreMissing(()->send(gateway,"DELETE",operationPath(session,operation)+"/close",null,false,true));}
    public void closeSession(String session){ignoreMissing(()->send(gateway,"DELETE",sessionPath(session),null,false,true));}

    /** Returns the first payload, including jobID; operation FINISHED is not Flink job completion. */
    public Map<String,Object> execute(String session,String sql){String operation=statement(session,sql);long deadline=System.nanoTime()+Duration.ofSeconds(operationTimeoutSeconds).toNanos();
        while(System.nanoTime()<deadline){String state=status(session,operation);if(Set.of("ERROR","CANCELED","CLOSED").contains(state)){
                if(state.equals("ERROR"))result(session,operation,0);throw remoteProtocol("SQL Gateway 操作结束于 "+state);}
            if(state.equals("FINISHED")||state.equals("RUNNING")){var result=result(session,operation,0);if(!Objects.equals(result.get("resultType"),"NOT_READY")){var value=new LinkedHashMap<>(result);value.put("operationHandle",operation);return value;}}
            pause(200);
        }
        throw new StudioException("FLINK_OPERATION_TIMEOUT","Flink SQL 操作等待超时，操作结果需先对账，不能重复提交",504);
    }

    public Map<String,Object> job(String id){return request(flink,"GET",jobPath(id),null);}
    public Map<String,Object> jobsOverview(){return request(flink,"GET","jobs/overview",null);}
    public Map<String,Object> findJobByName(String name){var found=list(jobsOverview().get("jobs")).stream().map(FlinkGatewayClient::map).filter(j->Objects.equals(j.get("name"),name)).toList();if(found.size()>1)throw StudioException.conflict("FLINK_JOB_AMBIGUOUS","作业身份出现重复，请人工核对");return found.isEmpty()?Map.of():found.getFirst();}
    public Map<String,Object> checkpoints(String id){return request(flink,"GET",jobPath(id)+"/checkpoints",null);}
    public Map<String,Object> checkpointConfig(String id){return request(flink,"GET",jobPath(id)+"/checkpoints/config",null);}
    public Map<String,Object> configuration(String id){return request(flink,"GET",jobPath(id)+"/config",null);}
    public Map<String,Object> exceptions(String id){return sanitized(request(flink,"GET",jobPath(id)+"/exceptions",null));}
    public Map<String,Object> plan(String id){return sanitized(request(flink,"GET",jobPath(id)+"/plan",null));}
    public void cancel(String id){var current=job(id);if(!TERMINAL.contains(Objects.toString(current.get("state"),"")))request(flink,"PATCH",jobPath(id)+"?mode=cancel",null);}
    public boolean terminal(String id){return TERMINAL.contains(Objects.toString(job(id).get("state"),""));}
    public boolean awaitTerminal(String id,int seconds){long end=System.nanoTime()+Duration.ofSeconds(seconds).toNanos();while(System.nanoTime()<end){try{if(terminal(id))return true;}catch(StudioException e){if(e.status()==404)return true;throw e;}pause(250);}return false;}

    public String stopSavepoint(String id){return stopSavepoint(id,UUID.randomUUID().toString().replace("-",""));}
    public String stopSavepoint(String id,String trigger){checkJob(trigger);return trigger(request(flink,"POST",jobPath(id)+"/stop",Map.of("targetDirectory",STATE_ROOT+"savepoints","drain",false,"formatType","CANONICAL","triggerId",trigger)));}
    public String triggerSavepoint(String id){return triggerSavepoint(id,UUID.randomUUID().toString().replace("-",""));}
    public String triggerSavepoint(String id,String trigger){checkJob(trigger);return trigger(request(flink,"POST",jobPath(id)+"/savepoints",Map.of("target-directory",STATE_ROOT+"savepoints","cancel-job",false,"formatType","CANONICAL","triggerId",trigger)));}
    public Map<String,Object> savepointStatus(String id,String trigger){checkJob(trigger);var result=request(flink,"GET",jobPath(id)+"/savepoints/"+trigger,null);var operation=map(result.get("operation"));if(operation.get("location") instanceof String location)validateStatePath(location);return sanitized(result);}
    private static String trigger(Map<String,Object> result){if(!(result.get("request-id") instanceof String id)||!JOB.matcher(id).matches())throw remoteProtocol("Flink 未返回有效保存点触发 ID，操作结果需先对账");return id;}
    public static String validateStatePath(String path){
        try{var uri=URI.create(path);String raw=uri.getRawPath();if(!"file".equals(uri.getScheme())||uri.getAuthority()!=null&& !uri.getAuthority().isEmpty()||uri.getQuery()!=null||uri.getFragment()!=null||raw==null||raw.contains("%")||raw.contains("\\")||raw.contains("//")||!uri.normalize().equals(uri)||!raw.startsWith("/opt/flink/state/savepoints/")&&!raw.startsWith("/opt/flink/state/checkpoints/")||raw.equals("/opt/flink/state/savepoints/")||raw.equals("/opt/flink/state/checkpoints/"))throw new IllegalArgumentException();return uri.toString();}
        catch(Exception e){throw StudioException.bad("INVALID_FLINK_STATE_PATH","恢复路径必须是当前集群状态卷内的 Checkpoint 或 Savepoint");}
    }

    public Map<String,Object> metrics(String id){
        var detail=job(id);var result=new LinkedHashMap<String,Object>();result.put("job",metricValues(jobPath(id)+"/metrics"));var vertices=new ArrayList<Map<String,Object>>();double input=0,output=0;boolean hasInput=false,hasOutput=false;long watermark=Long.MAX_VALUE;
        for(Object vertex:list(detail.get("vertices"))){var v=map(vertex);String vertexId=Objects.toString(v.get("id"),"");if(!JOB.matcher(vertexId).matches())continue;var values=metricValues(jobPath(id)+"/vertices/"+vertexId+"/metrics");vertices.add(Map.of("id",vertexId,"name",Objects.toString(v.get("name"),""),"metrics",values));
            for(var metric:values.entrySet()){String name=metric.getKey();Double number=metricNumber(metric.getValue());if(number==null)continue;
                if(name.contains(".Source__")&&name.endsWith(".numRecordsOutPerSecond")){input+=number;hasInput=true;}
                if((name.contains(".Sink__")||name.contains("__Writer."))&&name.endsWith(".numRecordsInPerSecond")){output+=number;hasOutput=true;}
                if(name.endsWith(".currentOutputWatermark")&&number>0&&number<Long.MAX_VALUE)watermark=Math.min(watermark,number.longValue());}
        }
        result.put("vertices",vertices);if(hasInput)result.put("inputRate",input);if(hasOutput)result.put("outputRate",output);if(watermark!=Long.MAX_VALUE)result.put("watermarkLagMs",Math.max(0,System.currentTimeMillis()-watermark));result.put("sampledAt",java.time.Instant.now().toString());return result;
    }
    private Map<String,Object> metricValues(String path){var result=new LinkedHashMap<String,Object>();try{var ids=requestArray(flink,path);if(ids.isEmpty())return result;String selected=String.join(",",ids.stream().map(v->Objects.toString(v.get("id"),"")).filter(v->!v.isBlank()).toList());if(selected.length()>60000)return result;for(var value:requestArray(flink,path+"?get="+encoded(selected)))result.put(Objects.toString(value.get("id")),value.get("value"));}catch(StudioException e){result.put("unavailable",true);}return result;}
    private static Double metricNumber(Object value){try{double n=Double.parseDouble(value.toString());return Double.isFinite(n)?n:null;}catch(Exception e){return null;}}

    /** Node logs are shared; return labelled, bounded, redacted tails and job-specific exceptions. */
    public Map<String,Object> logs(String id){checkJob(id);var nodes=new ArrayList<Map<String,Object>>();try{appendLogs(nodes,"JobManager","jobmanager/logs",id);}catch(StudioException e){nodes.add(Map.of("node","JobManager","unavailable",true));}
        var detail=job(id);var managers=new LinkedHashSet<String>();for(Object vertex:list(detail.get("vertices"))){var v=map(vertex);String vertexId=Objects.toString(v.get("id"),"");if(!JOB.matcher(vertexId).matches())continue;try{var data=request(flink,"GET",jobPath(id)+"/vertices/"+vertexId,null);for(Object subtask:list(data.get("subtasks"))){String tm=Objects.toString(map(subtask).get("taskmanager-id"),"");if(!tm.isBlank())managers.add(tm);}}catch(StudioException ignored){}}
        if(managers.isEmpty())try{for(Object tm:list(request(flink,"GET","taskmanagers",null).get("taskmanagers")))managers.add(Objects.toString(map(tm).get("id"),""));}catch(StudioException ignored){}
        for(String tm:managers)try{appendLogs(nodes,"TaskManager "+tm,"taskmanagers/"+encoded(tm)+"/logs",id);}catch(StudioException e){nodes.add(Map.of("node","TaskManager "+tm,"unavailable",true));}
        Map<String,Object> errors;try{errors=exceptions(id);}catch(StudioException e){errors=Map.of("unavailable",true);}return Map.of("nodes",nodes,"exceptions",errors,"sharedNodeLogs",true);
    }
    private void appendLogs(List<Map<String,Object>> nodes,String node,String path,String jobId){var files=list(request(flink,"GET",path,null).get("logs")).stream().map(FlinkGatewayClient::map).sorted(Comparator.comparingLong(v->-((Number)v.getOrDefault("mtime",0)).longValue())).limit(2).toList();
        for(var file:files){String name=Objects.toString(file.get("name"),"");if(!name.matches("[a-zA-Z0-9._-]{1,250}"))continue;String content=requestText(flink,path+"/"+encoded(name));String[] lines=content.split("\\R");int start=Math.max(0,lines.length-300);String tail=String.join("\n",Arrays.copyOfRange(lines,start,lines.length));nodes.add(Map.of("node",node,"file",name,"content",redact(tail),"containsJobId",tail.contains(jobId),"truncated",start>0));}
    }

    private Map<String,Object> request(URI base,String method,String path,Object body){String raw=send(base,method,path,body,false);if(raw.isBlank())return new LinkedHashMap<>();try{var result=json.map(raw);if(result==null)return new LinkedHashMap<>();return result;}catch(Exception e){throw remoteProtocol("Flink 响应 ["+method+" "+path+"] 的 JSON 无效，操作结果需先对账");}}
    @SuppressWarnings("unchecked") private List<Map<String,Object>> requestArray(URI base,String path){String raw=send(base,"GET",path,null,false);try{return (List<Map<String,Object>>)(List<?>)json.read(raw,List.class);}catch(Exception e){throw remoteProtocol("Flink 返回的指标 JSON 无效");}}
    private String requestText(URI base,String path){return send(base,"GET",path,null,true);}
    private String send(URI base,String method,String path,Object body,boolean text){return send(base,method,path,body,text,false);}
    private String send(URI base,String method,String path,Object body,boolean text,boolean idempotentClose){
        URI target=base.resolve(path);if(!Objects.equals(base.getScheme(),target.getScheme())||!Objects.equals(base.getAuthority(),target.getAuthority())||path.startsWith("/")||path.contains(".."))throw protocol("非法 Flink 控制路径");
        var builder=HttpRequest.newBuilder(target).timeout(requestTimeout).header("Accept",text?"text/plain":"application/json");if(body!=null)builder.header("Content-Type","application/json");builder.method(method,body==null?HttpRequest.BodyPublishers.noBody():HttpRequest.BodyPublishers.ofString(json.write(body),StandardCharsets.UTF_8));
        var subscriber=new BoundedBodySubscriber(text?2_000_000:8_000_000);CompletableFuture<HttpResponse<byte[]>> pending=null;
        try{pending=http.sendAsync(builder.build(),info->subscriber);var response=pending.get(requestTimeout.toMillis(),TimeUnit.MILLISECONDS);String raw=new String(response.body(),StandardCharsets.UTF_8);
            if(response.statusCode()<200||response.statusCode()>=300){if(idempotentClose&&response.statusCode()==500&&missingCloseResource(raw,path))return "{}";String detail=errorSummary(raw);int status=response.statusCode()==404?404:response.statusCode()==409?409:502;throw new StudioException(response.statusCode()==404?"FLINK_NOT_FOUND":"FLINK_REQUEST_FAILED","Flink HTTP "+response.statusCode()+" ["+method+" "+path+"]："+detail,status);}
            return raw;
        }catch(StudioException e){throw e;}catch(TimeoutException e){throw new StudioException("FLINK_REQUEST_TIMEOUT","Flink 请求超时（包括响应体读取），结果需先对账",504);}catch(InterruptedException e){Thread.currentThread().interrupt();throw new StudioException("FLINK_REQUEST_INTERRUPTED","Flink 请求被中断，结果需先对账",503);}catch(ExecutionException e){Throwable cause=e.getCause();for(Throwable reason=cause;reason!=null;reason=reason.getCause()){if(reason instanceof BodyLimitException)throw new StudioException("FLINK_RESPONSE_TOO_LARGE","Flink 响应超过大小限制，结果需先对账",502);if(reason instanceof java.net.http.HttpTimeoutException)throw new StudioException("FLINK_REQUEST_TIMEOUT","Flink 请求超时（包括响应体读取），结果需先对账",504);}throw new StudioException("FLINK_UNAVAILABLE","无法连接 Flink 控制服务："+redact(Objects.toString(cause.getMessage(),cause.getClass().getSimpleName())),503);}catch(Exception e){throw new StudioException("FLINK_UNAVAILABLE","无法连接 Flink 控制服务："+redact(Objects.toString(e.getMessage(),e.getClass().getSimpleName())),503);}
        finally{if(pending!=null&&!pending.isDone())pending.cancel(true);subscriber.abort();}
    }
    private static final class BodyLimitException extends RuntimeException{}
    /** Completes only after a bounded body arrives; cancel releases the HTTP subscription on deadline. */
    private static final class BoundedBodySubscriber implements HttpResponse.BodySubscriber<byte[]> {
        private final int limit;
        private final CompletableFuture<byte[]> body=new CompletableFuture<>();
        private final ByteArrayOutputStream bytes=new ByteArrayOutputStream();
        private Flow.Subscription subscription;
        BoundedBodySubscriber(int limit){this.limit=limit;}
        @Override public CompletionStage<byte[]> getBody(){return body;}
        @Override public synchronized void onSubscribe(Flow.Subscription incoming){if(subscription!=null||body.isDone()){incoming.cancel();return;}subscription=incoming;incoming.request(1);}
        @Override public synchronized void onNext(List<ByteBuffer> buffers){
            if(body.isDone())return;long incoming=buffers.stream().mapToLong(ByteBuffer::remaining).sum();if(incoming>limit-bytes.size()){body.completeExceptionally(new BodyLimitException());subscription.cancel();bytes.reset();return;}
            for(var buffer:buffers){byte[] chunk=new byte[buffer.remaining()];buffer.get(chunk);bytes.writeBytes(chunk);}subscription.request(1);
        }
        @Override public synchronized void onError(Throwable failure){body.completeExceptionally(failure);bytes.reset();}
        @Override public synchronized void onComplete(){if(!body.isDone())body.complete(bytes.toByteArray());bytes.reset();}
        synchronized void abort(){if(!body.isDone())body.cancel(false);if(subscription!=null)subscription.cancel();bytes.reset();}
    }
    /** Flink 2.2 reports absent sessions as HTTP 500; classify the full cause before diagnostic truncation. */
    private boolean missingCloseResource(String response,String path){
        String[] parts=path.split("/");if(parts.length<3||!parts[0].equals("v1")||!parts[1].equals("sessions"))return false;String session=parts[2];
        String diagnostic=errorText(response);
        if(Pattern.compile("(?i)\\bSession '"+Pattern.quote(session)+"' does not exist\\.").matcher(diagnostic).find())return true;
        // Typed exceptions are accepted only on these two resource-release routes.
        String types=parts.length==6&&parts[3].equals("operations")&&parts[5].equals("close")?"(?:SessionNotFoundException|OperationNotFoundException)":"SessionNotFoundException";
        return Pattern.compile("(?:^|[\\\"\\r\\n]|\\\\[nr]|\\bCaused by:?\\s*)(?:org\\.apache\\.flink\\.[a-zA-Z0-9_.]+\\.)?"+types+"(?=[:\\\"]|$)").matcher(diagnostic).find();
    }
    private String errorText(String response){
        try{var out=new StringBuilder();appendErrorText(json.read(response,Object.class),out);if(!out.isEmpty())return out.toString();}catch(RuntimeException ignored){/* Plain-text upstream errors are also supported. */}return response;
    }
    /** Long Netty wrapper stacks must not hide the connector's validation or submission cause. */
    private String errorSummary(String response){
        String diagnostic=redact(errorText(response));if(diagnostic.length()<=3000)return diagnostic;
        int cause=diagnostic.lastIndexOf("Caused by:");String deepest=cause>=0?diagnostic.substring(cause):diagnostic.substring(diagnostic.length()-1500);
        if(deepest.length()>1500)deepest=deepest.substring(0,1500);
        return diagnostic.substring(0,1400)+"\n[truncated]\nDeepest cause:\n"+deepest;
    }
    private static void appendErrorText(Object value,StringBuilder out){if(value instanceof String text)out.append(text).append('\n');else if(value instanceof Map<?,?> map)map.values().forEach(item->appendErrorText(item,out));else if(value instanceof List<?> list)list.forEach(item->appendErrorText(item,out));}
    @SuppressWarnings("unchecked") private Map<String,Object> sanitized(Map<String,Object> value){return (Map<String,Object>)sanitize(value);}
    private Object sanitize(Object value){if(value instanceof String s)return redact(s);if(value instanceof Map<?,?> map){var out=new LinkedHashMap<String,Object>();map.forEach((k,v)->out.put(k.toString(),sanitize(v)));return out;}if(value instanceof List<?> list)return list.stream().map(this::sanitize).toList();return value;}
    private static String sessionPath(String session){if(!HANDLE.matcher(session).matches())throw protocol("无效的 Gateway 会话 ID");return "v1/sessions/"+session;}
    private static String operationPath(String session,String operation){if(!HANDLE.matcher(operation).matches())throw protocol("无效的 Gateway 操作 ID");return sessionPath(session)+"/operations/"+operation;}
    private static String jobPath(String id){checkJob(id);return "jobs/"+id;}
    private static void checkJob(String id){if(id==null||!JOB.matcher(id).matches())throw protocol("无效的 Flink 作业或触发 ID");}
    private static String encoded(String value){return URLEncoder.encode(value,StandardCharsets.UTF_8).replace("+","%20");}
    private static StudioException protocol(String message){return StudioException.bad("FLINK_PROTOCOL_ERROR",message);}
    /** A bad remote response cannot establish whether an accepted POST already created a job. */
    private static StudioException remoteProtocol(String message){return new StudioException("FLINK_PROTOCOL_ERROR",message,502);}
    private static void pause(long millis){try{Thread.sleep(millis);}catch(InterruptedException e){Thread.currentThread().interrupt();throw new StudioException("FLINK_REQUEST_INTERRUPTED","Flink 等待被中断",503);}}
    private static void ignoreMissing(Runnable action){try{action.run();}catch(StudioException e){if(e.status()!=404)throw e;}}
    @SuppressWarnings("unchecked") private static Map<String,Object> map(Object value){return value instanceof Map<?,?> m?(Map<String,Object>)m:Map.of();}
    @SuppressWarnings("unchecked") private static List<Object> list(Object value){return value instanceof List<?> l?(List<Object>)l:List.of();}
}
