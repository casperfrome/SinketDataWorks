package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.exception.StudioException;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class FlinkGatewayClientTest {
    static final String JOB="0123456789abcdef0123456789abcdef";
    @Test void detachedSubmissionCapturesJobAndClosingSessionDoesNotSendJobCancellation() throws Exception {
        var calls=new ArrayList<String>();var polls=new AtomicInteger();HttpServer server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/",exchange->{calls.add(exchange.getRequestMethod()+" "+exchange.getRequestURI().getPath());String path=exchange.getRequestURI().getPath();String body=path.equals("/v1/sessions")?"{\"sessionHandle\":\"session-1\"}":path.endsWith("/statements")?"{\"operationHandle\":\"operation-1\"}":path.endsWith("/status")?"{\"status\":\"FINISHED\"}":path.contains("/result/")?(polls.incrementAndGet()==1?"{\"resultType\":\"NOT_READY\"}":"{\"resultType\":\"PAYLOAD\",\"jobID\":\""+JOB+"\",\"queryResult\":false,\"results\":{\"data\":[]}}"):"{}";byte[] bytes=body.getBytes(StandardCharsets.UTF_8);exchange.sendResponseHeaders(200,bytes.length);exchange.getResponseBody().write(bytes);exchange.close();});server.start();
        try{String base="http://127.0.0.1:"+server.getAddress().getPort();var client=new FlinkGatewayClient(new JsonCodec(),base,base,2);String session=client.openSession(Map.of("execution.attached","false"));var result=client.execute(session,"INSERT INTO t SELECT * FROM s");assertEquals(JOB,result.get("jobID"));client.closeOperation(session,result.get("operationHandle").toString());client.closeSession(session);assertTrue(calls.contains("DELETE /v1/sessions/session-1"));assertFalse(calls.stream().anyMatch(c->c.contains("/jobs/")));assertEquals(2,polls.get());}
        finally{server.stop(0);}
    }
    @Test void resultPaginationCannotEscapeSessionOperationOrEndpoint(){var client=new FlinkGatewayClient(new JsonCodec(),"http://127.0.0.1:1","http://127.0.0.1:1");
        for(String path:List.of("https://evil.test/result/0","/v1/sessions/other/operations/operation/result/0","/v1/sessions/session/operations/operation/result/../0","/v1/sessions/session/operations/operation/result/%30"))assertEquals("FLINK_PROTOCOL_ERROR",assertThrows(StudioException.class,()->client.result("session","operation",path)).code());}
    @Test void statePathsCannotTraverseOutsidePersistentVolume(){
        assertEquals("file:/opt/flink/state/savepoints/savepoint-a",FlinkGatewayClient.validateStatePath("file:/opt/flink/state/savepoints/savepoint-a"));assertEquals("file:///opt/flink/state/checkpoints/job/chk-1",FlinkGatewayClient.validateStatePath("file:///opt/flink/state/checkpoints/job/chk-1"));
        for(String path:List.of("mock://savepoints/x","http://evil/x","file:///etc/passwd","file:///opt/flink/state/savepoints/../../etc","file:///opt/flink/state/savepoints/%2e%2e/x","file://other/opt/flink/state/savepoints/a","file:///opt/flink/state/savepoints/","file:///opt/flink/state/savepoints/x?foo=1"))assertThrows(StudioException.class,()->FlinkGatewayClient.validateStatePath(path));
    }
    @Test void remoteErrorAndKnownPasswordsAreRedacted() throws Exception {HttpServer server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);server.createContext("/",exchange->{byte[] bytes="{\"errors\":[\"authentication rejected secret-value; 'password'='different-secret'\"]}".getBytes(StandardCharsets.UTF_8);exchange.sendResponseHeaders(400,bytes.length);exchange.getResponseBody().write(bytes);exchange.close();});server.start();try{String url="http://127.0.0.1:"+server.getAddress().getPort();var client=new FlinkGatewayClient(new JsonCodec(),url,url);client.rememberSecrets(List.of("secret-value"));var e=assertThrows(StudioException.class,()->client.job(JOB));assertFalse(e.getMessage().contains("secret-value"));assertFalse(e.getMessage().contains("different-secret"));}finally{server.stop(0);}}
    @Test void suspendedJobIsNotGloballyTerminalAndStillNeedsCancellation() throws Exception {
        var calls=new ArrayList<String>();HttpServer server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);server.createContext("/",exchange->{calls.add(exchange.getRequestMethod());byte[] bytes="{\"state\":\"SUSPENDED\"}".getBytes(StandardCharsets.UTF_8);exchange.sendResponseHeaders(200,bytes.length);exchange.getResponseBody().write(bytes);exchange.close();});server.start();
        try{String url="http://127.0.0.1:"+server.getAddress().getPort();var client=new FlinkGatewayClient(new JsonCodec(),url,url);assertFalse(client.terminal(JOB));client.cancel(JOB);assertTrue(calls.contains("PATCH"));}finally{server.stop(0);}
    }
    @Test void closeTreatsExactMissingCauseAtEndOfLongHttp500AsAlreadyReleased() throws Exception {
        String session="0257d67a-a0a1-4981-86b7-18c70bd29b6b";var message=new java.util.concurrent.atomic.AtomicReference<>("wrapper stack "+"padding ".repeat(600)+"Caused by: org.apache.flink.table.gateway.api.utils.SqlGatewayException: Session '"+session+"' does not exist.");var json=new JsonCodec();var calls=new ArrayList<String>();
        HttpServer server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);server.createContext("/",exchange->{calls.add(exchange.getRequestMethod()+" "+exchange.getRequestURI().getPath());byte[] bytes=json.write(Map.of("errors",List.of(message.get()))).getBytes(StandardCharsets.UTF_8);exchange.sendResponseHeaders(500,bytes.length);exchange.getResponseBody().write(bytes);exchange.close();});server.start();
        try{String url="http://127.0.0.1:"+server.getAddress().getPort();var client=new FlinkGatewayClient(json,url,url);assertDoesNotThrow(()->client.closeOperation(session,"operation-1"));assertDoesNotThrow(()->client.closeSession(session));
            message.set("org.apache.flink.table.gateway.api.utils.OperationNotFoundException: operation-1 was already released");assertDoesNotThrow(()->client.closeOperation(session,"operation-1"));assertThrows(StudioException.class,()->client.closeSession(session));
            message.set("org.apache.flink.table.gateway.api.utils.SessionNotFoundException: session is absent");assertDoesNotThrow(()->client.closeSession(session));assertThrows(StudioException.class,()->client.status(session,"operation-1"));
            message.set("Session 'ffffffff-ffff-ffff-ffff-ffffffffffff' does not exist.");assertThrows(StudioException.class,()->client.closeSession(session));
            message.set("SqlGatewayException: unable to close session because storage failed");assertEquals("FLINK_REQUEST_FAILED",assertThrows(StudioException.class,()->client.closeOperation(session,"operation-1")).code());
            message.set("Expected SessionNotFoundException during a test but actual storage failed");assertThrows(StudioException.class,()->client.closeSession(session));
            assertTrue(calls.stream().filter(call->call.contains("/close")||!call.contains("/operations/")).allMatch(call->call.startsWith("DELETE ")));
        }finally{server.stop(0);}
    }
    @Test void longGatewayErrorPreservesDeepestValidationCauseAndRedactsDecodedSecrets() throws Exception {
        String wrapper="org.apache.flink.table.gateway.api.utils.SqlGatewayException: Failed to execute operation\n"+" at org.apache.flink.shaded.netty.Handler.invoke(Handler.java:1)\n".repeat(100);
        String cause="Caused by: org.apache.flink.table.api.ValidationException: The server-id 3941105408-3941105663 is not a valid numeric value; password='secret-value'\n at ServerIdRange.parseServerId(ServerIdRange.java:1)";
        var json=new JsonCodec();HttpServer server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);server.createContext("/",exchange->{byte[] bytes=json.write(Map.of("errors",List.of(wrapper+cause))).getBytes(StandardCharsets.UTF_8);exchange.sendResponseHeaders(500,bytes.length);exchange.getResponseBody().write(bytes);exchange.close();});server.start();
        try{String url="http://127.0.0.1:"+server.getAddress().getPort();var client=new FlinkGatewayClient(json,url,url);client.rememberSecrets(List.of("secret-value"));var failure=assertThrows(StudioException.class,()->client.result("session","operation",0));assertTrue(failure.getMessage().contains("ValidationException: The server-id"));assertTrue(failure.getMessage().contains("not a valid numeric value"));assertTrue(failure.getMessage().contains("HTTP 500 [GET v1/sessions/session/operations/operation/result/0"));assertTrue(failure.getMessage().contains("[truncated]"));assertFalse(failure.getMessage().contains("secret-value"));assertTrue(failure.getMessage().length()<3200);}
        finally{server.stop(0);}
    }
    @Test void requestDeadlineIncludesAStalledBodyAndCancellationAllowsNextRequest() throws Exception {
        var headersSent=new CountDownLatch(1);var release=new CountDownLatch(1);var calls=new AtomicInteger();HttpServer server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/",exchange->{try{if(calls.incrementAndGet()==1){exchange.sendResponseHeaders(200,0);exchange.getResponseBody().write('{');exchange.getResponseBody().flush();headersSent.countDown();try{release.await(5,TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();}}else{byte[] bytes="{\"state\":\"RUNNING\"}".getBytes(StandardCharsets.UTF_8);exchange.sendResponseHeaders(200,bytes.length);exchange.getResponseBody().write(bytes);}}finally{exchange.close();}});server.start();
        try{String url="http://127.0.0.1:"+server.getAddress().getPort();var client=new FlinkGatewayClient(new JsonCodec(),url,url,90,Duration.ofMillis(700));long started=System.nanoTime();var failure=assertThrows(StudioException.class,()->client.job(JOB));assertEquals("FLINK_REQUEST_TIMEOUT",failure.code());assertEquals(504,failure.status());assertTrue(headersSent.await(1,TimeUnit.SECONDS));assertTrue(Duration.ofNanos(System.nanoTime()-started).toMillis()<2500);release.countDown();assertEquals("RUNNING",client.job(JOB).get("state"));}
        finally{release.countDown();server.stop(0);}
    }
    @Test void oversizedChunkedResponseIsAbortedBeforeJsonParsing() throws Exception {
        HttpServer server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);server.createContext("/",exchange->{try{exchange.sendResponseHeaders(200,0);byte[] chunk=new byte[32_768];Arrays.fill(chunk,(byte)' ');for(int count=0;count<280;count++)exchange.getResponseBody().write(chunk);}catch(java.io.IOException ignored){/* Client cancels the subscription when the byte cap is reached. */}finally{exchange.close();}});server.start();
        try{String url="http://127.0.0.1:"+server.getAddress().getPort();var client=new FlinkGatewayClient(new JsonCodec(),url,url);var failure=assertThrows(StudioException.class,()->client.job(JOB));assertEquals("FLINK_RESPONSE_TOO_LARGE",failure.code());assertEquals(502,failure.status());}
        finally{server.stop(0);}
    }
    @Test void successfulPostWithoutValidOperationHandleLeavesSubmissionUnknown() throws Exception {
        var response=new java.util.concurrent.atomic.AtomicReference<>("{}");var posts=new AtomicInteger();HttpServer server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/",exchange->{if("POST".equals(exchange.getRequestMethod()))posts.incrementAndGet();byte[] bytes=response.get().getBytes(StandardCharsets.UTF_8);exchange.sendResponseHeaders(200,bytes.length);exchange.getResponseBody().write(bytes);exchange.close();});server.start();
        try{String url="http://127.0.0.1:"+server.getAddress().getPort();var client=new FlinkGatewayClient(new JsonCodec(),url,url);
            for(String body:List.of("{\"jobID\":\""+JOB+"\"}","{\"operationHandle\":17}","{\"operationHandle\":\"../other\"}")){response.set(body);var failure=assertThrows(StudioException.class,()->client.statement("session","INSERT INTO target SELECT * FROM source"));assertEquals("FLINK_PROTOCOL_ERROR",failure.code());assertEquals(502,failure.status());}
            assertEquals(3,posts.get());
        }finally{server.stop(0);}
    }
    @Test void successfulPostWithMalformedJsonLeavesSubmissionUnknownWithoutLeakingBody() throws Exception {
        HttpServer server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);server.createContext("/",exchange->{byte[] bytes="{\"operationHandle\":\"secret-response-value\"".getBytes(StandardCharsets.UTF_8);exchange.sendResponseHeaders(200,bytes.length);exchange.getResponseBody().write(bytes);exchange.close();});server.start();
        try{String url="http://127.0.0.1:"+server.getAddress().getPort();var client=new FlinkGatewayClient(new JsonCodec(),url,url);var failure=assertThrows(StudioException.class,()->client.statement("session","INSERT INTO target SELECT * FROM source"));assertEquals("FLINK_PROTOCOL_ERROR",failure.code());assertEquals(502,failure.status());assertTrue(failure.getMessage().contains("POST v1/sessions/session/statements"));assertFalse(failure.getMessage().contains("secret-response-value"));}
        finally{server.stop(0);}
    }
}
