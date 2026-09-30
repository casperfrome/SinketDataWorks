package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.exception.StudioException;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.casperfrome.dunnelean.RunSpec;
import io.github.casperfrome.dunnelean.RunSpecJson;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises the studio facade through the real SDK, with no engine or database dependencies. */
class DunneleanClientTest {
    private static final String STORE = "store-fixture";
    private static final String MYSQL_PASSWORD = "source-secret-中文";
    private static final String DORIS_PASSWORD = "target-secret-😀";
    private static final BigInteger UINT64_MAX = new BigInteger("18446744073709551615");
    private static final String CREATED_AT = "2026-09-30T02:03:04.123456Z";
    private static final String FORWARD_SPEC = """
            {
              "request_id":"local-fixture",
              "reader":{"type":"mysql","connection":{"database":"source",
                "credentials":{"username":"test","password":"source-secret-中文"}},
                "source":{"table":"orders","where":"note = ?",
                  "params":[{"type":"string","value":"中文😀' OR 1=1 --"}]}},
              "writer":{"type":"doris","sql":{"database":"target",
                "credentials":{"username":"test","password":"target-secret-😀"}},
                "fe_http_urls":["http://127.0.0.1:8030"],"table":"orders"}
            }
            """;
    private static final String REVERSE_SPEC = """
            {
              "request_id":"local-fixture",
              "reader":{"type":"doris","flight_uri":"grpc://127.0.0.1:8070",
                "database":"source","credentials":{"username":"test","password":"source-secret-中文"},
                "source":{"table":"orders"}},
              "writer":{"type":"mysql","connection":{"database":"target",
                "credentials":{"username":"test","password":"target-secret-😀"}},"table":"orders"}
            }
            """;
    private static final String HEALTH = """
            {"status":"ok","service":"dunnelean","version":"0.1.0","state_store_id":"store-fixture"}
            """;
    private static final String VALIDATION = """
            {"valid":true,"source_schema":[{"name":"id","type":"UInt64","nullable":false}],
              "target_schema":[{"name":"id","type":"UInt64","nullable":false}],"semantics":"batch commits"}
            """;
    private static final String BATCHES = """
            {"batches":[{"batch_id":18446744073709551615,"state":"CONFIRMED",
              "rows":18446744073709551615,"bytes":18446744073709551615,
              "label":"fixture-label","detail":"opaque receipt"},
              {"batch_id":2,"state":"FUTURE_STATE","rows":0,"bytes":0,"label":null,"detail":null}]}
            """;

    private final JsonCodec json = new JsonCodec();
    private final BlockingQueue<RecordedRequest> requests = new LinkedBlockingQueue<>();
    private final Map<String,Response> responses = new ConcurrentHashMap<>();
    private final List<DunneleanClient> clients = new ArrayList<>();
    private HttpServer server;
    private String base;
    private DunneleanClient client;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::respond);
        server.start();
        base = "http://127.0.0.1:" + server.getAddress().getPort();
        client = newClient(base + "/");
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
        clients.forEach(DunneleanClient::close);
    }

    @Test
    void healthPreservesTheStateStoreIdentityAndConfiguredAddress() throws Exception {
        assertEquals(base, client.baseUrl);
        var result = client.health(base);
        assertEquals("ok", result.get("status"));
        assertEquals("dunnelean", result.get("service"));
        assertEquals("0.1.0", result.get("version"));
        assertEquals(STORE, result.get("state_store_id"));
        assertFalse(result.containsKey("stateStoreId"));
        assertRequest("GET", "/healthz", null, false);
    }

    @Test
    void legacyOrBlankHealthIdentityRequiresUpgradeWhileMalformedHealthRemainsUnavailable() throws Exception {
        for (String health : List.of(HEALTH.replace(",\"state_store_id\":\"store-fixture\"", ""),
                HEALTH.replace("store-fixture", "   "))) {
            responses.put("GET /healthz", new Response(200, health));
            var exception = assertThrows(StudioException.class, () -> client.health(base));
            assertEquals("SYNC_UPGRADE_REQUIRED", exception.code());
            assertEquals(400, exception.status());
            assertRequest("GET", "/healthz", null, false);
            assertTrue(requests.isEmpty(), "An incompatible health response must not be retried");
        }
        for (String health : List.of("not JSON", "{}")) {
            responses.put("GET /healthz", new Response(200, health));
            var exception = assertThrows(StudioException.class, () -> client.health(base));
            assertEquals("SYNC_UNAVAILABLE", exception.code());
            assertEquals(502, exception.status());
            assertRequest("GET", "/healthz", null, false);
            assertTrue(requests.isEmpty(), "A malformed health response must not be retried");
        }
    }

    @Test
    void validatesBothDirectionsUsingSdkSerialization() throws Exception {
        for (String input : List.of(FORWARD_SPEC, REVERSE_SPEC)) {
            RunSpec spec = RunSpecJson.fromJson(input);
            client.validate(spec);
            var request = assertRequest("POST", "/v1/validate", null, true);
            assertEquals(spec, RunSpecJson.fromJson(request.body()));
            assertTrue(request.contentType().contains("charset=UTF-8"));
            assertTrue(request.body().contains("local-fixture"));
            assertTrue(request.body().contains(MYSQL_PASSWORD));
            assertTrue(request.body().contains(DORIS_PASSWORD));
        }
    }

    @Test
    void submitPreservesUnsignedCountersAndIsoTimesWithoutExposingSdkConfig() throws Exception {
        var result = client.submit(base, RunSpecJson.fromJson(FORWARD_SPEC), STORE);
        assertRun(result);
        var request = assertRequest("POST", "/v1/runs", STORE, true);
        assertEquals(RunSpecJson.fromJson(FORWARD_SPEC), RunSpecJson.fromJson(request.body()));
        assertTrue(requests.isEmpty(), "Submitting through the SDK must issue exactly one request");
        String encoded = assertDoesNotThrow(() -> json.write(result));
        var tree = JsonParser.parseString(encoded).getAsJsonObject();
        assertEquals(UINT64_MAX, tree.get("rows_committed").getAsBigInteger());
        assertEquals(CREATED_AT, tree.get("created_at").getAsString());
        assertFalse(encoded.contains(MYSQL_PASSWORD));
        assertFalse(encoded.contains(DORIS_PASSWORD));
    }

    @Test
    @SuppressWarnings("unchecked")
    void requestLookupUsesTheSavedServiceAndGuardRatherThanTheDefaultClientAddress() throws Exception {
        var differentDefault = newClient("http://127.0.0.1:1");
        var result = differentDefault.getRequest(base, "local-fixture", STORE);
        assertEquals("local-fixture", result.get("request_id"));
        assertEquals(false, result.get("cancel_requested"));
        assertRun((Map<String,Object>) result.get("run"));
        assertRequest("GET", "/v1/requests/local-fixture", STORE, false);
    }

    @Test
    void cancellationBeforeSubmissionRetainsTheEmptyRunTombstone() throws Exception {
        var result = client.cancelRequest(base, "local-fixture", STORE);
        assertEquals("local-fixture", result.get("request_id"));
        assertEquals(true, result.get("cancel_requested"));
        assertNull(result.get("run"));
        assertFalse(result.containsKey("cancelRequested"));
        assertRequest("POST", "/v1/requests/local-fixture/cancel", STORE, false);
        assertTrue(requests.isEmpty(), "Cancelling a request must not submit it");
    }

    @Test
    @SuppressWarnings("unchecked")
    void batchReceiptsRetainTheFrontendWireShapeAndExactUnsignedIds() throws Exception {
        var result = client.listBatches(base, "remote-fixture", STORE);
        var batches = (List<Map<String,Object>>) result.get("batches");
        assertEquals(2, batches.size());
        var first = batches.getFirst();
        assertEquals(UINT64_MAX, first.get("batch_id"));
        assertEquals(UINT64_MAX, first.get("rows"));
        assertEquals(UINT64_MAX, first.get("bytes"));
        assertEquals("CONFIRMED", first.get("state"));
        assertEquals("fixture-label", first.get("label"));
        assertEquals("opaque receipt", first.get("detail"));
        assertFalse(first.containsKey("batchId"));
        assertEquals("FUTURE_STATE", batches.get(1).get("state"));
        assertNull(batches.get(1).get("label"));
        assertNull(batches.get(1).get("detail"));
        assertDoesNotThrow(() -> json.write(result));
        assertRequest("GET", "/v1/runs/remote-fixture/batches", STORE, false);
    }

    @Test
    void apiFailuresRetainStudioErrorCodesAndHttpStatusWithoutRetrying() throws Exception {
        for (var failure : Map.of("NOT_FOUND", 404, "STATE_STORE_CHANGED", 409, "BUSY", 429).entrySet()) {
            responses.put("GET /v1/requests/local-fixture", new Response(failure.getValue(),
                    "{\"error\":" + error(failure.getKey(), "同步控制失败", false) + "}"));
            var exception = assertThrows(StudioException.class,
                    () -> client.getRequest(base, "local-fixture", STORE));
            assertEquals("SYNC_" + failure.getKey(), exception.code());
            assertEquals(failure.getValue().intValue(), exception.status());
            assertEquals("同步控制失败", exception.getMessage());
            assertRequest("GET", "/v1/requests/local-fixture", STORE, false);
            assertTrue(requests.isEmpty());
        }
    }

    @Test
    void submitErrorsRedactBothConnectorPasswords() throws Exception {
        responses.put("POST /v1/runs", new Response(400,
                "{\"error\":" + error("VALIDATION", "拒绝 " + MYSQL_PASSWORD + " / " + DORIS_PASSWORD, false) + "}"));
        var exception = assertThrows(StudioException.class,
                () -> client.submit(base, RunSpecJson.fromJson(FORWARD_SPEC), STORE));
        assertEquals("SYNC_VALIDATION", exception.code());
        assertEquals(400, exception.status());
        assertRedacted(exception.getMessage());
        assertRequest("POST", "/v1/runs", STORE, true);
        assertTrue(requests.isEmpty(), "A rejected submit must not be retried automatically");
    }

    @Test
    void nonstandardHttpErrorsRetainTheStatusAndHideRawDiagnostics() throws Exception {
        responses.put("POST /v1/runs", new Response(502, "gateway rejected " + MYSQL_PASSWORD));
        var exception = assertThrows(StudioException.class,
                () -> client.submit(base, RunSpecJson.fromJson(FORWARD_SPEC), STORE));
        assertEquals("SYNC_REMOTE_ERROR", exception.code());
        assertEquals(502, exception.status());
        assertEquals("同步引擎请求失败", exception.getMessage());
        assertFalse(exception.getMessage().contains(MYSQL_PASSWORD));
        assertRequest("POST", "/v1/runs", STORE, true);
        assertTrue(requests.isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    void rememberedPasswordsRemainRedactedDuringRecoveryAndReceiptLookup() throws Exception {
        client.submit(base, RunSpecJson.fromJson(FORWARD_SPEC), STORE);
        assertRequest("POST", "/v1/runs", STORE, true);
        String diagnostic = "提交结果未知 " + MYSQL_PASSWORD + " / " + DORIS_PASSWORD;
        responses.put("GET /v1/requests/local-fixture", new Response(200,
                requestJson(runJson(error("COMMIT_UNKNOWN", diagnostic, true)), false)));
        var status = client.getRequest(base, "local-fixture", STORE);
        var run = (Map<String,Object>) status.get("run");
        var embeddedError = (Map<String,Object>) run.get("error");
        assertEquals("COMMIT_UNKNOWN", embeddedError.get("code"));
        assertEquals(true, embeddedError.get("commit_unknown"));
        assertRedacted((String) embeddedError.get("message"));
        assertRequest("GET", "/v1/requests/local-fixture", STORE, false);

        responses.put("GET /v1/requests/local-fixture", new Response(409,
                "{\"error\":" + error("STATE_STORE_CHANGED", diagnostic, false) + "}"));
        var failure = assertThrows(StudioException.class,
                () -> client.getRequest(base, "local-fixture", STORE));
        assertRedacted(failure.getMessage());
        assertRequest("GET", "/v1/requests/local-fixture", STORE, false);

        responses.put("GET /v1/runs/remote-fixture/batches", new Response(200,
                BATCHES.replace("opaque receipt", diagnostic)));
        assertRedacted(json.write(client.listBatches(base, "remote-fixture", STORE)));
        assertRequest("GET", "/v1/runs/remote-fixture/batches", STORE, false);
    }

    @Test
    void successfulButMalformedResponsesBecomeUnavailableWithoutLeakingRawBodies() throws Exception {
        for (String malformed : List.of("not JSON " + MYSQL_PASSWORD, "{}")) {
            responses.put("GET /v1/requests/local-fixture", new Response(200, malformed));
            var exception = assertThrows(StudioException.class,
                    () -> client.getRequest(base, "local-fixture", STORE));
            assertEquals("SYNC_UNAVAILABLE", exception.code());
            assertEquals(502, exception.status());
            assertFalse(exception.getMessage().contains(MYSQL_PASSWORD));
            assertRequest("GET", "/v1/requests/local-fixture", STORE, false);
            assertTrue(requests.isEmpty());
        }
    }

    @Test
    void transportFailureBecomesUnavailable() {
        server.stop(0);
        var exception = assertThrows(StudioException.class, () -> client.getRequest(base, "local-fixture", STORE));
        assertEquals("SYNC_UNAVAILABLE", exception.code());
        assertEquals(502, exception.status());
        assertTrue(requests.isEmpty());
    }

    @Test
    void interruptedSdkCallsPreserveTheThreadInterruptFlag() {
        Thread.currentThread().interrupt();
        try {
            var exception = assertThrows(StudioException.class,
                    () -> client.getRequest(base, "local-fixture", STORE));
            assertEquals("SYNC_UNAVAILABLE", exception.code());
            assertEquals(502, exception.status());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void sdkClientsRetainTheExistingConnectionAndOperationTimeouts() {
        var control = (Duration) ReflectionTestUtils.getField(DunneleanClient.class, "CONTROL_TIMEOUT");
        var validation = (Duration) ReflectionTestUtils.getField(DunneleanClient.class, "VALIDATE_TIMEOUT");
        assertEquals(Duration.ofSeconds(15), control);
        assertEquals(Duration.ofSeconds(65), validation);
        for (Duration timeout : List.of(control, validation)) {
            var sdk = ReflectionTestUtils.invokeMethod(client, "sdk", base, STORE, timeout);
            assertNotNull(sdk);
            assertEquals(timeout, ReflectionTestUtils.getField(sdk, "requestTimeout"));
            var transport = (HttpClient) ReflectionTestUtils.getField(sdk, "http");
            assertNotNull(transport);
            assertEquals(Duration.ofSeconds(5), transport.connectTimeout().orElseThrow());
        }
        assertTrue(requests.isEmpty(), "Timeout policy checks must not contact a service");
    }

    @Test
    void configuredAndSavedServiceAddressesRemainRestrictedToLoopbackHttp() {
        for (String unsafe : List.of("http://example.com:9876", "https://127.0.0.1:9876",
                "http://user:password@127.0.0.1:9876", "http://127.0.0.1:9876?token=secret", "http://127.0.0.1:9876#fragment")) {
            assertThrows(IllegalArgumentException.class, () -> new DunneleanClient(json, unsafe));
            assertThrows(RuntimeException.class, () -> client.getRequest(unsafe, "local-fixture", STORE));
        }
        assertDoesNotThrow(() -> newClient("http://localhost:9876"));
        assertDoesNotThrow(() -> newClient("http://[::1]:9876"));
        assertTrue(requests.isEmpty());
    }

    private void assertRun(Map<String,Object> run) {
        assertEquals("remote-fixture", run.get("run_id"));
        assertEquals("local-fixture", run.get("request_id"));
        assertEquals("SUCCEEDED", run.get("state"));
        assertEquals("complete", run.get("stage"));
        assertEquals(CREATED_AT, run.get("created_at"));
        assertEquals("2026-09-30T02:03:05Z", run.get("updated_at"));
        for (String key : List.of("rows_read", "bytes_read", "rows_submitted", "rows_committed", "server_affected_rows")) {
            assertEquals(UINT64_MAX, run.get(key), key);
        }
        assertEquals(new BigInteger("3"), run.get("rows_filtered"));
        assertEquals(new BigInteger("4"), run.get("batches_committed"));
        assertEquals(false, run.get("partial_write"));
        assertEquals(false, run.get("commit_unknown"));
        assertFalse(run.containsKey("config"));
        assertFalse(run.containsKey("runId"));
        assertFalse(run.containsKey("createdAt"));
    }

    private DunneleanClient newClient(String url) {
        var value = new DunneleanClient(json, url);
        clients.add(value);
        return value;
    }

    private void assertRedacted(String value) {
        assertFalse(value.contains(MYSQL_PASSWORD), "Source password escaped redaction");
        assertFalse(value.contains(DORIS_PASSWORD), "Target password escaped redaction");
        assertTrue(value.contains("[REDACTED]"));
    }

    private RecordedRequest assertRequest(String method, String path, String stateStoreId, boolean hasBody) throws Exception {
        var request = requests.poll(2, TimeUnit.SECONDS);
        assertNotNull(request, "Expected an SDK request");
        assertEquals(method, request.method());
        assertEquals(path, request.path());
        assertEquals(stateStoreId, request.stateStoreId());
        assertEquals(hasBody, !request.body().isEmpty());
        return request;
    }

    private void respond(HttpExchange exchange) throws IOException {
        try (exchange) {
            var request = new RecordedRequest(exchange.getRequestMethod(), exchange.getRequestURI().getRawPath(),
                    exchange.getRequestHeaders().getFirst("x-dunnelean-state-store-id"),
                    exchange.getRequestHeaders().getFirst("Content-Type"),
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            requests.add(request);
            var response = responses.getOrDefault(request.method() + " " + request.path(), standardResponse(request));
            byte[] body = response.body().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
            exchange.sendResponseHeaders(response.status(), body.length);
            exchange.getResponseBody().write(body);
        }
    }

    private Response standardResponse(RecordedRequest request) {
        if (request.path().equals("/healthz")) return new Response(200, HEALTH);
        if (request.path().equals("/v1/validate")) return new Response(200, VALIDATION);
        if (request.path().equals("/v1/runs")) return new Response(200, runJson("null"));
        if (request.path().endsWith("/batches")) return new Response(200, BATCHES);
        if (request.path().endsWith("/cancel")) return new Response(200, requestJson("null", true));
        if (request.path().startsWith("/v1/requests/")) return new Response(200, requestJson(runJson("null"), false));
        return new Response(404, "{\"error\":" + error("NOT_FOUND", "missing", false) + "}");
    }

    private static String requestJson(String run, boolean cancelled) {
        return "{\"request_id\":\"local-fixture\",\"cancel_requested\":" + cancelled + ",\"run\":" + run + "}";
    }

    private static String error(String code, String message, boolean unknown) {
        return "{\"code\":\"" + code + "\",\"message\":\"" + message
                + "\",\"commit_unknown\":" + unknown + ",\"retryable\":false}";
    }

    private static String runJson(String error) {
        return """
                {"run_id":"remote-fixture","request_id":"local-fixture","state":"SUCCEEDED","stage":"complete",
                  "created_at":"2026-09-30T02:03:04.123456Z","updated_at":"2026-09-30T02:03:05Z",
                  "rows_read":18446744073709551615,"bytes_read":18446744073709551615,
                  "rows_submitted":18446744073709551615,"rows_committed":18446744073709551615,
                  "rows_filtered":3,"server_affected_rows":18446744073709551615,"batches_committed":4,
                  "partial_write":false,"commit_unknown":false,"error":%s,
                  "config":{"reader":{"password":"source-secret-中文"},"writer":{"password":"target-secret-😀"}},
                  "future_field":true}
                """.formatted(error);
    }

    private record RecordedRequest(String method, String path, String stateStoreId, String contentType, String body) {}
    private record Response(int status, String body) {}
}
