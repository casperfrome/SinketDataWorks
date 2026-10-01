package com.fake.dataworks.service;

import com.fake.dataworks.config.JsonCodec;
import com.fake.dataworks.exception.StudioException;
import java.util.*;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class KafkaDatasourceTest {
    private final JdbcTemplate jdbc=mock(JdbcTemplate.class);
    private final SecretCipher cipher=mock(SecretCipher.class);
    private final DatasourceService sources=new DatasourceService(jdbc,mock(ObjectService.class),cipher,new JsonCodec());
    private Map<String,Object> kafka() {
        var input=new LinkedHashMap<String,Object>();input.put("workspaceId","ws");input.put("name","Kafka");input.put("type","KAFKA");input.put("bootstrapServers","localhost:19092");return input;
    }
    @Test void plaintextNeedsNoPasswordAndCannotReachSqlExecution() {
        var source=sources.input(null,kafka());
        assertEquals("",source.encryptedPassword());assertFalse((boolean)source.publicView().get("passwordSet"));
        assertFalse(source.publicView().containsKey("database"));assertEquals("localhost:19092",source.publicView().get("bootstrapServers"));
        assertEquals("DATASOURCE_TYPE_MISMATCH",assertThrows(StudioException.class,()->sources.open(source,5)).code());
        verifyNoInteractions(cipher);
    }
    @Test void saslPasswordIsEncryptedAndNeverPartOfPublicConfiguration() {
        var input=kafka();input.put("securityProtocol","SASL_SSL");input.put("saslMechanism","SCRAM-SHA-512");input.put("username","writer");input.put("password","unique-secret");
        when(cipher.encrypt("unique-secret")).thenReturn("encrypted-value");
        var source=sources.input(null,input);
        assertEquals("encrypted-value",source.encryptedPassword());assertTrue((boolean)source.publicView().get("passwordSet"));
        assertFalse(new JsonCodec().write(source.publicView()).contains("unique-secret"));assertFalse(new JsonCodec().write(source.options()).contains("encrypted-value"));
    }
    @Test void authenticatedSaveRequiresCredentials() {
        var input=kafka();input.put("securityProtocol","SASL_PLAINTEXT");input.put("username","writer");
        assertEquals("PASSWORD_REQUIRED",assertThrows(StudioException.class,()->sources.input(null,input)).code());
    }
    @Test void runtimeEndpointIsValidatedAndKeptSeparate() {
        var input=kafka();input.put("flinkBootstrapServers","kafka_rlt_4_3_1:9092");
        var source=sources.input(null,input);
        assertEquals("localhost:19092",source.options().get("bootstrapServers"));assertEquals("kafka_rlt_4_3_1:9092",source.options().get("flinkBootstrapServers"));
        for(String invalid:List.of("host","http://host:9092","host:0","host:65536","host:9092,","host:9092\npassword=secret"))assertThrows(StudioException.class,()->DatasourceService.validateBrokers(invalid));
        assertDoesNotThrow(()->DatasourceService.validateBrokers("host-1:9092,[::1]:19092"));
    }
    @Test void adminTimeoutClosesClientWithinOneSecondAndRedactsFailure() throws Exception {
        var source=sources.input(null,kafka());var service=spy(sources);var client=mock(AdminClient.class);var cluster=mock(DescribeClusterResult.class);
        @SuppressWarnings("unchecked") KafkaFuture<String> future=mock(KafkaFuture.class);
        doReturn(client).when(service).kafkaClient(source);when(client.describeCluster()).thenReturn(cluster);when(cluster.clusterId()).thenReturn(future);
        when(future.get(8,java.util.concurrent.TimeUnit.SECONDS)).thenThrow(new TimeoutException("password=unique-secret"));
        var error=assertThrows(StudioException.class,()->service.testKafka(source));
        assertEquals("KAFKA_UNAVAILABLE",error.code());assertFalse(error.getMessage().contains("unique-secret"));verify(client).close(Duration.ofSeconds(1));verify(client,never()).close();
    }
}
