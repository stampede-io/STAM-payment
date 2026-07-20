package com.stampedeio.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.stampedeio.payment.repository.IdempotencyKeyRepository;

@Tag("integration")
@SpringBootTest
@Testcontainers
@Import(PaymentCommandConsumerIT.TestEventListener.class)
class PaymentCommandConsumerIT {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    static final CopyOnWriteArrayList<Map<String, Object>> receivedEvents = new CopyOnWriteArrayList<>();

    static class TestEventListener {
        @KafkaListener(topics = "payments.events", groupId = "test-verifier")
        public void listen(Map<String, Object> event) {
            receivedEvents.add(event);
        }
    }

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private IdempotencyKeyRepository idempotencyKeyRepository;

    @BeforeEach
    void setUp() {
        receivedEvents.clear();
        idempotencyKeyRepository.deleteAll();
    }

    @Test
    void ac1_authorizePayment_emitsPaymentAuthorized_whenFailureRateIsZero() throws Exception {
        UUID correlationId = UUID.randomUUID();
        UUID aggregateId = UUID.randomUUID();

        sendCommand("AuthorizePayment", correlationId, aggregateId);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(receivedEvents).anyMatch(e ->
                    "PaymentAuthorized".equals(e.get("eventType"))
                            && correlationId.toString().equals(e.get("correlationId")));
        });
    }

    @Test
    void ac4_idempotency_duplicateCommand_emitsSameOutcome() throws Exception {
        UUID correlationId = UUID.randomUUID();
        UUID aggregateId = UUID.randomUUID();

        sendCommand("AuthorizePayment", correlationId, aggregateId);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(receivedEvents.stream()
                        .filter(e -> correlationId.toString().equals(e.get("correlationId")))
                        .count()).isEqualTo(1));

        sendCommand("AuthorizePayment", correlationId, aggregateId);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(receivedEvents.stream()
                        .filter(e -> correlationId.toString().equals(e.get("correlationId")))
                        .count()).isEqualTo(2));

        String firstOutcome = receivedEvents.stream()
                .filter(e -> correlationId.toString().equals(e.get("correlationId")))
                .map(e -> (String) e.get("eventType"))
                .findFirst().orElseThrow();
        String secondOutcome = receivedEvents.stream()
                .filter(e -> correlationId.toString().equals(e.get("correlationId")))
                .skip(1)
                .map(e -> (String) e.get("eventType"))
                .findFirst().orElseThrow();
        assertThat(firstOutcome).isEqualTo(secondOutcome);

        assertThat(idempotencyKeyRepository.count()).isEqualTo(1);
    }

    @Test
    void ac5_refundPayment_emitsRefundIssued() throws Exception {
        UUID correlationId = UUID.randomUUID();
        UUID aggregateId = UUID.randomUUID();

        sendCommand("RefundPayment", correlationId, aggregateId);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(receivedEvents).anyMatch(e ->
                    "RefundIssued".equals(e.get("eventType"))
                            && correlationId.toString().equals(e.get("correlationId")));
        });
    }

    private void sendCommand(String commandType, UUID correlationId, UUID aggregateId) throws Exception {
        Map<String, Object> command = new HashMap<>();
        command.put("eventId", UUID.randomUUID().toString());
        command.put("eventType", commandType);
        command.put("version", 1);
        command.put("occurredAt", Instant.now().toString());
        command.put("correlationId", correlationId.toString());
        command.put("aggregateId", aggregateId.toString());
        command.put("payload", Map.of("amount", 9999, "currency", "USD"));

        kafkaTemplate.send("payments.commands", aggregateId.toString(), command)
                .get(10, TimeUnit.SECONDS);
    }
}
