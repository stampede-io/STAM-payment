package com.stampedeio.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stampedeio.payment.domain.Payment;
import com.stampedeio.payment.repository.PaymentRepository;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * STAM-255 integration test — exercises both entry points end-to-end:
 * <ol>
 *   <li>POST /api/v1/payments → gateway succeeds → PaymentAuthorized on
 *       payments.events.</li>
 *   <li>Existing payment sitting in REQUIRES_ACTION → Stripe-shaped webhook
 *       with a valid HMAC signature arrives → PaymentAuthorized on
 *       payments.events.</li>
 * </ol>
 * Runs against the mock gateway so it stays free and offline. The real Stripe
 * test-mode path is wired identically (same interface, same controller); it
 * activates when STRIPE_API_KEY is exported and payment.gateway=stripe.
 */
@Tag("integration")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@Import(StripePaymentFlowIT.EventCollector.class)
class StripePaymentFlowIT {

    private static final String WEBHOOK_SECRET = "whsec_test_secret_for_it";

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
        registry.add("payment.gateway", () -> "mock");
        registry.add("payment.failure-rate", () -> "0.0");
        registry.add("stripe.webhook-secret", () -> WEBHOOK_SECRET);
    }

    static final CopyOnWriteArrayList<Map<String, Object>> receivedEvents = new CopyOnWriteArrayList<>();

    @Component
    static class EventCollector {
        @KafkaListener(topics = "payments.events", groupId = "it-verifier-flow")
        public void listen(Map<String, Object> event) {
            receivedEvents.add(event);
        }
    }

    @LocalServerPort
    int port;

    @Autowired
    private PaymentRepository paymentRepository;

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    @BeforeEach
    void resetState() {
        receivedEvents.clear();
    }

    @Test
    void chargeViaHttp_authorizesAndEmitsPaymentAuthorized() throws Exception {
        UUID cid = UUID.randomUUID();
        UUID aggregateId = UUID.randomUUID();

        String body = JSON.writeValueAsString(Map.of(
                "correlationId", cid.toString(),
                "aggregateId", aggregateId.toString(),
                "amountCents", 4200,
                "currency", "USD",
                "paymentMethodId", "pm_card_visa"));

        HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder()
                        .uri(URI.create(url("/api/v1/payments")))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(201);
        Map<String, Object> respBody = JSON.readValue(response.body(), Map.class);
        assertThat((String) respBody.get("pspRef")).startsWith("mock_");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(receivedEvents)
                        .anyMatch(e -> "PaymentAuthorized".equals(e.get("eventType"))
                                && cid.toString().equals(e.get("correlationId"))));

        Payment persisted = paymentRepository.findById(cid).orElseThrow();
        assertThat(persisted.getStatus()).isEqualTo("AUTHORIZED");
        assertThat(persisted.getAmountCents()).isEqualTo(4200L);
        assertThat(persisted.getCurrency()).isEqualTo("USD");
    }

    @Test
    void webhookWithValidSignature_authorizesPendingPayment_andEmitsPaymentAuthorized() throws Exception {
        UUID cid = UUID.randomUUID();
        String pspRef = "pi_it_" + cid;
        Payment pending = new Payment(cid, 5000L, "USD");
        pending.markRequiresAction(pspRef);
        paymentRepository.save(pending);

        String payload = String.format(
                "{\"id\":\"evt_it_%s\",\"type\":\"payment_intent.succeeded\","
                        + "\"api_version\":\"2024-04-10\","
                        + "\"data\":{\"object\":{\"id\":\"%s\",\"object\":\"payment_intent\","
                        + "\"amount\":5000,\"currency\":\"usd\",\"status\":\"succeeded\"}}}",
                UUID.randomUUID(), pspRef);

        long ts = System.currentTimeMillis() / 1000L;
        HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder()
                        .uri(URI.create(url("/api/v1/payments/webhook")))
                        .header("Content-Type", "application/json")
                        .header("Stripe-Signature", stripeSignature(ts, payload, WEBHOOK_SECRET))
                        .POST(HttpRequest.BodyPublishers.ofString(payload))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isBetween(200, 299);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(receivedEvents)
                        .anyMatch(e -> "PaymentAuthorized".equals(e.get("eventType"))
                                && cid.toString().equals(e.get("correlationId"))));

        assertThat(paymentRepository.findById(cid).orElseThrow().getStatus()).isEqualTo("AUTHORIZED");
    }

    @Test
    void webhookWithBadSignature_isRejected_andDoesNotEmit() throws Exception {
        String payload = "{\"id\":\"evt_it_bad\",\"type\":\"payment_intent.succeeded\",\"data\":{\"object\":{\"id\":\"pi_none\"}}}";

        HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder()
                        .uri(URI.create(url("/api/v1/payments/webhook")))
                        .header("Content-Type", "application/json")
                        .header("Stripe-Signature", "t=1,v1=deadbeef")
                        .POST(HttpRequest.BodyPublishers.ofString(payload))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(400);

        List<Map<String, Object>> snapshot = List.copyOf(receivedEvents);
        assertThat(snapshot).noneMatch(e -> {
            Object p = e.get("payload");
            return p instanceof Map<?, ?> pm && "pi_none".equals(pm.get("pspRef"));
        });
    }

    private static String stripeSignature(long timestamp, String payload, String secret) throws Exception {
        String signedPayload = timestamp + "." + payload;
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        byte[] hmac = mac.doFinal(signedPayload.getBytes(StandardCharsets.UTF_8));
        StringBuilder hex = new StringBuilder(hmac.length * 2);
        for (byte b : hmac) hex.append(String.format("%02x", b));
        return "t=" + timestamp + ",v1=" + hex;
    }
}
