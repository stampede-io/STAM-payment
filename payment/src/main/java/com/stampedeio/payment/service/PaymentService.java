package com.stampedeio.payment.service;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.stampedeio.payment.domain.IdempotencyKey;
import com.stampedeio.payment.event.EventEnvelope;
import com.stampedeio.payment.repository.IdempotencyKeyRepository;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);
    private static final String TOPIC_PAYMENTS_EVENTS = "payments.events";

    private final IdempotencyKeyRepository idempotencyKeyRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final double failureRate;

    public PaymentService(IdempotencyKeyRepository idempotencyKeyRepository,
                          KafkaTemplate<String, Object> kafkaTemplate,
                          @Value("${payment.failure-rate:0.0}") double failureRate) {
        this.idempotencyKeyRepository = idempotencyKeyRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.failureRate = failureRate;
    }

    @Transactional
    public void authorizePayment(UUID correlationId, UUID aggregateId, Object payload) {
        Optional<IdempotencyKey> existing = idempotencyKeyRepository.findById(correlationId);
        if (existing.isPresent()) {
            log.info("Duplicate AuthorizePayment correlationId={}, replaying outcome={}", correlationId, existing.get().getOutcome());
            emitOutcome(existing.get().getOutcome(), correlationId, aggregateId, payload);
            return;
        }

        String outcome = shouldFail() ? "PaymentFailed" : "PaymentAuthorized";
        idempotencyKeyRepository.save(new IdempotencyKey(correlationId, "AuthorizePayment", outcome));
        emitOutcome(outcome, correlationId, aggregateId, payload);
        log.info("Processed AuthorizePayment correlationId={} outcome={}", correlationId, outcome);
    }

    @Transactional
    public void refundPayment(UUID correlationId, UUID aggregateId, Object payload) {
        Optional<IdempotencyKey> existing = idempotencyKeyRepository.findById(correlationId);
        if (existing.isPresent()) {
            log.info("Duplicate RefundPayment correlationId={}, replaying outcome={}", correlationId, existing.get().getOutcome());
            emitOutcome(existing.get().getOutcome(), correlationId, aggregateId, payload);
            return;
        }

        String outcome = "RefundIssued";
        idempotencyKeyRepository.save(new IdempotencyKey(correlationId, "RefundPayment", outcome));
        emitOutcome(outcome, correlationId, aggregateId, payload);
        log.info("Processed RefundPayment correlationId={} outcome={}", correlationId, outcome);
    }

    private boolean shouldFail() {
        return ThreadLocalRandom.current().nextDouble() < failureRate;
    }

    private void emitOutcome(String eventType, UUID correlationId, UUID aggregateId, Object payload) {
        EventEnvelope envelope = EventEnvelope.create(eventType, 1, correlationId, aggregateId, payload);
        kafkaTemplate.send(TOPIC_PAYMENTS_EVENTS, aggregateId.toString(), envelope);
    }
}
