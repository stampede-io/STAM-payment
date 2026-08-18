package com.stampedeio.payment.service;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.stampedeio.payment.domain.IdempotencyKey;
import com.stampedeio.payment.domain.Payment;
import com.stampedeio.payment.event.EventEnvelope;
import com.stampedeio.payment.gateway.AuthorizeRequest;
import com.stampedeio.payment.gateway.AuthorizeResult;
import com.stampedeio.payment.gateway.ChargeStatus;
import com.stampedeio.payment.gateway.PaymentGateway;
import com.stampedeio.payment.gateway.RefundRequest;
import com.stampedeio.payment.gateway.RefundResult;
import com.stampedeio.payment.repository.IdempotencyKeyRepository;
import com.stampedeio.payment.repository.PaymentRepository;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);
    private static final String TOPIC_PAYMENTS_EVENTS = "payments.events";
    private static final String DEFAULT_CURRENCY = "USD";
    private static final String DEFAULT_TEST_PAYMENT_METHOD = "pm_card_visa";

    private final PaymentGateway paymentGateway;
    private final PaymentRepository paymentRepository;
    private final IdempotencyKeyRepository idempotencyKeyRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PaymentService(PaymentGateway paymentGateway,
                          PaymentRepository paymentRepository,
                          IdempotencyKeyRepository idempotencyKeyRepository,
                          KafkaTemplate<String, Object> kafkaTemplate) {
        this.paymentGateway = paymentGateway;
        this.paymentRepository = paymentRepository;
        this.idempotencyKeyRepository = idempotencyKeyRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Transactional
    public AuthorizeResult authorizePayment(UUID correlationId, UUID aggregateId, Object payload) {
        Optional<IdempotencyKey> existing = idempotencyKeyRepository.findById(correlationId);
        if (existing.isPresent()) {
            log.info("Duplicate AuthorizePayment correlationId={}, replaying outcome={}",
                    correlationId, existing.get().getOutcome());
            emitOutcome(existing.get().getOutcome(), correlationId, aggregateId, replayPayload(correlationId));
            return replayResult(correlationId);
        }

        PaymentPayload parsed = PaymentPayload.from(payload);
        Payment payment = new Payment(correlationId, parsed.amountCents(), parsed.currency());
        payment = paymentRepository.save(payment);

        AuthorizeRequest req = new AuthorizeRequest(
                correlationId,
                parsed.amountCents(),
                parsed.currency(),
                parsed.paymentMethodId(),
                correlationId.toString());

        AuthorizeResult result = paymentGateway.authorize(req);

        String outcomeEvent;
        if (result.status() == ChargeStatus.AUTHORIZED) {
            payment.markAuthorized(result.pspRef());
            outcomeEvent = "PaymentAuthorized";
        } else if (result.status() == ChargeStatus.REQUIRES_ACTION) {
            payment.markRequiresAction(result.pspRef());
            outcomeEvent = "PaymentRequiresAction";
        } else {
            payment.markFailed(result.pspRef(), result.failureReason());
            outcomeEvent = "PaymentFailed";
        }

        idempotencyKeyRepository.save(new IdempotencyKey(correlationId, "AuthorizePayment", outcomeEvent));
        emitOutcome(outcomeEvent, correlationId, aggregateId,
                Map.of(
                        "pspRef", result.pspRef() == null ? "" : result.pspRef(),
                        "amountCents", parsed.amountCents(),
                        "currency", parsed.currency(),
                        "gateway", paymentGateway.name()));
        log.info("Processed AuthorizePayment correlationId={} outcome={} gateway={}",
                correlationId, outcomeEvent, paymentGateway.name());
        return result;
    }

    @Transactional
    public RefundResult refundPayment(UUID correlationId, UUID aggregateId, Object payload) {
        Optional<IdempotencyKey> existing = idempotencyKeyRepository.findById(correlationId);
        if (existing.isPresent()) {
            log.info("Duplicate RefundPayment correlationId={}, replaying outcome={}",
                    correlationId, existing.get().getOutcome());
            emitOutcome(existing.get().getOutcome(), correlationId, aggregateId, Map.of());
            return RefundResult.ok(existing.get().getOutcome());
        }

        PaymentPayload parsed = PaymentPayload.from(payload);
        String pspRef = parsed.pspRef();
        if (pspRef == null || pspRef.isBlank()) {
            // Refund flows arriving via Kafka carry the original payment correlation-id;
            // look up the psp_ref from our payments row.
            Payment original = parsed.originalCorrelationId() != null
                    ? paymentRepository.findById(parsed.originalCorrelationId()).orElse(null)
                    : null;
            if (original != null) {
                pspRef = original.getPspRef();
            }
        }

        RefundRequest req = new RefundRequest(correlationId, pspRef, parsed.amountCents(), correlationId.toString());
        RefundResult result = paymentGateway.refund(req);

        String outcome = result.succeeded() ? "RefundIssued" : "RefundFailed";
        idempotencyKeyRepository.save(new IdempotencyKey(correlationId, "RefundPayment", outcome));

        if (result.succeeded() && pspRef != null) {
            paymentRepository.findByPspRef(pspRef).ifPresent(Payment::markRefunded);
        }

        emitOutcome(outcome, correlationId, aggregateId,
                Map.of(
                        "refundRef", result.refundRef() == null ? "" : result.refundRef(),
                        "pspRef", pspRef == null ? "" : pspRef,
                        "gateway", paymentGateway.name()));
        log.info("Processed RefundPayment correlationId={} outcome={} gateway={}",
                correlationId, outcome, paymentGateway.name());
        return result;
    }

    /**
     * Called by the Stripe webhook consumer when Stripe confirms an intent's terminal
     * state out-of-band (e.g. async payment methods or delayed confirmation). Uses the
     * PaymentIntent id (psp_ref) as the identity, not correlationId.
     */
    @Transactional
    public void applyWebhookOutcome(String pspRef, boolean succeeded, String failureReason) {
        Optional<Payment> maybe = paymentRepository.findByPspRef(pspRef);
        if (maybe.isEmpty()) {
            log.warn("Webhook for unknown pspRef={} — ignoring", pspRef);
            return;
        }
        Payment payment = maybe.get();
        String current = payment.getStatus();
        if ("AUTHORIZED".equals(current) || "FAILED".equals(current) || "REFUNDED".equals(current)) {
            log.info("Webhook idempotent replay pspRef={} status={} — no state change", pspRef, current);
            return;
        }

        String outcomeEvent;
        if (succeeded) {
            payment.markAuthorized(pspRef);
            outcomeEvent = "PaymentAuthorized";
        } else {
            payment.markFailed(pspRef, failureReason);
            outcomeEvent = "PaymentFailed";
        }
        idempotencyKeyRepository.save(new IdempotencyKey(
                payment.getCorrelationId(), "AuthorizePayment", outcomeEvent));
        emitOutcome(outcomeEvent, payment.getCorrelationId(), payment.getCorrelationId(),
                Map.of(
                        "pspRef", pspRef,
                        "amountCents", payment.getAmountCents(),
                        "currency", payment.getCurrency(),
                        "gateway", paymentGateway.name(),
                        "source", "webhook"));
        log.info("Applied webhook outcome pspRef={} → {}", pspRef, outcomeEvent);
    }

    private void emitOutcome(String eventType, UUID correlationId, UUID aggregateId, Object payload) {
        EventEnvelope envelope = EventEnvelope.create(eventType, 1, correlationId, aggregateId, payload);
        kafkaTemplate.send(TOPIC_PAYMENTS_EVENTS, aggregateId.toString(), envelope);
    }

    private Map<String, Object> replayPayload(UUID correlationId) {
        return paymentRepository.findById(correlationId)
                .map(p -> Map.<String, Object>of(
                        "pspRef", p.getPspRef() == null ? "" : p.getPspRef(),
                        "amountCents", p.getAmountCents(),
                        "currency", p.getCurrency()))
                .orElse(Map.of());
    }

    private AuthorizeResult replayResult(UUID correlationId) {
        Payment p = paymentRepository.findById(correlationId).orElse(null);
        if (p == null) {
            return new AuthorizeResult(null, ChargeStatus.FAILED, "no_payment_row");
        }
        return switch (p.getStatus()) {
            case "AUTHORIZED" -> AuthorizeResult.authorized(p.getPspRef());
            case "REQUIRES_ACTION" -> new AuthorizeResult(p.getPspRef(), ChargeStatus.REQUIRES_ACTION, null);
            default -> AuthorizeResult.failed(p.getPspRef(), p.getFailureReason());
        };
    }

    private record PaymentPayload(long amountCents, String currency, String paymentMethodId,
                                  String pspRef, UUID originalCorrelationId) {

        @SuppressWarnings("unchecked")
        static PaymentPayload from(Object payload) {
            long amount = 0L;
            String currency = DEFAULT_CURRENCY;
            String pm = DEFAULT_TEST_PAYMENT_METHOD;
            String pspRef = null;
            UUID original = null;
            if (payload instanceof Map<?, ?> map) {
                Map<String, Object> m = (Map<String, Object>) map;
                Object a = m.getOrDefault("amountCents", m.get("amount"));
                if (a instanceof Number n) amount = n.longValue();
                if (m.get("currency") instanceof String c && !c.isBlank()) currency = c;
                if (m.get("paymentMethodId") instanceof String p && !p.isBlank()) pm = p;
                if (m.get("pspRef") instanceof String r && !r.isBlank()) pspRef = r;
                if (m.get("originalCorrelationId") instanceof String o && !o.isBlank()) {
                    try {
                        original = UUID.fromString(o);
                    } catch (IllegalArgumentException ignored) {
                    }
                }
            }
            return new PaymentPayload(amount, currency, pm, pspRef, original);
        }
    }
}
