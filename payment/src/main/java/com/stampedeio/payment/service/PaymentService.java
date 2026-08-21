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
            Payment prior = paymentRepository.findById(correlationId).orElse(null);
            emitOutcome(existing.get().getOutcome(), correlationId,
                    prior != null && prior.getAggregateId() != null ? prior.getAggregateId() : aggregateId,
                    authorizePayload(prior, existing.get(), paymentGateway.name()));
            return authorizeResult(prior, existing.get());
        }

        PaymentPayload parsed = PaymentPayload.from(payload);
        if (parsed.paymentMethodId() == null || parsed.paymentMethodId().isBlank()) {
            String reason = "missing_payment_method";
            Payment payment = new Payment(correlationId, aggregateId, parsed.amountCents(), parsed.currency());
            payment.markFailed(null, reason);
            paymentRepository.save(payment);
            idempotencyKeyRepository.save(new IdempotencyKey(
                    correlationId, "AuthorizePayment", "PaymentFailed", null));
            emitOutcome("PaymentFailed", correlationId, aggregateId,
                    Map.of(
                            "pspRef", "",
                            "amountCents", parsed.amountCents(),
                            "currency", parsed.currency(),
                            "gateway", paymentGateway.name(),
                            "failureReason", reason));
            return AuthorizeResult.failed(null, reason);
        }

        Payment payment = new Payment(correlationId, aggregateId, parsed.amountCents(), parsed.currency());
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

        idempotencyKeyRepository.save(new IdempotencyKey(
                correlationId, "AuthorizePayment", outcomeEvent, result.pspRef()));
        emitOutcome(outcomeEvent, correlationId, aggregateId,
                authorizeEventPayload(result.pspRef(), parsed.amountCents(), parsed.currency(),
                        paymentGateway.name(), result.failureReason(), null));
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
            emitOutcome(existing.get().getOutcome(), correlationId, aggregateId,
                    refundEventPayload(existing.get().getPspRef(), null, paymentGateway.name()));
            return replayRefundResult(existing.get());
        }

        PaymentPayload parsed = PaymentPayload.from(payload);
        String pspRef = parsed.pspRef();
        Payment original = null;
        if ((pspRef == null || pspRef.isBlank()) && parsed.originalCorrelationId() != null) {
            original = paymentRepository.findById(parsed.originalCorrelationId()).orElse(null);
            if (original != null) {
                pspRef = original.getPspRef();
            }
        }

        RefundRequest req = new RefundRequest(correlationId, pspRef, parsed.amountCents(), correlationId.toString());
        RefundResult result = paymentGateway.refund(req);

        String outcome = result.succeeded() ? "RefundIssued" : "RefundFailed";
        idempotencyKeyRepository.save(new IdempotencyKey(
                correlationId, "RefundPayment", outcome, result.refundRef()));

        if (result.succeeded()) {
            if (original == null && pspRef != null) {
                original = paymentRepository.findByPspRef(pspRef).orElse(null);
            }
            if (original != null) {
                original.markRefunded();
            }
        }

        emitOutcome(outcome, correlationId, aggregateId,
                refundEventPayload(result.refundRef(), pspRef, paymentGateway.name()));
        log.info("Processed RefundPayment correlationId={} outcome={} gateway={}",
                correlationId, outcome, paymentGateway.name());
        return result;
    }

    /**
     * Applied when Stripe confirms a PaymentIntent's terminal state out-of-band
     * (async payment methods, delayed confirmation). Uses the PaymentIntent id
     * as the identity and re-emits keyed on the original aggregate so booking's
     * saga consumer, partitioned by reservation id, receives it.
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
                payment.getCorrelationId(), "AuthorizePayment", outcomeEvent, pspRef));

        UUID emitAggregate = payment.getAggregateId() != null ? payment.getAggregateId() : payment.getCorrelationId();
        emitOutcome(outcomeEvent, payment.getCorrelationId(), emitAggregate,
                authorizeEventPayload(pspRef, payment.getAmountCents(), payment.getCurrency(),
                        paymentGateway.name(), failureReason, "webhook"));
        log.info("Applied webhook outcome pspRef={} → {} aggregateId={}",
                pspRef, outcomeEvent, emitAggregate);
    }

    private void emitOutcome(String eventType, UUID correlationId, UUID aggregateId, Object payload) {
        EventEnvelope envelope = EventEnvelope.create(eventType, 1, correlationId, aggregateId, payload);
        kafkaTemplate.send(TOPIC_PAYMENTS_EVENTS, aggregateId.toString(), envelope);
    }

    private static Map<String, Object> authorizeEventPayload(String pspRef, long amountCents, String currency,
                                                             String gateway, String failureReason, String source) {
        Map<String, Object> payload = new java.util.HashMap<>();
        payload.put("pspRef", pspRef == null ? "" : pspRef);
        payload.put("amountCents", amountCents);
        payload.put("currency", currency);
        payload.put("gateway", gateway);
        if (failureReason != null) payload.put("failureReason", failureReason);
        if (source != null) payload.put("source", source);
        return payload;
    }

    private static Map<String, Object> refundEventPayload(String refundRef, String pspRef, String gateway) {
        return Map.of(
                "refundRef", refundRef == null ? "" : refundRef,
                "pspRef", pspRef == null ? "" : pspRef,
                "gateway", gateway);
    }

    private static Map<String, Object> authorizePayload(Payment payment, IdempotencyKey key, String gateway) {
        long amountCents = payment != null ? payment.getAmountCents() : 0L;
        String currency = payment != null ? payment.getCurrency() : DEFAULT_CURRENCY;
        String failureReason = payment != null ? payment.getFailureReason() : null;
        return authorizeEventPayload(key.getPspRef(), amountCents, currency, gateway, failureReason, "replay");
    }

    private static AuthorizeResult authorizeResult(Payment payment, IdempotencyKey key) {
        String pspRef = key.getPspRef();
        return switch (key.getOutcome()) {
            case "PaymentAuthorized" -> AuthorizeResult.authorized(pspRef);
            case "PaymentRequiresAction" -> new AuthorizeResult(pspRef, ChargeStatus.REQUIRES_ACTION, null);
            default -> AuthorizeResult.failed(pspRef, payment != null ? payment.getFailureReason() : null);
        };
    }

    private static RefundResult replayRefundResult(IdempotencyKey key) {
        if ("RefundIssued".equals(key.getOutcome())) {
            return RefundResult.ok(key.getPspRef());
        }
        return RefundResult.failed("previously_failed");
    }

    private record PaymentPayload(long amountCents, String currency, String paymentMethodId,
                                  String pspRef, UUID originalCorrelationId) {

        @SuppressWarnings("unchecked")
        static PaymentPayload from(Object payload) {
            long amount = 0L;
            String currency = DEFAULT_CURRENCY;
            String pm = null;
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
