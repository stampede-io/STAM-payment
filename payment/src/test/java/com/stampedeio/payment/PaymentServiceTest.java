package com.stampedeio.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import com.stampedeio.payment.domain.IdempotencyKey;
import com.stampedeio.payment.domain.Payment;
import com.stampedeio.payment.event.EventEnvelope;
import com.stampedeio.payment.gateway.AuthorizeRequest;
import com.stampedeio.payment.gateway.AuthorizeResult;
import com.stampedeio.payment.gateway.PaymentGateway;
import com.stampedeio.payment.gateway.RefundRequest;
import com.stampedeio.payment.gateway.RefundResult;
import com.stampedeio.payment.repository.IdempotencyKeyRepository;
import com.stampedeio.payment.repository.PaymentRepository;
import com.stampedeio.payment.service.PaymentService;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    private PaymentGateway paymentGateway;

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private IdempotencyKeyRepository idempotencyKeyRepository;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Captor
    private ArgumentCaptor<Object> eventCaptor;

    private static Map<String, Object> payload() {
        return Map.of("amountCents", 9999L, "currency", "USD", "paymentMethodId", "pm_card_visa");
    }

    private PaymentService svc() {
        return new PaymentService(paymentGateway, paymentRepository, idempotencyKeyRepository, kafkaTemplate);
    }

    @Test
    void authorize_success_emitsPaymentAuthorized_andPersistsOnlyReferenceData() {
        UUID cid = UUID.randomUUID();
        when(idempotencyKeyRepository.findById(cid)).thenReturn(Optional.empty());
        when(paymentRepository.save(any(Payment.class))).thenAnswer(inv -> inv.getArgument(0));
        when(paymentGateway.authorize(any(AuthorizeRequest.class)))
                .thenReturn(AuthorizeResult.authorized("pi_test_123"));
        when(paymentGateway.name()).thenReturn("stripe");

        svc().authorizePayment(cid, UUID.randomUUID(), payload());

        ArgumentCaptor<Payment> pCap = ArgumentCaptor.forClass(Payment.class);
        verify(paymentRepository).save(pCap.capture());
        Payment persisted = pCap.getValue();
        assertThat(persisted.getCorrelationId()).isEqualTo(cid);
        assertThat(persisted.getPspRef()).isEqualTo("pi_test_123");
        assertThat(persisted.getAmountCents()).isEqualTo(9999L);
        assertThat(persisted.getCurrency()).isEqualTo("USD");
        assertThat(persisted.getStatus()).isEqualTo("AUTHORIZED");

        verify(kafkaTemplate).send(eq("payments.events"), any(), eventCaptor.capture());
        EventEnvelope env = (EventEnvelope) eventCaptor.getValue();
        assertThat(env.eventType()).isEqualTo("PaymentAuthorized");
    }

    @Test
    void authorize_declined_emitsPaymentFailed() {
        UUID cid = UUID.randomUUID();
        when(idempotencyKeyRepository.findById(cid)).thenReturn(Optional.empty());
        when(paymentRepository.save(any(Payment.class))).thenAnswer(inv -> inv.getArgument(0));
        when(paymentGateway.authorize(any(AuthorizeRequest.class)))
                .thenReturn(AuthorizeResult.failed("pi_test_declined", "card_declined"));
        when(paymentGateway.name()).thenReturn("stripe");

        svc().authorizePayment(cid, UUID.randomUUID(), payload());

        verify(kafkaTemplate).send(eq("payments.events"), any(), eventCaptor.capture());
        EventEnvelope env = (EventEnvelope) eventCaptor.getValue();
        assertThat(env.eventType()).isEqualTo("PaymentFailed");
    }

    @Test
    void authorize_forwardsCorrelationIdAsStripeIdempotencyKey() {
        UUID cid = UUID.randomUUID();
        when(idempotencyKeyRepository.findById(cid)).thenReturn(Optional.empty());
        when(paymentRepository.save(any(Payment.class))).thenAnswer(inv -> inv.getArgument(0));
        when(paymentGateway.authorize(any(AuthorizeRequest.class)))
                .thenReturn(AuthorizeResult.authorized("pi_x"));
        when(paymentGateway.name()).thenReturn("stripe");

        svc().authorizePayment(cid, UUID.randomUUID(), payload());

        ArgumentCaptor<AuthorizeRequest> reqCap = ArgumentCaptor.forClass(AuthorizeRequest.class);
        verify(paymentGateway).authorize(reqCap.capture());
        assertThat(reqCap.getValue().idempotencyKey()).isEqualTo(cid.toString());
    }

    @Test
    void authorize_duplicateCommand_replaysWithoutCallingGateway() {
        UUID cid = UUID.randomUUID();
        IdempotencyKey existing = new IdempotencyKey(cid, "AuthorizePayment", "PaymentAuthorized", "pi_saved");
        when(idempotencyKeyRepository.findById(cid)).thenReturn(Optional.of(existing));
        when(paymentRepository.findById(cid)).thenReturn(Optional.of(
                paymentAuthorized(cid, "pi_saved", 9999L, "USD")));

        svc().authorizePayment(cid, UUID.randomUUID(), payload());

        verify(paymentGateway, never()).authorize(any());
        verify(idempotencyKeyRepository, never()).save(any());
        verify(kafkaTemplate).send(eq("payments.events"), any(), eventCaptor.capture());
        assertThat(((EventEnvelope) eventCaptor.getValue()).eventType()).isEqualTo("PaymentAuthorized");
    }

    @Test
    void authorize_missingPaymentMethod_emitsPaymentFailed_withoutCallingGateway() {
        UUID cid = UUID.randomUUID();
        when(idempotencyKeyRepository.findById(cid)).thenReturn(Optional.empty());
        when(paymentRepository.save(any(Payment.class))).thenAnswer(inv -> inv.getArgument(0));
        when(paymentGateway.name()).thenReturn("stripe");

        AuthorizeResult result = svc().authorizePayment(cid, UUID.randomUUID(),
                Map.of("amountCents", 500L, "currency", "USD"));

        assertThat(result.status()).isEqualTo(com.stampedeio.payment.gateway.ChargeStatus.FAILED);
        assertThat(result.failureReason()).isEqualTo("missing_payment_method");
        verify(paymentGateway, never()).authorize(any());
        verify(kafkaTemplate).send(eq("payments.events"), any(), eventCaptor.capture());
        assertThat(((EventEnvelope) eventCaptor.getValue()).eventType()).isEqualTo("PaymentFailed");
    }

    @Test
    void refund_duplicateReplay_returnsProperOutcome_forFailedOriginal() {
        UUID cid = UUID.randomUUID();
        IdempotencyKey existing = new IdempotencyKey(cid, "RefundPayment", "RefundFailed", null);
        when(idempotencyKeyRepository.findById(cid)).thenReturn(Optional.of(existing));
        when(paymentGateway.name()).thenReturn("stripe");

        RefundResult result = svc().refundPayment(cid, UUID.randomUUID(), Map.of());

        assertThat(result.succeeded()).isFalse();
        verify(paymentGateway, never()).refund(any());
    }

    @Test
    void refund_duplicateReplay_returnsOk_withStoredRefundRef_forSuccessOriginal() {
        UUID cid = UUID.randomUUID();
        IdempotencyKey existing = new IdempotencyKey(cid, "RefundPayment", "RefundIssued", "re_saved");
        when(idempotencyKeyRepository.findById(cid)).thenReturn(Optional.of(existing));
        when(paymentGateway.name()).thenReturn("stripe");

        RefundResult result = svc().refundPayment(cid, UUID.randomUUID(), Map.of());

        assertThat(result.succeeded()).isTrue();
        assertThat(result.refundRef()).isEqualTo("re_saved");
        verify(paymentGateway, never()).refund(any());
    }

    @Test
    void webhook_emitsWithOriginalAggregateId_notCorrelationId() {
        UUID cid = UUID.randomUUID();
        UUID aggregateId = UUID.randomUUID();
        Payment p = new Payment(cid, aggregateId, 1000L, "USD");
        p.markRequiresAction("pi_wh");
        when(paymentRepository.findByPspRef("pi_wh")).thenReturn(Optional.of(p));
        when(paymentGateway.name()).thenReturn("stripe");

        svc().applyWebhookOutcome("pi_wh", true, null);

        verify(kafkaTemplate).send(eq("payments.events"), eq(aggregateId.toString()), eventCaptor.capture());
        EventEnvelope env = (EventEnvelope) eventCaptor.getValue();
        assertThat(env.aggregateId()).isEqualTo(aggregateId);
        assertThat(env.correlationId()).isEqualTo(cid);
        assertThat(env.eventType()).isEqualTo("PaymentAuthorized");
    }

    @Test
    void refund_success_emitsRefundIssued() {
        UUID cid = UUID.randomUUID();
        when(idempotencyKeyRepository.findById(cid)).thenReturn(Optional.empty());
        when(paymentGateway.refund(any(RefundRequest.class)))
                .thenReturn(RefundResult.ok("re_test_123"));
        when(paymentGateway.name()).thenReturn("stripe");

        svc().refundPayment(cid, UUID.randomUUID(),
                Map.of("pspRef", "pi_test_123", "amountCents", 500L));

        verify(kafkaTemplate).send(eq("payments.events"), any(), eventCaptor.capture());
        assertThat(((EventEnvelope) eventCaptor.getValue()).eventType()).isEqualTo("RefundIssued");
    }

    @Test
    void refund_forwardsCorrelationIdAsStripeIdempotencyKey() {
        UUID cid = UUID.randomUUID();
        when(idempotencyKeyRepository.findById(cid)).thenReturn(Optional.empty());
        when(paymentGateway.refund(any(RefundRequest.class))).thenReturn(RefundResult.ok("re_x"));
        when(paymentGateway.name()).thenReturn("stripe");

        svc().refundPayment(cid, UUID.randomUUID(),
                Map.of("pspRef", "pi_test_123", "amountCents", 0L));

        ArgumentCaptor<RefundRequest> reqCap = ArgumentCaptor.forClass(RefundRequest.class);
        verify(paymentGateway).refund(reqCap.capture());
        assertThat(reqCap.getValue().idempotencyKey()).isEqualTo(cid.toString());
    }

    @Test
    void refund_lookupPspRefViaOriginalCorrelationId_whenNotSuppliedDirectly() {
        UUID refundCid = UUID.randomUUID();
        UUID chargeCid = UUID.randomUUID();
        Payment original = paymentAuthorized(chargeCid, "pi_original", 1000L, "USD");

        when(idempotencyKeyRepository.findById(refundCid)).thenReturn(Optional.empty());
        when(paymentRepository.findById(chargeCid)).thenReturn(Optional.of(original));
        when(paymentGateway.refund(any(RefundRequest.class))).thenReturn(RefundResult.ok("re_x"));
        when(paymentGateway.name()).thenReturn("stripe");

        svc().refundPayment(refundCid, UUID.randomUUID(),
                Map.of("originalCorrelationId", chargeCid.toString(), "amountCents", 1000L));

        ArgumentCaptor<RefundRequest> reqCap = ArgumentCaptor.forClass(RefundRequest.class);
        verify(paymentGateway).refund(reqCap.capture());
        assertThat(reqCap.getValue().pspRef()).isEqualTo("pi_original");
        assertThat(original.getStatus()).isEqualTo("REFUNDED");
    }

    @Test
    void refund_failure_emitsRefundFailed() {
        UUID cid = UUID.randomUUID();
        when(idempotencyKeyRepository.findById(cid)).thenReturn(Optional.empty());
        when(paymentGateway.refund(any(RefundRequest.class)))
                .thenReturn(RefundResult.failed("charge_already_refunded"));
        when(paymentGateway.name()).thenReturn("stripe");

        svc().refundPayment(cid, UUID.randomUUID(),
                Map.of("pspRef", "pi_test_123", "amountCents", 500L));

        verify(kafkaTemplate).send(eq("payments.events"), any(), eventCaptor.capture());
        assertThat(((EventEnvelope) eventCaptor.getValue()).eventType()).isEqualTo("RefundFailed");
    }

    @Test
    void webhook_authorized_isIdempotent_secondCallDoesNotEmit() {
        UUID cid = UUID.randomUUID();
        Payment p = new Payment(cid, UUID.randomUUID(), 1000L, "USD");
        p.markAuthorized("pi_web");
        when(paymentRepository.findByPspRef("pi_web")).thenReturn(Optional.of(p));

        svc().applyWebhookOutcome("pi_web", true, null);
        verify(kafkaTemplate, never()).send(eq("payments.events"), any(), any());
    }

    @Test
    void webhook_pendingPayment_transitionsAndEmits() {
        UUID cid = UUID.randomUUID();
        Payment p = new Payment(cid, UUID.randomUUID(), 1000L, "USD");
        p.markRequiresAction("pi_web2");
        when(paymentRepository.findByPspRef("pi_web2")).thenReturn(Optional.of(p));
        when(paymentGateway.name()).thenReturn("stripe");

        svc().applyWebhookOutcome("pi_web2", true, null);

        assertThat(p.getStatus()).isEqualTo("AUTHORIZED");
        verify(kafkaTemplate, times(1)).send(eq("payments.events"), any(), eventCaptor.capture());
        assertThat(((EventEnvelope) eventCaptor.getValue()).eventType()).isEqualTo("PaymentAuthorized");
    }

    private static Payment paymentAuthorized(UUID cid, String pspRef, long amount, String currency) {
        Payment p = new Payment(cid, UUID.randomUUID(), amount, currency);
        p.markAuthorized(pspRef);
        return p;
    }
}
