package com.stampedeio.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
import com.stampedeio.payment.event.EventEnvelope;
import com.stampedeio.payment.repository.IdempotencyKeyRepository;
import com.stampedeio.payment.service.PaymentService;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    private IdempotencyKeyRepository idempotencyKeyRepository;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Captor
    private ArgumentCaptor<Object> eventCaptor;

    @Test
    void ac2_failureRateZero_alwaysAuthorizes() {
        PaymentService service = new PaymentService(idempotencyKeyRepository, kafkaTemplate, 0.0);
        when(idempotencyKeyRepository.findById(any())).thenReturn(Optional.empty());

        for (int i = 0; i < 50; i++) {
            service.authorizePayment(UUID.randomUUID(), UUID.randomUUID(), Map.of());
        }

        verify(kafkaTemplate, times(50)).send(eq("payments.events"), any(), eventCaptor.capture());
        assertThat(eventCaptor.getAllValues())
                .allMatch(e -> ((EventEnvelope) e).eventType().equals("PaymentAuthorized"));
    }

    @Test
    void ac3_failureRateOne_alwaysFails() {
        PaymentService service = new PaymentService(idempotencyKeyRepository, kafkaTemplate, 1.0);
        when(idempotencyKeyRepository.findById(any())).thenReturn(Optional.empty());

        for (int i = 0; i < 50; i++) {
            service.authorizePayment(UUID.randomUUID(), UUID.randomUUID(), Map.of());
        }

        verify(kafkaTemplate, times(50)).send(eq("payments.events"), any(), eventCaptor.capture());
        assertThat(eventCaptor.getAllValues())
                .allMatch(e -> ((EventEnvelope) e).eventType().equals("PaymentFailed"));
    }

    @Test
    void idempotencyKey_savedOnFirstCall() {
        PaymentService service = new PaymentService(idempotencyKeyRepository, kafkaTemplate, 0.0);
        UUID correlationId = UUID.randomUUID();
        when(idempotencyKeyRepository.findById(correlationId)).thenReturn(Optional.empty());

        service.authorizePayment(correlationId, UUID.randomUUID(), Map.of());

        ArgumentCaptor<IdempotencyKey> keyCaptor = ArgumentCaptor.forClass(IdempotencyKey.class);
        verify(idempotencyKeyRepository).save(keyCaptor.capture());
        assertThat(keyCaptor.getValue().getCorrelationId()).isEqualTo(correlationId);
        assertThat(keyCaptor.getValue().getCommandType()).isEqualTo("AuthorizePayment");
        assertThat(keyCaptor.getValue().getOutcome()).isEqualTo("PaymentAuthorized");
    }

    @Test
    void idempotencyKey_existingKey_replaysOutcome() {
        PaymentService service = new PaymentService(idempotencyKeyRepository, kafkaTemplate, 0.0);
        UUID correlationId = UUID.randomUUID();
        IdempotencyKey existing = new IdempotencyKey(correlationId, "AuthorizePayment", "PaymentFailed");
        when(idempotencyKeyRepository.findById(correlationId)).thenReturn(Optional.of(existing));

        service.authorizePayment(correlationId, UUID.randomUUID(), Map.of());

        verify(idempotencyKeyRepository, times(0)).save(any());
        verify(kafkaTemplate).send(eq("payments.events"), any(), eventCaptor.capture());
        assertThat(((EventEnvelope) eventCaptor.getValue()).eventType()).isEqualTo("PaymentFailed");
    }

    @Test
    void refundPayment_emitsRefundIssued() {
        PaymentService service = new PaymentService(idempotencyKeyRepository, kafkaTemplate, 0.0);
        UUID correlationId = UUID.randomUUID();
        when(idempotencyKeyRepository.findById(correlationId)).thenReturn(Optional.empty());

        service.refundPayment(correlationId, UUID.randomUUID(), Map.of());

        verify(kafkaTemplate).send(eq("payments.events"), any(), eventCaptor.capture());
        assertThat(((EventEnvelope) eventCaptor.getValue()).eventType()).isEqualTo("RefundIssued");
    }
}
