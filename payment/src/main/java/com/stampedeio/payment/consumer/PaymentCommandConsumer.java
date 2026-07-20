package com.stampedeio.payment.consumer;

import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.stampedeio.payment.service.PaymentService;

@Component
public class PaymentCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentCommandConsumer.class);

    private final PaymentService paymentService;

    public PaymentCommandConsumer(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @KafkaListener(topics = "payments.commands", groupId = "payment-service")
    public void consume(Map<String, Object> message) {
        String eventType = (String) message.get("eventType");
        UUID correlationId = UUID.fromString((String) message.get("correlationId"));
        UUID aggregateId = UUID.fromString((String) message.get("aggregateId"));
        Object payload = message.get("payload");

        switch (eventType) {
            case "AuthorizePayment" -> paymentService.authorizePayment(correlationId, aggregateId, payload);
            case "RefundPayment" -> paymentService.refundPayment(correlationId, aggregateId, payload);
            default -> log.warn("Unknown command type: {}", eventType);
        }
    }
}
